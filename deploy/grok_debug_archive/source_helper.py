#!/usr/bin/env python3
"""Confined source-side archive protocol for the isolated group 78 capture store."""
from __future__ import annotations

import argparse
import contextlib
import datetime as dt
import fcntl
import gzip
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import sys
import time
import uuid
import zlib

GROUP_ID = 78
SOURCE_DIR = Path('/opt/getoken-consolidated/builds/pool-kiro-pricing-20261020/sub2api-g4/data/request-debug/grok-test')
INSTALL_PATH = Path('/opt/getoken-consolidated/builds/grok-test-20261102/archive/source_helper.py')
ACK_STATE_DIR = Path('/opt/getoken-consolidated/builds/grok-test-20261102/archive/ack-state')
SOURCE_UID = 1000
MAX_RAW = 256 * 1024 ** 2
MAX_PAYLOAD = 512 * 1024 ** 2
MAX_METADATA = 65536
MAX_HEADERS_RAW = 8 * 1024 ** 2
MAX_HEADERS_COMPRESSED = 16 * 1024 ** 2
HEADERS_POLICY = 'credential_names_v1'
CREDENTIAL_COMPONENTS = {'authorization','cookie','key','apikey','token','secret','password','credential',
                         'signature','auth','authentication','jwt','session','csrf','xsrf','credentials'}
HEADER_NAME_RE = re.compile(r"[!#$%&'*+.^_`|~0-9A-Za-z-]+\Z")
MAX_RESPONSE = 2 * 1024 ** 2
MAX_SCAN = 100000
UUID_RE = re.compile(r'[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}\Z')
SHA_RE = re.compile(r'[0-9a-f]{64}\Z')
INPUT_FIELDS = {'schema_version','capture_id','captured_at','sealed_at','group_id','api_key_id','user_id',
                'request_id','method','route','headers','content_length','body_present','payload_file',
                'raw_bytes','raw_sha256','compressed_bytes','compressed_sha256','body_complete','read_error'}
HEADER_FIELDS = {'headers_file','headers_raw_bytes','headers_raw_sha256','headers_compressed_bytes',
                 'headers_compressed_sha256','headers_policy','headers_redacted_count'}
OUTCOME_FIELDS = {'schema_version','capture_id','finished_at','http_status','context_canceled','error_code','request_released'}
ACK_FIELDS = {'schema_version','group_id','capture_id','kind','object_sha256','receipt_sha256','accepted'}


class HelperError(Exception):
    pass


def require(value, code):
    if not value:
        raise HelperError(code)


def encode(value):
    return (json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False) + '\n').encode()


def decode(raw):
    def unique(pairs):
        result = {}
        for key, value in pairs:
            require(key not in result, 'duplicate_json_key')
            result[key] = value
        return result
    try:
        result = json.loads(raw, object_pairs_hook=unique, parse_constant=lambda _: (_ for _ in ()).throw(ValueError()))
    except (ValueError, UnicodeDecodeError, TypeError):
        raise HelperError('invalid_json') from None
    require(isinstance(result, dict), 'json_not_object')
    return result


def timestamp(value):
    require(isinstance(value, str) and re.fullmatch(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,9})?Z',value), 'timestamp_not_utc')
    try:
        return dt.datetime.fromisoformat(value[:-1] + '+00:00')
    except ValueError:
        raise HelperError('invalid_timestamp') from None


def capture_id(value):
    require(isinstance(value, str) and UUID_RE.fullmatch(value), 'invalid_capture_id')
    require(str(uuid.UUID(value)) == value, 'invalid_capture_id')
    return value


def sha(value):
    require(isinstance(value, str) and SHA_RE.fullmatch(value), 'invalid_sha256')
    return value


def bounds(objects, size):
    require(type(objects) is int and 1 <= objects <= 1000, 'invalid_object_limit')
    require(type(size) is int and 1 <= size <= 10 * 1024 ** 3, 'invalid_byte_limit')


def validate_headers(value, expected_count):
    require(set(value) == {'schema_version','policy','headers','redacted_count'}
            and type(value['schema_version']) is int and value['schema_version'] == 1
            and value['policy'] == HEADERS_POLICY, 'invalid_headers_document')
    headers = value['headers']
    require(isinstance(headers,dict) and len(headers) <= MAX_HEADERS_RAW // 5, 'invalid_headers_map')
    count = 0; values = 0
    for name, items in headers.items():
        require(isinstance(name,str) and len(name) <= MAX_HEADERS_RAW and HEADER_NAME_RE.fullmatch(name)
                and name == '-'.join(p[:1].upper()+p[1:].lower() for p in name.split('-')), 'invalid_header_name')
        require(isinstance(items,list) and all(isinstance(v,str) for v in items), 'invalid_header_values')
        values += len(items); require(values <= MAX_HEADERS_RAW // 3, 'header_value_count_limit')
        require(all(len(v) <= MAX_HEADERS_RAW and not any(ord(c) < 32 and c != '\t' or ord(c) == 127 for c in v)
                    for v in items), 'invalid_header_value')
        if CREDENTIAL_COMPONENTS.intersection(re.split('[^a-z0-9]+',name.lower())):
            require(items == ['[REDACTED]'], 'unredacted_credential_header'); count += 1
    require(type(value['redacted_count']) is int and value['redacted_count'] == count == expected_count,
            'headers_redacted_count_mismatch')


class SourceStore:
    """Tests inject a temporary root/UID; the public CLI exposes no such override."""
    def __init__(self, root=SOURCE_DIR, source_uid=SOURCE_UID, now=None, max_seconds=35, state_root=ACK_STATE_DIR):
        self.root = Path(root)
        self.state_root = Path(state_root)
        self.source_uid = source_uid
        self.now = now or dt.datetime.now(dt.timezone.utc)
        self.deadline = time.monotonic() + max_seconds
        self.fd = None
        self.state_parent_fd = None

    def __enter__(self):
        require(self.root.is_absolute() and '..' not in self.root.parts, 'unsafe_source_root')
        fd = os.open('/', os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            for part in self.root.parts[1:]:
                child = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
                os.close(fd); fd = child
            info = os.fstat(fd)
            require(info.st_uid == self.source_uid and stat.S_IMODE(info.st_mode) == 0o700, 'unsafe_source_directory')
            self.fd = fd
        except BaseException:
            os.close(fd)
            raise
        return self

    def __exit__(self, *args):
        if self.state_parent_fd is not None:
            os.close(self.state_parent_fd)
        os.close(self.fd)

    def budget(self):
        require(time.monotonic() < self.deadline, 'helper_time_limit')

    def regular(self, info, owner):
        require(stat.S_ISREG(info.st_mode), 'unsafe_source_file')
        require(info.st_uid == owner and stat.S_IMODE(info.st_mode) == 0o600, 'unsafe_source_owner_or_mode')
        require(info.st_nlink == 1, 'hardlinked_source_file')

    @staticmethod
    def identity(info):
        return (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns)

    def read(self, name, cap, directory=None, owner=None, allow_missing=False):
        directory = self.fd if directory is None else directory
        owner = self.source_uid if owner is None else owner
        require('/' not in name and name not in ['', '.', '..'], 'unsafe_object_name')
        self.budget()
        try:
            fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory)
        except FileNotFoundError:
            if allow_missing:
                return None
            raise HelperError('missing_source_file') from None
        except OSError:
            raise HelperError('unsafe_source_file') from None
        with os.fdopen(fd, 'rb') as source:
            info = os.fstat(source.fileno()); self.regular(info, owner)
            require(0 < info.st_size <= cap, 'object_size_limit')
            raw = source.read(cap + 1)
            require(len(raw) == info.st_size <= cap, 'object_size_changed')
            require(self.identity(info) == self.identity(os.fstat(source.fileno())), 'source_object_changed')
        return raw, info

    def compressed_descriptor(self, cid, manifest, headers=False, verify_raw=False):
        prefix = 'headers_' if headers else ''
        name = cid + ('.headers.gz' if headers else '.request.gz')
        try:
            fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=self.fd)
        except FileNotFoundError:
            raise HelperError('missing_source_file') from None
        except OSError:
            raise HelperError('unsafe_source_file') from None
        with os.fdopen(fd, 'rb') as source:
            info = os.fstat(source.fileno()); self.regular(info, self.source_uid)
            label = 'headers' if headers else 'payload'
            require(info.st_size == manifest[prefix+'compressed_bytes'], label+'_size_mismatch')
            digest = hashlib.sha256()
            for block in iter(lambda: source.read(1024 ** 2), b''):
                self.budget(); digest.update(block)
            require(self.identity(info) == self.identity(os.fstat(source.fileno())), 'source_object_changed')
            require(digest.hexdigest() == manifest[prefix+'compressed_sha256'], label+'_hash_mismatch')
            if verify_raw or headers:
                source.seek(0); size = 0; raw_hash = hashlib.sha256(); chunks = []
                try:
                    with gzip.GzipFile(fileobj=source,mode='rb') as raw:
                        while True:
                            self.budget(); block = raw.read(65536)
                            if not block: break
                            size += len(block)
                            require(size <= manifest[prefix+'raw_bytes'] and size <= (MAX_HEADERS_RAW if headers else MAX_RAW),
                                    label+'_raw_expansion_limit')
                            raw_hash.update(block)
                            if headers: chunks.append(block)
                except (OSError,EOFError,zlib.error):
                    raise HelperError(label+'_invalid_gzip') from None
                require(size == manifest[prefix+'raw_bytes'] and raw_hash.hexdigest() == manifest[prefix+'raw_sha256'],
                        label+'_raw_hash_or_size_mismatch')
                if headers: validate_headers(decode(b''.join(chunks)),manifest['headers_redacted_count'])
                require(self.identity(info) == self.identity(os.fstat(source.fileno())), 'source_object_changed')
        return {'path': name, 'bytes': info.st_size, 'sha256': digest.hexdigest()}, info

    def payload_descriptor(self, cid, manifest):
        return self.compressed_descriptor(cid,manifest)

    def headers_descriptor(self, cid, manifest):
        return self.compressed_descriptor(cid,manifest,headers=True) if manifest['schema_version'] == 2 else (None,None)

    def input(self, cid):
        raw, info = self.read(cid + '.input.json', MAX_METADATA)
        value = decode(raw)
        version = value.get('schema_version')
        require(type(version) is int and version in [1,2], 'invalid_input_identity')
        require(set(value) == INPUT_FIELDS | (HEADER_FIELDS if version == 2 else set()), 'invalid_input_fields')
        require(type(value['schema_version']) is int and value['schema_version'] in [1,2]
                and value['capture_id'] == cid, 'invalid_input_identity')
        require(type(value['group_id']) is int and value['group_id'] == GROUP_ID, 'wrong_input_group')
        captured, sealed = timestamp(value['captured_at']), timestamp(value['sealed_at'])
        require(captured <= sealed <= self.now + dt.timedelta(minutes=5), 'invalid_capture_time')
        require(value['payload_file'] == cid + '.request.gz', 'unsafe_payload_file')
        require(type(value['raw_bytes']) is int and 0 <= value['raw_bytes'] <= MAX_RAW
                and type(value['compressed_bytes']) is int and 0 < value['compressed_bytes'] <= MAX_PAYLOAD, 'invalid_body_size')
        sha(value['raw_sha256']); sha(value['compressed_sha256'])
        if version == 2:
            require(value['headers_file'] == cid + '.headers.gz', 'unsafe_headers_file')
            require(value['headers_policy'] == HEADERS_POLICY, 'invalid_headers_policy')
            require(type(value['headers_raw_bytes']) is int and 0 < value['headers_raw_bytes'] <= MAX_HEADERS_RAW
                    and type(value['headers_compressed_bytes']) is int and 0 < value['headers_compressed_bytes'] <= MAX_HEADERS_COMPRESSED,
                    'invalid_headers_size')
            sha(value['headers_raw_sha256']); sha(value['headers_compressed_sha256'])
            require(type(value['headers_redacted_count']) is int and 0 <= value['headers_redacted_count'] <= MAX_HEADERS_RAW // 5,
                    'invalid_headers_redacted_count')
        require(type(value['body_complete']) is bool and type(value['body_present']) is bool, 'invalid_body_flags')
        require(value['read_error'] in ['', 'read_error', 'context_canceled', 'body_too_large']
                and value['body_complete'] == (value['read_error'] == ''), 'invalid_body_state')
        require(type(value['content_length']) is int and value['content_length'] >= -1, 'invalid_content_length')
        require(all(type(value[k]) is int and value[k] > 0 for k in ['api_key_id','user_id']), 'invalid_input_ids')
        require(all(isinstance(value[k], str) and len(value[k]) <= 4096 and '\x00' not in value[k]
                    for k in ['request_id','method','route']), 'invalid_request_metadata')
        require(re.fullmatch(r'[A-Z]{3,12}', value['method']) and value['route'].startswith('/'), 'invalid_request_metadata')
        headers = value['headers']
        require(isinstance(headers, dict) and all(isinstance(k, str) and k.lower() in {'content-type','content-encoding','accept'}
                and isinstance(v, str) and len(v) <= 4096 and not any(c in v for c in '\r\n\x00')
                for k,v in headers.items()), 'unsafe_input_headers')
        return value, {'path': cid + '.input.json', 'bytes':len(raw),'sha256':hashlib.sha256(raw).hexdigest()}, info

    def outcome(self, cid, manifest):
        obj = self.read(cid + '.outcome.json', MAX_METADATA, allow_missing=True)
        if obj is None:
            return None, None
        raw, info = obj; value = decode(raw)
        require(set(value) == OUTCOME_FIELDS, 'invalid_outcome_fields')
        require(type(value['schema_version']) is int and value['schema_version'] == 1
                and value['capture_id'] == cid, 'invalid_outcome_identity')
        require(timestamp(manifest['sealed_at']) <= timestamp(value['finished_at']) <= self.now + dt.timedelta(minutes=5), 'invalid_outcome_time')
        require(type(value['http_status']) is int and 0 <= value['http_status'] <= 599
                and type(value['request_released']) is bool and type(value['context_canceled']) is bool, 'invalid_outcome_flags')
        require(isinstance(value['error_code'], str) and re.fullmatch(r'[a-z0-9_]{0,96}',value['error_code']), 'invalid_outcome_code')
        return {'path':cid + '.outcome.json','bytes':len(raw),'sha256':hashlib.sha256(raw).hexdigest()}, info

    def entries(self):
        # Four sealed files per schema-2 capture, plus bounded partial files.
        with os.scandir(self.fd) as entries:
            for count, entry in enumerate(entries, 1):
                self.budget()
                require(count <= 8 * MAX_SCAN, 'namespace_scan_limit')
                yield entry.name

    def names(self):
        count = 0
        for name in self.entries():
            if name.endswith('.input.json'):
                cid = name[:-len('.input.json')]
                if UUID_RE.fullmatch(cid):
                    count += 1
                    require(count <= MAX_SCAN, 'capture_scan_limit')
                    yield cid

    def state_parent(self):
        """Open the independent root control plane, never under producer data."""
        if self.state_parent_fd is not None:
            return self.state_parent_fd
        require(self.state_root.is_absolute() and '..' not in self.state_root.parts
                and self.state_root.name == 'ack-state', 'unsafe_ack_root')
        require(not self.state_root.is_relative_to(self.root), 'ack_root_inside_source')
        fd = os.open('/', os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            for part in self.state_root.parent.parts[1:]:
                child = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
                os.close(fd); fd = child
                info = os.fstat(fd); mode = stat.S_IMODE(info.st_mode)
                # Root-owned sticky temp ancestors are safe for private test
                # roots. Production /opt ancestors are root-owned/nonwritable.
                trusted_sticky = info.st_uid == 0 and bool(info.st_mode & stat.S_ISVTX)
                require(info.st_uid in {0,os.geteuid()} and (mode & 0o022 == 0 or trusted_sticky), 'unsafe_ack_ancestor')
            info = os.fstat(fd)
            require(info.st_uid == os.geteuid() and stat.S_IMODE(info.st_mode) == 0o700, 'unsafe_ack_parent')
            self.state_parent_fd = fd
            return fd
        except BaseException:
            os.close(fd); raise

    def state(self, create=False):
        parent = self.state_parent()
        if create:
            try:
                os.mkdir('ack-state', 0o700, dir_fd=parent)
            except FileExistsError:
                pass
            # Also settle a directory entry left visible by an earlier failed fsync.
            os.fsync(parent)
        try:
            fd = os.open('ack-state', os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=parent)
        except FileNotFoundError:
            return None
        except OSError:
            raise HelperError('unsafe_ack_directory') from None
        info = os.fstat(fd)
        try:
            require(info.st_uid == os.geteuid() and stat.S_IMODE(info.st_mode) == 0o700, 'unsafe_ack_directory')
        except BaseException:
            os.close(fd); raise
        return fd

    @contextlib.contextmanager
    def locked(self, wait=False):
        state = self.state(create=True)
        lock = None
        try:
            lock = os.open('helper.lock', os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | os.O_NONBLOCK, 0o600, dir_fd=state)
            self.regular(os.fstat(lock), os.geteuid())
            while True:
                try:
                    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB); break
                except BlockingIOError:
                    if not wait: raise HelperError('helper_locked') from None
                    self.budget(); time.sleep(0.01)
            yield state
        finally:
            if lock is not None:
                os.close(lock)
            os.close(state)

    def ack_record(self, cid, kind, state=None, durable=False):
        owned = state is None
        state = self.state() if owned else state
        if state is None:
            return None
        try:
            obj = self.read(cid + '.' + kind + '.ack.json', MAX_METADATA, state, os.geteuid(), allow_missing=True)
            if obj is None:
                return None
            value = decode(obj[0])
            require(set(value) == ACK_FIELDS and type(value['schema_version']) is int and value['schema_version'] == 1
                    and type(value['group_id']) is int and value['group_id'] == GROUP_ID
                    and value['capture_id'] == cid and value['kind'] == kind and value['accepted'] is True, 'invalid_ack_record')
            sha(value['object_sha256']); sha(value['receipt_sha256'])
            if durable:
                # An interrupted publication can leave the final name visible even
                # though directory fsync failed. Read-only reconciliation must
                # settle that existing entry before claiming it is accepted. No
                # filename/content is created or rewritten by these barriers.
                os.fsync(state); os.fsync(self.state_parent())
            return value
        finally:
            if owned:
                os.close(state)

    def ack_status(self, cid, kind):
        capture_id(cid); require(kind in ['input','outcome'], 'invalid_ack_kind')
        return self.ack_record(cid, kind, durable=True) or {'schema_version':1, 'accepted':False}

    def list(self, max_objects, max_bytes, after=''):
        bounds(max_objects, max_bytes)
        require(after == '' or UUID_RE.fullmatch(after), 'invalid_list_cursor')
        # Filenames are scheduling hints only; selected objects still undergo all
        # ownership, schema, hash and credential-redaction checks below.
        names = set(self.entries())
        ids = sorted(name[:-11] for name in names
                     if name.endswith('.input.json') and UUID_RE.fullmatch(name[:-11]))
        require(len(ids) <= MAX_SCAN, 'capture_scan_limit')
        state = self.state()
        ack_names = set()
        if state is not None:
            try:
                with os.scandir(state) as entries:
                    for n, entry in enumerate(entries, 1):
                        self.budget(); require(n <= 4 * MAX_SCAN, 'ack_scan_limit')
                        ack_names.add(entry.name)
            finally:
                os.close(state)
        # Revisit terminal captures until source GC completes, but do not keep
        # downloading already-ACKed inputs whose request is still in flight.
        pending = [cid for cid in ids if cid+'.input.ack.json' not in ack_names
                   or cid+'.outcome.json' in names]
        candidates = [cid for cid in pending if cid > after]
        if not candidates and after:
            candidates = pending
        candidates.sort(key=lambda cid:(0 if cid+'.input.ack.json' in ack_names and cid+'.outcome.ack.json' not in ack_names
                                        else 1 if cid+'.input.ack.json' not in ack_names else 2,cid))
        items = []; total = 0; rejected = {}; last = after; scanned = 0
        for cid in candidates:
            # Return durable progress before the helper's hard timeout instead
            # of discarding an entire batch after walking a growing namespace.
            if self.deadline-time.monotonic() < 5 or scanned >= max(16,max_objects*2):
                break
            last = cid; scanned += 1
            try:
                manifest, inp, _ = self.input(cid)
                outcome, _ = self.outcome(cid, manifest)
                ia, oa = self.ack_record(cid,'input'), self.ack_record(cid,'outcome')
                if ia: require(ia['object_sha256'] == inp['sha256'], 'input_ack_object_conflict')
                if oa: require(outcome is not None and oa['object_sha256'] == outcome['sha256'], 'outcome_ack_object_conflict')
                size = inp['bytes'] + manifest['compressed_bytes'] + manifest.get('headers_compressed_bytes',0) + (outcome['bytes'] if outcome else 0)
                if size > max_bytes-total:
                    continue
                payload, _ = self.payload_descriptor(cid, manifest)
                headers, _ = self.headers_descriptor(cid, manifest)
                item = {'capture_id':cid,'input':inp,'payload':payload,'outcome':outcome}
                if headers is not None: item['headers'] = headers
                items.append(item); total += size
                if len(items) >= max_objects: break
            except HelperError as exc:
                rejected[str(exc)] = rejected.get(str(exc),0)+1
                if str(exc) == 'helper_time_limit': break
        result = {'schema_version':1,'group_id':GROUP_ID,'items':items,'rejected':rejected,
                  'next_cursor':last,'scanned':scanned,'more':scanned < len(candidates)}
        require(len(encode(result)) <= MAX_RESPONSE, 'response_size_limit')
        return result

    def ack(self, cid, kind, object_sha, receipt_sha):
        capture_id(cid); require(kind in ['input','outcome'], 'invalid_ack_kind'); sha(object_sha); sha(receipt_sha)
        expected = {'schema_version':1,'group_id':GROUP_ID,'capture_id':cid,'kind':kind,
                    'object_sha256':object_sha,'receipt_sha256':receipt_sha,'accepted':True}
        with self.locked(wait=True) as state:
            old = self.ack_record(cid,kind,state,durable=True)
            if old is not None:
                require(old == expected, 'ack_conflict')
                return old
            manifest, inp, _ = self.input(cid)
            self.compressed_descriptor(cid,manifest,verify_raw=True)
            self.headers_descriptor(cid,manifest)
            outcome, _ = self.outcome(cid,manifest)
            obj = inp if kind == 'input' else outcome
            require(obj is not None and obj['sha256'] == object_sha, 'ack_object_mismatch')
            name = cid + '.' + kind + '.ack.json'; temp = '.' + name + '.pending-' + uuid.uuid4().hex
            fd = os.open(temp, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600, dir_fd=state)
            with os.fdopen(fd,'wb') as output:
                output.write(encode(expected)); output.flush(); os.fsync(output.fileno())
            os.link(temp, name, src_dir_fd=state, dst_dir_fd=state, follow_symlinks=False)
            os.unlink(temp,dir_fd=state); os.fsync(state); os.fsync(self.state_parent())
        return expected

    def ack_batch(self, claims):
        require(isinstance(claims,list) and 0<len(claims)<=64,'invalid_ack_batch')
        fields={'capture_id','kind','object_sha256','receipt_sha256'}
        seen=set()
        for claim in claims:
            require(isinstance(claim,dict) and set(claim)==fields,'invalid_ack_claim')
            capture_id(claim['capture_id']); sha(claim['object_sha256']); sha(claim['receipt_sha256'])
            key=(claim['capture_id'],claim['kind'])
            require(claim['kind'] in ['input','outcome'] and key not in seen,'duplicate_or_invalid_ack_claim')
            seen.add(key)
        results=[]
        for claim in claims:
            try:
                results.append(self.ack(claim['capture_id'],claim['kind'],claim['object_sha256'],claim['receipt_sha256']))
            except HelperError as exc:
                results.append({'capture_id':claim['capture_id'],'kind':claim['kind'],'accepted':False,'code':str(exc)})
        return {'schema_version':1,'group_id':GROUP_ID,'results':results}

    def gc(self, max_objects=100, max_bytes=1024 ** 3, capture_ids=None):
        bounds(max_objects,max_bytes)
        if capture_ids is not None:
            require(0 < len(capture_ids) <= max_objects and len(set(capture_ids)) == len(capture_ids), 'invalid_gc_capture_ids')
            for cid in capture_ids: capture_id(cid)
        result = {'schema_version':1,'group_id':GROUP_ID,'scanned':0,'deleted':0,'deleted_bytes':0,'preserved':0,'rejected':{}}
        with self.locked() as state:
            blocked = set()
            for name in self.entries():
                cid = name[:36]
                if UUID_RE.fullmatch(cid) and (name == cid or name.startswith(cid + '.')):
                    if name not in {cid + s for s in ['.input.json','.request.gz','.headers.gz','.outcome.json']}:
                        blocked.add(cid)
            for cid in (self.names() if capture_ids is None else capture_ids):
                if self.deadline-time.monotonic() < 2: break
                self.budget(); result['scanned'] += 1
                try:
                    manifest, inp, ii = self.input(cid)
                    if manifest['schema_version'] == 1:
                        try: os.stat(cid+'.headers.gz',dir_fd=self.fd,follow_symlinks=False)
                        except FileNotFoundError: pass
                        else: raise HelperError('gc_partial_or_unknown_file')
                    if capture_ids is None and timestamp(manifest['captured_at']) >= self.now - dt.timedelta(hours=24):
                        result['preserved'] += 1; continue
                    ia, oa = self.ack_record(cid,'input',state,durable=True), self.ack_record(cid,'outcome',state,durable=True)
                    if ia is None or oa is None:
                        result['preserved'] += 1; continue
                    outcome, oi = self.outcome(cid,manifest)
                    if outcome is None:
                        result['preserved'] += 1; continue
                    require(ia['object_sha256'] == inp['sha256'] and oa['object_sha256'] == outcome['sha256'], 'gc_ack_object_conflict')
                    require(cid not in blocked, 'gc_partial_or_unknown_file')
                    size = inp['bytes'] + manifest['compressed_bytes'] + manifest.get('headers_compressed_bytes',0) + outcome['bytes']
                    if size > max_bytes - result['deleted_bytes']:
                        result['preserved'] += 1; continue
                    payload, pi = self.payload_descriptor(cid,manifest)
                    snapshots = [(inp,ii),(payload,pi),(outcome,oi)]
                    headers, hi = self.headers_descriptor(cid,manifest)
                    if headers is not None: snapshots.append((headers,hi))
                    for desc, info in snapshots:
                        current = os.stat(desc['path'],dir_fd=self.fd,follow_symlinks=False)
                        self.regular(current,self.source_uid)
                        require(self.identity(info) == self.identity(current), 'gc_source_changed')
                    # Remove ready marker first; a crash can leave acknowledged orphan
                    # payload/outcome, never a ready input pointing at deleted payload.
                    os.unlink(inp['path'],dir_fd=self.fd); os.fsync(self.fd)
                    for desc,_ in snapshots[1:]: os.unlink(desc['path'],dir_fd=self.fd)
                    os.fsync(self.fd)
                    result['deleted'] += 1; result['deleted_bytes'] += size
                    if result['deleted'] >= max_objects:
                        break
                except HelperError as exc:
                    result['preserved'] += 1
                    result['rejected'][str(exc)] = result['rejected'].get(str(exc),0) + 1
        return result


def main(argv=None):
    os.umask(0o077)
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='action',required=True)
    for action in ['manifest-list','ack','ack-status','ack-batch','gc']:
        p = sub.add_parser(action); p.add_argument('--group-id',type=int,required=True)
        if action in ['manifest-list','gc']:
            p.add_argument('--max-objects',type=int,default=100,required=action=='manifest-list')
            p.add_argument('--max-bytes',type=int,default=1024**3,required=action=='manifest-list')
            if action == 'manifest-list': p.add_argument('--after',default='')
            if action == 'gc': p.add_argument('--capture-ids')
        elif action == 'ack-batch':
            p.add_argument('--claims',required=True)
        else:
            p.add_argument('--capture-id',required=True); p.add_argument('--kind',choices=['input','outcome'],required=True)
            if action == 'ack':
                p.add_argument('--object-sha256',required=True); p.add_argument('--receipt-sha256',required=True)
    args = parser.parse_args(argv)
    try:
        require(args.group_id == GROUP_ID, 'wrong_group')
        require(os.geteuid() == 0, 'helper_requires_root')
        with SourceStore() as store:
            if args.action == 'manifest-list': result = store.list(args.max_objects,args.max_bytes,args.after)
            elif args.action == 'ack': result = store.ack(args.capture_id,args.kind,args.object_sha256,args.receipt_sha256)
            elif args.action == 'ack-batch':
                require(len(args.claims)<=32768,'ack_batch_size_limit')
                envelope=decode(args.claims.encode())
                require(set(envelope)=={'claims'},'invalid_ack_envelope')
                result=store.ack_batch(envelope['claims'])
            elif args.action == 'ack-status': result = store.ack_status(args.capture_id,args.kind)
            else: result = store.gc(args.max_objects,args.max_bytes,None if args.capture_ids is None else args.capture_ids.split(','))
        sys.stdout.buffer.write(encode(result)); return 0
    except Exception as exc:
        code = str(exc) if isinstance(exc,HelperError) else 'source_helper_failed'
        sys.stdout.buffer.write(encode({'schema_version':1,'status':'failed','code':code})); return 1


if __name__ == '__main__':
    raise SystemExit(main())
