#!/usr/bin/env python3
"""Confined source-side archive protocol for the isolated group 78 capture store."""
from __future__ import annotations

import argparse
import contextlib
import datetime as dt
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import sys
import time
import uuid

GROUP_ID = 78
SOURCE_DIR = Path('/opt/getoken-consolidated/builds/pool-kiro-pricing-20261020/sub2api-g4/data/request-debug/grok-test')
INSTALL_PATH = Path('/opt/getoken-consolidated/builds/grok-test-20261102/archive/source_helper.py')
ACK_STATE_DIR = Path('/opt/getoken-consolidated/builds/grok-test-20261102/archive/ack-state')
SOURCE_UID = 1000
MAX_RAW = 256 * 1024 ** 2
MAX_PAYLOAD = 512 * 1024 ** 2
MAX_METADATA = 65536
MAX_RESPONSE = 2 * 1024 ** 2
MAX_SCAN = 100000
UUID_RE = re.compile(r'[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}\Z')
SHA_RE = re.compile(r'[0-9a-f]{64}\Z')
INPUT_FIELDS = {'schema_version','capture_id','captured_at','sealed_at','group_id','api_key_id','user_id',
                'request_id','method','route','headers','content_length','body_present','payload_file',
                'raw_bytes','raw_sha256','compressed_bytes','compressed_sha256','body_complete','read_error'}
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

    def payload_descriptor(self, cid, manifest):
        name = cid + '.request.gz'
        try:
            fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=self.fd)
        except FileNotFoundError:
            raise HelperError('missing_source_file') from None
        except OSError:
            raise HelperError('unsafe_source_file') from None
        with os.fdopen(fd, 'rb') as source:
            info = os.fstat(source.fileno()); self.regular(info, self.source_uid)
            require(info.st_size == manifest['compressed_bytes'], 'payload_size_mismatch')
            digest = hashlib.sha256()
            for block in iter(lambda: source.read(1024 ** 2), b''):
                self.budget(); digest.update(block)
            require(self.identity(info) == self.identity(os.fstat(source.fileno())), 'source_object_changed')
            require(digest.hexdigest() == manifest['compressed_sha256'], 'payload_hash_mismatch')
        return {'path': name, 'bytes': info.st_size, 'sha256': digest.hexdigest()}, info

    def input(self, cid):
        raw, info = self.read(cid + '.input.json', MAX_METADATA)
        value = decode(raw)
        require(set(value) == INPUT_FIELDS, 'invalid_input_fields')
        require(type(value['schema_version']) is int and value['schema_version'] == 1
                and value['capture_id'] == cid, 'invalid_input_identity')
        require(type(value['group_id']) is int and value['group_id'] == GROUP_ID, 'wrong_input_group')
        captured, sealed = timestamp(value['captured_at']), timestamp(value['sealed_at'])
        require(captured <= sealed <= self.now + dt.timedelta(minutes=5), 'invalid_capture_time')
        require(value['payload_file'] == cid + '.request.gz', 'unsafe_payload_file')
        require(type(value['raw_bytes']) is int and 0 <= value['raw_bytes'] <= MAX_RAW
                and type(value['compressed_bytes']) is int and 0 < value['compressed_bytes'] <= MAX_PAYLOAD, 'invalid_body_size')
        sha(value['raw_sha256']); sha(value['compressed_sha256'])
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

    def names(self):
        count = 0
        with os.scandir(self.fd) as entries:
            for entry in entries:
                self.budget(); count += 1
                require(count <= MAX_SCAN, 'namespace_scan_limit')
                if entry.name.endswith('.input.json'):
                    cid = entry.name[:-len('.input.json')]
                    if UUID_RE.fullmatch(cid):
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
    def locked(self):
        state = self.state(create=True)
        lock = None
        try:
            lock = os.open('helper.lock', os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | os.O_NONBLOCK, 0o600, dir_fd=state)
            self.regular(os.fstat(lock), os.geteuid())
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise HelperError('helper_locked') from None
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

    def list(self, max_objects, max_bytes):
        bounds(max_objects, max_bytes)
        candidates = []; rejected = {}
        def reject(exc):
            rejected[str(exc)] = rejected.get(str(exc), 0) + 1
        for cid in self.names():
            try:
                manifest, inp, _ = self.input(cid)
                outcome, _ = self.outcome(cid, manifest)
                ia, oa = self.ack_record(cid,'input'), self.ack_record(cid,'outcome')
                if ia: require(ia['object_sha256'] == inp['sha256'], 'input_ack_object_conflict')
                if oa: require(outcome is not None and oa['object_sha256'] == outcome['sha256'], 'outcome_ack_object_conflict')
                pending = ia is None or (outcome is not None and oa is None)
                # Retain only small scheduling keys, never all request metadata.
                candidates.append((0 if pending else 1, manifest['captured_at'], cid))
            except HelperError as exc:
                reject(exc)
        candidates.sort(key=lambda x:x[:3])
        items = []; total = 0
        for _, _, cid in candidates:
            self.budget()
            try:
                manifest, inp, _ = self.input(cid)
                outcome, _ = self.outcome(cid, manifest)
                size = inp['bytes'] + manifest['compressed_bytes'] + (outcome['bytes'] if outcome else 0)
                if size > max_bytes - total:
                    continue
                payload, _ = self.payload_descriptor(cid, manifest)
                items.append({'capture_id':cid,'input':inp,'payload':payload,'outcome':outcome}); total += size
            except HelperError as exc:
                reject(exc); continue
            if len(items) >= max_objects:
                break
        result = {'schema_version':1,'group_id':GROUP_ID,'items':items,'rejected':rejected}
        require(len(encode(result)) <= MAX_RESPONSE, 'response_size_limit')
        return result

    def ack(self, cid, kind, object_sha, receipt_sha):
        capture_id(cid); require(kind in ['input','outcome'], 'invalid_ack_kind'); sha(object_sha); sha(receipt_sha)
        expected = {'schema_version':1,'group_id':GROUP_ID,'capture_id':cid,'kind':kind,
                    'object_sha256':object_sha,'receipt_sha256':receipt_sha,'accepted':True}
        with self.locked() as state:
            old = self.ack_record(cid,kind,state,durable=True)
            if old is not None:
                require(old == expected, 'ack_conflict')
                return old
            manifest, inp, _ = self.input(cid)
            self.payload_descriptor(cid,manifest)
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

    def gc(self, max_objects=100, max_bytes=1024 ** 3):
        bounds(max_objects,max_bytes)
        result = {'schema_version':1,'group_id':GROUP_ID,'scanned':0,'deleted':0,'deleted_bytes':0,'preserved':0,'rejected':{}}
        with self.locked() as state:
            blocked = set()
            with os.scandir(self.fd) as entries:
                for count, entry in enumerate(entries, 1):
                    self.budget(); require(count <= MAX_SCAN, 'namespace_scan_limit')
                    cid = entry.name[:36]
                    if UUID_RE.fullmatch(cid) and (entry.name == cid or entry.name.startswith(cid + '.')):
                        if entry.name not in {cid + s for s in ['.input.json','.request.gz','.outcome.json']}:
                            blocked.add(cid)
            for cid in self.names():
                self.budget(); result['scanned'] += 1
                try:
                    manifest, inp, ii = self.input(cid)
                    if timestamp(manifest['captured_at']) >= self.now - dt.timedelta(hours=24):
                        result['preserved'] += 1; continue
                    ia, oa = self.ack_record(cid,'input',state,durable=True), self.ack_record(cid,'outcome',state,durable=True)
                    if ia is None or oa is None:
                        result['preserved'] += 1; continue
                    outcome, oi = self.outcome(cid,manifest)
                    if outcome is None:
                        result['preserved'] += 1; continue
                    require(ia['object_sha256'] == inp['sha256'] and oa['object_sha256'] == outcome['sha256'], 'gc_ack_object_conflict')
                    require(cid not in blocked, 'gc_partial_or_unknown_file')
                    size = inp['bytes'] + manifest['compressed_bytes'] + outcome['bytes']
                    if size > max_bytes - result['deleted_bytes']:
                        result['preserved'] += 1; continue
                    payload, pi = self.payload_descriptor(cid,manifest)
                    snapshots = [(inp,ii),(payload,pi),(outcome,oi)]
                    for desc, info in snapshots:
                        current = os.stat(desc['path'],dir_fd=self.fd,follow_symlinks=False)
                        self.regular(current,self.source_uid)
                        require(self.identity(info) == self.identity(current), 'gc_source_changed')
                    # Remove ready marker first; a crash can leave acknowledged orphan
                    # payload/outcome, never a ready input pointing at deleted payload.
                    os.unlink(inp['path'],dir_fd=self.fd); os.fsync(self.fd)
                    os.unlink(payload['path'],dir_fd=self.fd); os.unlink(outcome['path'],dir_fd=self.fd); os.fsync(self.fd)
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
    for action in ['manifest-list','ack','ack-status','gc']:
        p = sub.add_parser(action); p.add_argument('--group-id',type=int,required=True)
        if action in ['manifest-list','gc']:
            p.add_argument('--max-objects',type=int,default=100,required=action=='manifest-list')
            p.add_argument('--max-bytes',type=int,default=1024**3,required=action=='manifest-list')
        else:
            p.add_argument('--capture-id',required=True); p.add_argument('--kind',choices=['input','outcome'],required=True)
            if action == 'ack':
                p.add_argument('--object-sha256',required=True); p.add_argument('--receipt-sha256',required=True)
    args = parser.parse_args(argv)
    try:
        require(args.group_id == GROUP_ID, 'wrong_group')
        require(os.geteuid() == 0, 'helper_requires_root')
        with SourceStore() as store:
            if args.action == 'manifest-list': result = store.list(args.max_objects,args.max_bytes)
            elif args.action == 'ack': result = store.ack(args.capture_id,args.kind,args.object_sha256,args.receipt_sha256)
            elif args.action == 'ack-status': result = store.ack_status(args.capture_id,args.kind)
            else: result = store.gc(args.max_objects,args.max_bytes)
        sys.stdout.buffer.write(encode(result)); return 0
    except Exception as exc:
        code = str(exc) if isinstance(exc,HelperError) else 'source_helper_failed'
        sys.stdout.buffer.write(encode({'schema_version':1,'status':'failed','code':code})); return 1


if __name__ == '__main__':
    raise SystemExit(main())
