#!/usr/bin/env python3
"""Bounded, durable pull of the isolated Grok test capture namespace."""
from __future__ import annotations

import argparse
import datetime as dt
import fcntl
import gzip
import hashlib
import heapq
import json
import os
from pathlib import Path
import re
import resource
import selectors
import shlex
import shutil
import stat
import subprocess
import sys
import time
import threading
import tempfile
from concurrent.futures import ThreadPoolExecutor
import uuid
import zlib
from dataclasses import dataclass

UTC = dt.timezone.utc
UUID_RE = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}\Z")
SHA_RE = re.compile(r"[0-9a-f]{64}\Z")
REMOTE_PATH_RE = re.compile(r"/[A-Za-z0-9_./-]+\Z")
HEADER_NAMES = {"content-type", "content-encoding", "accept"}
MAX_HEADERS_RAW = 8 * 1024 ** 2
MAX_HEADERS_COMPRESSED = 16 * 1024 ** 2
MAX_ACK_CLAIMS = 256
MAX_ACK_BATCH_BYTES = 98304
HEADERS_POLICY = "credential_names_v1"
CREDENTIAL_COMPONENTS = {"authorization","cookie","key","apikey","token","secret","password","credential",
                         "signature","auth","authentication","jwt","session","csrf","xsrf","credentials"}
HEADER_NAME_RE = re.compile(r"[!#$%&'*+.^_`|~0-9A-Za-z-]+\Z")
HEADER_FIELDS = {"headers_file","headers_raw_bytes","headers_raw_sha256","headers_compressed_bytes",
                 "headers_compressed_sha256","headers_policy","headers_redacted_count"}
INPUT_FIELDS = {"schema_version","capture_id","captured_at","sealed_at","group_id","api_key_id","user_id",
                "request_id","method","route","headers","content_length","body_present","payload_file",
                "raw_bytes","raw_sha256","compressed_bytes","compressed_sha256","body_complete","read_error"}
HEADER_REJECTION_CODES = {"invalid_headers_document","invalid_headers_map","invalid_header_name","invalid_header_values",
    "header_value_count_limit","invalid_header_value","unredacted_credential_header","headers_redacted_count_mismatch",
    "unsafe_headers_file","invalid_headers_policy","invalid_headers_size","invalid_headers_redacted_count",
    "headers_size_mismatch","headers_hash_mismatch","headers_invalid_gzip","headers_raw_expansion_limit",
    "headers_raw_hash_or_size_mismatch"}


class ArchiveError(Exception):
    pass


class LimitReached(ArchiveError):
    pass


def require(value, code):
    if not value:
        raise ArchiveError(code)


def encode(value):
    return (json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False) + "\n").encode()


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def utc_now():
    return dt.datetime.now(UTC)


def parse_time(value):
    require(isinstance(value, str) and value.endswith("Z"), "timestamp_not_utc")
    try:
        result = dt.datetime.fromisoformat(value[:-1] + "+00:00")
    except (ValueError, TypeError):
        raise ArchiveError("invalid_timestamp") from None
    require(result.tzinfo is not None, "invalid_timestamp")
    return result


def canonical_id(value):
    require(isinstance(value, str) and UUID_RE.fullmatch(value), "invalid_capture_id")
    require(str(uuid.UUID(value)) == value, "noncanonical_capture_id")
    return value


def regular(path):
    try:
        info = path.lstat()
    except FileNotFoundError:
        raise ArchiveError("missing_local_file") from None
    require(stat.S_ISREG(info.st_mode) and not path.is_symlink(), "unsafe_local_file")
    require(info.st_uid == os.geteuid(), "foreign_local_owner")
    require(info.st_mode & 0o077 == 0, "unsafe_local_permissions")
    require(info.st_nlink == 1, "hardlinked_local_file")
    return info


def namespace_size(root, time_budget):
    total = 0
    # Keep only the current directory depth; fd-relative opens cannot follow a swapped link.
    stack = []

    def descend(name, parent_fd=None):
        fd = os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=parent_fd)
        try:
            entries = os.scandir(fd)
        except BaseException:
            os.close(fd)
            raise
        stack.append((fd, entries))

    try:
        time_budget()
        descend(root)
        while stack:
            time_budget()
            fd, entries = stack[-1]
            entry = next(entries, None)
            if entry is None:
                entries.close()
                os.close(fd)
                stack.pop()
                continue
            if entry.is_symlink():
                code = "symlink_in_namespace" if entry.is_dir() else "unsafe_namespace_object"
                raise ArchiveError(code)
            if entry.is_dir(follow_symlinks=False):
                descend(entry.name, fd)
                continue
            info = entry.stat(follow_symlinks=False)
            if stat.S_ISREG(info.st_mode):
                total += info.st_size
            elif not stat.S_ISSOCK(info.st_mode):
                raise ArchiveError("unsafe_namespace_object")
    finally:
        for fd, entries in reversed(stack):
            entries.close()
            os.close(fd)
    return total


def private_dir(path, create=False):
    require(path.is_absolute(), "relative_local_root")
    # Check the configured path, including its parents, before following anything.
    for part in reversed([path, *path.parents]):
        if part.exists() or part.is_symlink():
            require(not part.is_symlink(), "symlink_directory")
    if create:
        path.mkdir(mode=0o700, parents=False, exist_ok=True)
    info = path.lstat()
    require(stat.S_ISDIR(info.st_mode) and not path.is_symlink(), "unsafe_directory")
    require(info.st_uid == os.geteuid() and info.st_mode & 0o077 == 0, "unsafe_directory_owner_or_mode")


def fsync_dir(path):
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def read_small(path, cap):
    info = regular(path)
    require(info.st_size <= cap, "metadata_too_large")
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(fd, "rb") as source:
        raw = source.read(cap + 1)
    require(len(raw) <= cap, "metadata_too_large")
    return raw


def decode(raw):
    def unique(pairs):
        out = {}
        for key, value in pairs:
            require(key not in out, "duplicate_json_key")
            out[key] = value
        return out
    try:
        result = json.loads(raw, object_pairs_hook=unique)
    except (ValueError, UnicodeDecodeError, TypeError):
        raise ArchiveError("invalid_json") from None
    require(isinstance(result, dict), "json_not_object")
    return result


def validate_headers(value, expected_count):
    require(set(value) == {"schema_version","policy","headers","redacted_count"}
            and type(value["schema_version"]) is int and value["schema_version"] == 1
            and value["policy"] == HEADERS_POLICY, "invalid_headers_document")
    headers = value["headers"]
    # These bounds follow from the encoded 8 MiB cap, rather than imposing
    # smaller arbitrary limits on otherwise valid parsed HTTP headers.
    require(isinstance(headers,dict) and len(headers) <= MAX_HEADERS_RAW // 5, "invalid_headers_map")
    count = 0; values = 0
    for name, items in headers.items():
        require(isinstance(name,str) and len(name) <= MAX_HEADERS_RAW and HEADER_NAME_RE.fullmatch(name)
                and name == "-".join(p[:1].upper()+p[1:].lower() for p in name.split("-")), "invalid_header_name")
        require(isinstance(items,list) and all(isinstance(v,str) for v in items), "invalid_header_values")
        values += len(items); require(values <= MAX_HEADERS_RAW // 3, "header_value_count_limit")
        require(all(len(v) <= MAX_HEADERS_RAW and not any(ord(c) < 32 and c != "\t" or ord(c) == 127 for c in v)
                    for v in items), "invalid_header_value")
        if CREDENTIAL_COMPONENTS.intersection(re.split("[^a-z0-9]+",name.lower())):
            require(items == ["[REDACTED]"], "unredacted_credential_header"); count += 1
    require(type(value["redacted_count"]) is int and value["redacted_count"] == count == expected_count,
            "headers_redacted_count_mismatch")


def atomic_json(path, value):
    """Immutable durable publication. Existing content must be exactly identical."""
    raw = encode(value)
    if path.exists() or path.is_symlink():
        require(read_small(path, max(len(raw), 65536)) == raw, "immutable_local_conflict")
        return
    temp = path.with_name("." + path.name + ".pending-" + uuid.uuid4().hex)
    fd = os.open(temp, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    try:
        with os.fdopen(fd, "wb") as output:
            output.write(raw); output.flush(); os.fsync(output.fileno())
        # Link publishes without overwriting a concurrent or malicious existing path.
        os.link(temp, path, follow_symlinks=False)
        temp.unlink(); fsync_dir(path.parent)
    except BaseException:
        # A failed pending file is evidence and is deliberately left for review.
        raise


def replace_state(path,value):
    """Replace a known mutable scheduler state file under the held local lock."""
    if path.exists() or path.is_symlink(): regular(path)
    pending=path.with_name("."+path.name+".pending-"+uuid.uuid4().hex)
    fd=os.open(pending,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600)
    with os.fdopen(fd,"wb") as output:
        output.write(encode(value)); output.flush(); os.fsync(output.fileno())
    os.replace(pending,path); fsync_dir(path.parent)


@dataclass(frozen=True)
class Config:
    root: Path
    group_id: int
    source_dir: str
    helper_path: str
    ssh_host: str = "netcup"
    max_objects: int = 100
    workers: int = 1
    max_bytes: int = 1024 ** 3
    max_payload_bytes: int = 256 * 1024 ** 2
    max_raw_bytes: int = 128 * 1024 ** 2
    min_free_bytes: int = 5 * 1024 ** 3
    max_seconds: int = 240
    max_retention_deletes: int = 100
    max_namespace_bytes: int = 32 * 1024 ** 3
    max_retention_scan: int = 1000

    def validate(self):
        require(self.root.is_absolute() and self.root.name == "netcup-grok-test", "wrong_namespace")
        require(type(self.group_id) is int and self.group_id > 0, "invalid_group")
        require(self.ssh_host == "netcup", "unexpected_ssh_alias")
        for remote in [self.source_dir, self.helper_path]:
            require(isinstance(remote, str) and REMOTE_PATH_RE.fullmatch(remote), "unsafe_remote_path")
            require(".." not in remote.split("/") and remote != "/", "unsafe_remote_path")
        require(type(self.workers) is int and 1 <= self.workers <= 8, "invalid_worker_limit")
        require(1 <= self.max_objects <= 1000 and 1 <= self.max_bytes <= 10 * 1024 ** 3, "invalid_run_limit")
        require(1 <= self.max_payload_bytes <= 512 * 1024 ** 2, "invalid_payload_limit")
        require(1 <= self.max_raw_bytes <= 512 * 1024 ** 2, "invalid_raw_limit")
        require(30 <= self.max_seconds <= 290 and self.min_free_bytes >= 0, "invalid_resource_limit")
        require(0 <= self.max_retention_deletes <= 1000, "invalid_retention_limit")
        require(self.max_payload_bytes <= self.max_namespace_bytes <= 1024 ** 4, "invalid_namespace_limit")
        require(1 <= self.max_retention_scan <= 10000,"invalid_retention_scan_limit")


class SSHTransport:
    def __init__(self, config, deadline=None):
        self.config = config
        self.deadline = deadline
        self.control = str(config.root / ".run" / "ssh-control")
        require(len(os.fsencode(self.control)) < 100, "ssh_socket_path_too_long")

    def options(self):
        return ["-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=yes",
                "-o", "ForwardAgent=no", "-o", "IdentitiesOnly=yes",
                "-o", "ConnectTimeout=12", "-o", "ServerAliveInterval=15",
                "-o", "ServerAliveCountMax=3", "-o", "ControlMaster=auto",
                "-o", "ControlPersist=60", "-o", "ControlPath=" + self.control]

    @staticmethod
    def run(args, timeout=45, output_limit=2 * 1024 ** 2, file_size_limit=None, guard=None):
        """Bound stdout/stderr while the child runs, including failed SSH responses."""
        if file_size_limit is not None:
            # preexec_fn can deadlock after fork in a threaded archiver.
            wrapper = ("import os,resource,sys;n=int(sys.argv[1]);"
                       "resource.setrlimit(resource.RLIMIT_FSIZE,(n,n));"
                       "os.execvp(sys.argv[2],sys.argv[2:])")
            args = [sys.executable, "-c", wrapper, str(file_size_limit), *args]
        child = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, stdin=subprocess.DEVNULL)
        selector = selectors.DefaultSelector()
        selector.register(child.stdout, selectors.EVENT_READ, "out")
        selector.register(child.stderr, selectors.EVENT_READ, "err")
        data = {"out": bytearray(), "err": bytearray()}
        deadline = time.monotonic() + timeout
        try:
            while selector.get_map():
                require(time.monotonic() < deadline, "transport_timeout")
                if guard is not None: guard()
                for key, _ in selector.select(min(0.2, max(0, deadline - time.monotonic()))):
                    chunk = os.read(key.fileobj.fileno(), 65536)
                    if not chunk:
                        selector.unregister(key.fileobj); continue
                    data[key.data].extend(chunk)
                    require(sum(map(len, data.values())) <= output_limit, "transport_output_limit")
            code = child.wait(timeout=max(0.1, deadline - time.monotonic()))
            require(code == 0, "transport_failed")
            return bytes(data["out"])
        except BaseException:
            child.kill(); child.wait()
            raise
        finally:
            selector.close(); child.stdout.close(); child.stderr.close()

    def helper(self, action, *args):
        command = shlex.join([self.config.helper_path, action, "--group-id", str(self.config.group_id), *args])
        timeout=45 if self.deadline is None else max(0.01,min(45,self.deadline-time.monotonic()))
        return decode(self.run(["ssh", *self.options(), self.config.ssh_host, command],timeout=timeout))

    def manifest_list(self):
        path=self.config.root/".run"/"source-cursor.json"
        cursor=decode(read_small(path,65536)).get("last_uuid","") if path.exists() else ""
        require(cursor=="" or UUID_RE.fullmatch(cursor),"invalid_source_cursor")
        return self.helper("manifest-list", "--max-objects", str(self.config.max_objects), "--max-bytes", str(self.config.max_bytes),"--after",cursor)

    def fetch(self, relative_path, destination, timeout, max_bytes):
        # These names were schema checked before this method; check again at the boundary.
        require(isinstance(relative_path,str) and UUID_RE.fullmatch(relative_path[:36])
                and relative_path[36:] in {".input.json",".request.gz",".headers.gz",".outcome.json"}, "unsafe_fetch_path")
        source = self.config.ssh_host + ":" + self.config.source_dir.rstrip("/") + "/" + relative_path
        self.run(["scp", "-q", *self.options(), "--", source, str(destination)], timeout=timeout,
                 output_limit=65536,file_size_limit=max_bytes)

    def ack(self, capture_id, kind, object_sha, receipt_sha):
        return self.helper("ack", "--capture-id", capture_id, "--kind", kind,
                           "--object-sha256", object_sha, "--receipt-sha256", receipt_sha)

    def ack_status(self, capture_id, kind):
        return self.helper("ack-status", "--capture-id", capture_id, "--kind", kind)

    def ack_batch(self, claims):
        require(isinstance(claims,list) and 0<len(claims)<=MAX_ACK_CLAIMS,'invalid_ack_batch')
        raw=encode({'claims':claims})
        require(len(raw)<=MAX_ACK_BATCH_BYTES,'ack_batch_size_limit')
        return self.helper("ack-batch","--claims",raw.decode())

    def fetch_batch(self, descriptors, directory, timeout):
        # One SSH/SFTP round trip per file dominates on intercontinental links.
        # rsync transfers only this bounded, schema-checked list into a new private
        # staging directory. Nothing is trusted until Puller verifies every hash.
        require(0<len(descriptors)<=512,"invalid_transfer_batch")
        expected={d['path']:d['bytes'] for d in descriptors}
        require(len(expected)==len(descriptors),"duplicate_batch_path")
        for name in expected:
            require(UUID_RE.fullmatch(name[:36]) and name[36:] in
                    {'.input.json','.request.gz','.headers.gz','.outcome.json'},'unsafe_fetch_path')
        names=directory/'files.txt'
        fd=os.open(names,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600)
        with os.fdopen(fd,'w')as f:f.write('\n'.join(expected)+'\n')
        target=directory/'files';private_dir(target,create=True)
        def guard():
            for path in target.iterdir():
                require(path.name in expected,'unexpected_batch_file')
                info=path.lstat()
                require(stat.S_ISREG(info.st_mode) and info.st_nlink==1 and info.st_uid==os.geteuid()
                        and info.st_size<=expected[path.name],'unsafe_or_oversized_batch_file')
        source=self.config.ssh_host+':'+self.config.source_dir.rstrip('/')+'/'
        self.run(['rsync','--inplace','--no-implied-dirs','--files-from='+str(names),
                  '--max-size='+str(max(expected.values())),'-e',shlex.join(['ssh',*self.options()]),
                  '--',source,str(target)+'/'],timeout=timeout,output_limit=65536,
                  file_size_limit=max(expected.values()),guard=guard)
        guard()
        return target

    def gc(self):
        return self.helper("gc", "--max-objects", str(self.config.max_objects), "--max-bytes", str(self.config.max_bytes))

    def gc_confirmed(self, ids):
        for cid in ids: canonical_id(cid)
        require(0<len(ids)<=self.config.max_objects,"invalid_gc_capture_ids")
        return self.helper("gc","--max-objects",str(self.config.max_objects),"--max-bytes",str(self.config.max_bytes),"--capture-ids",",".join(ids))


class Puller:
    def __init__(self, config, transport=None, now=None):
        config.validate()
        self.config = config
        self.now = now or utc_now()
        self.deadline = time.monotonic() + config.max_seconds
        self.cleanup_reserve = min(45, config.max_seconds // 3)
        self.fetched_bytes = 0
        self.inflight_bytes = 0
        self.state_lock = threading.RLock()
        self.namespace_bytes = 0
        self.reconciled = set()
        self.gc_ready = set()
        self.pending_acks = {}
        self.prefetched = {}
        self.transport = transport
        self.result = {"status":"ok", "listed":0, "input_archived":0, "outcome_archived":0,
                       "acks_confirmed":0, "acks_pending":0, "expired":0, "errors":[], "bytes_fetched":0,
                       "retention_pending_inputs":0}

    def increment(self, key):
        with self.state_lock:
            self.result[key] += 1

    def record_error(self, value):
        with self.state_lock:
            self.result["errors"].append(value)

    def process_bounded(self, item, captures):
        try:
            self.time_budget()
            self.process(item, captures)
        except ArchiveError as exc:
            self.record_error({"capture_id":item["capture_id"],"code":str(exc)})
        except Exception:
            self.record_error({"capture_id":item["capture_id"],"code":"capture_transfer_failed"})

    def time_budget(self):
        left = self.deadline - self.cleanup_reserve - time.monotonic()
        if left <= 0:
            raise LimitReached("run_time_limit")
        return min(45, left)

    def descriptor(self, value, cid, suffix, cap):
        require(isinstance(value, dict) and set(value) == {"path","bytes","sha256"}, "invalid_descriptor")
        require(value["path"] == cid + suffix, "unsafe_descriptor_path")
        require(type(value["bytes"]) is int and 0 < value["bytes"] <= cap, "descriptor_size_limit")
        require(isinstance(value["sha256"],str) and SHA_RE.fullmatch(value["sha256"]), "invalid_descriptor_hash")
        return value

    def source_rejections(self,response,stage):
        counts=response.get("rejected",{})
        require(isinstance(counts,dict)and len(counts)<=1000,"invalid_source_rejections")
        require(all(type(n)is int and 0<=n<=1000000 for n in counts.values()),"invalid_source_rejection_count")
        count=sum(counts.values())
        if count:
            error={"code":"source_"+stage+"_rejected","count":count}
            reasons={code:n for code,n in counts.items() if code in HEADER_REJECTION_CODES and n}
            if reasons: error["reasons"]=reasons
            self.result["errors"].append(error)

    def validate_listing(self, listing):
        require(listing.get("schema_version") == 1 and type(listing.get("group_id")) is int
                and listing["group_id"] == self.config.group_id, "wrong_listing_group_or_version")
        items = listing.get("items")
        require(isinstance(items,list) and len(items) <= self.config.max_objects, "listing_object_limit")
        seen = set(); total = 0
        for item in items:
            require(isinstance(item,dict), "invalid_listing_item")
            cid = canonical_id(item.get("capture_id"))
            require(cid not in seen, "duplicate_capture")
            seen.add(cid)
            self.descriptor(item.get("input"),cid,".input.json",65536)
            self.descriptor(item.get("payload"),cid,".request.gz",self.config.max_payload_bytes)
            require(set(item) in [{"capture_id","input","payload","outcome"},
                                  {"capture_id","input","payload","headers","outcome"}], "invalid_listing_item_fields")
            if "headers" in item:
                self.descriptor(item["headers"],cid,".headers.gz",MAX_HEADERS_COMPRESSED)
            if item.get("outcome") is not None:
                self.descriptor(item["outcome"],cid,".outcome.json",65536)
            total += sum(item[k]["bytes"] for k in ["input","payload","headers","outcome"] if item.get(k) is not None)
        require(total <= self.config.max_bytes, "listing_byte_limit")
        return items

    def verify_file(self, path, desc):
        info = regular(path)
        require(info.st_size == desc["bytes"], "object_size_mismatch")
        h = hashlib.sha256()
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
        with os.fdopen(fd,"rb") as source:
            for block in iter(lambda:source.read(1024 ** 2), b""):
                self.time_budget(); h.update(block)
        require(h.hexdigest() == desc["sha256"], "object_hash_mismatch")

    def fetch(self, directory, desc):
        self.time_budget()
        destination = directory / desc["path"]
        if destination.exists() or destination.is_symlink():
            self.verify_file(destination, desc)
            return destination
        size = desc["bytes"]
        staged=self.prefetched.pop(desc['path'],None)
        if staged is not None:
            self.verify_file(staged,desc)
            fd=os.open(staged,os.O_RDONLY|os.O_NOFOLLOW)
            try:os.fsync(fd)
            finally:os.close(fd)
            os.link(staged,destination,follow_symlinks=False)
            staged.unlink();fsync_dir(directory)
            return destination
        with self.state_lock:
            if self.fetched_bytes + size > self.config.max_bytes:
                raise LimitReached("run_byte_limit")
            require(self.namespace_bytes + size <= self.config.max_namespace_bytes, "namespace_space_limit")
            require(shutil.disk_usage(self.config.root).free - size - self.inflight_bytes >= self.config.min_free_bytes,
                    "nas_free_space_limit")
            # Failed attempts can leave complete copies: retain their budget.
            self.fetched_bytes += size
            self.namespace_bytes += size
            self.inflight_bytes += size
        pending = directory / ("." + desc["path"] + ".pending-" + uuid.uuid4().hex)
        try:
            fd = os.open(pending, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
            os.close(fd)
            self.transport.fetch(desc["path"], pending, self.time_budget(),size)
        finally:
            with self.state_lock:
                self.inflight_bytes -= size
        self.verify_file(pending,desc)
        fd = os.open(pending, os.O_RDONLY | os.O_NOFOLLOW)
        try: os.fsync(fd)
        finally: os.close(fd)
        os.link(pending,destination,follow_symlinks=False)
        pending.unlink(); fsync_dir(directory)
        return destination

    def input_manifest(self, path, cid):
        value = decode(read_small(path,65536))
        version = value.get("schema_version")
        require(type(version) is int and version in [1,2] and value.get("capture_id") == cid, "invalid_input_identity")
        require((set(value) == INPUT_FIELDS | HEADER_FIELDS if version == 2 else not HEADER_FIELDS.intersection(value)), "invalid_input_header_fields")
        require(type(value.get("group_id")) is int and value["group_id"] == self.config.group_id, "wrong_input_group")
        captured = parse_time(value.get("captured_at")); sealed = parse_time(value.get("sealed_at"))
        require(captured <= sealed <= self.now + dt.timedelta(minutes=5), "invalid_capture_time")
        require(value.get("payload_file") == cid + ".request.gz", "unsafe_payload_file")
        for key in ["raw_bytes","compressed_bytes"]:
            require(type(value.get(key)) is int and value[key] >= 0, "invalid_body_size")
        require(value["raw_bytes"] <= self.config.max_raw_bytes and 0 < value["compressed_bytes"] <= self.config.max_payload_bytes, "body_size_limit")
        for key in ["raw_sha256","compressed_sha256"]:
            require(isinstance(value.get(key),str) and SHA_RE.fullmatch(value[key]), "invalid_body_hash")
        if version == 2:
            require(value["headers_file"] == cid + ".headers.gz", "unsafe_headers_file")
            require(value["headers_policy"] == HEADERS_POLICY, "invalid_headers_policy")
            require(type(value["headers_raw_bytes"]) is int and 0 < value["headers_raw_bytes"] <= MAX_HEADERS_RAW
                    and type(value["headers_compressed_bytes"]) is int and 0 < value["headers_compressed_bytes"] <= MAX_HEADERS_COMPRESSED,
                    "invalid_headers_size")
            for key in ["headers_raw_sha256","headers_compressed_sha256"]:
                require(isinstance(value[key],str) and SHA_RE.fullmatch(value[key]), "invalid_headers_hash")
            require(type(value["headers_redacted_count"]) is int and 0 <= value["headers_redacted_count"] <= MAX_HEADERS_RAW // 5,
                    "invalid_headers_redacted_count")
        require(type(value.get("body_complete")) is bool and type(value.get("body_present")) is bool, "invalid_body_flags")
        headers = value.get("headers")
        require(isinstance(headers,dict) and all(isinstance(k,str) and k.lower() in HEADER_NAMES
                and isinstance(v,(str,list)) for k,v in headers.items()), "unsafe_input_headers")
        require(value.get("read_error") in ["","read_error","context_canceled","body_too_large"], "invalid_read_error")
        require(value["body_complete"] == (value["read_error"] == ""), "contradictory_body_state")
        return value

    def verify_raw(self, path, manifest, headers=False):
        size = 0; h = hashlib.sha256(); chunks = []
        prefix = "headers_" if headers else ""
        cap = MAX_HEADERS_RAW if headers else self.config.max_raw_bytes
        label = "headers_" if headers else ""
        try:
            fd = os.open(path,os.O_RDONLY | os.O_NOFOLLOW)
            with os.fdopen(fd,"rb") as source, gzip.GzipFile(fileobj=source,mode="rb") as raw:
                while True:
                    self.time_budget()
                    block = raw.read(65536)
                    if not block: break
                    size += len(block)
                    require(size <= manifest[prefix+"raw_bytes"] and size <= cap, label+"raw_expansion_limit")
                    h.update(block)
                    if headers: chunks.append(block)
        except (OSError, EOFError, zlib.error):
            raise ArchiveError(label+"invalid_gzip") from None
        require(size == manifest[prefix+"raw_bytes"] and h.hexdigest() == manifest[prefix+"raw_sha256"], label+"raw_hash_or_size_mismatch")
        if headers: validate_headers(decode(b"".join(chunks)),manifest["headers_redacted_count"])

    def input_receipt(self, cid, manifest, input_sha):
        value = {"schema_version":1,"group_id":self.config.group_id,"capture_id":cid,
                 "captured_at":manifest["captured_at"],"input_sha256":input_sha,
                 "compressed_sha256":manifest["compressed_sha256"],"compressed_bytes":manifest["compressed_bytes"],
                 "raw_sha256":manifest["raw_sha256"],"raw_bytes":manifest["raw_bytes"],"body_complete":manifest["body_complete"]}
        if manifest["schema_version"] == 2:
            value["schema_version"] = 2
            value.update({key:manifest[key] for key in HEADER_FIELDS})
        return value

    def expected_ack(self,cid,kind,object_sha,receipt_sha):
        return {"schema_version":1,"group_id":self.config.group_id,"capture_id":cid,"kind":kind,
                "object_sha256":object_sha,"receipt_sha256":receipt_sha,"accepted":True}

    def acknowledge(self,directory,cid,kind,object_sha,receipt_path,allow_reissue=False):
        if (cid,kind) in self.reconciled:
            return
        receipt_sha = digest(read_small(receipt_path,65536))
        expected = self.expected_ack(cid,kind,object_sha,receipt_sha)
        intent = directory / (cid + "." + kind + "-ack.intent.json")
        done = directory / (cid + "." + kind + "-ack.done.json")
        if done.exists() or done.is_symlink():
            require(decode(read_small(done,65536)) == expected, "local_ack_conflict")
            return
        if allow_reissue and hasattr(self.transport,'ack_batch'):
            # Only process(), after full content/receipt verification, can queue
            # these idempotent exact claims. An uncertain batch is never retried
            # here, and cannot authorize source GC without the exact reply.
            if intent.exists() or intent.is_symlink():
                require(decode(read_small(intent,65536))==expected,'ack_intent_conflict')
            else:atomic_json(intent,expected)
            with self.state_lock:self.pending_acks[(cid,kind)]=(directory,expected)
            return
        if intent.exists() or intent.is_symlink():
            require(decode(read_small(intent,65536)) == expected, "ack_intent_conflict")
            # Read-only observation is the only permitted recovery after intent.
            status = self.transport.ack_status(cid,kind)
            if status == expected:
                atomic_json(done,expected); self.increment("acks_confirmed")
            elif status == {"schema_version":1,"accepted":False} and allow_reissue:
                # process() has just reverified the current source descriptors,
                # payload and receipts. Reissue the identical conditional ACK,
                # never a model request or an altered receipt.
                try:
                    status=self.transport.ack(cid,kind,object_sha,receipt_sha)
                    require(status==expected,"uncertain_ack_response")
                    atomic_json(done,expected); self.increment("acks_confirmed")
                except Exception:
                    self.increment("acks_pending")
                    self.record_error({"capture_id":cid,"code":"ack_outcome_unknown"})
            else:
                self.increment("acks_pending")
                self.record_error({"capture_id":cid,"code":"ack_status_absent_or_conflicting"})
            return
        atomic_json(intent,expected)
        try:
            status = self.transport.ack(cid,kind,object_sha,receipt_sha)
            require(status == expected,"uncertain_ack_response")
            atomic_json(done,expected); self.increment("acks_confirmed")
        except Exception:
            self.increment("acks_pending")
            # Do not serialize transport stderr or remote body; report a fixed code.
            self.record_error({"capture_id":cid,"code":"ack_outcome_unknown"})

    def reconcile(self,captures):
        """Resolve durable local intents even after source GC stops listing them."""
        state=self.config.root/".run"/"ack-cursor.json"
        cursor=decode(read_small(state,65536)).get("last_uuid","") if state.exists() else ""
        require(cursor=="" or UUID_RE.fullmatch(cursor),"invalid_ack_cursor")
        def candidates(after):
            for directory in captures.iterdir():
                self.time_budget()
                cid=directory.name
                if UUID_RE.fullmatch(cid) and cid>after:
                    if any((directory/(cid+"."+kind+"-ack.intent.json")).exists()
                           and not (directory/(cid+"."+kind+"-ack.done.json")).exists()
                           for kind in ["input","outcome"]):
                        yield cid
        names=heapq.nsmallest(self.config.max_retention_scan,candidates(cursor))
        if not names and cursor:
            names=heapq.nsmallest(self.config.max_retention_scan,candidates(""))
        last=cursor
        for cid in names:
            last=cid;directory=captures/cid
            try:
                private_dir(directory)
                for kind in ["input","outcome"]:
                    intent=directory/(cid+"."+kind+"-ack.intent.json")
                    done=directory/(cid+"."+kind+"-ack.done.json")
                    if not intent.exists() or done.exists(): continue
                    receipt=directory/(cid+"."+kind+"-receipt.json")
                    value=decode(read_small(intent,65536))
                    expected=self.expected_ack(cid,kind,value.get("object_sha256"),digest(read_small(receipt,65536)))
                    require(value==expected and SHA_RE.fullmatch(value["object_sha256"]),"ack_intent_conflict")
                    self.acknowledge(directory,cid,kind,value["object_sha256"],receipt)
                    if done.exists(): self.reconciled.add((cid,kind))
            except ArchiveError as exc:
                self.result["errors"].append({"capture_id":cid,"code":str(exc)})
        replace_state(state,{"last_uuid":last})
        if any(True for _ in candidates("")):
            self.result["status"]="pending"

    def flush_acks(self):
        queued=list(self.pending_acks.values());self.pending_acks.clear()
        for offset in range(0,len(queued),MAX_ACK_CLAIMS):
            batch=queued[offset:offset+MAX_ACK_CLAIMS]
            claims=[{k:value[k]for k in ['capture_id','kind','object_sha256','receipt_sha256']}for _,value in batch]
            try:
                self.time_budget()
                result=self.transport.ack_batch(claims)
                require(result.get('schema_version')==1 and result.get('group_id')==self.config.group_id
                        and isinstance(result.get('results'),list) and len(result['results'])==len(batch),'invalid_ack_batch_response')
                for (directory,expected),actual in zip(batch,result['results']):
                    if actual!=expected:
                        self.increment('acks_pending');continue
                    cid=expected['capture_id'];kind=expected['kind']
                    atomic_json(directory/(cid+'.'+kind+'-ack.done.json'),expected)
                    self.increment('acks_confirmed')
                    if all((directory/(cid+'.'+k+'-ack.done.json')).exists()for k in ['input','outcome']):
                        self.gc_ready.add(cid)
            except Exception:
                self.result['acks_pending']+=len(batch)
                self.record_error({'code':'ack_batch_outcome_unknown'})

    def process_batch(self,items,captures):
        if not hasattr(self.transport,'fetch_batch'):
            with ThreadPoolExecutor(max_workers=self.config.workers)as pool:
                list(pool.map(lambda item:self.process_bounded(item,captures),items))
            return
        with tempfile.TemporaryDirectory(prefix='transfer-',dir=self.config.root/'.run')as folder:
            descriptors=[]
            for item in items:
                for key in ['input','payload','headers','outcome']:
                    desc=item.get(key)
                    if desc and not (captures/item['capture_id']/desc['path']).exists():descriptors.append(desc)
            if descriptors:
                size=sum(d['bytes']for d in descriptors)
                require(self.fetched_bytes+size<=self.config.max_bytes,'run_byte_limit')
                require(self.namespace_bytes+size<=self.config.max_namespace_bytes,'namespace_space_limit')
                require(shutil.disk_usage(self.config.root).free-size>=self.config.min_free_bytes,'nas_free_space_limit')
                self.fetched_bytes+=size;self.namespace_bytes+=size
                target=self.transport.fetch_batch(descriptors,Path(folder),self.time_budget())
                self.prefetched={d['path']:target/d['path']for d in descriptors}
            with ThreadPoolExecutor(max_workers=self.config.workers)as pool:
                list(pool.map(lambda item:self.process_bounded(item,captures),items))
            self.prefetched.clear()
            self.flush_acks()

    def process(self,item,captures):
        cid = item["capture_id"]
        directory = captures / cid
        private_dir(directory,create=True)
        input_path = self.fetch(directory,item["input"])
        manifest = self.input_manifest(input_path,cid)
        require(item["payload"]["bytes"] == manifest["compressed_bytes"]
                and item["payload"]["sha256"] == manifest["compressed_sha256"], "payload_descriptor_conflict")
        payload = self.fetch(directory,item["payload"])
        self.verify_raw(payload,manifest)
        require(("headers" in item) == (manifest["schema_version"] == 2), "headers_descriptor_missing_or_unexpected")
        if manifest["schema_version"] == 2:
            require(item["headers"]["bytes"] == manifest["headers_compressed_bytes"]
                    and item["headers"]["sha256"] == manifest["headers_compressed_sha256"], "headers_descriptor_conflict")
            header_path = self.fetch(directory,item["headers"])
            self.verify_raw(header_path,manifest,headers=True)
        input_receipt = directory / (cid + ".input-receipt.json")
        atomic_json(input_receipt,self.input_receipt(cid,manifest,item["input"]["sha256"]))
        self.increment("input_archived")
        self.acknowledge(directory,cid,"input",item["input"]["sha256"],input_receipt,allow_reissue=True)
        if item.get("outcome") is not None:
            outcome_path = self.fetch(directory,item["outcome"])
            outcome = decode(read_small(outcome_path,65536))
            require(outcome.get("schema_version") == 1 and outcome.get("capture_id") == cid, "invalid_outcome_identity")
            finished = parse_time(outcome.get("finished_at"))
            require(parse_time(manifest["captured_at"]) <= finished <= self.now + dt.timedelta(minutes=5), "invalid_outcome_time")
            require(type(outcome.get("http_status")) is int and 0 <= outcome["http_status"] <= 599
                    and type(outcome.get("request_released")) is bool and type(outcome.get("context_canceled")) is bool, "invalid_outcome_flags")
            receipt = directory / (cid + ".outcome-receipt.json")
            atomic_json(receipt,{"schema_version":1,"group_id":self.config.group_id,"capture_id":cid,
                                "outcome_sha256":item["outcome"]["sha256"],"input_sha256":item["input"]["sha256"]})
            self.increment("outcome_archived")
            self.acknowledge(directory,cid,"outcome",item["outcome"]["sha256"],receipt,allow_reissue=True)
            if all((directory/(cid+"."+kind+"-ack.done.json")).exists() for kind in ["input","outcome"]):
                with self.state_lock: self.gc_ready.add(cid)

    def pending_input_only(self, directory, cid):
        """Recognize uncommitted downloads, without trusting or adopting bytes."""
        found = False
        pattern = re.compile(r'\.' + re.escape(cid) + r'\.input\.json\.pending-[a-f0-9]{32}\Z')
        for path in directory.iterdir():
            self.time_budget()
            # A canonical artifact, receipt/ACK or unknown file must retain the
            # existing missing-manifest alarm; it is not a fresh partial input.
            if not pattern.fullmatch(path.name):
                return False
            require(regular(path).st_size <= 65536, "metadata_too_large")
            found = True
        return found

    def expire(self,captures):
        cutoff = self.now - dt.timedelta(days=30)
        allowed_suffixes = {".input.json",".request.gz",".outcome.json",".input-receipt.json",".outcome-receipt.json",
                            ".input-ack.intent.json",".input-ack.done.json",".outcome-ack.intent.json",".outcome-ack.done.json"}
        cursor_path=self.config.root/".run"/"retention-cursor.json"
        cursor=decode(read_small(cursor_path,65536)).get("last_uuid","") if cursor_path.exists() else ""
        require(cursor=="" or UUID_RE.fullmatch(cursor),"invalid_retention_cursor")
        def candidates(after):
            for path in captures.iterdir():
                self.time_budget()
                if UUID_RE.fullmatch(path.name) and path.name>after: yield path.name
        # Bound retained names in memory and advance a durable cursor, so recent
        # captures cannot permanently starve older objects later in the directory.
        names=heapq.nsmallest(self.config.max_retention_scan,candidates(cursor))
        if not names and cursor:
            names=heapq.nsmallest(self.config.max_retention_scan,candidates(""))
        checked = 0; last=cursor
        self.result["retention_scanned"]=0
        for name in names:
            self.time_budget()
            if checked >= self.config.max_retention_deletes: break
            directory=captures/name; last=name; self.result["retention_scanned"]+=1
            cid = canonical_id(directory.name)
            try:
                private_dir(directory)
                input_path = directory / (cid+".input.json")
                if not input_path.exists() and not input_path.is_symlink() and self.pending_input_only(directory,cid):
                    self.result["retention_pending_inputs"] += 1
                    continue
                manifest = self.input_manifest(input_path,cid)
                if parse_time(manifest["captured_at"]) >= cutoff: continue
                capture_suffixes = allowed_suffixes | ({".headers.gz"} if manifest["schema_version"] == 2 else set())
                children=list(directory.iterdir())
                # Unexpected files or pending transfers prevent deletion, never broaden it.
                require(all(f.name.startswith(cid) and f.name[len(cid):] in capture_suffixes for f in children), "retention_unknown_file")
                for f in children: regular(f)
                receipt=directory/(cid+".input-receipt.json")
                require(receipt.exists(),"retention_missing_receipt")
                r=decode(read_small(receipt,65536))
                input_sha=digest(read_small(directory/(cid+".input.json"),65536))
                require(r == self.input_receipt(cid,manifest,input_sha),"retention_receipt_conflict")
                artifacts=[(".request.gz", "")]
                if manifest["schema_version"] == 2: artifacts.append((".headers.gz", "headers_"))
                for suffix,prefix in artifacts:
                    artifact=directory/(cid+suffix)
                    self.verify_file(artifact,{"bytes":manifest[prefix+"compressed_bytes"],"sha256":manifest[prefix+"compressed_sha256"]})
                    self.verify_raw(artifact,manifest,headers=bool(prefix))
                expected=self.expected_ack(cid,"input",r["input_sha256"],digest(read_small(receipt,65536)))
                require(decode(read_small(directory/(cid+".input-ack.done.json"),65536))==expected,"retention_input_unacked")
                outcome_path=directory/(cid+".outcome.json")
                require(outcome_path.exists(),"retention_outcome_pending")
                if outcome_path.exists():
                    oreceipt=directory/(cid+".outcome-receipt.json")
                    o=decode(read_small(oreceipt,65536))
                    require(o.get("outcome_sha256")==digest(read_small(outcome_path,65536)),"retention_outcome_conflict")
                    expected=self.expected_ack(cid,"outcome",o["outcome_sha256"],digest(read_small(oreceipt,65536)))
                    require(decode(read_small(directory/(cid+".outcome-ack.done.json"),65536))==expected,"retention_outcome_unacked")
                # Remove only enumerated regular files in this validated capture directory.
                for f in children: f.unlink()
                fsync_dir(directory); directory.rmdir(); fsync_dir(captures)
                self.result["expired"]+=1; checked+=1
            except ArchiveError as exc:
                self.result["errors"].append({"capture_id":cid,"code":str(exc)})
        replace_state(cursor_path,{"last_uuid":last})

    def run(self):
        os.umask(0o077)
        root=self.config.root
        private_dir(root,create=True)
        for path in [root/".run",root/"captures"]: private_dir(path,create=True)
        lock_path=root/".run"/"pull.lock"
        fd=os.open(lock_path,os.O_RDWR|os.O_CREAT|os.O_NOFOLLOW,0o600)
        try:
            try: fcntl.flock(fd,fcntl.LOCK_EX|fcntl.LOCK_NB)
            except BlockingIOError: return {"status":"locked"}
            regular(lock_path)
            atomic_json(root/".run"/"namespace.json",{"schema_version":1,"group_id":self.config.group_id,
                        "ssh_host":self.config.ssh_host,"source_dir":self.config.source_dir,
                        "helper_path":self.config.helper_path})
            # Count existing files, including failed pending transfers, before admitting
            # more data. Never follow symlinks while computing the namespace budget.
            self.namespace_bytes = namespace_size(root, self.time_budget)
            self.transport=self.transport or SSHTransport(self.config,self.deadline-self.cleanup_reserve)
            listing=self.transport.manifest_list()
            items=self.validate_listing(listing); self.result["listed"]=len(items)
            self.source_rejections(listing,"list")
            # Reprocess listed intents after verifying their files. Scanning all
            # historic intents first can consume the whole transfer window.
            if not hasattr(self.transport,'ack_batch'):self.reconcile(root/"captures")
            for offset in range(0,len(items),128):
                try:self.process_batch(items[offset:offset+128],root/'captures')
                except ArchiveError as exc:
                    self.record_error({'code':str(exc)});break
            try:
                self.cleanup_reserve = 0
                if isinstance(self.transport,SSHTransport): self.transport.deadline=self.deadline
                self.time_budget()
                if hasattr(self.transport,"gc_confirmed"):
                    collection=(self.transport.gc_confirmed(sorted(self.gc_ready)) if self.gc_ready else
                                {"schema_version":1,"group_id":self.config.group_id,"deleted":0,"deleted_bytes":0})
                else:
                    collection=self.transport.gc()
                require(collection.get("schema_version")==1 and collection.get("group_id")==self.config.group_id,
                        "wrong_source_gc_identity")
                for key in ["deleted","deleted_bytes"]:
                    require(type(collection.get(key)) is int and collection[key]>=0,"invalid_source_gc_result")
                self.result["source_deleted"]=collection["deleted"]
                self.result["source_deleted_bytes"]=collection["deleted_bytes"]
                self.source_rejections(collection,"gc")
            except Exception:
                self.result["errors"].append({"code":"source_gc_unconfirmed"})
            cursor=listing.get("next_cursor")
            if cursor is not None:
                require(cursor=="" or UUID_RE.fullmatch(cursor),"invalid_source_cursor")
                replace_state(root/".run"/"source-cursor.json",{"last_uuid":cursor})
            try:
                self.expire(root/"captures")
            except LimitReached:
                self.result["errors"].append({"code":"retention_deferred_time_limit"})
            self.result["bytes_fetched"]=self.fetched_bytes
            if self.result["errors"] or self.result["acks_pending"] or self.result["retention_pending_inputs"]:
                self.result["status"]="pending"
            state=root/".run"/("run-"+utc_now().strftime("%Y%m%dT%H%M%S")+"-"+uuid.uuid4().hex+".json")
            atomic_json(state,self.result)
            return self.result
        finally:
            os.close(fd)


def run_once(config_path):
    try:
        raw=decode(read_small(config_path,65536))
        raw["root"]=Path(raw["root"])
        return Puller(Config(**raw)).run()
    except Exception as exc:
        code=str(exc) if isinstance(exc,ArchiveError) else "archive_run_failed"
        return {"status":"failed","code":code}


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config",type=Path,required=True)
    parser.add_argument("--continuous",action="store_true")
    args=parser.parse_args(); failures=0
    while True:
        result=run_once(args.config)
        print(json.dumps(result,sort_keys=True),flush=True)
        if not args.continuous:
            return 0 if result["status"] in ["ok","locked"] else 1
        progress=result.get("acks_confirmed",0)>0 or result.get("source_deleted",0)>0
        failures=0 if progress else min(failures+1,4)
        time.sleep(0.2 if progress else min(60,5*2**failures))


if __name__=="__main__":
    raise SystemExit(main())
