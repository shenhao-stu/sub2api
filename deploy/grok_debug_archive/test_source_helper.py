"""Real filesystem source protocol behavior and NAS integration, without SSH."""
import datetime as dt
import gzip
import hashlib
import json
import os
from pathlib import Path
import shutil
import stat
import subprocess
import sys
import tempfile
import unittest
from unittest import mock
import uuid

import source_helper as source
import nas_archive as nas

NOW = dt.datetime(2026, 11, 3, 0, 0, tzinfo=dt.timezone.utc)


def sha(raw): return hashlib.sha256(raw).hexdigest()
def stamp(value): return value.isoformat().replace('+00:00', 'Z')


class Producer:
    """Synthetic files shaped like the actual Go producer, not helper descriptors."""
    def __init__(self, root): self.root = root

    def put(self, name, raw):
        path = self.root / name; path.write_bytes(raw); path.chmod(0o600)

    def add(self, raw=b'{"messages":[{"role":"user","content":"synthetic"}]}', when=None,
            complete=True, outcome=False, group=78):
        cid = str(uuid.uuid4()); when = when or NOW - dt.timedelta(minutes=1)
        compressed = gzip.compress(raw, mtime=0)
        manifest = {'schema_version':1,'capture_id':cid,'captured_at':stamp(when),'sealed_at':stamp(when),
                    'group_id':group,'api_key_id':10,'user_id':20,'request_id':'synthetic',
                    'method':'POST','route':'/v1/chat/completions','headers':{'Content-Type':'application/json'},
                    'content_length':len(raw),'body_present':True,'payload_file':cid+'.request.gz',
                    'raw_bytes':len(raw),'raw_sha256':sha(raw),'compressed_bytes':len(compressed),
                    'compressed_sha256':sha(compressed),'body_complete':complete,
                    'read_error':'' if complete else 'context_canceled'}
        self.put(cid+'.request.gz', compressed); self.put(cid+'.input.json', source.encode(manifest))
        if outcome: self.finish(cid,released=complete)
        return cid

    def finish(self, cid, released=True):
        manifest = json.loads((self.root/(cid+'.input.json')).read_bytes())
        when = source.timestamp(manifest['sealed_at']) + dt.timedelta(seconds=1)
        self.put(cid+'.outcome.json',source.encode({'schema_version':1,'capture_id':cid,'finished_at':stamp(when),
                 'http_status':200 if released else 400,'context_canceled':not released,
                 'error_code':'' if released else 'context_canceled','request_released':released}))

    def change(self, cid, mutate):
        value = json.loads((self.root/(cid+'.input.json')).read_bytes()); mutate(value)
        self.put(cid+'.input.json',source.encode(value))


class LocalTransport:
    """NAS transport seam backed by the real source helper and source files."""
    def __init__(self, root, config):
        self.root=root; self.config=config; self.uncertain=False; self.ack_calls=[]; self.status_calls=[]

    def store(self): return source.SourceStore(self.root,os.geteuid(),NOW)
    def manifest_list(self):
        with self.store() as store: return store.list(self.config.max_objects,self.config.max_bytes)
    def fetch(self,name,destination,timeout,max_bytes):
        require_name = name[:36]
        source.capture_id(require_name)
        assert name in {require_name+s for s in ['.input.json','.request.gz','.outcome.json']}
        fd=os.open(self.root/name,os.O_RDONLY|os.O_NOFOLLOW)
        with os.fdopen(fd,'rb') as src, destination.open('wb') as target:
            shutil.copyfileobj(src,target)
        destination.chmod(0o600)
    def ack(self,cid,kind,object_sha,receipt_sha):
        self.ack_calls.append((cid,kind))
        with self.store() as store: result=store.ack(cid,kind,object_sha,receipt_sha)
        if self.uncertain: raise nas.ArchiveError('synthetic_ack_disconnect')
        return result
    def ack_status(self,cid,kind):
        self.status_calls.append((cid,kind))
        with self.store() as store: return store.ack_status(cid,kind)
    def gc(self):
        with self.store() as store: return store.gc(100,1024**3)


class SourceBehavior(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory(); self.base=Path(self.temp.name).resolve()
        self.root=self.base/'source'; self.root.mkdir(mode=0o700); self.producer=Producer(self.root)

    def tearDown(self): self.temp.cleanup()
    def store(self,now=NOW): return source.SourceStore(self.root,os.geteuid(),now)
    def listing(self,n=100,size=1024**3):
        with self.store() as store: return store.list(n,size)
    def ack(self,cid,kind):
        with self.store() as store:
            manifest,inp,_=store.input(cid); outcome,_=store.outcome(cid,manifest)
            desc=inp if kind=='input' else outcome
            return store.ack(cid,kind,desc['sha256'],sha(('durable NAS '+kind).encode()))
    def both(self,cid): return self.ack(cid,'input'),self.ack(cid,'outcome')
    def gc(self,**kwargs):
        with self.store() as store: return store.gc(**kwargs)

    def test_ready_input_lists_before_outcome_and_read_only_creates_no_state(self):
        cid=self.producer.add(); before={p.name:p.read_bytes()for p in self.root.iterdir()}
        item=self.listing()['items'][0]
        self.assertEqual(item['capture_id'],cid); self.assertIsNone(item['outcome'])
        for key in ['input','payload']:
            raw=(self.root/item[key]['path']).read_bytes()
            self.assertEqual(item[key],{'path':item[key]['path'],'bytes':len(raw),'sha256':sha(raw)})
        self.assertEqual(before,{p.name:p.read_bytes()for p in self.root.iterdir()})
        with self.store() as store: self.assertEqual(store.ack_status(cid,'input'),{'schema_version':1,'accepted':False})
        self.assertFalse((self.root/'.archive-helper').exists())

    def test_cli_rejects_foreign_group_before_opening_source_and_has_no_path_override(self):
        script=Path(source.__file__)
        result=subprocess.run([sys.executable,str(script),'manifest-list','--group-id','79','--max-objects','1','--max-bytes','1000'],capture_output=True,text=True)
        self.assertEqual(result.returncode,1); self.assertEqual(json.loads(result.stdout)['code'],'wrong_group')
        result=subprocess.run([sys.executable,str(script),'gc','--group-id','78','--source-dir',str(self.root)],capture_output=True,text=True)
        self.assertEqual(result.returncode,2)

    def test_orphans_partial_files_and_noncanonical_names_are_never_ready(self):
        cid=str(uuid.uuid4())
        self.producer.put(cid+'.request.gz',gzip.compress(b'synthetic'))
        self.producer.put(cid+'.outcome.json',b'{}')
        self.producer.put(cid+'.request.gz.partial',b'synthetic partial')
        self.producer.put('not-a-uuid.input.json',b'{}')
        before={p.name:p.read_bytes()for p in self.root.iterdir()}
        self.assertEqual(self.listing()['items'],[]); self.assertEqual(self.gc()['deleted'],0)
        self.assertEqual(before,{p.name:p.read_bytes()for p in self.root.iterdir()if p.is_file()})

    def test_invalid_manifest_identity_group_headers_and_paths_are_rejected(self):
        changes=[lambda x:x.update(group_id=79),lambda x:x.update(capture_id=str(uuid.uuid4())),
                 lambda x:x.update(payload_file='../escape.gz'),lambda x:x.update(schema_version=True),
                 lambda x:x['headers'].update(Authorization='synthetic forbidden'),
                 lambda x:x.update(body_complete=False),lambda x:x.update(raw_bytes=source.MAX_RAW+1)]
        for change in changes:
            with self.subTest(change=changes.index(change)):
                cid=self.producer.add(); self.producer.change(cid,change)
                self.assertNotIn(cid,[x['capture_id']for x in self.listing()['items']])
                with self.store() as store:
                    with self.assertRaises(source.HelperError): store.ack(cid,'input','0'*64,'1'*64)

    def test_duplicate_json_keys_are_rejected(self):
        cid=self.producer.add(); path=self.root/(cid+'.input.json')
        path.write_bytes(path.read_bytes().rstrip()[:-1]+b',"group_id":78}\n')
        self.assertEqual(self.listing()['items'],[])
        self.assertIn('duplicate_json_key',self.listing()['rejected'])

    def test_non_utc_or_incomplete_timestamps_and_nonprivate_modes_are_rejected(self):
        cid=self.producer.add()
        original=(self.root/(cid+'.input.json')).read_bytes()
        for value in ['2026-11-03Z','2026-11-03T00:00:00+00:00','2026-11-03T00:00:00+01:00Z']:
            self.producer.put(cid+'.input.json',original)
            self.producer.change(cid,lambda x:x.update(captured_at=value))
            self.assertEqual(self.listing()['items'],[])
        self.producer.put(cid+'.input.json',original)
        for mode in [0o644,0o400,0o700]:
            (self.root/(cid+'.request.gz')).chmod(mode)
            self.assertEqual(self.listing()['items'],[])
        (self.root/(cid+'.request.gz')).chmod(0o600)
        self.assertEqual(len(self.listing()['items']),1)

    def test_source_owner_and_parent_symlink_are_rejected(self):
        with self.assertRaises(source.HelperError):
            with source.SourceStore(self.root,os.geteuid()+100,NOW): pass
        link=self.base/'link'; link.symlink_to(self.root,target_is_directory=True)
        with self.assertRaises(OSError):
            with source.SourceStore(link,os.geteuid(),NOW): pass

    def test_source_symlinks_and_hardlinks_never_expose_outside_data(self):
        for suffix in ['.input.json','.request.gz','.outcome.json']:
            for kind in ['symlink','hardlink']:
                with self.subTest(suffix=suffix,kind=kind):
                    cid=self.producer.add(outcome=True); path=self.root/(cid+suffix)
                    outside=self.base/('outside-'+uuid.uuid4().hex); outside.write_bytes(path.read_bytes()); outside.chmod(0o600)
                    path.unlink()
                    if kind=='symlink': path.symlink_to(outside)
                    else: os.link(outside,path)
                    self.assertNotIn(cid,[x['capture_id']for x in self.listing()['items']])
                    self.assertTrue(outside.exists())

    def test_corrupted_payload_is_not_listed_or_acknowledged(self):
        cid=self.producer.add(); path=self.root/(cid+'.request.gz'); raw=bytearray(path.read_bytes()); raw[-1]^=1; path.write_bytes(raw)
        result=self.listing(); self.assertEqual(result['items'],[]); self.assertIn('payload_hash_mismatch',result['rejected'])
        with self.store() as store:
            _,inp,_=store.input(cid)
            with self.assertRaises(source.HelperError): store.ack(cid,'input',inp['sha256'],'0'*64)

    def test_ack_is_immutable_durable_and_does_not_modify_producer_input(self):
        cid=self.producer.add(outcome=True); before={p.name:p.read_bytes()for p in self.root.iterdir()}
        ack=self.ack(cid,'input'); path=self.root/'.archive-helper'/(cid+'.input.ack.json'); original=path.read_bytes()
        self.assertEqual(path.stat().st_mode&0o777,0o600); self.assertEqual(path.stat().st_nlink,1)
        with self.store() as store:
            self.assertEqual(store.ack_status(cid,'input'),ack)
            self.assertEqual(store.ack(cid,'input',ack['object_sha256'],ack['receipt_sha256']),ack)
            with self.assertRaises(source.HelperError): store.ack(cid,'input',ack['object_sha256'],'0'*64)
        self.assertEqual(path.read_bytes(),original)
        self.assertEqual(before,{p.name:p.read_bytes()for p in self.root.iterdir()if p.is_file()})

    def test_outcome_ack_can_precede_input_ack(self):
        cid=self.producer.add(outcome=True); self.ack(cid,'outcome')
        with self.store() as store: self.assertFalse(store.ack_status(cid,'input')['accepted'])
        self.assertEqual(self.gc()['deleted'],0)

    def test_failed_fsync_never_publishes_an_accepted_ack(self):
        cid=self.producer.add(); real=os.fsync
        def fail_file(fd):
            if stat.S_ISREG(os.fstat(fd).st_mode): raise OSError('synthetic disk failure')
            return real(fd)
        with mock.patch.object(source.os,'fsync',side_effect=fail_file):
            with self.assertRaises(OSError): self.ack(cid,'input')
        with self.store() as store: self.assertFalse(store.ack_status(cid,'input')['accepted'])
        self.assertTrue(list((self.root/'.archive-helper').glob('*.pending-*')))
        self.assertTrue((self.root/(cid+'.input.json')).exists())

    def test_directory_fsync_failure_cannot_be_confirmed_until_durability_recovers(self):
        cid=self.producer.add(); real=os.fsync
        def fail_ack_directory(fd):
            state=self.root/'.archive-helper'; info=os.fstat(fd)
            if state.exists() and stat.S_ISDIR(info.st_mode) and info.st_ino==state.stat().st_ino:
                raise OSError('synthetic ACK directory fsync failure')
            return real(fd)
        with mock.patch.object(source.os,'fsync',side_effect=fail_ack_directory):
            with self.assertRaises(OSError): self.ack(cid,'input')
            # Publication may already be visible; visibility alone is not commit.
            path=self.root/'.archive-helper'/(cid+'.input.ack.json')
            self.assertTrue(path.exists()); value=json.loads(path.read_bytes())
            with self.store() as store:
                with self.assertRaises(OSError): store.ack_status(cid,'input')
                with self.assertRaises(OSError): store.ack(cid,'input',value['object_sha256'],value['receipt_sha256'])
        with self.store() as store:
            self.assertEqual(store.ack_status(cid,'input'),value)
            self.assertEqual(store.ack(cid,'input',value['object_sha256'],value['receipt_sha256']),value)

    def test_symlink_ack_state_and_hardlinked_ack_are_rejected(self):
        cid=self.producer.add(); outside=self.base/'outside'; outside.mkdir(mode=0o700)
        (self.root/'.archive-helper').symlink_to(outside,target_is_directory=True)
        with self.assertRaises(source.HelperError): self.ack(cid,'input')
        self.assertEqual(list(outside.iterdir()),[])
        (self.root/'.archive-helper').unlink(); self.ack(cid,'input')
        ackpath=self.root/'.archive-helper'/(cid+'.input.ack.json'); os.link(ackpath,outside/'ack')
        with self.store() as store:
            with self.assertRaises(source.HelperError): store.ack_status(cid,'input')

    def test_pending_input_and_late_outcome_are_not_starved_by_acknowledged_captures(self):
        old=[self.producer.add(when=NOW-dt.timedelta(hours=3),outcome=True)for _ in range(3)]
        for cid in old: self.both(cid)
        late=self.producer.add(when=NOW-dt.timedelta(hours=2)); self.ack(late,'input'); self.producer.finish(late)
        new=self.producer.add(when=NOW-dt.timedelta(hours=1))
        first=self.listing(n=1)['items'][0]; self.assertEqual(first['capture_id'],late); self.ack(late,'outcome')
        self.assertEqual(self.listing(n=1)['items'][0]['capture_id'],new)

    def test_oversize_first_capture_does_not_block_later_fitting_capture(self):
        large=self.producer.add(raw=os.urandom(10000),when=NOW-dt.timedelta(minutes=2))
        small=self.producer.add(raw=b'x'); item=next(x for x in self.listing()['items']if x['capture_id']==small)
        size=sum(item[k]['bytes']for k in ['input','payload'])
        self.assertEqual([x['capture_id']for x in self.listing(n=1,size=size)['items']],[small])
        self.assertTrue((self.root/(large+'.input.json')).exists())

    def test_gc_requires_strictly_more_than_24h_and_keeps_ack_tombstones(self):
        old=self.producer.add(when=NOW-dt.timedelta(hours=24,seconds=1),outcome=True)
        exact=self.producer.add(when=NOW-dt.timedelta(hours=24),outcome=True)
        older=self.both(old); self.both(exact); result=self.gc()
        self.assertEqual(result['deleted'],1)
        self.assertFalse((self.root/(old+'.input.json')).exists()); self.assertTrue((self.root/(exact+'.input.json')).exists())
        with self.store() as store:
            self.assertEqual(store.ack_status(old,'input'),older[0]); self.assertEqual(store.ack_status(old,'outcome'),older[1])

    def test_gc_terminal_false_is_eligible_but_active_unacked_and_disk_partial_are_preserved(self):
        when=NOW-dt.timedelta(days=2)
        terminal=self.producer.add(raw=b'{partial',when=when,complete=False,outcome=True); self.both(terminal)
        active=self.producer.add(when=when); self.ack(active,'input')
        no_input_ack=self.producer.add(when=when,outcome=True); self.ack(no_input_ack,'outcome')
        no_outcome_ack=self.producer.add(when=when,outcome=True); self.ack(no_outcome_ack,'input')
        disk_partial=self.producer.add(when=when,outcome=True); self.both(disk_partial)
        self.producer.put(disk_partial+'.request.gz.partial',b'preserve')
        self.assertEqual(self.gc()['deleted'],1)
        for cid in [active,no_input_ack,no_outcome_ack,disk_partial]: self.assertTrue((self.root/(cid+'.input.json')).exists())
        self.assertTrue((self.root/(disk_partial+'.request.gz.partial')).exists())

    def test_gc_limits_and_changed_source_do_not_broaden_deletion(self):
        ids=[self.producer.add(when=NOW-dt.timedelta(days=2),outcome=True)for _ in range(3)]
        for cid in ids:self.both(cid)
        self.assertEqual(self.gc(max_objects=1)['deleted'],1)
        remaining=[cid for cid in ids if (self.root/(cid+'.input.json')).exists()]
        self.producer.change(remaining[0],lambda x:x.update(request_id='synthetic changed producer'))
        result=self.gc(); self.assertEqual(result['deleted'],1)
        self.assertTrue((self.root/(remaining[0]+'.input.json')).exists())
        self.assertIn('gc_ack_object_conflict',result['rejected'])

    def test_gc_byte_budget_skips_large_capture_and_preserves_it(self):
        when=NOW-dt.timedelta(days=2)
        large=self.producer.add(raw=os.urandom(8000),when=when,outcome=True)
        small=self.producer.add(raw=b'x',when=when,outcome=True)
        for cid in [large,small]:self.both(cid)
        item=next(x for x in self.listing()['items']if x['capture_id']==small)
        size=sum(item[k]['bytes']for k in ['input','payload','outcome'])
        result=self.gc(max_bytes=size)
        self.assertEqual(result['deleted'],1); self.assertEqual(result['deleted_bytes'],size)
        self.assertTrue((self.root/(large+'.input.json')).exists())
        self.assertFalse((self.root/(small+'.input.json')).exists())

    def test_gc_rechecks_identity_after_validation_before_unlink(self):
        cid=self.producer.add(when=NOW-dt.timedelta(days=2),outcome=True); self.both(cid)
        with self.store() as store:
            original=store.payload_descriptor
            def mutate_after_validation(capture,manifest):
                result=original(capture,manifest)
                self.producer.put(capture+'.request.gz',b'synthetic concurrently replaced file')
                return result
            with mock.patch.object(store,'payload_descriptor',side_effect=mutate_after_validation):
                result=store.gc()
        self.assertEqual(result['deleted'],0); self.assertIn('gc_source_changed',result['rejected'])
        self.assertTrue((self.root/(cid+'.input.json')).exists())

    def test_nas_archives_immediate_input_then_late_outcome_using_actual_helper(self):
        cid=self.producer.add(); config=nas.Config(root=self.base/'netcup-grok-test',group_id=78,
             source_dir=str(self.root),helper_path=str(source.INSTALL_PATH),min_free_bytes=0)
        transport=LocalTransport(self.root,config)
        first=nas.Puller(config,transport,NOW).run(); self.assertEqual(first['status'],'ok')
        self.assertEqual(transport.ack_calls,[(cid,'input')])
        self.producer.finish(cid,released=False)
        second=nas.Puller(config,transport,NOW).run(); self.assertEqual(second['outcome_archived'],1)
        self.assertEqual(transport.ack_calls,[(cid,'input'),(cid,'outcome')])

    def test_nas_reconciles_ack_tombstones_after_source_gc_removed_listing(self):
        cid=self.producer.add(when=NOW-dt.timedelta(days=2),outcome=True)
        config=nas.Config(root=self.base/'netcup-grok-test',group_id=78,source_dir=str(self.root),
                          helper_path=str(source.INSTALL_PATH),min_free_bytes=0)
        transport=LocalTransport(self.root,config); transport.uncertain=True
        first=nas.Puller(config,transport,NOW).run(); self.assertEqual(first['acks_pending'],2)
        self.assertEqual(first['source_deleted'],1)
        self.assertEqual(transport.gc()['deleted'],0); self.assertEqual(transport.manifest_list()['items'],[])
        with transport.store() as store:
            self.assertTrue(store.ack_status(cid,'input')['accepted'])
            self.assertTrue(store.ack_status(cid,'outcome')['accepted'])
        transport.uncertain=False
        second=nas.Puller(config,transport,NOW).run(); self.assertEqual(second['status'],'ok')
        directory=config.root/'captures'/cid
        self.assertTrue((directory/(cid+'.input-ack.done.json')).exists())
        self.assertTrue((directory/(cid+'.outcome-ack.done.json')).exists())
        self.assertEqual(sorted(transport.status_calls),[(cid,'input'),(cid,'outcome')])
        self.assertEqual(transport.ack_calls,[(cid,'input'),(cid,'outcome')])


if __name__ == '__main__': unittest.main()
