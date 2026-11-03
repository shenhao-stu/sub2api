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
            complete=True, outcome=False, group=78, headers=None):
        cid = str(uuid.uuid4()); when = when or NOW - dt.timedelta(minutes=1)
        compressed = gzip.compress(raw, mtime=0)
        manifest = {'schema_version':1,'capture_id':cid,'captured_at':stamp(when),'sealed_at':stamp(when),
                    'group_id':group,'api_key_id':10,'user_id':20,'request_id':'synthetic',
                    'method':'POST','route':'/v1/chat/completions','headers':{'Content-Type':'application/json'},
                    'content_length':len(raw),'body_present':True,'payload_file':cid+'.request.gz',
                    'raw_bytes':len(raw),'raw_sha256':sha(raw),'compressed_bytes':len(compressed),
                    'compressed_sha256':sha(compressed),'body_complete':complete,
                    'read_error':'' if complete else 'context_canceled'}
        if headers is not None:
            header_raw = source.encode({'schema_version':1,'policy':'credential_names_v1',
                                       'headers':headers,'redacted_count':sum(v == ['[REDACTED]'] for v in headers.values())})
            packed = gzip.compress(header_raw,mtime=0)
            manifest.update(schema_version=2,headers_file=cid+'.headers.gz',headers_raw_bytes=len(header_raw),
                headers_raw_sha256=sha(header_raw),headers_compressed_bytes=len(packed),
                headers_compressed_sha256=sha(packed),headers_policy='credential_names_v1',
                headers_redacted_count=sum(v == ['[REDACTED]'] for v in headers.values()))
            self.put(cid+'.headers.gz',packed)
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
    def __init__(self, root, config, state_root):
        self.root=root; self.config=config; self.state_root=state_root; self.uncertain=False; self.ack_calls=[]; self.status_calls=[]

    def store(self): return source.SourceStore(self.root,os.geteuid(),NOW,state_root=self.state_root)
    def manifest_list(self):
        with self.store() as store: return store.list(self.config.max_objects,self.config.max_bytes)
    def fetch(self,name,destination,timeout,max_bytes):
        require_name = name[:36]
        source.capture_id(require_name)
        assert name in {require_name+s for s in ['.input.json','.request.gz','.headers.gz','.outcome.json']}
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
        self.state_parent=self.base/'root-control'; self.state_parent.mkdir(mode=0o700)
        self.state_root=self.state_parent/'ack-state'

    def test_scan_budget_counts_captures_not_schema2_companion_files(self):
        ids=[self.producer.add(when=NOW-dt.timedelta(days=2), outcome=True,
                              headers={'X-Request-Id':['one','two']}) for _ in range(2)]
        with mock.patch.object(source,'MAX_SCAN',2):
            self.assertEqual({x['capture_id'] for x in self.listing()['items']},set(ids))
            for cid in ids:
                self.ack(cid,'input');self.ack(cid,'outcome')
            with self.store() as store:result=store.gc()
            self.assertEqual(result['deleted'],2)

    def test_capture_count_and_unknown_file_flood_remain_bounded(self):
        for _ in range(3):self.producer.add(outcome=True,headers={})
        with mock.patch.object(source,'MAX_SCAN',2),self.assertRaises(source.HelperError):
            self.listing()
        for p in self.root.iterdir():p.unlink()
        for i in range(17):self.producer.put('unknown-'+str(i),b'x')
        with mock.patch.object(source,'MAX_SCAN',2):
            with self.assertRaises(source.HelperError):self.listing()
            with self.store() as store,self.assertRaises(source.HelperError):store.gc()

    def tearDown(self): self.temp.cleanup()
    def store(self,now=NOW): return source.SourceStore(self.root,os.geteuid(),now,state_root=self.state_root)
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

    def test_v2_roundtrip_with_real_helper_header_bytes_and_late_outcome(self):
        headers={'Host':['example.invalid'],'X-Fingerprint':['one','two'],
                 'Authorization':['[REDACTED]'],'X-Goog-Api-Key':['[REDACTED]'],
                 'X-Codex-Turn-Metadata':['full synthetic diagnostic'],
                 'X-Tokenize':['keep'],'Secretariat':['keep'],'Monkey':['keep']}
        cid=self.producer.add(raw=b'raw entity\x00\xff',headers=headers)
        item=self.listing()['items'][0]
        packed=(self.root/(cid+'.headers.gz')).read_bytes()
        self.assertEqual(item['headers'],{'path':cid+'.headers.gz','bytes':len(packed),'sha256':sha(packed)})
        config=nas.Config(root=self.base/'netcup-grok-test',group_id=78,source_dir=str(self.root),
                          helper_path=str(source.INSTALL_PATH),min_free_bytes=0)
        transport=LocalTransport(self.root,config,self.state_root)
        first=nas.Puller(config,transport,NOW).run();self.assertEqual(first['status'],'ok')
        self.assertEqual(transport.ack_calls,[(cid,'input')])
        directory=config.root/'captures'/cid
        self.assertEqual(json.loads(gzip.decompress((directory/(cid+'.headers.gz')).read_bytes()))['headers'],headers)
        receipt=json.loads((directory/(cid+'.input-receipt.json')).read_bytes())
        self.assertEqual(receipt['schema_version'],2);self.assertEqual(receipt['headers_compressed_sha256'],sha(packed))
        self.producer.finish(cid,released=False)
        second=nas.Puller(config,transport,NOW).run();self.assertEqual(second['status'],'ok')
        self.assertEqual(transport.ack_calls,[(cid,'input'),(cid,'outcome')])

    def test_v2_sensitive_header_policy_never_lists_or_acknowledges_values(self):
        for name in ['Authorization','Cookie','Set-Cookie','X-Api-Key','X-Jwt','X-Authentication',
                     'X-Session','X-Csrf','X-Xsrf','X-Credentials','Proxy-Authorization','X-Signature']:
            with self.subTest(name=name):
                cid=self.producer.add(headers={name:['synthetic forbidden']})
                listing=self.listing();self.assertNotIn(cid,[i['capture_id']for i in listing['items']])
                self.assertIn('unredacted_credential_header',listing['rejected'])
                with self.assertRaises(source.HelperError):self.ack(cid,'input')
                self.assertNotIn('synthetic forbidden',json.dumps(listing))

    def replace_headers(self,cid,raw):
        packed=gzip.compress(raw,mtime=0);self.producer.put(cid+'.headers.gz',packed)
        self.producer.change(cid,lambda v:v.update(headers_raw_bytes=len(raw),headers_raw_sha256=sha(raw),
                             headers_compressed_bytes=len(packed),headers_compressed_sha256=sha(packed)))

    def test_v2_missing_bad_gzip_raw_hash_size_count_and_policy_never_ack(self):
        for variant in ['missing','compressed','gzip','rawhash','expansion','count','policy','oversize','path']:
            with self.subTest(variant=variant):
                cid=self.producer.add(headers={'X-Trace':['one','two']});path=self.root/(cid+'.headers.gz')
                if variant=='missing':path.unlink()
                elif variant=='compressed':path.write_bytes(path.read_bytes()+b'changed')
                elif variant=='gzip':
                    path.write_bytes(b'not gzip')
                    self.producer.change(cid,lambda v:v.update(headers_compressed_bytes=8,headers_compressed_sha256=sha(b'not gzip')))
                elif variant=='rawhash':self.producer.change(cid,lambda v:v.update(headers_raw_sha256='0'*64))
                elif variant=='expansion':self.producer.change(cid,lambda v:v.update(headers_raw_bytes=1))
                elif variant=='count':self.producer.change(cid,lambda v:v.update(headers_redacted_count=1))
                elif variant=='policy':self.producer.change(cid,lambda v:v.update(headers_policy='unknown'))
                elif variant=='oversize':self.producer.change(cid,lambda v:v.update(headers_raw_bytes=source.MAX_HEADERS_RAW+1))
                else:self.producer.change(cid,lambda v:v.update(headers_file='../escape.headers.gz'))
                self.assertNotIn(cid,[x['capture_id']for x in self.listing()['items']])
                with self.assertRaises(source.HelperError):self.ack(cid,'input')
                with self.store()as store:self.assertFalse(store.ack_status(cid,'input')['accepted'])

    def test_v2_header_symlink_and_hardlink_never_list_or_ack(self):
        for link in ['symlink','hardlink']:
            cid=self.producer.add(headers={'X-Trace':['synthetic']});path=self.root/(cid+'.headers.gz')
            outside=self.base/uuid.uuid4().hex;outside.write_bytes(path.read_bytes());outside.chmod(0o600);path.unlink()
            if link=='symlink':path.symlink_to(outside)
            else:os.link(outside,path)
            self.assertNotIn(cid,[x['capture_id']for x in self.listing()['items']])
            with self.assertRaises(source.HelperError):self.ack(cid,'input')
            self.assertTrue(outside.exists())

    def test_v2_header_json_duplicate_keys_invalid_names_and_values_are_rejected(self):
        bad=[b'{"schema_version":1,"policy":"credential_names_v1","headers":{"X-Trace":["a"],"X-Trace":["b"]},"redacted_count":0}\n',
             source.encode({'schema_version':1,'policy':'credential_names_v1','headers':{'X-Trace':['a\n']},'redacted_count':0}),
             source.encode({'schema_version':1,'policy':'credential_names_v1','headers':{'x-trace':['a']},'redacted_count':0})]
        for raw in bad:
            cid=self.producer.add(headers={'X-Trace':['a']});self.replace_headers(cid,raw)
            self.assertNotIn(cid,[x['capture_id']for x in self.listing()['items']])
            with self.assertRaises(source.HelperError):self.ack(cid,'input')

    def test_v2_raw_body_must_verify_before_source_input_ack(self):
        cid=self.producer.add(headers={'X-Trace':['a']})
        self.producer.change(cid,lambda v:v.update(raw_sha256='0'*64))
        with self.assertRaisesRegex(source.HelperError,'payload_raw_hash_or_size_mismatch'):self.ack(cid,'input')

    def test_v2_list_and_gc_bytes_include_headers_and_keep_v1_extra_headers(self):
        old=self.producer.add(when=NOW-dt.timedelta(days=2),outcome=True,headers={'X-Trace':['a']})
        item=self.listing()['items'][0];size=sum(item[k]['bytes']for k in ['input','payload','headers','outcome'])
        self.assertEqual(self.listing(size=size-1)['items'],[])
        self.both(old);self.assertEqual(self.gc(max_bytes=size-1)['deleted'],0)
        result=self.gc(max_bytes=size);self.assertEqual(result['deleted'],1);self.assertEqual(result['deleted_bytes'],size)
        self.assertFalse((self.root/(old+'.headers.gz')).exists())
        with self.store()as store:self.assertTrue(store.ack_status(old,'input')['accepted'])
        legacy=self.producer.add(when=NOW-dt.timedelta(days=2),outcome=True);self.both(legacy)
        self.producer.put(legacy+'.headers.gz',b'unknown v1 sidecar')
        self.assertEqual(self.gc()['deleted'],0);self.assertTrue((self.root/(legacy+'.headers.gz')).exists())

    def test_v2_partial_active_unacked_and_unknown_files_preserved(self):
        when=NOW-dt.timedelta(days=2)
        active=self.producer.add(when=when,headers={});self.ack(active,'input')
        unacked=self.producer.add(when=when,outcome=True,headers={})
        partial=self.producer.add(when=when,outcome=True,headers={});self.both(partial)
        self.producer.put(partial+'.headers.gz.partial',b'preserve')
        terminal=self.producer.add(when=when,outcome=True,headers={},complete=False);self.both(terminal)
        self.assertEqual(self.gc()['deleted'],1)
        for cid in [active,unacked,partial]:self.assertTrue((self.root/(cid+'.headers.gz')).exists())

    def test_v2_source_gc_rechecks_header_identity_before_unlink(self):
        cid=self.producer.add(when=NOW-dt.timedelta(days=2),outcome=True,headers={});self.both(cid)
        with self.store()as store:
            original=store.headers_descriptor
            def changed(capture,manifest):
                result=original(capture,manifest);self.producer.put(capture+'.headers.gz',b'changed');return result
            with mock.patch.object(store,'headers_descriptor',side_effect=changed):result=store.gc()
        self.assertEqual(result['deleted'],0);self.assertIn('gc_source_changed',result['rejected'])
        self.assertTrue((self.root/(cid+'.input.json')).exists())

    def test_producer_chown_damaged_legacy_state_cannot_block_external_ack_namespace(self):
        cid=self.producer.add(outcome=True)
        legacy=self.root/'.archive-helper';legacy.mkdir(mode=0o700)
        (legacy/'helper.lock').write_bytes(b'');legacy.chmod(0o777)
        # The producer's recursive ownership/mode changes must affect only its
        # data tree. A damaged retired state directory there is not authoritative.
        self.assertEqual([item['capture_id']for item in self.listing()['items']],[cid])
        first=self.ack(cid,'input');self.assertTrue(first['accepted'])
        self.assertEqual((legacy/'helper.lock').read_bytes(),b'')

    def test_cli_rejects_foreign_group_before_opening_source_and_has_no_path_override(self):
        script=Path(source.__file__)
        result=subprocess.run([sys.executable,str(script),'manifest-list','--group-id','79','--max-objects','1','--max-bytes','1000'],capture_output=True,text=True)
        self.assertEqual(result.returncode,1); self.assertEqual(json.loads(result.stdout)['code'],'wrong_group')
        result=subprocess.run([sys.executable,str(script),'gc','--group-id','78','--source-dir',str(self.root)],capture_output=True,text=True)
        self.assertEqual(result.returncode,2)
        result=subprocess.run([sys.executable,str(script),'gc','--group-id','78','--state-root',str(self.state_root)],capture_output=True,text=True)
        self.assertEqual(result.returncode,2)

    def test_bad_legacy_ack_and_symlink_are_preserved_but_never_authoritative(self):
        cid=self.producer.add(outcome=True);outside=self.base/'legacy-outside';outside.mkdir(mode=0o700)
        (outside/(cid+'.input.ack.json')).write_bytes(b'invalid retired ack')
        legacy=self.root/'.archive-helper';legacy.symlink_to(outside,target_is_directory=True)
        self.assertEqual([x['capture_id']for x in self.listing()['items']],[cid])
        self.both(cid)
        self.assertEqual((outside/(cid+'.input.ack.json')).read_bytes(),b'invalid retired ack')
        self.assertTrue(legacy.is_symlink())

    def test_recursive_producer_tree_chown_does_not_touch_external_ack_tombstones(self):
        cid=self.producer.add(outcome=True);expected=self.both(cid)
        ackpath=self.state_root/(cid+'.input.ack.json');before=(ackpath.stat().st_ino,ackpath.stat().st_uid,ackpath.read_bytes())
        # Run the same ownership operation over the producer tree with the
        # fixture UID, which is available without administrator permissions.
        for path in [self.root,*self.root.rglob('*')]:os.chown(path,os.geteuid(),os.getegid())
        after=(ackpath.stat().st_ino,ackpath.stat().st_uid,ackpath.read_bytes())
        self.assertEqual(after,before)
        with self.store()as store:
            self.assertEqual(store.ack_status(cid,'input'),expected[0]);self.assertEqual(store.ack_status(cid,'outcome'),expected[1])

    def test_external_ack_parent_must_be_private_and_cannot_be_under_source(self):
        cid=self.producer.add();self.state_parent.chmod(0o777)
        with self.assertRaises(source.HelperError):self.ack(cid,'input')
        self.state_parent.chmod(0o700)
        with source.SourceStore(self.root,os.geteuid(),NOW,state_root=self.root/'ack-state')as store:
            with self.assertRaises(source.HelperError):store.ack_status(cid,'input')

    def test_ack_parent_symlink_is_not_followed(self):
        cid=self.producer.add();linked=self.base/'linked-control';linked.symlink_to(self.state_parent,target_is_directory=True)
        with source.SourceStore(self.root,os.geteuid(),NOW,state_root=linked/'ack-state')as store:
            with self.assertRaises(OSError):store.ack_status(cid,'input')
        self.assertFalse(self.state_root.exists())

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
        ack=self.ack(cid,'input'); path=self.state_root/(cid+'.input.ack.json'); original=path.read_bytes()
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
        self.assertTrue(list(self.state_root.glob('*.pending-*')))
        self.assertTrue((self.root/(cid+'.input.json')).exists())

    def test_directory_fsync_failure_cannot_be_confirmed_until_durability_recovers(self):
        cid=self.producer.add(); real=os.fsync
        def fail_ack_directory(fd):
            state=self.state_root; info=os.fstat(fd)
            if state.exists() and stat.S_ISDIR(info.st_mode) and info.st_ino==state.stat().st_ino:
                raise OSError('synthetic ACK directory fsync failure')
            return real(fd)
        with mock.patch.object(source.os,'fsync',side_effect=fail_ack_directory):
            with self.assertRaises(OSError): self.ack(cid,'input')
            # Publication may already be visible; visibility alone is not commit.
            path=self.state_root/(cid+'.input.ack.json')
            self.assertTrue(path.exists()); value=json.loads(path.read_bytes())
            with self.store() as store:
                with self.assertRaises(OSError): store.ack_status(cid,'input')
                with self.assertRaises(OSError): store.ack(cid,'input',value['object_sha256'],value['receipt_sha256'])
        with self.store() as store:
            self.assertEqual(store.ack_status(cid,'input'),value)
            self.assertEqual(store.ack(cid,'input',value['object_sha256'],value['receipt_sha256']),value)

    def test_external_ack_parent_fsync_failure_is_not_settled_by_source_fsync(self):
        cid=self.producer.add();real=os.fsync;parent_inode=self.state_parent.stat().st_ino
        ackpath=self.state_root/(cid+'.input.ack.json')
        def fail_ack_parent_after_publish(fd):
            info=os.fstat(fd)
            if stat.S_ISDIR(info.st_mode)and info.st_ino==parent_inode and ackpath.exists():
                raise OSError('synthetic external parent fsync failure')
            return real(fd)
        with mock.patch.object(source.os,'fsync',side_effect=fail_ack_parent_after_publish):
            with self.assertRaises(OSError):self.ack(cid,'input')
            self.assertTrue(ackpath.exists())
            with self.store()as store:
                with self.assertRaises(OSError):store.ack_status(cid,'input')
        with self.store()as store:self.assertTrue(store.ack_status(cid,'input')['accepted'])

    def test_symlink_ack_state_and_hardlinked_ack_are_rejected(self):
        cid=self.producer.add(); outside=self.base/'outside'; outside.mkdir(mode=0o700)
        self.state_root.symlink_to(outside,target_is_directory=True)
        with self.assertRaises(source.HelperError): self.ack(cid,'input')
        self.assertEqual(list(outside.iterdir()),[])
        self.state_root.unlink(); self.ack(cid,'input')
        ackpath=self.state_root/(cid+'.input.ack.json'); os.link(ackpath,outside/'ack')
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
        transport=LocalTransport(self.root,config,self.state_root)
        first=nas.Puller(config,transport,NOW).run(); self.assertEqual(first['status'],'ok')
        self.assertEqual(transport.ack_calls,[(cid,'input')])
        self.producer.finish(cid,released=False)
        second=nas.Puller(config,transport,NOW).run(); self.assertEqual(second['outcome_archived'],1)
        self.assertEqual(transport.ack_calls,[(cid,'input'),(cid,'outcome')])

    def test_nas_reconciles_ack_tombstones_after_source_gc_removed_listing(self):
        cid=self.producer.add(when=NOW-dt.timedelta(days=2),outcome=True)
        config=nas.Config(root=self.base/'netcup-grok-test',group_id=78,source_dir=str(self.root),
                          helper_path=str(source.INSTALL_PATH),min_free_bytes=0)
        transport=LocalTransport(self.root,config,self.state_root); transport.uncertain=True
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
