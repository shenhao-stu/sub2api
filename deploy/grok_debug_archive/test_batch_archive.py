"""Real full-header/body durability under batched transport and uncertain ACKs."""
import gzip,json,os,shutil,stat,tempfile
from dataclasses import replace
from pathlib import Path
from unittest import mock
import nas_archive as nas
import source_helper as source
from test_source_helper import SourceBehavior,LocalTransport,NOW

class BatchTransport(LocalTransport):
    def __init__(self,*args):super().__init__(*args);self.batches=0;self.ack_batches=0;self.unknown=False;self.corrupt=False
    def fetch_batch(self,descriptors,directory,timeout):
        self.batches+=1;target=directory/'files';target.mkdir(mode=0o700)
        for d in descriptors:
            shutil.copyfile(self.root/d['path'],target/d['path']);(target/d['path']).chmod(0o600)
            if self.corrupt and d['path'].endswith('.request.gz'):(target/d['path']).write_bytes(b'bad')
        return target
    def ack_batch(self,claims):
        self.ack_batches+=1
        with self.store()as s:result=s.ack_batch(claims)
        if self.unknown:raise nas.ArchiveError('transport_timeout')
        return result
    def gc_confirmed(self,ids):
        with self.store()as s:return s.gc(1000,1024**3,capture_ids=ids)

class BatchArchive(SourceBehavior):
    def config(self):
        return nas.Config(root=self.base/'netcup-grok-test',group_id=78,source_dir=str(self.root),helper_path=str(source.INSTALL_PATH),min_free_bytes=0,max_objects=1000,workers=8,max_seconds=290)
    def claims(self,ids):
        claims=[]
        with self.store()as store:
            for cid in ids:
                manifest,inp,_=store.input(cid);outcome,_=store.outcome(cid,manifest)
                for kind,desc in [('input',inp),('outcome',outcome)]:
                    claims.append({'capture_id':cid,'kind':kind,'object_sha256':desc['sha256'],'receipt_sha256':'1'*64})
        return claims
    def body_raw_checks(self,spy):
        return [call.kwargs.get('verify_raw',False)for call in spy.call_args_list if not call.kwargs.get('headers',False)]

    def test_same_batch_reuses_only_raw_body_check_and_revalidates_headers(self):
        cid=self.producer.add(raw=b'x'*(6*1024**2),outcome=True,headers={'Authorization':['[REDACTED]']})
        claims=self.claims([cid])
        with self.store()as store,mock.patch.object(store,'compressed_descriptor',wraps=store.compressed_descriptor)as checked,mock.patch.object(source,'validate_headers',wraps=source.validate_headers)as privacy:
            result=store.ack_batch(claims)
        self.assertTrue(all(x['accepted']for x in result['results']))
        self.assertEqual(self.body_raw_checks(checked),[True,False])
        self.assertEqual(privacy.call_count,2)

    def test_raw_check_cache_does_not_cross_calls_or_use_old_ack_attestation(self):
        cid=self.producer.add(outcome=True,headers={});claims=self.claims([cid])
        with self.store()as store,mock.patch.object(store,'compressed_descriptor',wraps=store.compressed_descriptor)as checked:
            store.ack_batch(claims[:1])
            result=store.ack_batch(claims)
        self.assertTrue(all(x['accepted']for x in result['results']))
        self.assertEqual(self.body_raw_checks(checked),[True,True])

    def test_standalone_acks_each_revalidate_raw_body(self):
        cid=self.producer.add(outcome=True,headers={});claims=self.claims([cid])
        with self.store()as store,mock.patch.object(store,'compressed_descriptor',wraps=store.compressed_descriptor)as checked:
            for claim in claims:store.ack(claim['capture_id'],claim['kind'],claim['object_sha256'],claim['receipt_sha256'])
        self.assertEqual(self.body_raw_checks(checked),[True,True])

    def test_second_claim_rechecks_changed_manifest_payload_and_headers(self):
        for changed,expected in [('manifest','payload_raw_hash_or_size_mismatch'),('payload','payload_hash_mismatch'),('headers','headers_hash_mismatch')]:
            with self.subTest(changed=changed):
                cid=self.producer.add(outcome=True,headers={});claims=self.claims([cid])
                with self.store()as store:
                    original=store.ack_locked;calls=[0]
                    def mutate_after_first(*args,**kwargs):
                        result=original(*args,**kwargs);calls[0]+=1
                        if calls[0]==1:
                            if changed=='manifest':self.producer.change(cid,lambda value:value.update(raw_sha256='0'*64))
                            else:
                                path=self.root/(cid+('.request.gz' if changed=='payload' else '.headers.gz'))
                                raw=path.read_bytes();self.producer.put(path.name,raw[:-1]+bytes([raw[-1]^1]))
                        return result
                    with mock.patch.object(store,'ack_locked',side_effect=mutate_after_first),mock.patch.object(store,'compressed_descriptor',wraps=store.compressed_descriptor)as checked:
                        result=store.ack_batch(claims)
                self.assertEqual([x['accepted']for x in result['results']],[True,False])
                self.assertEqual(result['results'][1]['code'],expected)
                self.assertEqual(self.body_raw_checks(checked),[True,changed=='manifest'])
                self.assertFalse((self.state_root/(cid+'.outcome.ack.json')).exists())
                self.assertEqual(self.gc(capture_ids=[cid])['deleted'],0)

    def test_first_raw_validation_failure_cannot_seed_batch_cache(self):
        cid=self.producer.add(outcome=True,headers={})
        self.producer.change(cid,lambda value:value.update(raw_sha256='0'*64));claims=self.claims([cid])
        with self.store()as store,mock.patch.object(store,'compressed_descriptor',wraps=store.compressed_descriptor)as checked:
            result=store.ack_batch(claims)
        self.assertEqual([x['accepted']for x in result['results']],[False,False])
        self.assertEqual(self.body_raw_checks(checked),[True,True])
        self.assertFalse(any(self.state_root.glob('*.ack.json')))

    def test_ack_batch_one_lock_and_three_directory_barriers(self):
        ids=[self.producer.add(outcome=True,headers={})for _ in range(4)]
        claims=self.claims(ids);real=os.fsync
        for retry in [False,True]:
            counts={'file':0,'state':0,'parent':0}
            def sync(fd):
                info=os.fstat(fd)
                key=('file' if stat.S_ISREG(info.st_mode) else
                     'parent' if info.st_ino==self.state_parent.stat().st_ino else 'state')
                counts[key]+=1;return real(fd)
            with self.store()as store,mock.patch.object(store,'locked',wraps=store.locked)as lock,mock.patch.object(source.os,'fsync',side_effect=sync):
                result=store.ack_batch(claims)
            self.assertTrue(all(x['accepted']for x in result['results']))
            self.assertEqual(lock.call_count,1)
            self.assertEqual(counts,{'file':0 if retry else len(claims),'state':1,'parent':2})

    def test_ack_batch_file_sync_failure_aborts_reply_then_recovers(self):
        cid=self.producer.add(outcome=True,headers={});claims=self.claims([cid]);real=os.fsync;files=[0]
        def fail_second_file(fd):
            if stat.S_ISREG(os.fstat(fd).st_mode):
                files[0]+=1
                if files[0]==2:raise OSError('synthetic ACK file fsync failure')
            return real(fd)
        with self.store()as store,mock.patch.object(source.os,'fsync',side_effect=fail_second_file):
            with self.assertRaises(OSError):store.ack_batch(claims)
        self.assertTrue((self.root/(cid+'.input.json')).exists())
        self.assertFalse((self.state_root/(cid+'.outcome.ack.json')).exists())
        with self.store()as store:result=store.ack_batch(claims)
        self.assertTrue(all(x['accepted']for x in result['results']))
        self.assertEqual(self.gc(capture_ids=[cid])['deleted'],1)

    def test_ack_batch_final_directory_failure_cannot_confirm_or_collect(self):
        real=os.fsync
        for target in ['state','parent']:
            with self.subTest(target=target):
                cid=self.producer.add(outcome=True,headers={});claims=self.claims([cid])
                def fail_final_directory(fd):
                    info=os.fstat(fd)
                    directory=self.state_root if target=='state' else self.state_parent
                    if (stat.S_ISDIR(info.st_mode) and directory.exists() and info.st_ino==directory.stat().st_ino
                            and (self.state_root/(cid+'.input.ack.json')).exists()):
                        raise OSError('synthetic ACK directory fsync failure')
                    return real(fd)
                with self.store()as store,mock.patch.object(source.os,'fsync',side_effect=fail_final_directory):
                    with self.assertRaises(OSError):store.ack_batch(claims)
                    with self.assertRaises(OSError):store.ack_status(cid,'input')
                    with self.assertRaises(OSError):store.gc(capture_ids=[cid])
                self.assertTrue(all((self.root/(cid+s)).exists()for s in ['.input.json','.request.gz','.headers.gz','.outcome.json']))
                with self.store()as store:result=store.ack_batch(claims)
                self.assertTrue(all(x['accepted']for x in result['results']))
                self.assertEqual(self.gc(capture_ids=[cid])['deleted'],1)

    def test_ack_batch_commits_valid_claims_without_accepting_conflicts(self):
        ids=[self.producer.add(outcome=True)for _ in range(2)];claims=self.claims(ids)
        claims[1]['object_sha256']='0'*64
        with self.store()as store:result=store.ack_batch(claims)
        self.assertEqual([x['accepted']for x in result['results']],[True,False,True,True])
        self.assertEqual(result['results'][1]['code'],'ack_object_mismatch')
        result=self.gc(capture_ids=ids)
        self.assertEqual(result['deleted'],1);self.assertTrue((self.root/(ids[0]+'.input.json')).exists())

    def test_gc_ack_barrier_once_and_ready_marker_barrier_per_capture(self):
        ids=[self.producer.add(outcome=True,headers={})for _ in range(3)]
        for cid in ids:self.both(cid)
        events=[];real_sync=os.fsync;real_unlink=os.unlink
        source_inode=self.root.stat().st_ino
        def sync(fd):
            label='source' if os.fstat(fd).st_ino==source_inode else 'ack'
            events.append(('sync',label));return real_sync(fd)
        def unlink(name,**kwargs):
            events.append(('unlink',name));return real_unlink(name,**kwargs)
        with self.store()as store,mock.patch.object(source.os,'fsync',side_effect=sync),mock.patch.object(source.os,'unlink',side_effect=unlink):
            result=store.gc(capture_ids=ids)
        self.assertEqual(result['deleted'],3)
        self.assertEqual(events[:3],[('sync','ack')]*3)
        self.assertEqual(sum(event[0]=='sync' for event in events),2*len(ids)+3)
        for cid in ids:
            start=events.index(('unlink',cid+'.input.json'))
            self.assertEqual(events[start+1],('sync','source'))
            self.assertEqual(events[start+2:start+5],[('unlink',cid+s)for s in ['.request.gz','.outcome.json','.headers.gz']])
            self.assertEqual(events[start+5],('sync','source'))

    def test_gc_ready_marker_sync_failure_keeps_companion_files(self):
        cid=self.producer.add(outcome=True,headers={});self.both(cid);real=os.fsync
        def fail_source(fd):
            if os.fstat(fd).st_ino==self.root.stat().st_ino:raise OSError('synthetic ready marker fsync failure')
            return real(fd)
        with self.store()as store,mock.patch.object(source.os,'fsync',side_effect=fail_source):
            with self.assertRaises(OSError):store.gc(capture_ids=[cid])
        self.assertFalse((self.root/(cid+'.input.json')).exists())
        self.assertTrue(all((self.root/(cid+s)).exists()for s in ['.request.gz','.headers.gz','.outcome.json']))

    def test_500_complete_batched_archives_and_exact_gc(self):
        headers={'Authorization':['[REDACTED]'],'X-Trace':['one','two']}
        ids=[self.producer.add(raw=('full-body-'+str(i)).encode(),outcome=True,headers=headers)for i in range(500)]
        cfg=self.config();transport=BatchTransport(self.root,cfg,self.state_root)
        result=nas.Puller(cfg,transport,NOW).run()
        self.assertEqual(result['errors'],[]);self.assertEqual(result['source_deleted'],500)
        self.assertEqual(result['acks_confirmed'],1000);self.assertEqual(transport.batches,4);self.assertEqual(transport.ack_batches,16)
        for i,cid in enumerate(ids):
            d=cfg.root/'captures'/cid
            self.assertEqual(gzip.decompress((d/(cid+'.request.gz')).read_bytes()),('full-body-'+str(i)).encode())
            self.assertEqual(json.loads(gzip.decompress((d/(cid+'.headers.gz')).read_bytes()))['headers'],headers)
    def test_unknown_batch_ack_keeps_source_then_exact_recovery(self):
        cid=self.producer.add(outcome=True,headers={'X-Trace':['persist']});cfg=self.config();t=BatchTransport(self.root,cfg,self.state_root);t.unknown=True
        first=nas.Puller(cfg,t,NOW).run();self.assertEqual(first['source_deleted'],0)
        self.assertTrue((self.root/(cid+'.input.json')).exists());t.unknown=False
        second=nas.Puller(cfg,t,NOW).run();self.assertEqual(second['source_deleted'],1)
    def test_corrupt_body_never_acknowledged_or_collected(self):
        cid=self.producer.add(outcome=True);cfg=self.config();t=BatchTransport(self.root,cfg,self.state_root);t.corrupt=True
        r=nas.Puller(cfg,t,NOW).run();self.assertTrue(r['errors']);self.assertEqual(t.ack_batches,0);self.assertEqual(r['source_deleted'],0)
    def test_duplicate_and_oversized_ack_batch_rejected_before_writes(self):
        cid=self.producer.add(outcome=True)
        claim={'capture_id':cid,'kind':'input','object_sha256':'0'*64,'receipt_sha256':'1'*64}
        for claims in [[claim,claim],[claim]*65]:
            with self.store()as s:
                with self.assertRaises(source.HelperError):s.ack_batch(claims)
                self.assertFalse(s.ack_status(cid,'input')['accepted'])
    def test_rsync_is_bounded_no_delete_or_source_removal(self):
        cid=self.producer.add();cfg=replace(self.config(),root=Path('/tmp/getoken-batch-test'));t=nas.SSHTransport(cfg);name=cid+'.request.gz';raw=(self.root/name).read_bytes();desc={'path':name,'bytes':len(raw)}
        with tempfile.TemporaryDirectory(dir=self.base)as d:
            def run(args,**kwargs):
                self.assertNotIn('--delete',args);self.assertNotIn('--remove-source-files',args);self.assertNotIn('-L',args)
                self.assertEqual(kwargs['file_size_limit'],len(raw));target=Path(d)/'files'/name;target.write_bytes(raw);target.chmod(0o600);kwargs['guard']()
            with mock.patch.object(t,'run',side_effect=run):self.assertEqual((t.fetch_batch([desc],Path(d),10)/name).read_bytes(),raw)
        with tempfile.TemporaryDirectory(dir=self.base)as d:
            def malicious(args,**kwargs):
                target=Path(d)/'files'/name;target.write_bytes(raw+b'x');kwargs['guard']()
            with mock.patch.object(t,'run',side_effect=malicious):
                with self.assertRaises(nas.ArchiveError):t.fetch_batch([desc],Path(d),10)
    def test_real_cli_and_transport_envelope_roundtrip(self):
        import io,contextlib
        cid=self.producer.add(outcome=True);cfg=replace(self.config(),root=Path('/tmp/getoken-batch-test'))
        with self.store()as store:
            _,desc,_=store.input(cid)
            claim={'capture_id':cid,'kind':'input','object_sha256':desc['sha256'],'receipt_sha256':'1'*64}
            t=nas.SSHTransport(cfg)
            def helper(action,*args):
                output=io.BytesIO();writer=io.TextIOWrapper(output)
                original_uid=os.geteuid;calls=[0]
                def root_entry_only():
                    calls[0]+=1;return 0 if calls[0]==1 else original_uid()
                with mock.patch.object(source,'SourceStore',return_value=contextlib.nullcontext(store)),mock.patch.object(source.os,'geteuid',side_effect=root_entry_only),mock.patch.object(source.sys,'stdout',writer):
                    code=source.main([action,'--group-id','78',*args]);writer.flush();result=json.loads(output.getvalue())
                self.assertEqual(code,0);return result
            with mock.patch.object(t,'helper',side_effect=helper):
                result=t.ack_batch([claim]);self.assertTrue(result['results'][0]['accepted'])
