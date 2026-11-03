"""Real full-header/body durability under batched transport and uncertain ACKs."""
import gzip,json,os,shutil,tempfile
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
