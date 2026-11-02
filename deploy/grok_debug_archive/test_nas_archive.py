"""Behavior tests with real files/gzip and a source-side protocol fixture."""
import copy
import datetime as dt
import gzip
import fcntl
import hashlib
import json
import os
from pathlib import Path
import shutil
import tempfile
import unittest
import uuid

from nas_archive import ArchiveError, Config, Puller, SSHTransport, encode

NOW=dt.datetime(2026,11,2,16,0,tzinfo=dt.timezone.utc)


def sha(raw): return hashlib.sha256(raw).hexdigest()
def stamp(value): return value.isoformat().replace("+00:00","Z")


class SourceFixture:
    """Models a remote immutable capture store, not the puller's implementation."""
    def __init__(self,group=123):
        self.group=group; self.objects={}; self.items=[]; self.acks={}; self.ack_calls=[]
        self.fetch_calls=[]; self.status_calls=[]; self.fail_fetch=None; self.uncertain=None
        self.list_unreachable=False; self.on_ack=None

    def descriptor(self,name):
        raw=self.objects[name]
        return {"path":name,"bytes":len(raw),"sha256":sha(raw)}

    def add(self,raw=b'{"messages":[{"role":"user","content":"synthetic test"}]}',
            captured=None,complete=True,outcome=False,headers=None):
        cid=str(uuid.uuid4()); when=captured or NOW-dt.timedelta(minutes=1)
        payload=gzip.compress(raw,mtime=0)
        manifest={"schema_version":1,"capture_id":cid,"captured_at":stamp(when),"sealed_at":stamp(when),
                  "group_id":self.group,"api_key_id":10,"user_id":20,"request_id":"synthetic",
                  "method":"POST","route":"/v1/chat/completions","headers":{"Content-Type":"application/json"},
                  "content_length":len(raw),"body_present":True,"payload_file":cid+".request.gz",
                  "raw_bytes":len(raw),"raw_sha256":sha(raw),"compressed_bytes":len(payload),
                  "compressed_sha256":sha(payload),"body_complete":complete,"read_error":"" if complete else "read_error"}
        self.objects[cid+".request.gz"]=payload; self.objects[cid+".input.json"]=encode(manifest)
        item={"capture_id":cid,"input":self.descriptor(cid+".input.json"),
              "payload":self.descriptor(cid+".request.gz"),"outcome":None}
        if headers is not None:
            header_raw=encode({"schema_version":1,"policy":"credential_names_v1","headers":headers,
                               "redacted_count":sum(v==["[REDACTED]"] for v in headers.values())})
            packed=gzip.compress(header_raw,mtime=0);self.objects[cid+".headers.gz"]=packed
            manifest.update(schema_version=2,headers_file=cid+".headers.gz",headers_raw_bytes=len(header_raw),
                headers_raw_sha256=sha(header_raw),headers_compressed_bytes=len(packed),
                headers_compressed_sha256=sha(packed),headers_policy="credential_names_v1",
                headers_redacted_count=sum(v==["[REDACTED]"] for v in headers.values()))
            self.objects[cid+".input.json"]=encode(manifest)
            item["input"]=self.descriptor(cid+".input.json");item["headers"]=self.descriptor(cid+".headers.gz")
        self.items.append(item)
        if outcome: self.finish(cid,when+dt.timedelta(seconds=5),released=complete)
        return cid

    def finish(self,cid,when=None,released=True):
        value={"schema_version":1,"capture_id":cid,"finished_at":stamp(when or NOW),
               "http_status":200 if released else 400,"context_canceled":False,
               "error_code":"" if released else "ingest_failed","request_released":released}
        self.objects[cid+".outcome.json"]=encode(value)
        for item in self.items:
            if item["capture_id"]==cid: item["outcome"]=self.descriptor(cid+".outcome.json")

    def manifest_list(self):
        if self.list_unreachable: raise ArchiveError("source_unreachable")
        return {"schema_version":1,"group_id":self.group,"items":copy.deepcopy(self.items)}

    def fetch(self,name,destination,timeout,max_bytes):
        self.fetch_calls.append(name)
        if self.fail_fetch==name:
            destination.write_bytes(self.objects[name][:max(1,len(self.objects[name])//2)])
            raise ArchiveError("injected_disconnect")
        destination.write_bytes(self.objects[name]); destination.chmod(0o600)

    def ack(self,cid,kind,object_sha,receipt_sha):
        self.ack_calls.append((cid,kind))
        if self.on_ack: self.on_ack(cid,kind)
        value={"schema_version":1,"group_id":self.group,"capture_id":cid,"kind":kind,
               "object_sha256":object_sha,"receipt_sha256":receipt_sha,"accepted":True}
        if self.uncertain=="not_applied": raise ArchiveError("injected_ack_disconnect")
        self.acks[(cid,kind)]=value
        if self.uncertain=="applied": raise ArchiveError("injected_ack_disconnect")
        return value

    def ack_status(self,cid,kind):
        self.status_calls.append((cid,kind))
        return self.acks.get((cid,kind),{"schema_version":1,"accepted":False})

    def gc(self):
        return {"schema_version":1,"group_id":self.group,"deleted":0,"deleted_bytes":0}


class ArchiveBehavior(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory(); self.base=Path(self.temp.name).resolve()
        self.root=self.base/"netcup-grok-test"; self.source=SourceFixture()

    def tearDown(self): self.temp.cleanup()

    def config(self,**changes):
        value={"root":self.root,"group_id":123,"source_dir":"/opt/private/new-debug",
               "helper_path":"/usr/local/libexec/grok-debug-archive-helper","min_free_bytes":0}
        value.update(changes); return Config(**value)

    def pull(self,now=NOW,**config): return Puller(self.config(**config),self.source,now).run()
    def directory(self,cid): return self.root/"captures"/cid

    def test_input_archives_and_acknowledges_before_model_outcome(self):
        cid=self.source.add()
        def durable_receipt(capture,kind):
            directory=self.directory(capture)
            self.assertEqual(kind,"input")
            self.assertTrue((directory/(capture+".input-receipt.json")).is_file())
            self.assertEqual(gzip.decompress((directory/(capture+".request.gz")).read_bytes()),
                             gzip.decompress(self.source.objects[capture+".request.gz"]))
        self.source.on_ack=durable_receipt
        result=self.pull()
        self.assertEqual(result["status"],"ok"); self.assertEqual(result["input_archived"],1)
        self.assertFalse((self.directory(cid)/(cid+".outcome.json")).exists())
        self.assertEqual(self.source.ack_calls,[(cid,"input")])
        for file in self.directory(cid).iterdir(): self.assertEqual(file.stat().st_mode&0o777,0o600)

    def change_manifest(self,cid,**changes):
        value=json.loads(self.source.objects[cid+".input.json"]);value.update(changes)
        self.source.objects[cid+".input.json"]=encode(value)
        next(i for i in self.source.items if i['capture_id']==cid)['input']=self.source.descriptor(cid+'.input.json')

    def replace_headers(self,cid,raw):
        packed=gzip.compress(raw,mtime=0);self.source.objects[cid+'.headers.gz']=packed
        self.change_manifest(cid,headers_raw_bytes=len(raw),headers_raw_sha256=sha(raw),
                             headers_compressed_bytes=len(packed),headers_compressed_sha256=sha(packed))
        next(i for i in self.source.items if i['capture_id']==cid)['headers']=self.source.descriptor(cid+'.headers.gz')

    def test_v2_both_artifacts_and_receipt_are_durable_before_input_ack(self):
        headers={'User-Agent':['synthetic/1'],'X-Fingerprint':['one','two'],'Cookie':['[REDACTED]']}
        cid=self.source.add(raw=b'body\x00\xff',headers=headers,outcome=True)
        def before_ack(capture,kind):
            if kind!='input':return
            directory=self.directory(capture)
            self.assertEqual(json.loads(gzip.decompress((directory/(cid+'.headers.gz')).read_bytes()))['headers'],headers)
            self.assertEqual(gzip.decompress((directory/(cid+'.request.gz')).read_bytes()),b'body\x00\xff')
            receipt=json.loads((directory/(cid+'.input-receipt.json')).read_bytes())
            self.assertEqual(receipt['schema_version'],2)
            for name in ['headers_file','headers_raw_bytes','headers_raw_sha256','headers_compressed_bytes',
                         'headers_compressed_sha256','headers_policy','headers_redacted_count']:
                self.assertEqual(receipt[name],json.loads(self.source.objects[cid+'.input.json'])[name])
        self.source.on_ack=before_ack
        result=self.pull();self.assertEqual(result['status'],'ok');self.assertEqual(result['acks_confirmed'],2)
        receipt=(self.directory(cid)/(cid+'.input-receipt.json')).read_bytes()
        self.pull();self.assertEqual((self.directory(cid)/(cid+'.input-receipt.json')).read_bytes(),receipt)
        self.assertEqual(self.source.ack_calls,[(cid,'input'),(cid,'outcome')])

    def test_v1_receipts_are_byte_identical_after_v2_reader_repeated_pull(self):
        cid=self.source.add(outcome=True);self.pull()
        receipt=self.directory(cid)/(cid+'.input-receipt.json')
        before=(receipt.stat().st_ino,receipt.read_bytes())
        self.source.add(headers={'X-Trace':['one','two']});self.pull()
        self.assertEqual((receipt.stat().st_ino,receipt.read_bytes()),before)
        self.assertEqual(json.loads(receipt.read_bytes())['schema_version'],1)

    def test_v2_missing_corrupt_raw_hash_descriptor_and_partial_headers_never_ack(self):
        variants=['missing','compressed','raw_hash','gzip','expansion','disconnect','descriptor','path']
        for variant in variants:
            with self.subTest(variant=variant):
                self.source=SourceFixture();cid=self.source.add(headers={'X-Trace':['one','two']})
                self.root=self.base/variant/'netcup-grok-test';self.root.parent.mkdir()
                item=self.source.items[0]
                if variant=='missing':item.pop('headers')
                elif variant=='compressed':self.source.objects[cid+'.headers.gz']+=b'changed'
                elif variant=='raw_hash':self.change_manifest(cid,headers_raw_sha256='0'*64)
                elif variant=='gzip':
                    self.source.objects[cid+'.headers.gz']=b'not gzip'
                    item['headers']=self.source.descriptor(cid+'.headers.gz')
                    self.change_manifest(cid,headers_compressed_bytes=8,headers_compressed_sha256=sha(b'not gzip'))
                elif variant=='expansion':self.change_manifest(cid,headers_raw_bytes=1)
                elif variant=='disconnect':self.source.fail_fetch=cid+'.headers.gz'
                elif variant=='descriptor':self.change_manifest(cid,headers_compressed_sha256='0'*64)
                else:item['headers']['path']=cid+'.request.gz'
                try:result=self.pull()
                except ArchiveError:result={'status':'pending'}
                self.assertEqual(result['status'],'pending');self.assertEqual(self.source.ack_calls,[])
                self.assertIn(cid+'.request.gz',self.source.objects)

    def test_v2_invalid_policy_or_sensitive_value_never_ack(self):
        variants=[{'Authorization':['synthetic forbidden']},{'Cookie':['[REDACTED]','extra']},
                  {'X-Jwt':['[redacted]']},{'X-Credentials':['not redacted']},
                  {'X-Auth':['not redacted']},{'X-Csrf':['not redacted']}]
        for headers in variants:
            cid=self.source.add(headers=headers)
        result=self.pull();self.assertEqual(self.source.ack_calls,[])
        self.assertTrue(all(e['code']=='unredacted_credential_header'for e in result['errors']))
        self.assertNotIn('synthetic forbidden',json.dumps(result))

    def test_v2_document_fields_canonical_names_controls_counts_and_policy_checked(self):
        variants=[lambda v:v.update(policy='new_policy'),lambda v:v.update(schema_version=True),
                  lambda v:v.update(redacted_count=1),lambda v:v['headers'].update({'lower-case':['a']}),
                  lambda v:v['headers'].update({'Bad Name':['a']}),lambda v:v['headers'].update({'X-Trace':['a\r\nb']}),
                  lambda v:v['headers'].update({'X-Trace':['\x7f']}),lambda v:v['headers'].update({'X-Trace':None}),
                  lambda v:v.update(extra='unexpected')]
        for change in variants:
            cid=self.source.add(headers={'X-Trace':['a']});value=json.loads(gzip.decompress(self.source.objects[cid+'.headers.gz']))
            change(value);self.replace_headers(cid,encode(value))
        result=self.pull();self.assertEqual(self.source.ack_calls,[]);self.assertEqual(len(result['errors']),len(variants))

    def test_v2_large_names_escaped_values_empty_lists_and_obs_text_are_preserved(self):
        headers={'X-A'+('a'*9999):['quote"slash\\'*10000],'X-Obs':['\t\u00ff\ufffd'],
                 'X-Empty':[],'X-Codex-Turn-Metadata':['diagnostic'],'X-Tokenize':['plain'],'Secretariat':['plain'],'Monkey':['plain']}
        cid=self.source.add(headers=headers);result=self.pull()
        self.assertEqual(result['status'],'ok');self.assertEqual(self.source.ack_calls,[(cid,'input')])
        self.assertLess(len(self.source.objects[cid+'.input.json']),65536)
        self.assertGreater(len(gzip.decompress(self.source.objects[cid+'.headers.gz'])),65536)

    def test_v2_headers_bytes_count_against_run_namespace_and_disk_budgets(self):
        cid=self.source.add(headers={'X-Large':[os.urandom(10000).hex()]})
        total=sum(d['bytes']for k,d in self.source.items[0].items()if k in ['input','payload','headers'])
        with self.assertRaises(ArchiveError):self.pull(max_bytes=total-1)
        self.assertEqual(self.source.fetch_calls,[])
        result=self.pull(max_namespace_bytes=self.config().max_payload_bytes,min_free_bytes=10**18)
        self.assertEqual(result['status'],'pending');self.assertEqual(self.source.ack_calls,[])

    def test_v2_header_transfer_is_blocked_by_namespace_budget_after_body(self):
        cid=self.source.add(headers={'X-Large':[os.urandom(10000).hex()]})
        result=self.pull(max_payload_bytes=512,max_namespace_bytes=5000)
        self.assertIn('namespace_space_limit',[e['code']for e in result['errors']])
        self.assertEqual(self.source.fetch_calls,[cid+'.input.json',cid+'.request.gz'])
        self.assertEqual(self.source.ack_calls,[])
        self.assertTrue((self.directory(cid)/(cid+'.request.gz')).exists())

    def test_v2_transport_only_accepts_exact_uuid_headers_suffix(self):
        from unittest import mock
        transport=SSHTransport(self.config(root=Path('/tmp/netcup-grok-test')));cid=str(uuid.uuid4())
        with mock.patch.object(SSHTransport,'run',return_value=b'')as run:
            transport.fetch(cid+'.headers.gz',self.base/'destination',1,1024)
            self.assertEqual(run.call_count,1)
            for name in [cid+'.headers.gz.partial',cid+'.header.gz','../'+cid+'.headers.gz',
                         cid+'.headers.gz/child',('a'*36)+'.headers.gz',cid+'.other.gz']:
                with self.assertRaises(ArchiveError):transport.fetch(name,self.base/'destination',1,1024)
            self.assertEqual(run.call_count,1)

    def test_v2_header_hardlink_or_symlink_and_missing_retention_artifact_preserved(self):
        for link in ['hardlink','symlink','missing']:
            self.source=SourceFixture();cid=self.source.add(headers={},outcome=True)
            self.root=self.base/link/'netcup-grok-test';self.root.parent.mkdir();self.pull();self.source.items=[]
            header=self.directory(cid)/(cid+'.headers.gz');outside=self.base/(link+'.outside')
            outside.write_bytes(header.read_bytes());outside.chmod(0o600);header.unlink()
            if link=='hardlink':os.link(outside,header)
            elif link=='symlink':header.symlink_to(outside)
            try:result=self.pull(now=NOW+dt.timedelta(days=31))
            except ArchiveError:result={'expired':0}
            self.assertEqual(result['expired'],0);self.assertTrue(self.directory(cid).exists());self.assertTrue(outside.exists())

    def test_v2_header_hard_limits_are_not_configurable_or_silent_truncation(self):
        for changes in [{'headers_raw_bytes':8*1024**2+1},{'headers_compressed_bytes':16*1024**2+1},
                        {'headers_redacted_count':True},{'headers_policy':'other'},{'headers_file':'../elsewhere.gz'}]:
            cid=self.source.add(headers={});self.change_manifest(cid,**changes)
        result=self.pull();self.assertEqual(self.source.ack_calls,[])
        self.assertGreaterEqual(len(result['errors']),5)

    def test_v2_noncredential_redacted_literal_does_not_count_as_credential(self):
        cid=self.source.add(headers={'X-Text':['[REDACTED]']})
        raw=encode({'schema_version':1,'policy':'credential_names_v1','headers':{'X-Text':['[REDACTED]']},'redacted_count':0})
        self.replace_headers(cid,raw);self.change_manifest(cid,headers_redacted_count=0)
        self.assertEqual(self.pull()['status'],'ok');self.assertEqual(self.source.ack_calls,[(cid,'input')])

    def test_v2_unknown_input_fields_are_not_silently_acknowledged(self):
        cid=self.source.add(headers={});self.change_manifest(cid,unexpected='synthetic')
        result=self.pull();self.assertEqual(self.source.ack_calls,[])
        self.assertIn('invalid_input_header_fields',[e['code']for e in result['errors']])

    def test_v2_30day_retention_deletes_headers_only_with_both_acks_terminal_and_valid_files(self):
        cid=self.source.add(headers={'X-Trace':['synthetic']},outcome=True);self.pull();self.source.items=[]
        header=self.directory(cid)/(cid+'.headers.gz');header.write_bytes(b'corrupt')
        result=self.pull(now=NOW+dt.timedelta(days=31));self.assertEqual(result['expired'],0);self.assertTrue(header.exists())
        header.write_bytes(self.source.objects[cid+'.headers.gz'])
        partial=self.directory(cid)/(cid+'.headers.gz.partial');partial.write_bytes(b'keep');partial.chmod(0o600)
        self.assertEqual(self.pull(now=NOW+dt.timedelta(days=31))['expired'],0)
        partial.unlink();self.assertEqual(self.pull(now=NOW+dt.timedelta(days=31))['expired'],1)
        self.assertFalse(self.directory(cid).exists())

    def test_v1_unexpected_headers_artifact_is_never_adopted_by_retention(self):
        cid=self.source.add(outcome=True);self.pull();self.source.items=[]
        extra=self.directory(cid)/(cid+'.headers.gz');extra.write_bytes(b'keep');extra.chmod(0o600)
        self.assertEqual(self.pull(now=NOW+dt.timedelta(days=31))['expired'],0);self.assertTrue(extra.exists())

    def test_repeated_pull_is_idempotent_and_late_outcome_is_separate(self):
        cid=self.source.add(); self.pull(); count=len(self.source.fetch_calls)
        self.pull(); self.assertEqual(len(self.source.fetch_calls),count)
        self.assertEqual(self.source.ack_calls,[(cid,"input")])
        self.source.finish(cid); result=self.pull()
        self.assertEqual(result["outcome_archived"],1)
        self.assertEqual(self.source.ack_calls,[(cid,"input"),(cid,"outcome")])
        self.assertEqual(len(self.source.fetch_calls),count+1)

    def test_partial_entity_is_valid_archive_and_terminal_false_can_expire(self):
        when=NOW-dt.timedelta(days=31)
        cid=self.source.add(raw=b'{"partial":',captured=when,complete=False,outcome=True)
        result=self.pull()
        self.assertEqual(result["expired"],1); self.assertFalse(self.directory(cid).exists())
        self.assertEqual(self.source.ack_calls,[(cid,"input"),(cid,"outcome")])

    def test_malicious_relative_path_is_rejected_before_fetch(self):
        self.source.add(); self.source.items[0]["input"]["path"]="../outside.input.json"
        with self.assertRaises(ArchiveError): self.pull()
        self.assertEqual(self.source.fetch_calls,[]); self.assertEqual(self.source.ack_calls,[])

    def test_symlink_capture_directory_never_writes_outside_namespace(self):
        cid=self.source.add(); self.root.mkdir(mode=0o700); (self.root/"captures").mkdir(mode=0o700)
        outside=self.base/"outside"; outside.mkdir(); (self.root/"captures"/cid).symlink_to(outside,target_is_directory=True)
        with self.assertRaises(ArchiveError): self.pull()
        self.assertEqual(list(outside.iterdir()),[]); self.assertEqual(self.source.ack_calls,[])

    def test_symlink_payload_is_not_followed(self):
        cid=self.source.add(); self.pull(); directory=self.directory(cid)
        target=directory/(cid+".request.gz"); target.unlink()
        outside=self.base/"outside.gz"; outside.write_bytes(b"outside")
        target.symlink_to(outside)
        self.assertRaises(ArchiveError,self.pull)
        self.assertEqual(outside.read_bytes(),b"outside")

    def test_hash_corruption_never_acknowledges_and_keeps_pending(self):
        cid=self.source.add(); corrupt=bytearray(self.source.objects[cid+".request.gz"])
        corrupt[len(corrupt)//2]^=1; self.source.objects[cid+".request.gz"]=bytes(corrupt)
        result=self.pull()
        self.assertEqual(result["status"],"pending"); self.assertEqual(self.source.ack_calls,[])
        self.assertTrue(list(self.directory(cid).glob(".*.pending-*")))
        self.assertIn(cid+".request.gz",self.source.objects)
        self.assertIn("object_hash_mismatch",[x["code"] for x in result["errors"]])

    def test_raw_hash_and_gzip_expansion_are_independently_checked(self):
        cid=self.source.add(raw=b"z"*10000)
        manifest=json.loads(self.source.objects[cid+".input.json"]); manifest["raw_bytes"]=1
        self.source.objects[cid+".input.json"]=encode(manifest)
        self.source.items[0]["input"]=self.source.descriptor(cid+".input.json")
        result=self.pull(); self.assertEqual(self.source.ack_calls,[])
        self.assertIn("raw_expansion_limit",[x["code"] for x in result["errors"]])

    def test_disconnect_preserves_source_and_partial_download_without_ack(self):
        cid=self.source.add(); self.source.fail_fetch=cid+".request.gz"
        result=self.pull(); self.assertEqual(result["status"],"pending")
        self.assertEqual(self.source.ack_calls,[])
        self.assertTrue(list(self.directory(cid).glob(".*.pending-*")))
        self.source.fail_fetch=None; result=self.pull()
        self.assertEqual(result["input_archived"],1)

    def test_uncertain_ack_applied_is_only_reconciled_read_only(self):
        cid=self.source.add(); self.source.uncertain="applied"; result=self.pull()
        self.assertEqual(result["acks_pending"],1)
        self.source.uncertain=None; result=self.pull()
        self.assertEqual(self.source.ack_calls,[(cid,"input")])
        self.assertEqual(self.source.status_calls,[(cid,"input")])
        self.assertTrue((self.directory(cid)/(cid+".input-ack.done.json")).exists())

    def test_uncertain_ack_not_applied_is_not_retried_next_run(self):
        cid=self.source.add(); self.source.uncertain="not_applied"; self.pull()
        self.source.uncertain=None; result=self.pull()
        self.assertEqual(self.source.ack_calls,[(cid,"input")])
        self.assertEqual(result["acks_pending"],1)
        self.assertFalse((self.directory(cid)/(cid+".input-ack.done.json")).exists())

    def test_ack_is_reconciled_after_source_gc_hides_capture(self):
        cid=self.source.add(outcome=True); self.source.uncertain="applied"
        self.pull(); calls=list(self.source.ack_calls)
        self.source.items=[]; self.source.uncertain=None
        result=self.pull(now=NOW+dt.timedelta(days=2))
        self.assertEqual(result["acks_confirmed"],2)
        self.assertEqual(self.source.ack_calls,calls)
        self.assertEqual(set(self.source.status_calls),{(cid,"input"),(cid,"outcome")})
        self.assertTrue((self.directory(cid)/(cid+".outcome-ack.done.json")).exists())

    def test_missing_source_ack_stays_visible_when_capture_not_listed(self):
        cid=self.source.add(); self.source.uncertain="not_applied"; self.pull()
        self.source.items=[]; self.source.uncertain=None
        result=self.pull()
        self.assertEqual(result["status"],"pending")
        self.assertEqual(result["acks_pending"],1)
        self.assertEqual(self.source.ack_calls,[(cid,"input")])

    def test_local_ack_scan_is_bounded_and_advances(self):
        ids=[self.source.add()for _ in range(3)];self.source.uncertain="applied";self.pull()
        self.source.items=[];self.source.uncertain=None
        for _ in range(3):self.pull(max_retention_scan=1)
        self.assertEqual(set(self.source.status_calls),{(cid,"input")for cid in ids})

    def test_source_rejections_are_visible_without_echoing_source_details(self):
        listing=self.source.manifest_list
        self.source.manifest_list=lambda:{**listing(),"rejected":{"synthetic_private_detail":1}}
        self.source.gc=lambda:{"schema_version":1,"group_id":123,"deleted":0,"deleted_bytes":0,
                               "rejected":{"another_private_detail":2}}
        result=self.pull()
        self.assertEqual(result["status"],"pending")
        self.assertEqual(result["errors"],[{"code":"source_list_rejected","count":1},
                                            {"code":"source_gc_rejected","count":2}])
        self.assertNotIn("private_detail",json.dumps(result))

    def test_source_header_rejection_codes_are_observable_only_from_fixed_allowlist(self):
        self.source.manifest_list=lambda:{'schema_version':1,'group_id':123,'items':[],
            'rejected':{'unredacted_credential_header':2,'synthetic_private_value':1}}
        result=self.pull()
        self.assertEqual(result['errors'],[{'code':'source_list_rejected','count':3,
                                           'reasons':{'unredacted_credential_header':2}}])
        self.assertNotIn('synthetic_private_value',json.dumps(result))

    def test_source_unreachable_does_not_expire_or_ack_existing_archive(self):
        cid=self.source.add(outcome=True); self.pull(); old_calls=list(self.source.ack_calls)
        self.source.list_unreachable=True
        with self.assertRaises(ArchiveError): self.pull(now=NOW+dt.timedelta(days=40))
        self.assertTrue(self.directory(cid).exists()); self.assertEqual(self.source.ack_calls,old_calls)

    def test_retention_uses_server_capture_time_and_never_other_group_or_parent(self):
        cid=self.source.add(outcome=True); self.pull()
        outside=self.base/"old-other-project"; outside.write_text("keep")
        self.source.items=[]
        with self.assertRaises(ArchiveError):
            Puller(self.config(group_id=999),SourceFixture(999),NOW+dt.timedelta(days=40)).run()
        self.assertTrue(self.directory(cid).exists())
        result=self.pull(now=NOW+dt.timedelta(days=40))
        self.assertEqual(result["expired"],1); self.assertEqual(outside.read_text(),"keep")

    def test_retention_preserves_no_outcome_and_unknown_files(self):
        cid=self.source.add(); self.pull(); self.source.items=[]
        result=self.pull(now=NOW+dt.timedelta(days=40)); self.assertTrue(self.directory(cid).exists())
        self.assertIn("retention_outcome_pending",[x["code"] for x in result["errors"]])
        (self.directory(cid)/"unknown").write_text("keep")
        result=self.pull(now=NOW+dt.timedelta(days=41))
        self.assertTrue((self.directory(cid)/"unknown").exists())

    def test_listing_count_and_bytes_are_bounded_before_network_copy(self):
        self.source.add(); self.source.add()
        self.assertRaises(ArchiveError,self.pull,max_objects=1)
        self.assertEqual(self.source.fetch_calls,[])
        self.assertRaises(ArchiveError,self.pull,max_bytes=1)
        self.assertEqual(self.source.ack_calls,[])

    def test_insufficient_nas_space_never_fetches_or_acks(self):
        self.source.add(); result=self.pull(min_free_bytes=10**18)
        self.assertEqual(result["status"],"pending"); self.assertEqual(self.source.fetch_calls,[])
        self.assertEqual(self.source.ack_calls,[])

    def test_duplicate_capture_and_foreign_manifest_group_rejected(self):
        cid=self.source.add(); self.source.items.append(copy.deepcopy(self.source.items[0]))
        self.assertRaises(ArchiveError,self.pull); self.source.items.pop()
        manifest=json.loads(self.source.objects[cid+".input.json"]); manifest["group_id"]=999
        self.source.objects[cid+".input.json"]=encode(manifest)
        self.source.items[0]["input"]=self.source.descriptor(cid+".input.json")
        result=self.pull(); self.assertEqual(result["status"],"pending"); self.assertEqual(self.source.ack_calls,[])

    def test_production_transport_rejects_oversize_child_file(self):
        # Exercise the actual subprocess resource boundary without SSH/network.
        target=self.base/"bounded"
        import sys
        command=[sys.executable,"-c","import pathlib,sys; pathlib.Path(sys.argv[1]).write_bytes(b'x'*10000)",str(target)]
        with self.assertRaises(ArchiveError): SSHTransport.run(command,file_size_limit=1024)
        self.assertLessEqual(target.stat().st_size,1024)

    def test_production_transport_bounds_untrusted_stdout(self):
        import sys
        with self.assertRaises(ArchiveError):
            SSHTransport.run([sys.executable,"-c","print('x'*100000)"],output_limit=1024)

    def test_concurrent_run_observes_lock_without_transport(self):
        self.pull()
        with (self.root/".run"/"pull.lock").open("r+") as lock:
            fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
            self.source.list_unreachable=True
            self.assertEqual(self.pull(),{"status":"locked"})

    def test_raw_hash_mismatch_is_not_a_compressed_hash_success(self):
        cid=self.source.add(); manifest=json.loads(self.source.objects[cid+".input.json"])
        manifest["raw_sha256"]="0"*64
        self.source.objects[cid+".input.json"]=encode(manifest)
        self.source.items[0]["input"]=self.source.descriptor(cid+".input.json")
        result=self.pull(); self.assertEqual(self.source.ack_calls,[])
        self.assertIn("raw_hash_or_size_mismatch",[x["code"] for x in result["errors"]])

    def test_credential_header_in_manifest_is_rejected(self):
        cid=self.source.add(); manifest=json.loads(self.source.objects[cid+".input.json"])
        manifest["headers"]["Authorization"]="synthetic forbidden credential"
        self.source.objects[cid+".input.json"]=encode(manifest)
        self.source.items[0]["input"]=self.source.descriptor(cid+".input.json")
        result=self.pull(); self.assertEqual(self.source.ack_calls,[])
        self.assertIn("unsafe_input_headers",[x["code"] for x in result["errors"]])

    def test_retention_scan_budget_eventually_visits_all_captures(self):
        ids=[self.source.add(outcome=True) for _ in range(3)]
        self.pull(); self.source.items=[]
        for _ in range(3): self.pull(now=NOW+dt.timedelta(days=40),max_retention_scan=1)
        self.assertFalse(any(self.directory(cid).exists() for cid in ids))


if __name__=="__main__": unittest.main()
