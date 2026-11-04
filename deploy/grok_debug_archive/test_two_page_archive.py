"""At most two source pages share one complete namespace budget and hard deadline."""
import gzip
import json
import unittest
import datetime as dt
from dataclasses import replace
from unittest import mock

import nas_archive as nas
import source_helper as source
from test_batch_archive import BatchTransport
import test_source_helper as fixtures
NOW=fixtures.NOW


class PagedTransport(BatchTransport):
    def __init__(self,*args):
        super().__init__(*args);self.list_calls=0;self.gc_calls=[]
    def manifest_list(self):
        self.list_calls+=1
        path=self.config.root/'.run'/'source-cursor.json'
        cursor=json.loads(path.read_bytes())['last_uuid'] if path.exists() else ''
        with self.store() as store:return store.list(self.config.max_objects,self.config.max_bytes,cursor)
    def gc_confirmed(self,ids):
        self.gc_calls.append(list(ids));return super().gc_confirmed(ids)


class TwoPageArchive(unittest.TestCase):
    setUp=fixtures.SourceBehavior.setUp
    tearDown=fixtures.SourceBehavior.tearDown
    def config(self,objects=2):
        cfg=nas.Config(root=self.base/'netcup-grok-test',group_id=78,source_dir=str(self.root),
                       helper_path=str(source.INSTALL_PATH),min_free_bytes=0,max_objects=objects,
                       workers=8,max_seconds=600,max_namespace_bytes=512*1024**3)
        # This assignment keeps the baseline RED about actual one-page behavior.
        object.__setattr__(cfg,'max_pages_per_run',2)
        return cfg
    def test_two_pages_archive_full_body_headers_dual_ack_and_gc_with_one_scan(self):
        ids=[self.producer.add(raw=('body-'+str(i)).encode(),outcome=True,
                              headers={'Authorization':['[REDACTED]'],'X-Test':['synthetic']}) for i in range(4)]
        cfg=self.config();transport=PagedTransport(self.root,cfg,self.state_root)
        with mock.patch.object(nas,'namespace_size',wraps=nas.namespace_size) as scan:
            result=nas.Puller(cfg,transport,NOW).run()
        self.assertEqual(result['input_archived'],4)
        self.assertEqual(result['outcome_archived'],4)
        self.assertEqual(result['acks_confirmed'],8)
        self.assertEqual(result['source_deleted'],4)
        self.assertEqual(result['errors'],[])
        self.assertEqual(scan.call_count,1)
        self.assertEqual(transport.list_calls,2)
        self.assertEqual([len(v) for v in transport.gc_calls],[2,2])
        for i,cid in enumerate(ids):
            d=cfg.root/'captures'/cid
            self.assertEqual(gzip.decompress((d/(cid+'.request.gz')).read_bytes()),('body-'+str(i)).encode())
            self.assertEqual(json.loads(gzip.decompress((d/(cid+'.headers.gz')).read_bytes()))['headers']['Authorization'],['[REDACTED]'])
            self.assertTrue(all((d/(cid+'.'+kind+'-ack.done.json')).exists() for kind in ('input','outcome')))
            self.assertFalse((self.root/(cid+'.input.json')).exists())

    def test_second_page_listing_failure_keeps_first_page_gc_proof(self):
        ids=[self.producer.add(outcome=True,headers={}) for _ in range(4)]
        cfg=self.config();transport=PagedTransport(self.root,cfg,self.state_root)
        original=transport.manifest_list
        def listing():
            if transport.list_calls:raise nas.ArchiveError('transport_timeout')
            return original()
        with mock.patch.object(transport,'manifest_list',side_effect=listing):
            result=nas.Puller(cfg,transport,NOW).run()
        self.assertEqual(result['source_deleted'],2)
        self.assertEqual(result['pages_completed'],1)
        self.assertEqual(result['errors'],[{'code':'next_page_list_failed'}])
        self.assertEqual(sum((self.root/(cid+'.input.json')).exists() for cid in ids),2)

    def test_first_page_unknown_ack_preserves_source_and_does_not_list_again(self):
        ids=[self.producer.add(outcome=True,headers={}) for _ in range(4)]
        cfg=self.config();transport=PagedTransport(self.root,cfg,self.state_root);transport.unknown=True
        result=nas.Puller(cfg,transport,NOW).run()
        self.assertEqual(transport.list_calls,1)
        self.assertEqual(transport.ack_batches,1)
        self.assertEqual(result['acks_pending'],4)
        self.assertEqual(result['source_deleted'],0)
        self.assertEqual(result['errors'],[{'code':'ack_batch_outcome_unknown'}])
        self.assertTrue(all((self.root/(cid+'.input.json')).exists() for cid in ids))

    def test_unknown_gc_is_not_retried_or_counted_as_confirmed(self):
        ids=[self.producer.add(outcome=True,headers={}) for _ in range(4)]
        cfg=self.config();transport=PagedTransport(self.root,cfg,self.state_root);original=transport.gc_confirmed
        def lost_reply(cids):
            original(cids);raise nas.ArchiveError('transport_timeout')
        with mock.patch.object(transport,'gc_confirmed',side_effect=lost_reply):result=nas.Puller(cfg,transport,NOW).run()
        self.assertEqual(transport.list_calls,1);self.assertEqual(len(transport.gc_calls),1)
        self.assertEqual(result['source_deleted'],0)
        self.assertEqual(result['errors'],[{'code':'source_gc_unconfirmed'}])
        self.assertEqual(sum((self.root/(cid+'.input.json')).exists() for cid in ids),2)

    def test_second_page_excludes_first_page_active_uuid(self):
        active=[self.producer.add(headers={}) for _ in range(2)]
        terminal=self.producer.add(outcome=True,headers={})
        cfg=self.config();transport=PagedTransport(self.root,cfg,self.state_root)
        with transport.store() as store:by_id={v['capture_id']:v for v in store.list(3,1024**3)['items']}
        def listing():
            transport.list_calls+=1
            ids=active if transport.list_calls==1 else [active[0],terminal]
            return {'schema_version':1,'group_id':78,'items':[by_id[cid] for cid in ids],
                    'next_cursor':ids[-1],'rejected':{}}
        with mock.patch.object(transport,'manifest_list',side_effect=listing):result=nas.Puller(cfg,transport,NOW).run()
        self.assertEqual(result['input_archived'],3);self.assertEqual(result['listed'],3)
        self.assertEqual(result['acks_confirmed'],4);self.assertEqual(result['page_duplicates_skipped'],1)
        self.assertEqual(result['source_deleted'],1)
        self.assertTrue(all((self.root/(cid+'.input.json')).exists() for cid in active))

    def test_second_page_requires_90_seconds_of_work_budget(self):
        for remaining,expected_pages in [(89,1),(90,2)]:
            with self.subTest(remaining=remaining):
                # Separate namespaces preserve per-run and source protocol isolation.
                cfg=replace(self.config(),root=self.base/str(remaining)/'netcup-grok-test')
                cfg.root.parent.mkdir(mode=0o700)
                ids=[self.producer.add(outcome=True,headers={}) for _ in range(4)]
                transport=PagedTransport(self.root,cfg,self.state_root);clock=[0.0];scan=nas.namespace_size
                def delayed(root,budget):
                    clock[0]=600-45-remaining;return scan(root,budget)
                with mock.patch.object(nas.time,'monotonic',side_effect=lambda:clock[0]),mock.patch.object(nas,'namespace_size',side_effect=delayed):
                    result=nas.Puller(cfg,transport,NOW).run()
                self.assertEqual(transport.list_calls,expected_pages)
                self.assertEqual(result['pages_completed'],expected_pages)
                if remaining==89:self.assertEqual(result['next_page_deferred'],'time_limit')
                # Remove only fixture records left after the one-page subcase.
                for cid in ids:
                    for p in self.root.glob(cid+'.*'):p.unlink()

    def test_second_page_deferred_ack_retains_both_source_and_cursor(self):
        ids=[self.producer.add(outcome=True,headers={}) for _ in range(4)]
        cfg=self.config();transport=PagedTransport(self.root,cfg,self.state_root);clock=[0.0];fetch=transport.fetch_batch
        def delayed(*args):
            result=fetch(*args)
            if transport.list_calls==2:clock[0]=511
            return result
        with mock.patch.object(nas.time,'monotonic',side_effect=lambda:clock[0]),mock.patch.object(transport,'fetch_batch',side_effect=delayed):
            result=nas.Puller(cfg,transport,NOW).run()
        self.assertEqual(result['source_deleted'],2);self.assertEqual(result['acks_pending'],4)
        self.assertEqual(transport.ack_batches,1)
        self.assertEqual(result['errors'],[{'code':'ack_batch_deferred_time_limit'}])
        cursor=json.loads((cfg.root/'.run'/'source-cursor.json').read_bytes())['last_uuid']
        self.assertEqual(cursor,sorted(ids)[1])
        for cid in sorted(ids)[2:]:
            self.assertTrue((self.root/(cid+'.input.json')).exists())
            self.assertTrue((cfg.root/'captures'/cid/(cid+'.outcome-ack.intent.json')).exists())
            self.assertFalse((cfg.root/'captures'/cid/(cid+'.outcome-ack.done.json')).exists())

    def test_cumulative_namespace_budget_rejects_second_page_before_transfer(self):
        ids=[self.producer.add(outcome=True,headers={}) for _ in range(4)]
        cfg=replace(self.config(),max_payload_bytes=1024**2,max_namespace_bytes=2*8*65536+128*1024)
        transport=PagedTransport(self.root,cfg,self.state_root)
        result=nas.Puller(cfg,transport,NOW).run()
        self.assertEqual(result['source_deleted'],2)
        self.assertEqual(result['pages_completed'],1)
        self.assertEqual(transport.batches,1)
        self.assertEqual(result['errors'],[{'code':'namespace_space_limit'}])
        self.assertEqual(sum((self.root/(cid+'.input.json')).exists() for cid in ids),2)

    def test_total_descriptor_byte_limit_is_shared_by_both_pages(self):
        ids=[self.producer.add(outcome=True,headers={}) for _ in range(4)]
        cfg=self.config();transport=PagedTransport(self.root,cfg,self.state_root)
        with transport.store() as store:items=store.list(4,1024**3)['items']
        one_page=sum(v[k]['bytes'] for v in items[:2] for k in ['input','payload','headers','outcome'])
        cfg=replace(cfg,max_bytes=one_page);transport=PagedTransport(self.root,cfg,self.state_root)
        result=nas.Puller(cfg,transport,NOW).run()
        self.assertLessEqual(result['bytes_fetched'],one_page)
        self.assertEqual(result['source_deleted'],2)
        self.assertEqual(result['pages_completed'],1)
        self.assertEqual(result['next_page_deferred'],'byte_limit')
        self.assertEqual(sum((self.root/(cid+'.input.json')).exists() for cid in ids),2)

    def test_partial_gc_stops_second_page_and_reports_pending(self):
        ids=[self.producer.add(outcome=True,headers={}) for _ in range(4)]
        cfg=self.config();transport=PagedTransport(self.root,cfg,self.state_root);original=transport.gc_confirmed
        with mock.patch.object(transport,'gc_confirmed',side_effect=lambda cids:original(cids[:1])):
            result=nas.Puller(cfg,transport,NOW).run()
        self.assertEqual(transport.list_calls,1)
        self.assertEqual(result['source_deleted'],1)
        self.assertEqual(result['status'],'pending')
        self.assertEqual(result['errors'],[{'code':'source_gc_incomplete'}])
        self.assertEqual(sum((self.root/(cid+'.input.json')).exists() for cid in ids),3)

    def test_second_page_nas_free_floor_preserves_first_page_result(self):
        ids=[self.producer.add(outcome=True,headers={}) for _ in range(4)]
        cfg=self.config();transport=PagedTransport(self.root,cfg,self.state_root)
        def space(_):return mock.Mock(free=0 if transport.list_calls==2 else 1024**4)
        with mock.patch.object(nas.shutil,'disk_usage',side_effect=space):result=nas.Puller(cfg,transport,NOW).run()
        self.assertEqual(result['source_deleted'],2);self.assertEqual(transport.batches,1)
        self.assertEqual(result['errors'],[{'code':'nas_free_space_limit'}])
        self.assertEqual(sum((self.root/(cid+'.input.json')).exists() for cid in ids),2)

    def test_30_day_retention_preserves_recent_active_and_unknown(self):
        expired=self.producer.add(when=NOW-dt.timedelta(days=31),outcome=True,headers={})
        recent=self.producer.add(when=NOW-dt.timedelta(days=29),outcome=True,headers={})
        active=self.producer.add(when=NOW-dt.timedelta(days=31),headers={})
        unknown=self.producer.add(when=NOW-dt.timedelta(days=31),outcome=True,headers={})
        cfg=self.config();cfg.root.mkdir(mode=0o700);(cfg.root/'captures').mkdir(mode=0o700)
        directory=cfg.root/'captures'/unknown;directory.mkdir(mode=0o700)
        protected=directory/'unknown';protected.write_bytes(b'preserve');protected.chmod(0o600)
        transport=PagedTransport(self.root,cfg,self.state_root);result=nas.Puller(cfg,transport,NOW).run()
        self.assertEqual(result['source_deleted'],3);self.assertEqual(result['expired'],1)
        self.assertFalse((cfg.root/'captures'/expired).exists())
        self.assertTrue((cfg.root/'captures'/recent/(recent+'.outcome-ack.done.json')).exists())
        self.assertTrue((cfg.root/'captures'/active/(active+'.input.json')).exists())
        self.assertTrue((self.root/(active+'.input.json')).exists())
        self.assertEqual(protected.read_bytes(),b'preserve')

    def test_page_configuration_is_explicit_and_bounded(self):
        self.assertEqual(nas.Config.__dataclass_fields__['max_pages_per_run'].default,1)
        for pages in (0,3,True,2.0):
            with self.subTest(pages=pages),self.assertRaisesRegex(nas.ArchiveError,'invalid_page_limit'):
                replace(self.config(),max_pages_per_run=pages).validate()

    def test_two_full_1000_pages_share_slow_scan_and_original_600_second_budget(self):
        ids=[self.producer.add(raw=('full-body-'+str(i)).encode(),outcome=True,
                              headers={'Authorization':['[REDACTED]'],'X-Trace':['one','two']}) for i in range(2000)]
        cfg=self.config(1000);clock=[0.0];scan=nas.namespace_size
        class Timed(PagedTransport):
            def fetch_batch(inner,*args):
                clock[0]+=5;return super().fetch_batch(*args)
            def ack_batch(inner,claims):
                clock[0]+=3;return super().ack_batch(claims)
            def gc_confirmed(inner,cids):
                clock[0]+=10;return super().gc_confirmed(cids)
        def slow(root,budget):clock[0]+=240;return scan(root,budget)
        transport=Timed(self.root,cfg,self.state_root)
        with mock.patch.object(nas.time,'monotonic',side_effect=lambda:clock[0]),mock.patch.object(nas,'namespace_size',side_effect=slow) as counted:
            result=nas.Puller(cfg,transport,NOW).run()
        self.assertEqual(result['errors'],[]);self.assertEqual(result['pages_completed'],2)
        self.assertEqual(result['input_archived'],2000);self.assertEqual(result['outcome_archived'],2000)
        self.assertEqual(result['acks_confirmed'],4000);self.assertEqual(result['source_deleted'],2000)
        self.assertEqual(result['namespace_scan_seconds'],240);self.assertEqual(result['elapsed_seconds'],388)
        self.assertEqual(counted.call_count,1);self.assertEqual([len(v) for v in transport.gc_calls],[1000,1000])
        self.assertLessEqual(result['bytes_fetched'],1024**3)
        for i,cid in enumerate(ids):
            d=cfg.root/'captures'/cid
            self.assertEqual(gzip.decompress((d/(cid+'.request.gz')).read_bytes()),('full-body-'+str(i)).encode())
            self.assertEqual(json.loads(gzip.decompress((d/(cid+'.headers.gz')).read_bytes()))['headers']['X-Trace'],['one','two'])
            self.assertTrue(all((d/(cid+'.'+kind+'-ack.done.json')).exists() for kind in ('input','outcome')))
            self.assertFalse((self.root/(cid+'.input.json')).exists())


if __name__=='__main__':unittest.main()
