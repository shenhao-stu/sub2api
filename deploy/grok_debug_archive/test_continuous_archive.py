"""Bounded backlog progress and verified immediate cleanup using real files."""
import concurrent.futures
import datetime as dt
import gzip
import json
import os
import uuid
from unittest import mock

import source_helper as source
import nas_archive as nas
from test_source_helper import SourceBehavior, LocalTransport, NOW


class ContinuousArchive(SourceBehavior):
    def test_backlog_above_old_capture_cap_still_lists_and_exact_collects(self):
        ids=[self.producer.add(outcome=True,headers={'X-Trace':['kept']})for _ in range(5)]
        with mock.patch.object(source,'MAX_SCAN',2):
            with self.store()as store:
                listed=store.list(5,1024**3)
            self.assertEqual({x['capture_id']for x in listed['items']},set(ids))
            for cid in ids:self.both(cid)
            self.producer.put(ids[-1]+'.request.partial',b'preserve')
            self.assertEqual(self.gc(capture_ids=ids)['deleted'],4)
            self.assertTrue((self.root/(ids[-1]+'.input.json')).exists())

    def test_completed_ack_tombstones_cannot_stop_new_capture(self):
        old=[self.producer.add(outcome=True)for _ in range(6)]
        for cid in old:self.both(cid)
        self.assertEqual(self.gc(capture_ids=old)['deleted'],6)
        live=self.producer.add(outcome=True)
        with mock.patch.object(source,'MAX_SCAN',2):
            self.assertEqual([x['capture_id']for x in self.listing()['items']],[live])

    def test_large_namespace_reads_only_bounded_metadata_and_advances(self):
        for i in range(10000):
            cid=f'00000000-0000-4000-8000-{i:012x}'
            self.producer.put(cid+'.input.json',b'{}')
        valid=self.producer.add(outcome=True,headers={'X-Trace':['complete']})
        with self.store() as store, mock.patch.object(store,'input',wraps=store.input) as read:
            batch=store.list(5,1024**3)
            self.assertLessEqual(read.call_count,16)
            self.assertEqual(batch['scanned'],16)
            self.assertTrue(batch['next_cursor']); self.assertTrue(batch['more'])
        with self.store() as store:
            batch=store.list(5,1024**3,'00000000-0000-4000-8000-00000000270f')
            self.assertEqual([x['capture_id']for x in batch['items']],[valid])

    def test_uuid_cursor_revisits_active_late_outcome_and_new_lower_id(self):
        low='00000000-0000-4000-8000-000000000001'
        high='ffffffff-ffff-4fff-8fff-ffffffffffff'
        with mock.patch.object(source.uuid,'uuid4',return_value=uuid.UUID(low)):
            active=self.producer.add();self.ack(active,'input')
        with mock.patch.object(source.uuid,'uuid4',return_value=uuid.UUID(high)):
            other=self.producer.add(outcome=True)
        with self.store()as store:page=store.list(1,1024**3)
        self.assertEqual([x['capture_id']for x in page['items']],[other])
        self.producer.finish(active)
        with self.store()as store:again=store.list(1,1024**3,page['next_cursor'])
        self.assertEqual([x['capture_id']for x in again['items']],[active])

    def test_bad_high_uuid_outcomes_cannot_starve_lower_new_input(self):
        low='00000000-0000-4000-8000-000000000001'
        with mock.patch.object(source.uuid,'uuid4',return_value=uuid.UUID(low)):valid=self.producer.add(outcome=True)
        for i in range(16):
            cid=f'ffffffff-ffff-4fff-8fff-{i:012x}'
            with mock.patch.object(source.uuid,'uuid4',return_value=uuid.UUID(cid)):self.producer.add(outcome=True)
            self.ack(cid,'input');self.producer.put(cid+'.request.gz',b'corrupt')
        with self.store()as store:page=store.list(1,1024**3)
        self.assertEqual([x['capture_id']for x in page['items']],[valid])

    def test_100261_names_keep_one_page_not_whole_namespace(self):
        import tracemalloc
        def names():
            for i in range(100261):yield f'00000000-0000-4000-8000-{i:012x}'
        tracemalloc.start()
        try:
            with self.store()as store,mock.patch.object(store,'names',side_effect=names):
                page=store.list(1,1024**3)
            _,peak=tracemalloc.get_traced_memory()
        finally:tracemalloc.stop()
        self.assertEqual(page['scanned'],16);self.assertTrue(page['more'])
        self.assertLess(peak,2*1024**2)

    def test_cursor_wrap_uses_one_complete_namespace_scan(self):
        valid=self.producer.add(outcome=True)
        with self.store()as store,mock.patch.object(store,'names',wraps=store.names)as scan:
            page=store.list(1,1024**3,'ffffffff-ffff-4fff-bfff-ffffffffffff')
        self.assertEqual(scan.call_count,1)
        self.assertEqual([x['capture_id']for x in page['items']],[valid])

    def test_unfinished_name_scan_does_not_return_partial_cursor(self):
        def names():
            yield '00000000-0000-4000-8000-000000000001'
            raise source.HelperError('helper_time_limit')
        with self.store()as store,mock.patch.object(store,'names',side_effect=names):
            with self.assertRaisesRegex(source.HelperError,'helper_time_limit'):store.list(1,1024**3)

    def test_exact_cleanup_requires_terminal_two_acks_hashes_and_private_files(self):
        terminal=self.producer.add(outcome=True,headers={'Authorization':['[REDACTED]'],'X-Trace':['a','b']})
        active=self.producer.add(headers={'X-Trace':['active']})
        unacked=self.producer.add(outcome=True)
        self.both(terminal);self.ack(active,'input')
        # Existing broad GC keeps its 24 hour grace. Explicit confirmed cleanup
        # is permitted immediately, but cannot erase active/unacknowledged data.
        self.assertEqual(self.gc()['deleted'],0)
        result=self.gc(capture_ids=[terminal,active,unacked])
        self.assertEqual(result['deleted'],1)
        self.assertFalse((self.root/(terminal+'.input.json')).exists())
        self.assertTrue((self.root/(active+'.input.json')).exists())
        self.assertTrue((self.root/(unacked+'.input.json')).exists())

    def test_exact_cleanup_refuses_wrong_hash_and_partial(self):
        for partial in [False,True]:
            cid=self.producer.add(outcome=True);self.both(cid)
            if partial:self.producer.put(cid+'.request.partial',b'x')
            else:self.producer.put(cid+'.request.gz',b'changed')
            self.assertEqual(self.gc(capture_ids=[cid])['deleted'],0)
            self.assertTrue((self.root/(cid+'.input.json')).exists())

    def test_parallel_ack_wait_is_bounded_and_idempotent(self):
        ids=[self.producer.add(outcome=True,headers={'X-Trace':['a']})for _ in range(16)]
        with concurrent.futures.ThreadPoolExecutor(max_workers=8)as pool:
            list(pool.map(self.both,ids))
        for cid in ids:
            a=self.both(cid)
            self.assertTrue(all(x['accepted']for x in a))

    def test_500_complete_archives_eight_workers_then_exact_source_cleanup(self):
        headers={'Authorization':['[REDACTED]'],'X-Trace':['first','second']}
        ids=[self.producer.add(raw=('synthetic-body-'+str(i)).encode(),outcome=True,headers=headers)for i in range(500)]
        config=nas.Config(root=self.base/'netcup-grok-test',group_id=78,source_dir=str(self.root),
                          helper_path=str(source.INSTALL_PATH),min_free_bytes=0,max_objects=500,workers=8,max_seconds=290)
        class ExactTransport(LocalTransport):
            def gc_confirmed(inner,ids):
                with inner.store()as store:return store.gc(500,1024**3,capture_ids=ids)
        transport=ExactTransport(self.root,config,self.state_root)
        result=nas.Puller(config,transport,NOW).run()
        self.assertEqual(result['errors'],[])
        self.assertEqual(result['input_archived'],500)
        self.assertEqual(result['outcome_archived'],500)
        self.assertEqual(result['acks_confirmed'],1000)
        self.assertEqual(result['source_deleted'],500)
        self.assertEqual(list(self.root.iterdir()),[])
        for i,cid in enumerate(ids):
            d=config.root/'captures'/cid
            self.assertEqual(gzip.decompress((d/(cid+'.request.gz')).read_bytes()),('synthetic-body-'+str(i)).encode())
            self.assertEqual(json.loads(gzip.decompress((d/(cid+'.headers.gz')).read_bytes()))['headers'],headers)
            self.assertTrue((d/(cid+'.outcome-ack.done.json')).exists())

    def test_continuous_mode_has_idle_error_backoff_and_immediate_progress(self):
        results=[{"status":"ok","acks_confirmed":1},{"status":"failed"},{"status":"failed"},{"status":"failed"},{"status":"failed"}]
        delays=[]
        def sleep(delay):
            delays.append(delay)
            if len(delays)==5: raise KeyboardInterrupt()
        with mock.patch('sys.argv',['archive','--config','unused','--continuous']),mock.patch.object(nas,'run_once',side_effect=results),mock.patch.object(nas.time,'sleep',side_effect=sleep),mock.patch('builtins.print'):
            with self.assertRaises(KeyboardInterrupt):nas.main()
        self.assertEqual(delays,[0.2,10,20,40,60])
