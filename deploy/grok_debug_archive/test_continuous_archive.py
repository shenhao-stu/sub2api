"""Bounded backlog progress and verified immediate cleanup using real files."""
import concurrent.futures
import datetime as dt
import gzip
import json
import os
from unittest import mock

import source_helper as source
import nas_archive as nas
from test_source_helper import SourceBehavior, LocalTransport, NOW


class ContinuousArchive(SourceBehavior):
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
