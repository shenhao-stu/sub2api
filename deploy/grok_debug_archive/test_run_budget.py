"""Slow full scans must leave a bounded window for verified archive progress."""
import gzip
import json
import unittest
from dataclasses import replace
from pathlib import Path
from unittest import mock

import nas_archive as nas
import source_helper as source
from test_batch_archive import BatchTransport
import test_source_helper as fixtures


class RunBudget(unittest.TestCase):
    setUp = fixtures.SourceBehavior.setUp
    tearDown = fixtures.SourceBehavior.tearDown

    def config(self, seconds):
        return nas.Config(root=self.base/'netcup-grok-test', group_id=78,
                          source_dir=str(self.root), helper_path=str(source.INSTALL_PATH),
                          min_free_bytes=0, max_objects=1000, workers=8, max_seconds=seconds)

    def slow_workload(self, seconds):
        headers = {'Authorization': ['[REDACTED]'], 'X-Trace': ['one', 'two']}
        ids = [self.producer.add(raw=('body-'+str(i)).encode(), outcome=True, headers=headers)
               for i in range(1000)]
        clock = [0.0]
        config = self.config(seconds)
        scan = nas.namespace_size

        def slow_scan(root, budget):
            clock[0] += 180
            return scan(root, budget)

        class TimedTransport(BatchTransport):
            def fetch_batch(inner, descriptors, directory, timeout):
                clock[0] += min(25, timeout)
                nas.require(timeout >= 25, 'transport_timeout')
                return super().fetch_batch(descriptors, directory, timeout)

            def ack_batch(inner, claims):
                result = super().ack_batch(claims)
                clock[0] += 5
                return result

            def gc_confirmed(inner, captures):
                result = super().gc_confirmed(captures)
                clock[0] += 7
                return result

        transport = TimedTransport(self.root, config, self.state_root)
        with mock.patch.object(nas.time, 'monotonic', side_effect=lambda: clock[0]), \
                mock.patch.object(nas, 'namespace_size', side_effect=slow_scan) as counted:
            result = nas.Puller(config, transport, fixtures.NOW).run()
        self.assertEqual(counted.call_count, 1)
        return result, transport, ids, clock[0], headers

    def test_600_seconds_completes_existing_1000_limit_after_slow_complete_scan(self):
        result, transport, ids, elapsed, headers = self.slow_workload(600)
        self.assertEqual(result['errors'], [])
        self.assertEqual(result['input_archived'], 1000)
        self.assertEqual(result['outcome_archived'], 1000)
        self.assertEqual(result['acks_confirmed'], 2000)
        self.assertEqual(result['acks_pending'], 0)
        self.assertEqual(result['source_deleted'], 1000)
        self.assertEqual((transport.batches, transport.ack_batches), (8, 8))
        self.assertEqual(elapsed, 427)
        self.assertEqual(result['namespace_scan_seconds'], 180)
        self.assertEqual(result['elapsed_seconds'], 427)
        for i, cid in enumerate(ids):
            directory = self.config(600).root/'captures'/cid
            self.assertEqual(gzip.decompress((directory/(cid+'.request.gz')).read_bytes()),
                             ('body-'+str(i)).encode())
            self.assertEqual(json.loads(gzip.decompress((directory/(cid+'.headers.gz')).read_bytes()))['headers'], headers)
            self.assertTrue(all((directory/(cid+'.'+kind+'-ack.done.json')).exists()
                                for kind in ['input', 'outcome']))
            self.assertFalse((self.root/(cid+'.input.json')).exists())

    def test_expired_before_ack_dispatch_is_deferred_and_keeps_both_intents(self):
        cid = self.producer.add(outcome=True, headers={'Authorization': ['[REDACTED]']})
        config = self.config(600)
        transport = BatchTransport(self.root, config, self.state_root)
        clock = [0.0]
        publish = nas.atomic_json

        def slow_final_intent(path, value):
            publish(path, value)
            if path.name == cid+'.outcome-ack.intent.json':
                clock[0] = 555

        with mock.patch.object(nas.time, 'monotonic', side_effect=lambda: clock[0]), \
                mock.patch.object(nas, 'atomic_json', side_effect=slow_final_intent):
            result = nas.Puller(config, transport, fixtures.NOW).run()
        self.assertEqual(transport.ack_batches, 0)
        self.assertEqual(result['acks_confirmed'], 0)
        self.assertEqual(result['acks_pending'], 2)
        self.assertEqual(result['source_deleted'], 0)
        self.assertEqual(result['errors'], [{'code': 'ack_batch_deferred_time_limit'}])
        directory = config.root/'captures'/cid
        for kind in ['input', 'outcome']:
            self.assertTrue((directory/(cid+'.'+kind+'-ack.intent.json')).exists())
            self.assertFalse((directory/(cid+'.'+kind+'-ack.done.json')).exists())
        self.assertTrue(all((self.root/(cid+suffix)).exists() for suffix in
                            ['.input.json', '.request.gz', '.headers.gz', '.outcome.json']))
        recovered = nas.Puller(config, transport, fixtures.NOW).run()
        self.assertEqual(recovered['acks_confirmed'], 2)
        self.assertEqual(recovered['source_deleted'], 1)

    def test_short_run_keeps_source_when_transfer_cannot_preserve_ack_window(self):
        result, transport, ids, elapsed, _ = self.slow_workload(290)
        self.assertEqual(result['input_archived'], 0)
        self.assertEqual(result['acks_confirmed'], 0)
        self.assertEqual(result['source_deleted'], 0)
        self.assertEqual(result['acks_pending'], 0)
        self.assertEqual((transport.batches, transport.ack_batches), (0, 0))
        self.assertEqual(elapsed, 200)
        self.assertEqual(sum((self.root/(cid+'.input.json')).exists() for cid in ids), 1000)
        self.assertEqual(result['errors'], [{'code': 'transport_timeout'}])

    def ack_window(self, remaining):
        cid = self.producer.add(outcome=True, headers={})
        config = self.config(600)
        transport = BatchTransport(self.root, config, self.state_root)
        clock = [0.0]
        publish = nas.atomic_json

        def slow_intent(path, value):
            publish(path, value)
            if path.name == cid+'.outcome-ack.intent.json':
                clock[0] = 555-remaining

        with mock.patch.object(nas.time, 'monotonic', side_effect=lambda: clock[0]), \
                mock.patch.object(nas, 'atomic_json', side_effect=slow_intent):
            result = nas.Puller(config, transport, fixtures.NOW).run()
        return result, transport, cid

    def test_44_seconds_left_does_not_dispatch_ack_or_collect_source(self):
        result, transport, cid = self.ack_window(44)
        self.assertEqual(transport.ack_batches, 0)
        self.assertEqual(result['acks_confirmed'], 0)
        self.assertEqual(result['acks_pending'], 2)
        self.assertEqual(result['source_deleted'], 0)
        self.assertEqual(result['errors'], [{'code': 'ack_batch_deferred_time_limit'}])
        self.assertTrue((self.root/(cid+'.input.json')).exists())

    def test_45_seconds_left_allows_exact_ack_and_confirmed_gc(self):
        result, transport, cid = self.ack_window(45)
        self.assertEqual(transport.ack_batches, 1)
        self.assertEqual(result['acks_confirmed'], 2)
        self.assertEqual(result['acks_pending'], 0)
        self.assertEqual(result['source_deleted'], 1)
        self.assertEqual(result['errors'], [])
        self.assertFalse((self.root/(cid+'.input.json')).exists())

    def test_actual_unknown_transport_result_cannot_be_labelled_deferred_or_gc(self):
        for exception in [nas.ArchiveError('transport_timeout'), nas.LimitReached('run_time_limit')]:
            with self.subTest(exception=type(exception).__name__):
                cid = self.producer.add(outcome=True, headers={})
                config = self.config(600)

                class UnknownTransport(BatchTransport):
                    def ack_batch(inner, claims):
                        super().ack_batch(claims)
                        raise exception

                transport = UnknownTransport(self.root, config, self.state_root)
                result = nas.Puller(config, transport, fixtures.NOW).run()
                self.assertGreater(transport.ack_batches, 0)
                self.assertEqual(result['acks_confirmed'], 0)
                self.assertEqual(result['source_deleted'], 0)
                self.assertIn({'code': 'ack_batch_outcome_unknown'}, result['errors'])
                self.assertNotIn({'code': 'ack_batch_deferred_time_limit'}, result['errors'])
                self.assertTrue((self.state_root/(cid+'.input.ack.json')).exists())
                self.assertTrue((self.state_root/(cid+'.outcome.ack.json')).exists())
                self.assertTrue(all((self.root/(cid+suffix)).exists() for suffix in
                                    ['.input.json', '.request.gz', '.headers.gz', '.outcome.json']))

    def test_slow_scan_does_not_reset_or_escape_deadline(self):
        cid = self.producer.add(outcome=True, headers={})
        config = self.config(600)
        transport = BatchTransport(self.root, config, self.state_root)
        clock = [0.0]
        scan = nas.namespace_size

        def too_slow(root, budget):
            clock[0] = 555
            return scan(root, budget)

        with mock.patch.object(nas.time, 'monotonic', side_effect=lambda: clock[0]), \
                mock.patch.object(nas, 'namespace_size', side_effect=too_slow), \
                mock.patch.object(transport, 'manifest_list', wraps=transport.manifest_list) as listing:
            with self.assertRaisesRegex(nas.LimitReached, 'run_time_limit'):
                nas.Puller(config, transport, fixtures.NOW).run()
        listing.assert_not_called()
        self.assertEqual(transport.ack_batches, 0)
        self.assertTrue((self.root/(cid+'.input.json')).exists())

    def test_600_budget_still_counts_unknown_files_against_namespace_cap(self):
        cid = self.producer.add(outcome=True, headers={})
        config = replace(self.config(600), max_payload_bytes=1024**2, max_namespace_bytes=1024**2)
        config.root.mkdir(mode=0o700)
        unknown = config.root/'unrecognized-retained-object'
        unknown.write_bytes(b'x'*1024**2)
        transport = BatchTransport(self.root, config, self.state_root)
        result = nas.Puller(config, transport, fixtures.NOW).run()
        self.assertEqual(transport.batches, 0)
        self.assertEqual(transport.ack_batches, 0)
        self.assertEqual(result['source_deleted'], 0)
        self.assertIn({'code': 'namespace_space_limit'}, result['errors'])
        self.assertTrue(unknown.exists())
        self.assertTrue((self.root/(cid+'.input.json')).exists())

    def test_window_bounds_cleanup_and_single_ssh_timeouts_remain_bounded(self):
        for seconds in [30, 290, 600]:
            self.config(seconds).validate()
        for seconds in [29, 601]:
            with self.assertRaisesRegex(nas.ArchiveError, 'invalid_resource_limit'):
                self.config(seconds).validate()
        puller = nas.Puller(self.config(600))
        self.assertEqual(puller.cleanup_reserve, 45)
        with mock.patch.object(nas.time, 'monotonic', return_value=100), \
                mock.patch.object(nas.SSHTransport, 'run', return_value=b'{}') as run:
            transport = nas.SSHTransport(replace(self.config(600), root=Path('/tmp/netcup-grok-test')), deadline=555)
            transport.helper('ack-batch')
            self.assertEqual(run.call_args.kwargs['timeout'], 45)
            transport.deadline = 110
            transport.helper('ack-batch')
            self.assertEqual(run.call_args.kwargs['timeout'], 10)


if __name__ == '__main__':
    unittest.main()
