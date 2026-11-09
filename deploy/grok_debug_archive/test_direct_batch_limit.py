"""Slow direct links must make durable progress without changing ACK or GC rules."""
import gzip
import random
import unittest
from dataclasses import replace
from unittest import mock

import nas_archive as nas
import source_helper as source
from test_batch_archive import BatchTransport
import test_source_helper as fixtures


class DirectBatchLimit(unittest.TestCase):
    setUp = fixtures.SourceBehavior.setUp
    tearDown = fixtures.SourceBehavior.tearDown

    def config(self):
        return nas.Config(root=self.base/'netcup-grok-test', group_id=78,
                          source_dir=str(self.root), helper_path=str(source.INSTALL_PATH),
                          min_free_bytes=0, max_objects=1000, workers=8, max_seconds=600)

    def slow_link(self, limit):
        raw = random.Random(7).randbytes(192 * 1024)
        ids = [self.producer.add(raw=raw, outcome=True, headers={}) for _ in range(50)]
        config = self.config()
        if limit is not None:
            config = replace(config, max_transfer_batch_bytes=limit)
        clock = [0.0]
        sizes = []

        class SlowTransport(BatchTransport):
            def fetch_batch(inner, descriptors, directory, timeout):
                size = sum(d['bytes'] for d in descriptors)
                sizes.append(size)
                seconds = size / (40 * 1024)
                clock[0] += min(seconds, timeout)
                nas.require(seconds <= timeout, 'transport_timeout')
                return super().fetch_batch(descriptors, directory, timeout)

        transport = SlowTransport(self.root, config, self.state_root)
        with mock.patch.object(nas.time, 'monotonic', side_effect=lambda: clock[0]):
            result = nas.Puller(config, transport, fixtures.NOW).run()
        return config, result, ids, raw, sizes

    def test_default_batch_reproduces_direct_timeout_and_keeps_source(self):
        _, result, ids, _, _ = self.slow_link(None)
        self.assertIn({'code': 'transport_timeout'}, result['errors'])
        self.assertEqual(result['acks_confirmed'], 0)
        self.assertEqual(result['source_deleted'], 0)
        self.assertTrue(all((self.root/(cid+'.input.json')).exists() for cid in ids))

    def test_small_batches_finish_full_headers_body_and_double_ack(self):
        config, result, ids, raw, sizes = self.slow_link(1024**2)
        self.assertEqual(result['errors'], [])
        self.assertEqual(result['input_archived'], 50)
        self.assertEqual(result['outcome_archived'], 50)
        self.assertEqual(result['acks_confirmed'], 100)
        self.assertEqual(result['source_deleted'], 50)
        self.assertTrue(all(size <= 1024**2 for size in sizes))
        for cid in ids:
            directory = config.root/'captures'/cid
            self.assertEqual(gzip.decompress((directory/(cid+'.request.gz')).read_bytes()), raw)
            self.assertTrue((directory/(cid+'.headers.gz')).exists())
            self.assertTrue(all((directory/(cid+'.'+kind+'-ack.done.json')).exists()
                                for kind in ['input', 'outcome']))
            self.assertFalse((self.root/(cid+'.input.json')).exists())

    def test_limit_validation_and_oversized_capture_remains_whole(self):
        for value in [True, 0, 65535, 8*1024**2+1, '1048576']:
            with self.subTest(value=value), self.assertRaisesRegex(nas.ArchiveError, 'invalid_transfer_batch_limit'):
                replace(self.config(), max_transfer_batch_bytes=value).validate()
        for value in [65536, 1024**2, 8*1024**2]:
            replace(self.config(), max_transfer_batch_bytes=value).validate()
        sizes = [400000, 400000, 2000000, 400000]
        items = [{'payload': {'bytes': size}} for size in sizes]
        self.assertEqual(list(nas.transfer_batches(items, 1024**2)),
                         [items[:2], items[2:3], items[3:]])
