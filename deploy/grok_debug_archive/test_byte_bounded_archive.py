"""Large customer captures must make durable progress on a slower link."""
import gzip
import os
import unittest
from unittest import mock

import nas_archive as nas
import source_helper as source
from test_batch_archive import BatchTransport
from test_source_helper import SourceBehavior, NOW


class ByteBoundedArchive(unittest.TestCase):
    setUp = SourceBehavior.setUp
    tearDown = SourceBehavior.tearDown

    def config(self):
        return nas.Config(root=self.base/'netcup-grok-test', group_id=78,
                          source_dir=str(self.root), helper_path=str(source.INSTALL_PATH),
                          min_free_bytes=0, max_objects=1000, workers=8, max_seconds=600)

    def test_26_mib_page_archives_over_link_with_8_mib_transfer_window(self):
        raw = os.urandom(224 * 1024)
        ids = [self.producer.add(raw=raw, outcome=True, headers={'X-Test': ['synthetic']})
               for _ in range(128)]
        sizes = []

        class SlowLink(BatchTransport):
            def fetch_batch(inner, descriptors, directory, timeout):
                size = sum(d['bytes'] for d in descriptors)
                nas.require(size <= 8 * 1024**2, 'transport_timeout')
                sizes.append(size)
                return super().fetch_batch(descriptors, directory, timeout)

        transport = SlowLink(self.root, self.config(), self.state_root)
        result = nas.Puller(self.config(), transport, NOW).run()
        self.assertEqual(result['errors'], [])
        self.assertEqual(result['acks_confirmed'], 256)
        self.assertEqual(result['source_deleted'], 128)
        self.assertGreater(len(sizes), 1)
        for cid in ids:
            folder = self.config().root/'captures'/cid
            self.assertEqual(gzip.decompress((folder/(cid+'.request.gz')).read_bytes()), raw)
            self.assertTrue((folder/(cid+'.outcome-ack.done.json')).exists())
            self.assertFalse((self.root/(cid+'.request.gz')).exists())

    def test_single_large_transfer_has_bounded_time_and_keeps_ack_window(self):
        cid = self.producer.add(raw=os.urandom(9 * 1024**2), outcome=True, headers={})
        clock = [0.0]
        scan = nas.namespace_size
        def slow_scan(root, budget):
            clock[0] += 400
            return scan(root, budget)

        class SlowLink(BatchTransport):
            def fetch_batch(inner, descriptors, directory, timeout):
                self.assertGreaterEqual(timeout, 80)
                self.assertLessEqual(timeout, 180)
                self.assertLessEqual(timeout, 600-clock[0]-45-45)
                clock[0] += 80
                return super().fetch_batch(descriptors, directory, timeout)

        transport = SlowLink(self.root, self.config(), self.state_root)
        with mock.patch.object(nas.time, 'monotonic', side_effect=lambda: clock[0]), \
                mock.patch.object(nas, 'namespace_size', side_effect=slow_scan):
            result = nas.Puller(self.config(), transport, NOW).run()
        self.assertEqual(result['errors'], [])
        self.assertEqual(result['source_deleted'], 1)
        self.assertFalse((self.root/(cid+'.input.json')).exists())


if __name__ == '__main__':
    unittest.main()
