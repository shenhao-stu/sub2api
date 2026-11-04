"""Distinguish uncompleted input downloads from damaged acknowledged bundles."""
import copy
import datetime as dt
import os
from pathlib import Path
import unittest
import uuid

import test_nas_archive as fixture
from nas_archive import ArchiveError


class PendingInputRetention(unittest.TestCase):
    def setUp(self):
        self.f = fixture.ArchiveBehavior()
        self.f.setUp()

    def tearDown(self):
        self.f.tearDown()

    def pending(self, cid, raw=b'', name=None):
        directory = self.f.directory(cid)
        directory.mkdir(parents=True, mode=0o700, exist_ok=True)
        for path in [self.f.root, self.f.root/'captures', directory]:
            path.chmod(0o700)
        path = directory / (name or ('.'+cid+'.input.json.pending-'+uuid.uuid4().hex))
        path.write_bytes(raw)
        path.chmod(0o600)
        return path

    def test_pending_input_remains_visible_and_can_resume_when_relisted(self):
        for raw in [b'', b'{"unfinished":', b'complete-sized but unverified temporary data']:
            with self.subTest(raw_length=len(raw)):
                cid = self.f.source.add(headers={}, outcome=True)
                item = copy.deepcopy(self.f.source.items[-1])
                path = self.pending(cid, raw)
                self.f.source.items = []
                result = self.f.pull()
                self.assertEqual(result['errors'], [])
                self.assertGreaterEqual(result['retention_pending_inputs'], 1)
                self.assertEqual(result['status'], 'pending')
                self.assertEqual(result['expired'], 0)
                self.assertEqual(path.read_bytes(), raw)
                self.assertNotIn((cid, 'input'), self.f.source.ack_calls)
                self.f.source.items = [item]
                result = self.f.pull()
                self.assertTrue((self.f.directory(cid)/(cid+'.input-ack.done.json')).is_file())
                self.assertTrue((self.f.directory(cid)/(cid+'.outcome-ack.done.json')).is_file())
                self.assertEqual(path.read_bytes(), raw)

    def test_complete_bundle_missing_manifest_still_alarms_even_with_pending(self):
        cid = self.f.source.add(headers={}, outcome=True)
        self.f.pull()
        self.f.source.items = []
        path = self.pending(cid)
        (self.f.directory(cid)/(cid+'.input.json')).unlink()
        result = self.f.pull(now=fixture.NOW+dt.timedelta(days=31))
        self.assertIn('missing_local_file', [e['code'] for e in result['errors']])
        self.assertEqual(result['retention_pending_inputs'], 0)
        self.assertEqual(result['expired'], 0)
        self.assertTrue(path.is_file())
        self.assertTrue((self.f.directory(cid)/(cid+'.request.gz')).is_file())

    def test_unknown_file_and_bad_pending_are_not_silenced(self):
        for name, raw in [('unknown', b'x'), ('pending-no-capture-binding', b'x'),
                          (None, b'x'*65537)]:
            with self.subTest(name=name, length=len(raw)):
                cid = str(uuid.uuid4())
                path = self.pending(cid, raw, name)
                result = self.f.pull()
                self.assertTrue(result['errors'])
                self.assertEqual(result['retention_pending_inputs'], 0)
                self.assertEqual(result['expired'], 0)
                self.assertTrue(path.is_file())

    def test_pending_links_are_never_adopted(self):
        cid = str(uuid.uuid4())
        path = self.pending(cid, b'keep')
        alias = path.with_name('outside-link')
        os.link(path, alias)
        result = self.f.pull()
        self.assertTrue(result['errors'])
        self.assertEqual(result['expired'], 0)
        self.assertTrue(path.exists())

    def test_receipt_or_ack_alone_prevents_partial_classification(self):
        for suffix in ['.input-receipt.json', '.input-ack.intent.json', '.input-ack.done.json',
                       '.outcome-receipt.json', '.outcome-ack.done.json']:
            with self.subTest(suffix=suffix):
                cid = str(uuid.uuid4())
                self.pending(cid)
                marker = self.f.directory(cid)/(cid+suffix)
                marker.write_bytes(b'{}')
                marker.chmod(0o600)
                result = self.f.pull()
                self.assertIn('missing_local_file', [e['code'] for e in result['errors']])
                self.assertEqual(result['retention_pending_inputs'], 0)
                self.assertTrue(marker.is_file())

    def test_empty_directory_stays_unknown_and_pending_symlink_is_rejected(self):
        cid = str(uuid.uuid4())
        path = self.pending(cid)
        path.unlink()
        result = self.f.pull()
        self.assertIn('missing_local_file', [e['code'] for e in result['errors']])
        self.assertEqual(result['retention_pending_inputs'], 0)
        outside = self.f.base/'outside'
        outside.write_bytes(b'keep')
        outside.chmod(0o600)
        path.symlink_to(outside)
        with self.assertRaises(ArchiveError):
            self.f.pull()
        self.assertEqual(outside.read_bytes(), b'keep')


if __name__ == '__main__':
    unittest.main()
