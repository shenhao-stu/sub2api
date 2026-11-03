import copy
import json
from pathlib import Path
import tempfile
import unittest

import closeout


class CloseoutTests(unittest.TestCase):
    def setUp(self):
        self.probe = dict(id=7, request_id='57e632c2-5c03-4546-8ba4-85f04692c007',
            client_request_id='3b492f11-c22c-4322-b1a1-35a0e58145fb',
            api_key_id=2, user_id=1, group_id=3, user_agent='test-capture/1',
            proof_sha256='a' * 64)
        self.plan = {'schema_version': 1, 'probes': [self.probe]}
        self.observation = {'row': {k: self.probe[k] for k in closeout.MATCH},
            'system_upstream_rows': 0, 'billing': dict.fromkeys(closeout.TABLES, 0)}
        self.observation['row'].update(status_code=400, error_phase='request',
            error_message='Failed to parse request body', request_path='/v1/chat/completions',
            account_id=None, upstream_status_code=None, upstream_latency_ms=None,
            upstream_errors=None, resolved=False, resolved_at=None, resolved_by_user_id=None)

    def read(self, probe):
        return copy.deepcopy(self.observation)

    def resolve(self, probe):
        self.observation['row'].update(resolved=True, resolved_at='now', resolved_by_user_id=1)

    def test_default_is_read_only(self):
        def forbidden(_): self.fail('Unexpected mutation')
        result = closeout.closeout(self.plan, None, read=self.read, update=forbidden)
        self.assertTrue(result['read_only'])
        self.assertFalse(self.observation['row']['resolved'])

    def test_native_closeout_persists_intent_and_receipt(self):
        with tempfile.TemporaryDirectory() as d:
            closeout.closeout(self.plan, Path(d), True, self.read, self.resolve)
            self.assertTrue(json.loads((Path(d) / '7.receipt.json').read_text())['row']['resolved'])
            self.assertTrue((Path(d) / '7.intent.json').exists())

    def test_identity_and_error_changes_are_rejected(self):
        for key, value in [('id', 8), ('request_id', self.probe['client_request_id']),
                           ('api_key_id', 9), ('user_agent', 'customer'),
                           ('error_message', 'other error'), ('request_path', '/v1/messages')]:
            with self.subTest(key=key):
                obs = copy.deepcopy(self.observation)
                obs['row'][key] = value
                with self.assertRaises(ValueError): closeout.validate_observation(self.probe, obs)

    def test_upstream_evidence_rejects_closeout(self):
        for key, value in [('account_id', 12), ('upstream_status_code', 400),
                           ('upstream_latency_ms', 0), ('upstream_errors', [{}])]:
            with self.subTest(key=key):
                obs = copy.deepcopy(self.observation)
                obs['row'][key] = value
                with self.assertRaises(ValueError): closeout.validate_observation(self.probe, obs)
        self.observation['system_upstream_rows'] = 1
        with self.assertRaises(ValueError): closeout.validate_observation(self.probe, self.observation)

    def test_billing_must_be_complete_and_all_zero(self):
        for key in closeout.TABLES:
            for value in [1, -1, False]:
                obs = copy.deepcopy(self.observation)
                obs['billing'][key] = value
                with self.assertRaises(ValueError): closeout.validate_observation(self.probe, obs)
        del self.observation['billing']['provider_usage_receipts']
        with self.assertRaises(ValueError): closeout.validate_observation(self.probe, self.observation)

    def test_manifest_requires_unique_exact_ids_and_proof(self):
        for key, value in [('request_id', "'; SELECT 1"), ('id', True), ('proof_sha256', '')]:
            plan = copy.deepcopy(self.plan)
            plan['probes'][0][key] = value
            with self.assertRaises(ValueError): closeout.validate_plan(plan)
        self.plan['probes'].append(copy.deepcopy(self.probe))
        with self.assertRaises(ValueError): closeout.validate_plan(self.plan)

    def test_all_probes_are_checked_before_any_mutation(self):
        p2 = dict(self.probe, id=8, request_id='d22d7172-472f-48ab-ad62-a24986343f3c')
        self.plan['probes'].append(p2)
        def read(p):
            result = self.read(p)
            result['row'].update({k: p[k] for k in closeout.MATCH})
            if p['id'] == 8: result['billing']['usage_logs'] = 1
            return result
        def forbidden(_): self.fail('Mutated before validating all probes')
        with tempfile.TemporaryDirectory() as d, self.assertRaises(ValueError):
            closeout.closeout(self.plan, Path(d), True, read, forbidden)

    def test_unknown_put_result_is_not_retried(self):
        calls = []
        def unknown(p):
            calls.append(p['id'])
            raise TimeoutError('unknown native PUT result')
        with tempfile.TemporaryDirectory() as d:
            for error in (TimeoutError, FileExistsError):
                with self.assertRaises(error):
                    closeout.closeout(self.plan, Path(d), True, self.read, unknown)
            self.assertEqual(calls, [7])

    def test_already_resolved_needs_no_put(self):
        self.resolve(self.probe)
        with tempfile.TemporaryDirectory() as d:
            result = closeout.closeout(self.plan, Path(d), True, self.read,
                lambda _: self.fail('Repeated PUT'))
            self.assertTrue(result['results'][0]['already_resolved'])

    def test_race_before_put_is_rechecked(self):
        count = 0
        def changing(p):
            nonlocal count
            count += 1
            if count == 2: self.observation['row']['account_id'] = 5
            return self.read(p)
        with tempfile.TemporaryDirectory() as d, self.assertRaises(ValueError):
            closeout.closeout(self.plan, Path(d), True, changing,
                lambda _: self.fail('Unverified PUT'))

    def test_readback_must_show_native_persistence(self):
        with tempfile.TemporaryDirectory() as d, self.assertRaises(ValueError):
            closeout.closeout(self.plan, Path(d), True, self.read, lambda _: None)


if __name__ == '__main__':
    unittest.main()
