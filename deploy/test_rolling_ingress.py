import copy
import unittest

from rolling_ingress import candidate_config, ingress_config, replace_public_upstream, transition_nat, validate_overlap


class RollingIngressTests(unittest.TestCase):
    def test_candidate_retains_billing_and_auth_but_cannot_claim_live_alias(self):
        source = {"State": {"Running": True}, "Image": "sha256:old", "Config": {
            "Env": ["AUTH=opaque", "QUOTA=true"], "Entrypoint": ["/start"], "Labels": {}, "Hostname": "old",
        }, "HostConfig": {"Binds": ["/billing:/app/data"], "PortBindings": {"8181/tcp": []}, "NetworkMode": "app"}}
        saved = copy.deepcopy(source)
        result = candidate_config(source, "sha256:new", {"app": "172.22.0.241"}, 8197, {"release": "g30"})
        self.assertEqual(source, saved)
        self.assertEqual(result["Env"], saved["Config"]["Env"])
        self.assertEqual(result["HostConfig"]["Binds"], saved["HostConfig"]["Binds"])
        self.assertNotIn("Aliases", result["NetworkingConfig"]["EndpointsConfig"]["app"])
        self.assertEqual(result["HostConfig"]["PortBindings"]["8181/tcp"][0]["HostIp"], "127.0.0.1")

    def test_proxy_never_configures_automatic_retry_or_response_deadline(self):
        route = ingress_config("172.22.0.241:8181")["apps"]["http"]["servers"]["ingress"]["routes"][0]["handle"][0]
        self.assertEqual(route["load_balancing"], {"retries": 0, "try_duration": 0})
        self.assertNotIn("response_header_timeout", route["transport"])
        self.assertGreaterEqual(route["stream_close_delay"], 20 * 60 * 1_000_000_000)

    def test_public_cutover_changes_only_one_dial(self):
        old = ingress_config("127.0.0.1:8195")
        new = replace_public_upstream(old, "127.0.0.1:8195", "127.0.0.1:8196")
        self.assertEqual(replace_public_upstream(new, "127.0.0.1:8196", "127.0.0.1:8195"), old)
        with self.assertRaises(ValueError):
            replace_public_upstream(old, "not-present", "127.0.0.1:8196")

    def test_nat_has_rollback_exclusion_and_no_connection_flush(self):
        rules = transition_nat("test_handover", [("172.22.0.9", "172.22.0.240")])
        self.assertIn("ip saddr != 172.22.0.240", rules)
        self.assertNotIn("flush", rules)
        self.assertNotIn("reject", rules)
        with self.assertRaises(ValueError):
            transition_nat("invalid;table", [("172.22.0.9", "172.22.0.240")])

    def test_overlap_fails_closed_for_every_unsupported_condition(self):
        proof = {"captured_at": 100, "scheduled_paid_tests": 0, "active_v1_monitors": 0,
                 "platform_quota_rows": 0, "active_plugins": 0, "platform_quota_flusher_enabled": False,
                 "migration_equal": True, "journal_equal": True, "free_bytes": 20 * 1024**3}
        validate_overlap(proof, 101)
        for key, value in {"captured_at": 0, "scheduled_paid_tests": 1, "active_v1_monitors": 1,
                           "platform_quota_rows": 1, "active_plugins": 1, "platform_quota_flusher_enabled": True,
                           "migration_equal": False, "journal_equal": False, "free_bytes": 1}.items():
            with self.subTest(key=key), self.assertRaises(ValueError):
                validate_overlap(dict(proof, **{key: value}), 101)


if __name__ == "__main__":
    unittest.main()
