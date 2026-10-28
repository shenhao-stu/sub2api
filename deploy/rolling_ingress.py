"""Small building blocks for an operator-reviewed blue/green handover.

They never retry requests or decide when a billing process may be stopped.
Deployment-specific identities, background-job exclusions and settlement proofs
belong in the release manifest, not in an implicit 'latest' lookup.
"""
import copy
import ipaddress
import json


def ingress_config(upstream, listen=":8181"):
    host, port = upstream.rsplit(":", 1)
    ipaddress.ip_address(host)
    if not 1 <= int(port) <= 65535:
        raise ValueError("invalid upstream port")
    return {
        "admin": {"listen": "127.0.0.1:2019"},
        "apps": {"http": {"servers": {"ingress": {
            "listen": [listen],
            "routes": [{"handle": [{
                "handler": "reverse_proxy",
                "upstreams": [{"dial": upstream}],
                "flush_interval": -1,
                "stream_close_delay": 22 * 60 * 1_000_000_000,
                "transport": {
                    "protocol": "http", "dial_timeout": 5_000_000_000,
                    "keep_alive": {"idle_timeout": 60_000_000_000},
                },
                "load_balancing": {"retries": 0, "try_duration": 0},
            }]}],
        }}}},
    }


def candidate_config(source, image, endpoints, host_port, release):
    """Keep auth, persistent billing state and runtime settings; isolate ingress."""
    if not source["State"]["Running"]:
        raise ValueError("source must still be serving")
    if not image.startswith("sha256:") or image == source["Image"]:
        raise ValueError("candidate must have a distinct pinned image")
    config = copy.deepcopy(source["Config"])
    config["Image"] = image
    config.pop("Hostname", None)
    config["Labels"] = dict(config.get("Labels") or {}, **release)
    host = copy.deepcopy(source["HostConfig"])
    host["PortBindings"] = {"8181/tcp": [{"HostIp": "127.0.0.1", "HostPort": str(host_port)}]}
    host["RestartPolicy"] = {"Name": "no", "MaximumRetryCount": 0}
    host["NetworkMode"] = next(iter(endpoints))
    config["HostConfig"] = host
    config["NetworkingConfig"] = {"EndpointsConfig": {
        network: {"IPAMConfig": {"IPv4Address": address}}
        for network, address in endpoints.items()
    }}
    return config


def transition_nat(table, pairs, published_ports=()):
    """Redirect only new connections to retired addresses; preserve conntrack.

    Ingress-to-old traffic is excluded so rollback never loops through ingress.
    No flush of another table, no input-policy changes and no automatic expiry.
    """
    if not table.replace("_", "").isalnum():
        raise ValueError("invalid table name")
    rules = []
    for old, ingress in pairs:
        ipaddress.IPv4Address(old)
        ipaddress.IPv4Address(ingress)
        if old == ingress:
            raise ValueError("ingress cannot reuse the live source address")
        rules.append(f"ip daddr {old} ip saddr != {ingress} tcp dport 8181 tcp flags & (syn | ack) == syn dnat to {ingress}:8181")
    if not rules:
        raise ValueError("empty transition")
    chains = []
    for name, hook in (("incoming", "prerouting"), ("local", "output")):
        current = list(rules)
        if hook == "output":
            for old_port, ingress_port in published_ports:
                if any(type(port) is not int or not 1024 <= port <= 65535 for port in (old_port, ingress_port)):
                    raise ValueError("invalid loopback published port")
                if old_port == ingress_port:
                    raise ValueError("loopback handover must change port")
                current.append(f"ip daddr 127.0.0.1 tcp dport {old_port} tcp flags & (syn | ack) == syn dnat to 127.0.0.1:{ingress_port}")
        chains.append(f"chain {name} {{ type nat hook {hook} priority -110; policy accept;\n" + "\n".join(current) + "\n}")
    return f"table ip {table} {{\n" + "\n".join(chains) + "\n}\n"


def replace_public_upstream(config, old, new):
    result = copy.deepcopy(config)
    count = 0

    def visit(value):
        nonlocal count
        if isinstance(value, dict):
            if value.get("handler") == "reverse_proxy":
                for upstream in value.get("upstreams", []):
                    if upstream.get("dial") == old:
                        if value.get("stream_close_delay", 0) < 20 * 60 * 1_000_000_000:
                            raise ValueError("public ingress has insufficient existing stream grace")
                        upstream["dial"] = new
                        count += 1
            for item in value.values():
                visit(item)
        elif isinstance(value, list):
            for item in value:
                visit(item)

    visit(result)
    if count != 1:
        raise ValueError("public upstream must occur exactly once")
    return result


def validate_overlap(proof, now):
    """Conservative support boundary for this fork, not generic HA enablement."""
    if not 0 <= now - proof["captured_at"] <= 60:
        raise ValueError("stale overlap proof")
    for key in ("scheduled_paid_tests", "active_v1_monitors", "platform_quota_rows", "active_plugins"):
        if type(proof[key]) is not int or proof[key] != 0:
            raise ValueError("unsupported overlapping background work: " + key)
    if proof["platform_quota_flusher_enabled"] is not False:
        raise ValueError("quota snapshot flusher cannot overlap")
    if proof["migration_equal"] is not True or proof["journal_equal"] is not True:
        raise ValueError("billing persistence changed")
    if proof["free_bytes"] < 18 * 1024**3:
        raise ValueError("insufficient disk reserve")


def encode(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
