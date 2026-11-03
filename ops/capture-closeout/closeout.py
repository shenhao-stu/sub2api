"""Resolve proven synthetic Ops rejections through the native admin API.

Default: read-only validation. No inference requests or automatic mutation retries.
Run on the database/admin host with an operator-reviewed manifest; never expose
this command as a public endpoint or derive manifests from client headers.
"""
import argparse
import datetime
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import urllib.request
import uuid

TABLES = ('usage_logs', 'usage_billing_dedup', 'usage_billing_dedup_archive',
          'provider_usage_receipts')
MATCH = ('id', 'request_id', 'client_request_id', 'api_key_id', 'user_id',
         'group_id', 'user_agent')


def require(condition, message):
    if not condition:
        raise ValueError(message)


def validate_plan(plan):
    require(plan.get('schema_version') == 1, 'Unknown manifest schema')
    probes = plan.get('probes', [])
    require(0 < len(probes) <= 100, 'Expected 1–100 reviewed probes')
    ids, request_ids = set(), set()
    for p in probes:
        for key in ('id', 'api_key_id', 'user_id', 'group_id'):
            require(type(p.get(key)) is int and p[key] > 0, 'Invalid ' + key)
        for key in ('request_id', 'client_request_id'):
            require(str(uuid.UUID(p[key])) == p[key], 'Invalid ' + key)
        require(p['id'] not in ids and p['request_id'] not in request_ids,
                'Duplicate probe')
        require(bool(p.get('user_agent')), 'Missing expected agent')
        require(re.fullmatch(r'[0-9a-f]{64}', p.get('proof_sha256', '')),
                'Missing immutable acceptance proof hash')
        ids.add(p['id'])
        request_ids.add(p['request_id'])
    return probes


def validate_observation(probe, observation):
    row = observation['row']
    require(all(row.get(k) == probe[k] for k in MATCH), 'Probe identity changed')
    require(row['status_code'] == 400 and row['error_phase'] == 'request'
            and row['error_message'] == 'Failed to parse request body'
            and row['request_path'] == '/v1/chat/completions',
            'Not the expected local parse rejection')
    require(row['account_id'] is None and row['upstream_status_code'] is None
            and row['upstream_latency_ms'] is None
            and row['upstream_errors'] in (None, []), 'Upstream evidence present')
    require(observation['system_upstream_rows'] == 0, 'System upstream evidence present')
    require(set(observation['billing']) == set(TABLES)
            and all(type(v) is int and v == 0 for v in observation['billing'].values()),
            'Billing evidence present or incomplete')
    require(type(row['resolved']) is bool, 'Unknown resolution state')


def db(query):
    sql = ("BEGIN READ ONLY; SET LOCAL statement_timeout='12s'; "
           "SELECT COALESCE(jsonb_agg(t),'[]'::jsonb) FROM (" + query + ")t; COMMIT;")
    result = subprocess.run(
        ['docker', 'exec', '-i', 'relay-db-postgres-1', 'psql', '-X', '-U',
         'postgres', '-d', 'sub2api', '-At', '-v', 'ON_ERROR_STOP=1'],
        input=sql, capture_output=True, text=True, timeout=20, check=True)
    return next(json.loads(line) for line in result.stdout.splitlines() if line.startswith('['))


def observe(probe):
    fields = ','.join(MATCH + ('status_code', 'error_phase', 'error_message',
        'request_path', 'account_id', 'upstream_status_code', 'upstream_latency_ms',
        'upstream_errors', 'resolved', 'resolved_at', 'resolved_by_user_id'))
    rows = db('SELECT ' + fields + ' FROM ops_error_logs WHERE id=' + str(probe['id']))
    require(len(rows) == 1, 'Expected one durable Ops row')
    ids = [probe['request_id'], probe['client_request_id']]
    quoted = ','.join("'" + x + "'" for x in ids)
    system = db("SELECT count(*) n FROM ops_system_logs WHERE (request_id IN ("
        + quoted + ') OR client_request_id IN (' + quoted + ")) AND (account_id IS NOT NULL "
        "OR extra ?| ARRAY['upstream_request_id','selected_account_id','upstream_status_code'])")[0]['n']
    types = {r['table_name']: r['data_type'] for r in db(
        "SELECT table_name,data_type FROM information_schema.columns WHERE "
        "table_schema='public' AND column_name='request_id' AND table_name IN ("
        + ','.join("'" + t + "'" for t in TABLES) + ')')}
    require(set(types) == set(TABLES), 'Billing schema incomplete')
    billing = {}
    for table in TABLES:
        values = ids if types[table] == 'uuid' else ids + ['client:' + x for x in ids]
        literals = ','.join("'" + x + "'" for x in values)
        billing[table] = db('SELECT count(*) n FROM ' + table
                           + ' WHERE request_id IN (' + literals + ')')[0]['n']
    return {'row': rows[0], 'system_upstream_rows': system, 'billing': billing}


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None


def resolve(probe):
    key = db("SELECT value FROM settings WHERE key='admin_api_key'")[0]['value']
    require(bool(key), 'Admin API key unavailable')
    request = urllib.request.Request(
        'http://127.0.0.1:8195/api/v1/admin/ops/request-errors/'
        + str(probe['id']) + '/resolve', data=b'{"resolved":true}', method='PUT',
        headers={'Content-Type': 'application/json', 'x-api-key': key,
                 'User-Agent': 'capture-closeout/1'})
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect())
    with opener.open(request, timeout=15) as response:
        require(response.status == 200, 'Native resolve failed')
        result = json.load(response)
    require(result.get('code') == 0 and result.get('data', {}).get('ok') is True,
            'Unexpected native resolve response')


def save_once(path, value):
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, 'w') as f:
        json.dump(value, f, sort_keys=True)
        f.flush()
        os.fsync(f.fileno())
    fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def closeout(plan, directory, execute=False, read=observe, update=resolve):
    probes = validate_plan(plan)
    baseline = [read(p) for p in probes]
    for p, observation in zip(probes, baseline):
        validate_observation(p, observation)
    if not execute:
        return {'read_only': True, 'observations': baseline}
    directory.mkdir(mode=0o700, parents=True, exist_ok=True)
    digest = hashlib.sha256(json.dumps(plan, sort_keys=True).encode()).hexdigest()
    results = []
    for probe in probes:
        observation = read(probe)
        validate_observation(probe, observation)
        if observation['row']['resolved']:
            results.append({'id': probe['id'], 'already_resolved': True})
            continue
        prefix = directory / str(probe['id'])
        save_once(prefix.with_suffix('.intent.json'), {
            'at': datetime.datetime.now(datetime.timezone.utc).isoformat(),
            'manifest_sha256': digest, 'probe': probe, 'before': observation})
        # Any unknown result leaves the durable intent. Reconcile read-only;
        # do not automatically repeat a PUT or the original model request.
        update(probe)
        after = read(probe)
        validate_observation(probe, after)
        require(after['row']['resolved'] and after['row']['resolved_at']
                and after['row']['resolved_by_user_id'], 'Resolution was not persisted')
        save_once(prefix.with_suffix('.receipt.json'), after)
        results.append({'id': probe['id'], 'resolved': True})
    return {'read_only': False, 'results': results}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('manifest', type=Path)
    parser.add_argument('--execute', action='store_true')
    parser.add_argument('--receipt-dir', type=Path)
    args = parser.parse_args()
    require(not args.execute or args.receipt_dir is not None, 'Execution requires a receipt directory')
    print(json.dumps(closeout(json.loads(args.manifest.read_text()), args.receipt_dir,
                              args.execute), sort_keys=True))
