#!/usr/bin/env python3
"""Reconcile confirmed Grok Free OAuth accounts using native admin APIs."""
import argparse
import fcntl
import hashlib
import json
import pathlib
import sys
import urllib.error
import urllib.request

HELPER = pathlib.Path('/opt/getoken-consolidated/builds/cpa-pro-scheduling-20261022/evidence_query.py')
HELPER_SHA256 = 'ad7038e294571a17e0c9f141e35119d81d3fb3e6bffabbfae517a53230b8700d'
STATE = pathlib.Path('/opt/getoken-consolidated/builds/grok-capture-capacity-20261109')
ACCOUNT_QUERY = """select a.id,a.platform,a.type,a.schedulable,
 lower(trim(a.credentials->>'subscription_tier')) tier,
 (select array_agg(g.group_id order by g.group_id) from account_groups g where g.account_id=a.id) groups
 from accounts a where a.deleted_at is null and """


def actions(account):
    if not (account['platform'] == 'grok' and account['type'] == 'oauth'
            and account['tier'] == 'free'):
        return []
    groups = account['groups'] or []
    plan = [('pause', False)] if account['schedulable'] else []
    if 78 in groups and len(groups) > 1:
        plan.append(('groups', [g for g in groups if g != 78]))
    return plan


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        raise RuntimeError('Unexpected admin API redirect')


def reconcile(account, read, mutate):
    latest = read(account['id'])
    if latest != account:
        return {'id': account['id'], 'result': 'changed-before-update'}
    changes = actions(latest)
    for kind, value in changes:
        if read(account['id']) != latest:
            return {'id': account['id'], 'result': 'changed-before-update'}
        expected = dict(latest)
        expected['groups' if kind == 'groups' else 'schedulable'] = value
        try:
            mutate(account['id'], kind, value)
        except (TimeoutError, OSError, urllib.error.URLError) as exc:
            # Never repeat an uncertain mutation; read back its postcondition.
            if read(account['id']) != expected:
                raise RuntimeError(f'Account {account["id"]} update uncertain ({type(exc).__name__})') from None
        latest = read(account['id'])
        if latest != expected:
            raise RuntimeError(f'Account {account["id"]} postcondition changed')
    return {'id': account['id'], 'result': 'updated' if changes else 'unchanged',
            'actions': [kind for kind, _ in changes], 'schedulable': latest['schedulable'],
            'groups': latest['groups']}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--apply', action='store_true')
    args = parser.parse_args()
    if hashlib.sha256(HELPER.read_bytes()).hexdigest() != HELPER_SHA256:
        raise RuntimeError('Evidence database helper changed; review required')
    sys.path.insert(0, str(HELPER.parent))
    from evidence_query import db
    with (STATE / 'free-tier.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        candidates = db('sub2api', ACCOUNT_QUERY + "a.platform='grok' and a.type='oauth' and lower(trim(a.credentials->>'subscription_tier'))='free' order by a.id")
        planned = [(a, actions(a)) for a in candidates]
        planned = [(a, plan) for a, plan in planned if plan]
        if not args.apply:
            print(json.dumps({'planned': [{'id': a['id'], 'groups': a['groups'], 'actions': plan} for a, plan in planned]}))
            return
        if len(planned) > 300:
            raise RuntimeError('Unexpected fleet growth; review required')
        if not planned:
            print(json.dumps({'updated': 0, 'remaining': 0}))
            return
        secret, = db('sub2api', "select value from settings where key='admin_api_key'")
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect())

        def read(account_id):
            rows = db('sub2api', ACCOUNT_QUERY + f'a.id={int(account_id)}')
            return rows[0] if rows else None

        def mutate(account_id, kind, value):
            path = f'/api/v1/admin/accounts/{int(account_id)}'
            method, payload = 'PUT', {'group_ids': value}
            if kind == 'pause':
                path += '/schedulable'
                method, payload = 'POST', {'schedulable': False}
            req = urllib.request.Request('http://127.0.0.1:8195' + path,
                data=json.dumps(payload).encode(), method=method,
                headers={'x-api-key': secret['value'], 'Content-Type': 'application/json', 'User-Agent': 'Grok-Free-Guard/2'})
            try:
                with opener.open(req, timeout=12) as response:
                    result = json.load(response)
            except urllib.error.HTTPError as exc:
                raise RuntimeError(f'Account {account_id} admin HTTP {exc.code}') from None
            if result.get('code') != 0:
                raise RuntimeError(f'Account {account_id} admin update rejected')

        failures = 0
        for account, _ in planned:
            try:
                print(json.dumps(reconcile(account, read, mutate)), flush=True)
            except RuntimeError as exc:
                failures += 1
                print(json.dumps({'id': account['id'], 'error': str(exc)}), flush=True)
        remaining = db('sub2api', ACCOUNT_QUERY + "a.platform='grok' and a.type='oauth' and lower(trim(a.credentials->>'subscription_tier'))='free'")
        count = sum(bool(actions(a)) for a in remaining)
        print(json.dumps({'planned': len(planned), 'failures': failures, 'remaining': count}))
        if failures or count:
            raise SystemExit(1)


if __name__ == '__main__':
    main()
