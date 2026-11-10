import copy
import unittest
import grok_free_guard as guard


def account(groups=None, **updates):
    a = dict(id=1, platform='grok', type='oauth', tier='free', schedulable=True, groups=groups or [28])
    a.update(updates)
    return a


class FreeGuardTest(unittest.TestCase):
    def test_pause_reaches_ordinary_group_after_test_unbound(self):
        self.assertEqual(guard.actions(account([28])), [('pause', False)])

    def test_pause_removes_test_binding_after_stopping_shared_account(self):
        self.assertEqual(guard.actions(account([28, 78, 95])), [('pause', False), ('groups', [28, 95])])

    def test_single_test_group_stays_bound_but_disabled(self):
        self.assertEqual(guard.actions(account([78])), [('pause', False)])

    def test_image_only_free_is_paused_without_losing_groups(self):
        self.assertEqual(guard.actions(account([95])), [('pause', False)])

    def test_partially_completed_pause_retries_only_group_cleanup(self):
        self.assertEqual(guard.actions(account([28, 78], schedulable=False)), [('groups', [28])])

    def test_unknown_paid_apikey_and_already_paused_unchanged(self):
        for patch in [dict(tier=None), dict(tier='supergrok_heavy'), dict(type='apikey'), dict(platform='anthropic'), dict(schedulable=False)]:
            self.assertEqual(guard.actions(account(**patch)), [])

    def test_changed_plan_is_never_modified(self):
        before = account()
        def mutate(*args):
            self.fail('Unexpected update')
        result = guard.reconcile(before, lambda _: account(tier='supergrok_heavy'), mutate)
        self.assertEqual(result['result'], 'changed-before-update')

    def test_partial_success_timeout_not_repeated_and_second_run_idempotent(self):
        state = account([28, 78, 95])
        calls = []
        def read(_): return copy.deepcopy(state)
        def mutate(_, kind, value):
            calls.append(kind)
            state['schedulable' if kind == 'pause' else 'groups'] = value
            if kind == 'pause': raise TimeoutError()
        guard.reconcile(read(1), read, mutate)
        self.assertEqual(calls, ['pause', 'groups'])
        guard.reconcile(read(1), read, mutate)
        self.assertEqual(calls, ['pause', 'groups'])
        self.assertEqual(state['groups'], [28, 95])

    def test_failed_write_is_not_retried(self):
        state = account()
        calls = []
        def mutate(*args):
            calls.append(1)
            raise TimeoutError()
        with self.assertRaises(RuntimeError):
            guard.reconcile(state, lambda _: copy.deepcopy(state), mutate)
        self.assertEqual(calls, [1])


if __name__ == '__main__': unittest.main()
