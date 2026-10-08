import unittest
from tests import test_backend


class ForkTests(unittest.TestCase):
    setUp = test_backend.BackendTests.setUp
    tearDown = test_backend.BackendTests.tearDown
    settle = test_backend.BackendTests.settle

    def settled(self):
        w=self.backend.spawn('p',self.tmp.name,owner='a')
        self.fake.complete('answer'); self.settle()
        return w

    def test_native_fork_with_handoff_and_idempotent_retry(self):
        w=self.settled()
        fork=self.backend.fork(w.id,'a',last_turn_id='turn-1',context_handoff='continue with X',command_id='fork-1')
        retry=self.backend.fork(w.id,'a',last_turn_id='turn-1',context_handoff='continue with X',command_id='fork-1')
        self.assertEqual(fork.id,retry.id)
        self.assertEqual(fork.parent_id,w.id)
        calls=[(m,p) for m,p in self.fake.calls if m.startswith('thread/fork') or m=='thread/inject_items']
        self.assertEqual(len(calls),2)
        self.assertEqual(calls[0][1]['lastTurnId'],'turn-1')
        self.assertTrue(calls[0][1]['deferGoalContinuation'])
        self.assertEqual(calls[1][1]['items'][0]['role'],'developer')
        self.assertEqual([m for m,p in self.fake.calls].count('turn/start'),1)
        self.assertEqual(self.backend.status(fork.id,'a')['history'][-1]['state'],'delivered')

    def test_rollback_lists_native_history_before_reverting(self):
        w=self.settled()
        result=self.backend.rollback(w.id,'turn-1','a',command_id='rollback')
        self.assertFalse(result['filesystemReverted'])
        self.assertEqual(self.backend.status(w.id,'a')['status'],'idle')
        self.assertEqual([m for m,p in self.fake.calls][-3:],['thread/read','thread/turns/list','thread/revert'])
        self.backend.rollback(w.id,'turn-1','a',command_id='rollback')
        self.assertEqual([m for m,p in self.fake.calls].count('thread/revert'),1)

    def test_no_capability_never_dispatches_fork(self):
        w=self.settled(); self.fake.schema=lambda:{'methods':{},'responses':{}}
        with self.assertRaisesRegex(ValueError,'unsupported'): self.backend.fork(w.id,'a')
        self.assertFalse(self.backend.capabilities()['fork'])
        self.assertNotIn('thread/fork',[m for m,p in self.fake.calls])

    def test_legacy_history_is_explicitly_unsupported(self):
        w=self.settled(); original=self.fake.call
        def call(method,params,timeout=30):
            if method=='thread/read': return {'thread':{'id':w.thread_id,'historyMode':'legacy'}}
            return original(method,params,timeout)
        self.fake.call=call
        with self.assertRaisesRegex(ValueError,'legacy'): self.backend.rollback(w.id,'turn-1','a')
        self.assertNotIn('thread/revert',[m for m,p in self.fake.calls])

    def test_active_wrong_owner_and_invalid_boundary_are_refused(self):
        w=self.backend.spawn('p',self.tmp.name,owner='a')
        with self.assertRaises(ValueError): self.backend.fork(w.id,'a')
        with self.assertRaises(ValueError): self.backend.rollback(w.id,'turn-1','a')
        self.fake.complete(); self.settle()
        with self.assertRaises(PermissionError): self.backend.fork(w.id,'b')
        with self.assertRaises(ValueError): self.backend.fork(w.id,'a',last_turn_id='1',before_turn_id='2')
        with self.assertRaises(ValueError): self.backend.rollback(w.id,'unknown','a')
        self.assertNotIn('thread/revert',[m for m,p in self.fake.calls])
