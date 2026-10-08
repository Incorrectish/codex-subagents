import json
import sqlite3
import threading
import time
import unittest
from dataclasses import asdict
from pathlib import Path

from tests import test_backend
FakeAppServer = test_backend.FakeAppServer
from backend.codex_subagents import Backend, RpcError, Worker


class DurabilityTests(unittest.TestCase):
    setUp = test_backend.BackendTests.setUp
    tearDown = test_backend.BackendTests.tearDown
    settle = test_backend.BackendTests.settle
    def test_duplicate_command_concurrent_and_argument_conflict(self):
        found = []
        threads = [threading.Thread(target=lambda: found.append(self.backend.spawn('once',self.tmp.name,owner='a',command_id='same'))) for _ in range(6)]
        for t in threads: t.start()
        for t in threads: t.join()
        self.assertEqual(len({w.id for w in found}),1)
        self.assertEqual([m for m,p in self.fake.calls].count('turn/start'),1)
        with self.assertRaises(ValueError):
            self.backend.spawn('different',self.tmp.name,owner='a',command_id='same')
        other = self.backend.spawn('other',self.tmp.name,owner='b',command_id='same')
        self.assertNotEqual(found[0].id,other.id)

    def test_dispatch_uncertainty_does_not_replay_after_restart(self):
        original = self.fake.call
        def failed(method,params,timeout=30):
            response = original(method,params,timeout)
            if method=='turn/start': raise RpcError('lost response after dispatch')
            return response
        self.fake.call = failed
        with self.assertRaises(RpcError):
            self.backend.spawn('once',self.tmp.name,owner='a',command_id='uncertain')
        self.backend.close()
        self.fake = FakeAppServer()
        self.backend = Backend(Path(self.tmp.name),self.fake)
        with self.assertRaises(ValueError):
            self.backend.spawn('once',self.tmp.name,owner='a',command_id='uncertain')
        self.assertEqual(self.fake.calls,[])
        self.assertEqual(self.backend.list('a')[0]['status'],'interrupted')

    def test_completion_projection_event_outbox_are_one_transaction(self):
        w = self.backend.spawn('work',self.tmp.name,owner='a')
        baseline = self.backend.store.cursor()
        original = self.backend.store.append_event
        def crash(*args,**kwargs):
            original(*args,**kwargs)
            raise RuntimeError('crash before commit')
        self.backend.store.append_event = crash
        note = {'method':'turn/completed','params':{'threadId':w.thread_id,'turn':{'id':w.active_turn_id,'status':'completed','items':[]}}}
        with self.assertRaises(RuntimeError): self.backend._handle_notification(note)
        self.backend.store.append_event = original
        self.assertEqual(self.backend.store.cursor(),baseline)
        persisted = self.backend.store.load_workers()[0]
        self.assertEqual(persisted['status'],'running')
        self.assertEqual(self.backend.store.db.execute('SELECT COUNT(*) FROM outbox').fetchone()[0],0)

    def test_expired_claim_has_fenced_ack_and_survives_restart(self):
        w = self.backend.spawn('work',self.tmp.name,owner='a')
        self.fake.complete()
        self.settle()
        first = self.backend.claim_events('a',lease_seconds=0)[0]
        second = self.backend.claim_events('a')[0]
        self.backend.ack_events('a',[first])
        state = self.backend.store.db.execute('SELECT state FROM outbox').fetchone()[0]
        self.assertEqual(state,'leased')
        self.backend.ack_events('a',[second])
        self.assertEqual(self.backend.claim_events('a'),[])

    def test_stop_timeout_keeps_stopping_until_terminal_event(self):
        w = self.backend.spawn('work',self.tmp.name,owner='a')
        self.fake.auto_interrupt = False
        value = self.backend.stop(w.id,'a',timeout_sec=0.01)
        self.assertEqual(value.status,'stopping')
        self.assertEqual(self.backend.wait([w.id],'a',0),[])
        self.fake.notifications.put({'method':'turn/completed','params':{'threadId':w.thread_id,'turn':{'id':w.active_turn_id,'status':'interrupted','items':[]}}})
        self.settle()
        self.assertEqual(self.backend.status(w.id,'a')['status'],'stopped')

    def test_resume_checks_identity_and_resumes_completed_workers(self):
        w = self.backend.spawn('work',self.tmp.name,owner='a')
        self.fake.complete('answer'); self.settle()
        self.backend.close()
        self.fake = FakeAppServer(); self.backend = Backend(Path(self.tmp.name),self.fake)
        self.backend.send(w.id,'follow up','a')
        self.assertEqual([m for m,p in self.fake.calls],['thread/resume','turn/start'])
        self.backend.workers[w.id].needs_resume = True
        self.backend.workers[w.id].status = 'completed'
        self.fake.call = lambda *a,**k: {'thread':{'id':'wrong'}}
        with self.assertRaises(RpcError): self.backend.send(w.id,'unsafe','a')

    def test_late_terminal_and_duplicate_terminal_do_not_stop_new_turn(self):
        w = self.backend.spawn('work',self.tmp.name,owner='a')
        self.fake.complete(); self.settle()
        self.backend.send(w.id,'new','a')
        self.fake.complete('old','turn-1','thread-1'); self.settle()
        self.assertEqual(self.backend.status(w.id,'a')['active_turn_id'],'turn-2')
        self.assertEqual(self.backend.store.db.execute('SELECT COUNT(*) FROM outbox').fetchone()[0],1)

    def test_descendant_interrupt_and_parent_terminal_wait(self):
        w = self.backend.spawn('parent',self.tmp.name,owner='a')
        native = {'id':'child-thread','source':{'subAgent':{'threadSpawn':{'parentThreadId':w.thread_id}}}}
        self.backend._handle_notification({'method':'thread/started','params':{'thread':native}})
        self.backend._handle_notification({'method':'turn/started','params':{'threadId':'child-thread','turn':{'id':'child-turn'}}})
        self.fake.auto_interrupt = False
        value = self.backend.stop(w.id,'a',timeout_sec=0)
        calls = [p for m,p in self.fake.calls if m=='turn/interrupt']
        self.assertEqual([p['threadId'] for p in calls],['child-thread',w.thread_id])
        self.backend._handle_notification({'method':'turn/completed','params':{'threadId':w.thread_id,'turn':{'id':w.active_turn_id,'status':'interrupted'}}})
        self.assertEqual(self.backend.status(w.id,'a')['status'],'stopping')
        self.backend._handle_notification({'method':'turn/completed','params':{'threadId':'child-thread','turn':{'id':'child-turn','status':'interrupted'}}})
        self.assertEqual(self.backend.status(w.id,'a')['status'],'stopped')

    def test_database_is_private_and_migrations_are_ordered(self):
        self.assertEqual((Path(self.tmp.name)/'state.sqlite3').stat().st_mode & 0o777,0o600)
        self.assertEqual(self.backend.store.db.execute('PRAGMA user_version').fetchone()[0],len(self.backend.store.MIGRATIONS))

    def test_runtime_generation_reconciles_only_lost_process(self):
        old = self.backend.spawn('old',self.tmp.name,owner='a')
        self.fake.generation = 'replacement'
        new = self.backend.spawn('new',self.tmp.name,owner='a')
        self.backend._handle_notification({'method':'_runtime/lost','_runtime':'fixture'})
        self.assertEqual(self.backend.status(old.id,'a')['status'],'interrupted')
        self.assertEqual(self.backend.status(new.id,'a')['status'],'running')
        self.backend.send(old.id,'explicit continuation','a')
        resume = next(p for m,p in self.fake.calls if m=='thread/resume')
        self.assertEqual(resume['modelProvider'],'openai')
        self.assertEqual(resume['model'],old.model)

    def test_collaboration_receivers_are_interrupted_by_exact_native_turn(self):
        w = self.backend.spawn('parent',self.tmp.name,owner='a')
        self.backend._handle_notification({'method':'item/completed','params':{'threadId':w.thread_id,'item':{'type':'collabAgentToolCall','tool':'spawnAgent','receiverThreadIds':['child-native'],'agentsStates':{'child-native':{'status':'running'}}}}})
        original = self.fake.call
        def call(method,params,timeout=30):
            if method=='thread/turns/list' and params['threadId']=='child-native':
                self.fake.calls.append((method,params))
                return {'data':[{'id':'child-active','status':'inProgress'}]}
            return original(method,params,timeout)
        self.fake.call = call
        self.backend.stop(w.id,'a',timeout_sec=1)
        interrupts = [p for m,p in self.fake.calls if m=='turn/interrupt']
        self.assertIn({'threadId':'child-native','turnId':'child-active'},interrupts)
        self.assertEqual(self.backend.status(w.id,'a')['status'],'stopped')

    def test_native_child_attention_wakes_parent_wait(self):
        w = self.backend.spawn('parent',self.tmp.name,owner='a')
        self.backend._handle_notification({'method':'thread/started','params':{'thread':{'id':'child-native','parentThreadId':w.thread_id}}})
        self.backend._handle_notification({'id':77,'method':'item/commandExecution/requestApproval','params':{'threadId':'child-native'}})
        value = self.backend.wait_state([w.id],'a',10)
        self.assertEqual(value['finished'],[])
        self.assertEqual(len(value['pendingRequests']),1)
        self.assertNotEqual(value['pendingRequests'][0]['workerId'],w.id)

    def test_collaboration_after_stop_interrupts_previously_idle_child(self):
        w = self.backend.spawn('parent',self.tmp.name,owner='a')
        self.backend._handle_notification({'method':'thread/started','params':{'thread':{'id':'child-native','parentThreadId':w.thread_id}}})
        self.backend.stop(w.id,'a',timeout_sec=1)
        original = self.fake.call
        def call(method,params,timeout=30):
            if method=='thread/turns/list':
                return {'data':[{'id':'late-child-turn','status':'inProgress'}]}
            return original(method,params,timeout)
        self.fake.call = call
        self.fake.auto_interrupt = False
        self.backend._handle_notification({'method':'item/completed','params':{'threadId':w.thread_id,'item':{'type':'collabAgentToolCall','tool':'spawnAgent','receiverThreadIds':['child-native'],'agentsStates':{'child-native':{'status':'running'}}}}})
        self.assertIn(('turn/interrupt',{'threadId':'child-native','turnId':'late-child-turn'}),self.fake.calls)
        self.assertEqual(self.backend.status(w.id,'a')['status'],'stopping')
        self.backend._handle_notification({'method':'turn/completed','params':{'threadId':'child-native','turn':{'id':'late-child-turn','status':'interrupted'}}})
        self.assertEqual(self.backend.status(w.id,'a')['status'],'stopped')

    def test_grandchild_terminal_settles_all_stopping_ancestors(self):
        w = self.backend.spawn('parent',self.tmp.name,owner='a')
        for native,parent,turn in [('child',w.thread_id,'child-turn'),('grandchild','child','grandchild-turn')]:
            self.backend._handle_notification({'method':'thread/started','params':{'thread':{'id':native,'parentThreadId':parent}}})
            self.backend._handle_notification({'method':'turn/started','params':{'threadId':native,'turn':{'id':turn}}})
        self.fake.auto_interrupt = False
        self.backend.stop(w.id,'a',timeout_sec=0)
        for native,turn in [(w.thread_id,w.active_turn_id),('child','child-turn')]:
            self.backend._handle_notification({'method':'turn/completed','params':{'threadId':native,'turn':{'id':turn,'status':'interrupted'}}})
        self.assertEqual(self.backend.status(w.id,'a')['status'],'stopping')
        self.backend._handle_notification({'method':'turn/completed','params':{'threadId':'grandchild','turn':{'id':'grandchild-turn','status':'interrupted'}}})
        self.assertEqual(self.backend.status(w.id,'a')['status'],'stopped')
        self.assertFalse(any(x['status']=='stopping' for x in self.backend.list('a')))
