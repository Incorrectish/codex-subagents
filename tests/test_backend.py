import json
import queue
import tempfile
import threading
import time
import unittest
from unittest.mock import patch
from pathlib import Path

from backend.codex_subagents import Backend, SshAppServerClient


class FakeAppServer:
    def __init__(self):
        self.calls = []
        self.notifications = queue.Queue()
        self.turn = 0
        self.thread_count = 0
        self.responses = []
        self.auto_interrupt = True
        self.generation = "fixture"
        self.remote_pid = 321
        self.remote_uid = 1000

    def schema(self):
        approval = {'type':'object','properties':{'decision':{'enum':['accept','decline','cancel','acceptForSession']}},'required':['decision']}
        question = {'type':'object','properties':{'answers':{'type':'object','additionalProperties':{'type':'object','properties':{'answers':{'type':'array','items':{'type':'string'}}},'required':['answers']}}},'required':['answers']}
        return {'methods':{'thread/start':{'properties':{'historyMode':{}}},'thread/fork':{},'thread/read':{},'thread/revert':{},'thread/turns/list':{},'thread/inject_items':{}}, 'responses':{'item/commandExecution/requestApproval':approval,'item/fileChange/requestApproval':approval,'item/tool/requestUserInput':question}}

    def call(self, method, params, timeout=30):
        self.calls.append((method, params))
        if method == "initialize":
            return {}
        if method in {"thread/start","thread/fork"}:
            self.thread_count += 1
            return {"thread": {"id": f"thread-{self.thread_count}","historyMode":"paginated"}}
        if method == "turn/start":
            self.turn += 1
            return {"turn": {"id": f"turn-{self.turn}"}}
        if method == "thread/read":
            return {"thread": {"id":params["threadId"],"historyMode":"paginated"}}
        if method == "thread/turns/list":
            return {"data":[{"id":f"turn-{i}"} for i in range(1,self.turn+1)],"nextCursor":None}
        if method == "thread/resume":
            return {"thread": {"id": params["threadId"]}}
        if method == "turn/interrupt" and self.auto_interrupt:
            self.notifications.put({"method": "turn/completed", "params": {"threadId": params["threadId"], "turn": {"id": params["turnId"], "status": "interrupted", "items": []}}})
        return {}

    def drain_notifications(self):
        found = []
        while True:
            try:
                found.append(self.notifications.get_nowait())
            except queue.Empty:
                return found

    def respond(self, request_id, result=None, error=None):
        self.responses.append((request_id, result, error))

    def close(self):
        pass

    def complete(self, text="done", turn_id=None, thread_id=None):
        turn_id = turn_id or f"turn-{self.turn}"
        self.notifications.put({"method": "turn/completed", "params": {"threadId": thread_id or "thread-1", "turn": {"id": turn_id, "status": "completed", "items": [{"type": "agentMessage", "text": text}]}}})


class BackendTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.fake = FakeAppServer()
        self.backend = Backend(Path(self.tmp.name), self.fake)

    def tearDown(self):
        self.backend.close()
        self.tmp.cleanup()

    def settle(self):
        time.sleep(0.2)

    def test_spawn_send_idle_and_running_steer(self):
        worker = self.backend.spawn("first", self.tmp.name, owner="session-a")
        self.backend.send(worker.id, "steer now", "session-a")
        self.assertEqual([c[0] for c in self.fake.calls], ["thread/start", "turn/start", "turn/steer"])
        self.fake.complete()
        self.settle()
        self.backend.send(worker.id, "follow up", "session-a")
        self.assertEqual([c[0] for c in self.fake.calls][-1], "turn/start")

    def test_spawn_records_native_parent_id(self):
        worker = self.backend.spawn("child", self.tmp.name, owner="claude-session", parent_id="claude-session")
        self.assertEqual(worker.parent_id, "claude-session")
        stored = self.backend.store.load_workers()[0]
        self.assertEqual(stored["parent_id"], "claude-session")

    def test_lease_acquired_renewed_and_released(self):
        calls=[]
        def lease(method,path,payload=None):
            calls.append((method,path,payload)); return {'granted':True,'lease_id':'lease-1'}
        with patch.dict('os.environ',{'CODEX_SUBAGENTS_ROUTER_LEASES_ENABLED':'1'}), patch('backend.codex_subagents._lease_request',side_effect=lease):
            worker=self.backend.spawn('leased',self.tmp.name,owner='session-a')
            self.assertEqual(worker.lease_id,'lease-1')
            self.fake.complete(); self.settle()
            self.assertIn(('DELETE','/leases/lease-1',None),calls)

    def test_capacity_error_and_pending_ticket(self):
        calls=[]
        def lease(method,path,payload=None):
            calls.append((method,path,payload))
            if method=='POST':
                if payload.get('pending'): return {'granted':False,'pending':True,'ticket':'ticket-1'}
                return {'granted':False,'capacity':{'used':1,'capacity':1}}
            if method=='GET': return {'granted':False,'pending':True}
            return {}
        with patch.dict('os.environ',{'CODEX_SUBAGENTS_ROUTER_LEASES_ENABLED':'1'}), patch('backend.codex_subagents._lease_request',side_effect=lease):
            with self.assertRaisesRegex(Exception,'capacity full; retry later or use pending'):
                self.backend.spawn('full',self.tmp.name,owner='a')
            worker=self.backend.spawn('queued',self.tmp.name,owner='a',pending=True)
            self.assertEqual(worker.status,'pending'); self.assertEqual(worker.lease_ticket,'ticket-1')

    def test_wait_any_and_all_timeout(self):
        one = self.backend.spawn("one", self.tmp.name, owner="session-a")
        two = self.backend.spawn("two", self.tmp.name, owner="session-a")
        self.fake.complete("one", "turn-1", "thread-1")
        self.settle()
        self.assertEqual([x["id"] for x in self.backend.wait("any", "session-a", 0.2)], [one.id])
        self.assertEqual(self.backend.wait([one.id, two.id], "session-a", 0.01)[0]["id"], one.id)
        self.fake.complete("two", "turn-2", "thread-2")
        self.settle()
        self.assertEqual({x["id"] for x in self.backend.wait([one.id, two.id], "session-a", 0.2)}, {one.id, two.id})

    def test_stop_and_restart_recovery(self):
        worker = self.backend.spawn("stop", self.tmp.name, owner="session-a")
        self.backend.stop(worker.id, "session-a")
        self.assertEqual(self.backend.status(worker.id, "session-a")["status"], "stopped")
        self.backend.close()
        fake2 = FakeAppServer()
        recovered = Backend(Path(self.tmp.name), fake2)
        self.assertEqual(recovered.status(worker.id, "session-a")["status"], "stopped")
        recovered.close()

    def test_running_worker_is_marked_interrupted_on_restart(self):
        worker = self.backend.spawn("running", self.tmp.name, owner="session-a")
        self.backend.close()
        recovered = Backend(Path(self.tmp.name), FakeAppServer())
        self.assertEqual(recovered.status(worker.id, "session-a")["status"], "interrupted")
        recovered.close()

    def test_notifications_claim_once(self):
        worker = self.backend.spawn("notify", self.tmp.name, owner="session-a")
        self.fake.complete("exactly once")
        self.settle()
        first = self.backend.claim_events("session-a")
        self.backend.ack_events("session-a", [{"eventId":first[0]["eventId"],"leaseId":first[0]["leaseId"]}])
        second = self.backend.claim_events("session-a")
        self.assertEqual(len(first), 1)
        self.assertEqual(second, [])
        self.assertEqual(first[0]["workerId"], worker.id)

    def test_events_are_routed_only_to_owner(self):
        worker = self.backend.spawn("notify", self.tmp.name, owner="session-a")
        self.fake.complete("owned")
        self.settle()
        self.assertEqual(len(self.backend.claim_events("session-b")), 0)
        events = self.backend.claim_events("session-a")
        self.assertEqual([event["workerId"] for event in events], [worker.id])

    def test_cross_session_mutations_are_refused(self):
        worker = self.backend.spawn("owned", self.tmp.name, owner="session-a")
        with self.assertRaises(PermissionError):
            self.backend.send(worker.id, "no", "session-b")
        with self.assertRaises(PermissionError):
            self.backend.stop(worker.id, "session-b")
        self.backend.send(worker.id, "yes", "session-b", allow_other=True)

    def test_concurrent_spawns_are_unique_and_persisted(self):
        workers = []
        errors = []

        def spawn(index):
            try:
                workers.append(self.backend.spawn(f"prompt-{index}", self.tmp.name, owner=f"session-{index % 2}"))
            except Exception as exc:
                errors.append(exc)

        threads = [threading.Thread(target=spawn, args=(i,)) for i in range(12)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(errors, [])
        self.assertEqual(len({worker.id for worker in workers}), 12)
        self.assertEqual(len(self.backend.list("session-0")), 6)

    def test_owner_survives_restart(self):
        worker = self.backend.spawn("owned", self.tmp.name, owner="session-a")
        self.backend.close()
        recovered = Backend(Path(self.tmp.name), FakeAppServer())
        self.assertEqual(recovered.status(worker.id, "session-a")["owner"], "session-a")
        with self.assertRaises(PermissionError):
            recovered.status(worker.id, "session-b")
        recovered.close()

    def test_sqlite_store_has_monotonic_events_and_leased_outbox(self):
        one = self.backend.spawn("one", self.tmp.name, owner="session-a")
        self.fake.complete("one", "turn-1", "thread-1")
        self.settle()
        two = self.backend.spawn("two", self.tmp.name, owner="session-a")
        self.fake.complete("two", "turn-2", "thread-2")
        self.settle()
        events = self.backend.claim_events("session-a")
        self.assertEqual([event["seq"] for event in events], sorted(event["seq"] for event in events))
        self.assertEqual(len(events), 2)
        self.assertEqual(self.backend.claim_events("session-a"), [])

    def test_command_receipt_makes_spawn_retry_idempotent(self):
        first = self.backend.spawn("same", self.tmp.name, owner="session-a", command_id="req-1")
        second = self.backend.spawn("same", self.tmp.name, owner="session-a", command_id="req-1")
        self.assertEqual(first.id, second.id)
        self.assertEqual(len(self.backend.list("session-a")), 1)

    def test_stop_waits_for_terminal_notification(self):
        worker = self.backend.spawn("stop", self.tmp.name, owner="session-a")
        def complete_later():
            time.sleep(0.03)
            self.fake.complete("interrupted")
        threading.Thread(target=complete_later, daemon=True).start()
        stopped = self.backend.stop(worker.id, "session-a")
        self.assertEqual(stopped.status, "stopped")
        self.assertEqual(self.backend.status(worker.id, "session-a")["status"], "stopped")

    def test_pending_approval_is_visible_and_answerable(self):
        worker = self.backend.spawn("approve", self.tmp.name, owner="session-a")
        self.fake.notifications.put({"jsonrpc": "2.0", "id": 91, "method": "item/commandExecution/requestApproval", "params": {"threadId": worker.thread_id, "command": "rm -rf /"}})
        self.settle()
        pending = self.backend.pending_requests(caller="session-a")
        request_id = pending[0]["request_id"]
        self.assertTrue(request_id)
        self.backend.answer_request(request_id, {"decision": "decline"}, "session-a", command_id="answer-91")
        self.assertEqual(self.fake.responses[0][0], 91)
        self.assertEqual(self.backend.pending_requests(caller="session-a"), [])

    def test_usage_and_rate_limit_telemetry_is_persisted(self):
        worker = self.backend.spawn("telemetry", self.tmp.name, owner="session-a")
        self.fake.notifications.put({"method": "thread/tokenUsageUpdated", "params": {"threadId": worker.thread_id, "usage": {"inputTokens": 12}}})
        self.fake.notifications.put({"method": "rateLimits/updated", "params": {"threadId": worker.thread_id, "rateLimits": {"remaining": 4}}})
        self.settle()
        status = self.backend.status(worker.id, "session-a")
        self.assertEqual(status["token_usage"]["inputTokens"], 12)
        self.assertEqual(status["rate_limits"]["remaining"], 4)

    def test_remote_worker_records_host_and_is_owner_scoped(self):
        self.backend.apps["remote"] = self.fake
        worker = self.backend.spawn("remote", self.tmp.name, owner="session-a", host="remote")
        self.assertEqual(worker.host, "remote")
        self.assertEqual(self.backend.status(worker.id, "session-a")["host"], "remote")
        with self.assertRaises(PermissionError):
            self.backend.status(worker.id, "session-b")

    def test_remote_client_uses_mirrored_pool_without_refresh_commands(self):
        with patch.dict('os.environ', {'CODEX_SUBAGENTS_REMOTE_SSH_HOST':'example-host'}):
            client = SshAppServerClient()
        self.assertEqual(client.command[:2], ["ssh", "-T"])
        self.assertIn("example-host", client.command)
        self.assertEqual(client.provider_url,"")
        from backend.ssh_runtime import DRIVER
        self.assertNotIn("refresh",DRIVER)
        self.assertIn("proc.terminate()",DRIVER)



if __name__ == "__main__":
    unittest.main()
