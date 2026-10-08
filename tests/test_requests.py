import json
import unittest
from pathlib import Path
from tests import test_backend
from backend.codex_subagents import Backend


class RequestTests(unittest.TestCase):
    setUp = test_backend.BackendTests.setUp
    tearDown = test_backend.BackendTests.tearDown
    settle = test_backend.BackendTests.settle

    def request(self,worker,method='item/commandExecution/requestApproval',pid=12,params=None):
        self.backend._handle_notification({'id':pid,'method':method,'params':{'threadId':worker.thread_id,**(params or {})},'_runtime':self.fake.generation})
        return self.backend.pending_requests(caller=worker.owner)[0]['id']

    def test_wrong_owner_cannot_answer_or_enumerate_requests(self):
        w = self.backend.spawn('p',self.tmp.name,owner='a')
        rid = self.request(w)
        self.assertEqual(self.backend.pending_requests(caller='b'),[])
        with self.assertRaises(PermissionError): self.backend.answer_request(rid,{'decision':'accept'},'b')
        self.assertEqual(self.fake.responses,[])
        self.assertEqual(len(self.backend.pending_requests(caller='a')),1)

    def test_bad_approval_answer_has_no_callback_side_effect(self):
        w = self.backend.spawn('p',self.tmp.name,owner='a')
        rid = self.request(w)
        with self.assertRaises(ValueError): self.backend.answer_request(rid,{'approved':True},'a')
        self.assertEqual(self.fake.responses,[])
        self.backend.answer_request(rid,{'decision':'decline'},'a',command_id='answer')
        self.backend.answer_request(rid,{'decision':'decline'},'a',command_id='answer')
        self.assertEqual(len(self.fake.responses),1)
        self.assertIsInstance(self.fake.responses[0][0],int)

    def test_structured_question_answers_are_bound_to_question_ids(self):
        w = self.backend.spawn('p',self.tmp.name,owner='a')
        rid = self.request(w,'item/tool/requestUserInput',params={'questions':[{'id':'q1','question':'Which?','header':'Choice','options':[{'label':'A'}]}]})
        with self.assertRaises(ValueError): self.backend.answer_request(rid,{'answers':{'wrong':{'answers':['A']}}},'a')
        self.backend.answer_request(rid,{'answers':{'q1':{'answers':['A']}}},'a')
        self.assertEqual(self.fake.responses[0][1]['answers']['q1']['answers'],['A'])

    def test_provider_resolution_closes_request(self):
        w = self.backend.spawn('p',self.tmp.name,owner='a')
        rid = self.request(w)
        self.backend._handle_notification({'method':'serverRequest/resolved','params':{'threadId':w.thread_id,'requestId':12}})
        self.assertEqual(self.backend.pending_requests(caller='a'),[])
        with self.assertRaises(ValueError): self.backend.answer_request(rid,{'decision':'accept'},'a')

    def test_requests_do_not_survive_provider_restart(self):
        w = self.backend.spawn('p',self.tmp.name,owner='a')
        rid = self.request(w)
        self.backend.close()
        self.fake = test_backend.FakeAppServer(); self.backend = Backend(Path(self.tmp.name),self.fake)
        self.assertEqual(self.backend.pending_requests(caller='a'),[])
        with self.assertRaises(ValueError): self.backend.answer_request(rid,{'decision':'accept'},'a')
        self.assertEqual(self.fake.responses,[])

    def test_request_ids_are_scoped_to_runtime_and_typed_provider_id(self):
        w = self.backend.spawn('p',self.tmp.name,owner='a')
        first = self.request(w,pid=12)
        self.request(w,pid='12')
        ids = [r['id'] for r in self.backend.pending_requests(caller='a')]
        self.assertEqual(len(set(ids)),2)
        self.backend.answer_request(first,{'decision':'decline'},'a')
        self.assertEqual(self.fake.responses[0][0],12)

    def test_account_rate_limits_without_thread_are_attributed_to_host(self):
        w = self.backend.spawn('p',self.tmp.name,owner='a')
        self.backend._handle_notification({'method':'account/rateLimits/updated','params':{'rateLimits':{'primary':{'usedPercent':81},'planType':'pro'}}})
        self.backend._handle_notification({'method':'account/rateLimits/updated','params':{'rateLimits':{'primary':None,'planType':None}}})
        value = self.backend.status(w.id,'a')['rate_limits']
        self.assertEqual(value['primary']['usedPercent'],81)
        self.assertEqual(value['scope'],'provider-account')

    def test_unknown_and_credential_refresh_requests_are_refused(self):
        self.backend._handle_notification({'id':99,'method':'account/chatgptAuthTokens/refresh','params':{}})
        self.assertEqual(self.fake.responses[0][2]['code'],-32601)

    def test_notification_contains_only_request_reference(self):
        w = self.backend.spawn('p',self.tmp.name,owner='a')
        rid = self.request(w,params={'command':'UNTRUSTED_COMMAND_TEXT'})
        events = self.backend.claim_events('a')
        self.assertEqual(events[0]['requestId'],rid)
        self.assertNotIn('UNTRUSTED_COMMAND_TEXT',json.dumps(events))

    def test_retrying_429_and_usage_limit_errors_are_worker_telemetry(self):
        w = self.backend.spawn('p',self.tmp.name,owner='a')
        self.backend._handle_notification({'method':'error','params':{'threadId':w.thread_id,'turnId':w.active_turn_id,'willRetry':True,'error':{'message':'retrying','codexErrorInfo':{'httpConnectionFailed':{'httpStatusCode':429}}}}})
        value = self.backend.status(w.id,'a')
        self.assertEqual(value['status'],'running')
        self.assertEqual(value['rate_limits']['lastRateLimitError']['scope'],'worker-turn')
        self.assertTrue(value['rate_limits']['lastRateLimitError']['willRetry'])
        self.backend._handle_notification({'method':'error','params':{'threadId':w.thread_id,'willRetry':False,'error':{'codexErrorInfo':'usageLimitExceeded'}}})
        self.assertEqual(self.backend.status(w.id,'a')['rate_limits']['lastRateLimitError']['codexErrorInfo'],'usageLimitExceeded')

    def test_legacy_approval_uses_conversation_id(self):
        w = self.backend.spawn('p',self.tmp.name,owner='a')
        self.backend._handle_notification({'id':5,'method':'execCommandApproval','params':{'conversationId':w.thread_id,'command':['echo','fixture']}})
        pending = self.backend.pending_requests(caller='a')
        self.assertEqual(len(pending),1)
        self.assertEqual(pending[0]['worker_id'],w.id)
        self.assertEqual(pending[0]['method'],'execCommandApproval')

    def test_wait_and_status_surface_pending_attention_immediately(self):
        import time
        w = self.backend.spawn('p',self.tmp.name,owner='a')
        rid = self.request(w)
        start = time.monotonic()
        value = self.backend.wait_state([w.id],'a',10)
        self.assertLess(time.monotonic()-start,0.5)
        self.assertEqual(value['finished'],[])
        self.assertEqual(value['pendingRequests'][0]['requestId'],rid)
        self.assertEqual(self.backend.status(w.id,'a')['pendingRequests'][0]['requestId'],rid)
        self.assertEqual(self.backend.result(w.id,'a')['pendingRequests'][0]['requestId'],rid)
