"""Real installed app-server against a disposable faux Responses endpoint.

No hosted inference, account changes, existing threads, or user-session reloads.
Run with CODEX_SUBAGENTS_NATIVE_TESTS=1. Native files live in temporary CODEX_HOME.
"""
import json
import os
import tempfile
import threading
import unittest
import uuid
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
from pathlib import Path

from backend.codex_subagents import AppServerClient,Backend
from backend.provider import app_server_args


class FauxResponses(BaseHTTPRequestHandler):
    def log_message(self,*args): pass
    def do_POST(self):
        length=int(self.headers.get('Content-Length','0'))
        self.rfile.read(length)
        text='Fixture answer'
        item={'id':'msg-'+str(uuid.uuid4()),'type':'message','role':'assistant','status':'completed','content':[{'type':'output_text','text':text,'annotations':[]}]}
        response={'id':'resp-'+str(uuid.uuid4()),'object':'response','status':'completed','model':'gpt-6-luna','output':[item],'usage':{'input_tokens':12,'output_tokens':2,'total_tokens':14,'input_tokens_details':{'cached_tokens':0},'output_tokens_details':{'reasoning_tokens':0}}}
        events=[{'type':'response.created','response':{**response,'status':'in_progress','output':[]}},
                {'type':'response.output_item.added','output_index':0,'item':{**item,'status':'in_progress','content':[]}},
                {'type':'response.content_part.added','item_id':item['id'],'output_index':0,'content_index':0,'part':{'type':'output_text','text':'','annotations':[]}},
                {'type':'response.output_text.delta','item_id':item['id'],'output_index':0,'content_index':0,'delta':text},
                {'type':'response.output_text.done','item_id':item['id'],'output_index':0,'content_index':0,'text':text},
                {'type':'response.output_item.done','output_index':0,'item':item},
                {'type':'response.completed','response':response}]
        body=''.join('data: '+json.dumps(event)+'\n\n' for event in events).encode()
        self.send_response(200)
        self.send_header('Content-Type','text/event-stream')
        self.send_header('Content-Length',str(len(body)))
        self.end_headers(); self.wfile.write(body)


@unittest.skipUnless(os.environ.get('CODEX_SUBAGENTS_NATIVE_TESTS')=='1','opt-in real app-server / faux provider')
class NativeProtocolTests(unittest.TestCase):
    def test_native_turn_fork_handoff_revert_and_resume(self):
        with tempfile.TemporaryDirectory(prefix='codex-subagents-native-') as directory:
            root=Path(directory)
            home=root/'codex'; home.mkdir(mode=0o700)
            http=ThreadingHTTPServer(('127.0.0.1',0),FauxResponses)
            threading.Thread(target=http.serve_forever,daemon=True).start()
            previous_router = os.environ.get('CODEX_SUBAGENTS_ROUTER_URL')
            os.environ['CODEX_SUBAGENTS_ROUTER_URL'] = f'http://127.0.0.1:{http.server_port}/v1'
            env={k:v for k,v in os.environ.items() if k not in {'OPENAI_API_KEY','CODEX_API_KEY'}}
            env.update(CODEX_HOME=str(home),OPENAI_API_KEY='faux-key',CODEX_SUBAGENTS_ROUTER_URL=f'http://127.0.0.1:{http.server_port}/v1')
            client=AppServerClient(['codex','app-server',*app_server_args(f'http://127.0.0.1:{http.server_port}/v1'),'--stdio'],env)
            backend=Backend(root/'state',client)
            try:
                caps=backend.capabilities()
                self.assertTrue(caps['fork'] and caps['rollback'] and caps['contextHandoff'])
                worker=backend.spawn('Reply with fixture text',str(root),model='gpt-6-luna',effort='low',owner='fixture')
                result=backend.wait([worker.id],'fixture',30)
                self.assertEqual(result[0]['status'],'completed',result)
                self.assertEqual(result[0]['answer'],'Fixture answer')
                status=backend.status(worker.id,'fixture')
                self.assertEqual(status['history_mode'],'paginated')
                turn_id=status['last_completed_turn_id']
                child=backend.fork(worker.id,'fixture',last_turn_id=turn_id,context_handoff='Keep the fixture context',command_id='fork')
                self.assertNotEqual(child.thread_id,worker.thread_id)
                self.assertEqual(child.history[-1]['state'],'delivered')
                value=backend.rollback(worker.id,turn_id,'fixture',command_id='rollback')
                self.assertEqual(value['status'],'rolled_back')
                page=client.call('thread/turns/list',{'threadId':worker.thread_id,'limit':10})
                self.assertEqual(page['data'],[])
                events=backend.claim_events('fixture')
                self.assertTrue(events)
                backend.close()
                client=AppServerClient(['codex','app-server',*app_server_args(f'http://127.0.0.1:{http.server_port}/v1'),'--stdio'],env)
                backend=Backend(root/'state',client)
                backend.send(child.id,'Continue fixture','fixture')
                resumed=backend.wait([child.id],'fixture',30)
                self.assertTrue(resumed,resumed)
                self.assertEqual(resumed[0]['status'],'completed',resumed)
            finally:
                backend.close()
                http.shutdown(); http.server_close()
                if previous_router is None: os.environ.pop('CODEX_SUBAGENTS_ROUTER_URL', None)
                else: os.environ['CODEX_SUBAGENTS_ROUTER_URL'] = previous_router
