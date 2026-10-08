import json
import subprocess
import sys
import unittest
from pathlib import Path
from unittest.mock import patch
import mcp_server as mcp


class OneIteration:
    def __init__(self): self.calls=0
    def wait(self,_):
        self.calls+=1
        return self.calls>1


class McpTests(unittest.TestCase):
    def test_spawn_payload_records_explicit_native_parent(self):
        with patch.dict(mcp.os.environ, {'CODEX_THREAD_ID': 'codex-thread-1'}, clear=False):
            self.assertEqual(mcp._spawn_payload({'prompt': 'p', 'cwd': '/tmp'})['parentId'], 'codex-thread-1')

    def test_spawn_payload_leaves_parent_unset_when_mcp_cannot_obtain_one(self):
        with patch.dict(mcp.os.environ, {'CODEX_THREAD_ID': ''}, clear=False):
            mcp.MCP_CLIENT_INFO.clear()
            self.assertNotIn('parentId', mcp._spawn_payload({'prompt': 'p', 'cwd': '/tmp'}))

    def test_stdio_contract_and_pending_resource_without_inference(self):
        messages = [{'jsonrpc':'2.0','id':1,'method':'initialize','params':{'protocolVersion':'2024-11-05'}},
                    {'jsonrpc':'2.0','id':2,'method':'tools/list'},
                    {'jsonrpc':'2.0','id':3,'method':'resources/list'}]
        proc = subprocess.run([sys.executable,str(Path(mcp.__file__))],input=''.join(json.dumps(m)+'\n' for m in messages),capture_output=True,text=True,timeout=10)
        self.assertEqual(proc.returncode,0,proc.stderr)
        replies = [json.loads(line) for line in proc.stdout.splitlines()]
        names = {t['name'] for t in replies[1]['result']['tools']}
        self.assertTrue({'pending_requests','answer_request','spawn'}.issubset(names))
        self.assertIn('logging',replies[0]['result']['capabilities'])
        self.assertEqual(replies[2]['result']['resources'][0]['uri'],mcp.RESOURCE)

    def test_mcp_notification_and_fenced_ack(self):
        writes,calls = [],[]
        event = {'workerId':'w','status':'pending','kind':'request','requestId':'r','seq':12,'eventId':'e','leaseId':'lease','answer':'untrusted'}
        def request(method,path,payload=None):
            calls.append((method,path,payload))
            return {'events':[event]} if method=='GET' else {'ok':True}
        with patch.object(mcp,'_ensure_daemon'), patch.object(mcp,'_request',side_effect=request), patch.object(mcp,'_write',side_effect=writes.append):
            mcp._notification_loop(OneIteration())
        self.assertEqual(writes[0]['method'],'notifications/message')
        self.assertEqual(writes[0]['params']['data']['nextTool'],'pending_requests')
        self.assertNotIn('untrusted',json.dumps(writes))
        self.assertEqual(calls[-1][2]['claims'],[{'eventId':'e','leaseId':'lease'}])

    def test_failed_write_does_not_ack(self):
        calls = []
        def request(method,path,payload=None):
            calls.append(method)
            return {'events':[{'workerId':'w','eventId':'e','leaseId':'lease'}]}
        with patch.object(mcp,'_ensure_daemon'), patch.object(mcp,'_request',side_effect=request), patch.object(mcp,'_write',side_effect=BrokenPipeError):
            mcp._notification_loop(OneIteration())
        self.assertEqual(calls,['GET'])

    def test_old_daemon_is_refused_without_restarting_it(self):
        with patch.object(mcp,'_request',return_value={'workers':[]}), patch.object(mcp.subprocess,'run') as run:
            with self.assertRaisesRegex(RuntimeError,'older daemon'):
                mcp._ensure_daemon()
            run.assert_not_called()

    def test_tool_arguments_are_validated_before_backend_dispatch(self):
        with patch.object(mcp,'_ensure_daemon') as ensure:
            with self.assertRaises(ValueError): mcp._call_tool('stop',{'id':'worker','allowOtherSession':'false'})
            with self.assertRaises(ValueError): mcp._call_tool('unknown',{})
            ensure.assert_not_called()
