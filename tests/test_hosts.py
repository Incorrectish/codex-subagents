import base64
import json
import shlex
import subprocess
import sys
import unittest
from unittest.mock import patch
from tests import test_backend
from backend.codex_subagents import SshAppServerClient
from backend.ssh_runtime import DRIVER


class HostTests(unittest.TestCase):
    setUp = test_backend.BackendTests.setUp
    tearDown = test_backend.BackendTests.tearDown
    settle = test_backend.BackendTests.settle

    def test_pid_is_remote_and_owner_overrides_are_refused(self):
        remote=test_backend.FakeAppServer()
        self.backend.apps['remote']=remote
        w=self.backend.spawn('p',self.tmp.name,owner='a',host='remote')
        self.assertEqual(w.pid,321)
        self.assertEqual(w.runtime_uid,1000)
        self.assertNotEqual(w.pid,w.transport_pid)
        for method,args in ((self.backend.send,(w.id,'other','b',True)),(self.backend.stop,(w.id,'b',True)),(self.backend.status,(w.id,'b',True))):
            with self.assertRaises(PermissionError): method(*args)
        self.assertEqual(self.backend.list('b',True),[])

    def test_same_native_ids_are_scoped_by_host(self):
        remote=test_backend.FakeAppServer(); self.backend.apps['remote']=remote
        one=self.backend.spawn('local',self.tmp.name,owner='a')
        two=self.backend.spawn('remote',self.tmp.name,owner='a',host='remote')
        self.assertEqual(one.thread_id,two.thread_id)
        remote.complete('remote answer')
        self.settle()
        self.assertEqual(self.backend.status(one.id,'a')['status'],'running')
        self.assertEqual(self.backend.result(two.id,'a')['answer'],'remote answer')

    def test_remote_key_never_crosses_ssh_environment_and_command_is_quoted(self):
        with patch.dict('os.environ',{'OPENAI_API_KEY':'secret-local-key','CODEX_SUBAGENTS_REMOTE_SSH_HOST':'example-host','CODEX_SUBAGENTS_REMOTE_KEY_FILE':"~/with spaces/key"}):
            client=SshAppServerClient()
        self.assertNotIn('OPENAI_API_KEY',client.env)
        self.assertNotIn('secret-local-key',json.dumps(client.command))
        remote=shlex.split(client.command[-1])
        self.assertEqual(remote[:2],['python3','-u'])
        config=json.loads(remote[-1])
        self.assertIn('with spaces/key',config['keyFile'])
        self.assertEqual(config['args'],[])
        self.assertIn("remote key",DRIVER)
        self.assertNotIn('killpg',DRIVER)
        self.assertNotIn('pkill',DRIVER)
        self.assertNotIn('login',DRIVER)

    def test_remote_requests_are_not_exposed_by_machine_wide_view(self):
        remote=test_backend.FakeAppServer(); self.backend.apps['remote']=remote
        w=self.backend.spawn('p',self.tmp.name,owner='a',host='remote')
        self.backend._handle_notification({'id':1,'method':'item/commandExecution/requestApproval','params':{'threadId':w.thread_id}},'remote')
        self.assertEqual(self.backend.pending_requests(caller='b',all_workers=True),[])

    def test_remote_driver_refuses_readable_by_others_credentials(self):
        from pathlib import Path
        key = Path(self.tmp.name)/'api-key'
        key.write_text('fixture-secret')
        key.chmod(0o600)
        config = json.dumps({'keyFile':str(key),'args':[],'responses':{},'mode':'schema'})
        # Execute the real driver's admission boundary before its launch/schema code.
        command = [sys.executable,'-c',DRIVER.split('def schema():')[0],config]
        self.assertEqual(subprocess.run(command,capture_output=True).returncode,0)
        for path in (key,):
            path.chmod(0o644)
            result = subprocess.run(command,capture_output=True,text=True)
            self.assertNotEqual(result.returncode,0)
            self.assertIn('owner-only',result.stderr)
            self.assertNotIn('fixture-secret',result.stderr)
            path.chmod(0o600)
