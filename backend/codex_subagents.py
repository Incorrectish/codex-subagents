#!/usr/bin/env python3
"""Authenticated loopback daemon owning host-scoped Codex app-servers."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import queue
import secrets
import shlex
import subprocess
import threading
import time
import uuid
import urllib.request
from dataclasses import asdict, dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Callable
from urllib.parse import parse_qs, urlparse

try:
    from .ssh_runtime import ssh_command
    from .store import SQLiteStore
    from .protocol import inspect_local, validate, RESPONSE_TYPES
    from .provider import app_server_args, environment, request_defaults
    from .transport import ThreadingUnixHTTPServer, _remove_stale_socket, paths, write_endpoint
except ImportError:
    from ssh_runtime import ssh_command
    from store import SQLiteStore
    from protocol import inspect_local, validate, RESPONSE_TYPES
    from provider import app_server_args, environment, request_defaults
    from transport import ThreadingUnixHTTPServer, _remove_stale_socket, paths, write_endpoint

ROOT = Path(__file__).resolve().parent.parent
STATE_DIR = Path(os.environ.get('CODEX_SUBAGENTS_STATE_DIR', Path.home()/'.codex-subagents'))
TERMINAL = {'completed', 'failed', 'stopped', 'interrupted'}
ACTIVE = {'starting', 'running', 'stopping'}


def now_ms():
    return int(time.time()*1000)


def merge_sparse(old,new):
    result = dict(old)
    for key,value in new.items():
        if value is not None:
            result[key] = merge_sparse(result.get(key,{}) if isinstance(result.get(key),dict) else {},value) if isinstance(value,dict) else value
    return result


@dataclass
class Worker:
    id: str
    thread_id: str | None
    cwd: str
    model: str
    effort: str
    status: str
    created_at: int
    updated_at: int
    final_answer: str | None = None
    event_log_path: str = ''
    active_turn_id: str | None = None
    pending_event: bool = False
    error: str | None = None
    last_completed_turn_id: str | None = None
    needs_resume: bool = False
    history: list[dict[str, Any]] = field(default_factory=list)
    owner: str = ''
    host: str = 'local'
    pid: int | None = None
    stop_requested: bool = False
    stop_deadline: float | None = None
    parent_id: str | None = None
    token_usage: dict[str, Any] = field(default_factory=dict)
    rate_limits: dict[str, Any] = field(default_factory=dict)
    approval_policy: str = 'on-request'
    native_parent_thread: str | None = None
    completed_turns: list[str] = field(default_factory=list)
    terminal_received: bool = False
    transport_pid: int | None = None
    runtime_uid: int | None = None
    history_mode: str = "legacy"
    interrupt_descendants: bool = False
    terminal_status: str | None = None
    runtime_generation: str | None = None
    lease_id: str | None = None
    lease_ticket: str | None = None
    send_queue: list[dict[str, Any]] = field(default_factory=list)
    dropped_queue: list[dict[str, Any]] = field(default_factory=list)

    @classmethod
    def from_dict(cls, data):
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})


class RpcError(RuntimeError):
    pass

def _leases_enabled():
    return os.environ.get('CODEX_SUBAGENTS_ROUTER_LEASES_ENABLED','0').lower() in {'1','true','yes'}

def _lease_request(method, path, payload=None):
    base=os.environ.get('CODEX_SUBAGENTS_ROUTER_URL','').rstrip('/')
    headers={'Content-Type':'application/json'}
    if os.environ.get('CODEX_SUBAGENTS_ROUTER_API_KEY'): headers['Authorization']='Bearer '+os.environ['CODEX_SUBAGENTS_ROUTER_API_KEY']
    body=None if payload is None else json.dumps(payload).encode()
    req=urllib.request.Request(base+path,data=body,headers=headers,method=method)
    with urllib.request.urlopen(req,timeout=3) as response: return json.loads(response.read())

def _lease_ticket(ticket):
    return _lease_request('GET', '/leases/tickets/' + ticket)


class AppServerClient:
    def __init__(self, command=None, env=None):
        self.command = command or ['codex','app-server',*app_server_args(),'--stdio']
        self.env = env
        self.proc = None
        self.next_id = 1
        self.pending = {}
        self.notifications = queue.Queue()
        self.lock = threading.RLock()
        self.start_lock = threading.Lock()
        self.reader = None
        self.generation = str(uuid.uuid4())
        self.closing = False
        self._schema = None
        self.remote_pid = None
        self.remote_uid = None

    def schema(self):
        if self._schema is None:
            self._schema = inspect_local()
        return self._schema

    def start(self):
        with self.start_lock:
            if self.proc and self.proc.poll() is None:
                return
            if self.closing:
                raise RpcError('app-server client closed')
            if self.proc:
                self._close_streams()
            self.generation = str(uuid.uuid4())
            proc = subprocess.Popen(self.command, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE, text=True, bufsize=1, env=self.env or environment())
            self.proc = proc
            self.reader = threading.Thread(target=self._read, args=(proc, self.generation), daemon=True)
            self.reader.start()
            # Drain diagnostics so a full stderr pipe cannot block the server.
            self.stderr_reader = threading.Thread(target=self._drain_stderr,args=(proc,),daemon=True)
            self.stderr_reader.start()
            self._rpc('initialize', {'clientInfo': {'name':'codex-subagents','version':'0.2.0'}, 'capabilities': {'experimentalApi': True}}, 30)
            self.notify('initialized', {})

    @staticmethod
    def _drain_stderr(proc):
        for _ in proc.stderr:
            pass

    def _close_streams(self):
        if self.reader:
            self.reader.join(timeout=1)
        if getattr(self,'stderr_reader',None):
            self.stderr_reader.join(timeout=1)
        if self.proc:
            for stream in (self.proc.stdin,self.proc.stdout,self.proc.stderr):
                if stream:
                    stream.close()

    def _read(self, proc, generation):
        try:
            for line in proc.stdout:
                try:
                    message = json.loads(line)
                except ValueError:
                    continue
                if message.get('method')=='_process/started':
                    self.remote_pid = message['params']['pid']
                    self.remote_uid = message['params']['uid']
                    continue
                if 'id' in message and ('result' in message or 'error' in message):
                    with self.lock:
                        waiter = self.pending.get(message['id'])
                    if waiter:
                        waiter.put_nowait(message)
                else:
                    message['_runtime'] = generation
                    self.notifications.put(message)
        finally:
            with self.lock:
                for waiter in self.pending.values():
                    if waiter.empty():
                        waiter.put_nowait({'error': {'message': 'app-server connection closed'}})
            self.notifications.put({'method': '_runtime/lost', '_runtime': generation})

    def _write(self, message):
        with self.lock:
            if not self.proc or self.proc.poll() is not None or not self.proc.stdin:
                raise RpcError('app-server is not running')
            try:
                self.proc.stdin.write(json.dumps(message)+'\n')
                self.proc.stdin.flush()
            except OSError as exc:
                raise RpcError('app-server connection lost') from exc

    def notify(self, method, params):
        self._write({'jsonrpc':'2.0','method':method,'params':params})

    def _rpc(self, method, params, timeout):
        with self.lock:
            request_id = self.next_id
            self.next_id += 1
            waiter = queue.Queue(maxsize=1)
            self.pending[request_id] = waiter
        try:
            self._write({'jsonrpc':'2.0','id':request_id,'method':method,'params':params})
            try:
                response = waiter.get(timeout=timeout)
            except queue.Empty as exc:
                raise RpcError(f'app-server timeout calling {method}; dispatch outcome unknown') from exc
            if 'error' in response:
                raise RpcError(str(response['error']))
            return response.get('result', {})
        finally:
            with self.lock:
                self.pending.pop(request_id, None)

    def call(self, method, params, timeout=30):
        self.start()
        return self._rpc(method, params, timeout)

    def respond(self, request_id, result=None, error=None):
        self._write({'jsonrpc':'2.0','id':request_id, **({'error':error} if error else {'result':result or {}})})

    def drain_notifications(self):
        items = []
        while True:
            try:
                items.append(self.notifications.get_nowait())
            except queue.Empty:
                return items

    def close(self):
        self.closing = True
        if self.proc and self.proc.poll() is None:
            self.proc.terminate()  # exact child only; never pkill or killall
            try:
                self.proc.wait(timeout=2)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait(timeout=2)
        self._close_streams()


class SshAppServerClient(AppServerClient):
    def __init__(self):
        self.provider_url = os.environ.get('CODEX_SUBAGENTS_REMOTE_BASE_URL','')
        args = app_server_args(self.provider_url or None)
        super().__init__(ssh_command('launch',args,RESPONSE_TYPES),env={k:v for k,v in os.environ.items() if k not in {'OPENAI_API_KEY','CODEX_API_KEY','CODEX_SUBAGENTS_ROUTER_API_KEY'}})
        self.schema_command = ssh_command('schema',args,RESPONSE_TYPES)

    def schema(self):
        if self._schema is None:
            proc = subprocess.run(self.schema_command,env=self.env,capture_output=True,text=True,timeout=40)
            if proc.returncode:
                raise RpcError('remote schema probe failed; verify mirrored pool path and SSH owner')
            self._schema = json.loads(proc.stdout)
        return self._schema


class Backend:
    def __init__(self, state_dir=STATE_DIR, app_server=None):
        self.state_dir = Path(state_dir)
        self.state_file = self.state_dir/'workers.json'
        self.events_file = self.state_dir/'events.json'
        self.token_file = self.state_dir/'token'
        self.event_log_dir = self.state_dir/'events'
        self.state_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
        os.chmod(self.state_dir,0o700)
        # O_EXCL avoids two startup contenders replacing the bearer token.
        try:
            fd = os.open(self.token_file, os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
        except FileExistsError:
            pass
        else:
            with os.fdopen(fd,'w') as stream:
                stream.write(secrets.token_urlsafe(32))
        os.chmod(self.token_file,0o600)
        self.lock = threading.RLock()
        self.changed = threading.Condition(self.lock)
        self.store = SQLiteStore(self.state_dir/'state.sqlite3')
        self.app = app_server or AppServerClient()
        self.apps = {'local':self.app}
        self.workers = {}
        self.stop_event = threading.Event()
        self._closed = False
        self._last_lease_renew = 0.0
        with self.lock, self.store.transaction():
            self._load()
            self._recover()
        self.monitor = threading.Thread(target=self._monitor, daemon=True)
        self.monitor.start()

    def _load(self):
        rows = self.store.load_workers()
        if not rows and self.state_file.exists():
            rows = list(json.loads(self.state_file.read_text()).values())
        self.workers = {w['id']:Worker.from_dict(w) for w in rows}
        self._persist()
        if self.store.cursor() == 0 and self.events_file.exists():
            for event in json.loads(self.events_file.read_text()):
                worker = self.workers.get(event['workerId'])
                if not worker:
                    continue
                event['owner'] = worker.owner
                self.store.append_event(event)
                if worker.owner in event.get('acked',[]):
                    self.store.db.execute("UPDATE outbox SET state='acked' WHERE event_seq IN(SELECT seq FROM events WHERE event_id=?)",(event['eventId'],))

    def _persist(self):
        self.store.upsert_workers(asdict(w) for w in self.workers.values())

    def _recover(self, host=None, lost_runtime=None):
        for worker in self.workers.values():
            if host is not None and worker.host != host:
                continue
            if lost_runtime and worker.runtime_generation and worker.runtime_generation!=lost_runtime:
                continue
            self._close_requests(worker)
            worker.needs_resume = bool(worker.thread_id)
            worker.pid = None
            if worker.status in ACTIVE:
                worker.status = 'interrupted'
                worker.error = 'provider process lost; no work was replayed; send explicitly to resume'
                worker.active_turn_id = None
                worker.stop_requested = False
                worker.stop_deadline = None
                worker.updated_at = now_ms()
                if _leases_enabled() and worker.lease_id:
                    try: _lease_request('DELETE','/leases/'+worker.lease_id)
                    except Exception: pass
                    worker.lease_id = None
                self._emit(worker, {'kind':'recovery'})
        self._persist()

    def _emit(self, worker, extra=None, notify=True, event_id=None):
        event = {'eventId':event_id or str(uuid.uuid4()), 'workerId':worker.id, 'owner':worker.owner,
                 'status':worker.status, 'answer':worker.final_answer, 'error':worker.error,'createdAt':now_ms()}
        event.update(extra or {})
        # Always write the projection before the event (including legacy FK).
        self._persist()
        self.store.append_event(event, notify)

    def _log(self, worker, event):
        self.event_log_dir.mkdir(mode=0o700, exist_ok=True)
        path = self.event_log_dir/f'{worker.id}.jsonl'
        worker.event_log_path = str(path)
        # Provider frames are diagnostics. SQLite is authoritative.
        with path.open('a') as stream:
            stream.write(json.dumps(event)+'\n')
        os.chmod(path,0o600)

    def _client(self, host):
        if host not in {'local','remote'}:
            raise ValueError('host must be local or remote')
        if host not in self.apps:
            self.apps[host] = SshAppServerClient()
        return self.apps[host]

    def _monitor(self):
        while not self.stop_event.wait(0.03):
            if _leases_enabled() and time.monotonic() - self._last_lease_renew >= 30:
                self._last_lease_renew = time.monotonic()
                with self.lock:
                    for worker in self.workers.values():
                        if worker.lease_id:
                            try:
                                renewal = _lease_request('POST','/leases/'+worker.lease_id+'/renew', {})
                                if renewal.get('yield') and worker.status == 'running' and worker.active_turn_id:
                                    worker.stop_requested = True
                                    worker.status = 'stopping'
                                    self._client(worker.host).call('turn/interrupt', {'threadId':worker.thread_id,'turnId':worker.active_turn_id})
                            except Exception:
                                pass
            with self.lock:
                pending = [w for w in self.workers.values() if w.lease_ticket and w.status == 'pending']
                for worker in pending:
                    try:
                        state = _lease_ticket(worker.lease_ticket)
                    except Exception:
                        continue
                    if not state.get('granted'):
                        continue
                    worker.lease_id = state.get('lease_id')
                    worker.lease_ticket = None
                    try:
                        self._launch_worker(worker, worker._pending_prompt)
                        delattr(worker, '_pending_prompt')
                        self._emit(worker, {'kind':'lease_started'})
                    except Exception as exc:
                        worker.status = 'failed'; worker.error = str(exc)
                        if worker.lease_id:
                            try: _lease_request('DELETE','/leases/'+worker.lease_id)
                            except Exception: pass
                            worker.lease_id = None
                        delattr(worker, '_pending_prompt')
                        self._emit(worker, {'kind':'lease_failed'})
                clients = list(self.apps.items())
            for host, client in clients:
                for note in client.drain_notifications():
                    try:
                        self._handle_notification(note, host)
                    except Exception as exc:
                        # Keep the monitor alive; diagnostics contain no prompts/keys.
                        import sys
                        print(f'notification ingest failed: {type(exc).__name__}',file=sys.stderr)
                        if note.get('_ingestRetry',0)<2:
                            note['_ingestRetry'] = note.get('_ingestRetry',0)+1
                            client.notifications.put(note)

    def _runtime_request(self,note,worker,host):
        method, params = note.get('method',''), note.get('params',{})
        if 'id' in note:
            if method not in RESPONSE_TYPES:
                self._client(host).respond(note['id'],error={'code':-32601,'message':'unsupported worker runtime request; credential refresh is never performed'})
                return True
            runtime = note.get('_runtime') or getattr(self._client(host),'generation','fixture')
            request_id = str(uuid.uuid5(uuid.NAMESPACE_URL,host+':'+runtime+':'+json.dumps(note['id'])))
            if not self.store.db.execute('SELECT 1 FROM requests WHERE id=?',(request_id,)).fetchone():
                self.store.db.execute('INSERT INTO requests VALUES(?,?,?,?,?,?,?,?,?)',(request_id,worker.id,runtime,json.dumps(note['id']),method,json.dumps(params),'pending',None,now_ms()))
                self._emit(worker,{'kind':'request','requestId':request_id,'requestMethod':method,'status':'pending'})
            return True
        if method == 'error' and isinstance(params.get('error'),dict):
            info = params['error'].get('codexErrorInfo')
            limited = isinstance(info,str) and info in {'usageLimitExceeded','rateLimitExceeded','sessionBudgetExceeded'}
            limited = limited or isinstance(info,dict) and any(isinstance(detail,dict) and detail.get('httpStatusCode')==429 for detail in info.values())
            if limited:
                worker.rate_limits['lastRateLimitError'] = {'scope':'worker-turn','turnId':params.get('turnId') or worker.active_turn_id,'codexErrorInfo':info,'willRetry':bool(params.get('willRetry')),'observedAt':now_ms()}
                self._emit(worker,{'kind':'telemetry','rateLimitError':worker.rate_limits['lastRateLimitError']},notify=False)
        if method == 'serverRequest/resolved':
            self.store.db.execute("UPDATE requests SET status='resolved' WHERE worker_id=? AND provider_id=? AND status='pending'",(worker.id,json.dumps(params.get('requestId'))))
        if method in {'thread/tokenUsage/updated','thread/tokenUsageUpdated'}:
            worker.token_usage = dict(params.get('tokenUsage',params.get('usage',{})))
        elif method in {'account/rateLimits/updated','rateLimits/updated'}:
            worker.rate_limits = dict(params.get('rateLimits',{}))
        return False

    def _close_requests(self,worker):
        self.store.db.execute("UPDATE requests SET status='closed' WHERE worker_id=? AND status IN('pending','dispatching')",(worker.id,))

    def pending_requests(self,worker_id=None,caller='',all_workers=False):
        with self.lock:
            if worker_id:
                self._require_visible(self._get(worker_id),caller,all_workers)
            values = []
            for row in self.store.db.execute("SELECT * FROM requests WHERE status='pending' ORDER BY created_at"):
                worker = self._get(row['worker_id'])
                if worker_id and worker.id!=worker_id or worker.owner!=caller and (not all_workers or worker.host=='remote'):
                    continue
                value = dict(row)
                value['request_id'] = value['id']
                value['kind'] = 'question' if row['method'] in {'item/tool/requestUserInput','mcpServer/elicitation/request'} else 'approval'
                value['answerSchema'] = self._client(worker.host).schema()['responses'].get(row['method'],{})
                value['payload'] = json.loads(value.pop('payload'))
                values.append(value)
            return values

    def answer_request(self,request_id,answer,caller='',allow_other=False,command_id=None):
        with self.lock:
            row = self.store.db.execute('SELECT * FROM requests WHERE id=?',(request_id,)).fetchone()
            if not row:
                raise KeyError('unknown request')
            worker = self._get(row['worker_id'])
            self._require_mutation_owner(worker,caller,allow_other)
            client = self._client(worker.host)
            self._drain_host(worker.host)
            response_schema = client.schema()['responses'].get(row['method'])
            if not response_schema:
                raise ValueError('installed app-server has no supported response schema for this request')
            validate(answer,response_schema)
            params = json.loads(row['payload'])
            if row['method']=='item/tool/requestUserInput':
                expected = {q['id'] for q in params.get('questions',[])}
                if set(answer.get('answers',{})) != expected:
                    raise ValueError('answers must match all pending question ids')
            def action():
                live = self.store.db.execute('SELECT status FROM requests WHERE id=?',(request_id,)).fetchone()
                if live[0]!='pending' or row['runtime']!=getattr(client,'generation','fixture'):
                    raise ValueError('request is no longer pending')
                self.store.db.execute("UPDATE requests SET status='dispatching' WHERE id=?",(request_id,))
                self._client(worker.host).respond(json.loads(row['provider_id']),answer)
                self.store.db.execute("UPDATE requests SET status='answered',answer=? WHERE id=?",(json.dumps(answer),request_id))
                return {'requestId':request_id,'status':'answered'}
            return self._command(caller,command_id,'answer_request',{'requestId':request_id,'answer':answer,'allowOtherSession':allow_other},action)

    def _children(self, worker):
        return [w for w in self.workers.values() if w.parent_id == worker.id and w.native_parent_thread]

    def _handle_notification(self,note,host='local'):
        try:
            self._ingest_notification(note,host)
        except BaseException:
            with self.lock:
                self.workers = {w['id']:Worker.from_dict(w) for w in self.store.load_workers()}
            raise

    def _ingest_notification(self, note, host='local'):
        with self.changed, self.store.transaction():
            method, params = note.get('method',''), note.get('params',{})
            if method == '_runtime/lost':
                self._recover(host,note.get('_runtime'))
                self.changed.notify_all()
                return
            if method == 'account/rateLimits/updated':
                for w in self.workers.values():
                    if w.host == host:
                        w.rate_limits = {**merge_sparse(w.rate_limits,params.get('rateLimits',{})),'scope':'provider-account','observedAt':now_ms()}
                self._persist()
                return
            if 'id' in note and method not in RESPONSE_TYPES:
                self._client(host).respond(note['id'],error={'code':-32601,'message':'unsupported runtime request; credentials are never refreshed'})
                return
            thread_id = params.get('threadId') or params.get('conversationId') or params.get('thread',{}).get('id')
            worker = next((w for w in self.workers.values() if w.host==host and w.thread_id==thread_id),None)
            if method == 'thread/started' and not worker:
                thread = params.get('thread',{})
                source = thread.get('source')
                subagent = source.get('subAgent',{}) if isinstance(source,dict) else {}
                sub = subagent.get('thread_spawn',subagent.get('threadSpawn',{})) if isinstance(subagent,dict) else {}
                parent_thread = thread.get('parentThreadId') or sub.get('parent_thread_id') or sub.get('parentThreadId')
                parent = next((w for w in self.workers.values() if w.host==host and w.thread_id==parent_thread),None)
                if parent:
                    worker = Worker(str(uuid.uuid4()), thread_id, parent.cwd, parent.model, parent.effort,'idle',now_ms(),now_ms(),owner=parent.owner,host=host,parent_id=parent.id,native_parent_thread=parent_thread,stop_requested=parent.stop_requested or parent.interrupt_descendants,pid=parent.pid,transport_pid=parent.transport_pid,runtime_uid=parent.runtime_uid,runtime_generation=parent.runtime_generation)
                    self.workers[worker.id] = worker
            if not worker:
                return
            self._log(worker,note)
            item = params.get('item',{})
            if method in {'item/started','item/completed'} and item.get('type')=='collabAgentToolCall' and item.get('tool')=='spawnAgent':
                for child_thread in item.get('receiverThreadIds',[]):
                    state = item.get('agentsStates',{}).get(child_thread,{})
                    running = state.get('status') in {'running','pendingInit'}
                    child = next((w for w in self.workers.values() if w.host==host and w.thread_id==child_thread),None)
                    if not child:
                        child = Worker(str(uuid.uuid4()),child_thread,worker.cwd,worker.model,worker.effort,'starting' if running else 'idle',now_ms(),now_ms(),owner=worker.owner,host=host,parent_id=worker.id,native_parent_thread=worker.thread_id,stop_requested=worker.stop_requested or worker.interrupt_descendants,pid=worker.pid,transport_pid=worker.transport_pid,runtime_uid=worker.runtime_uid,runtime_generation=worker.runtime_generation)
                        self.workers[child.id] = child
                    elif running and child.status=='idle' and not child.completed_turns:
                        child.status = 'starting'
                    if child.status in ACTIVE and (worker.stop_requested or worker.interrupt_descendants):
                        worker.stop_requested = True
                        worker.status = 'stopping'
                        self._interrupt_tree(child)
                self._persist()
            if self._runtime_request(note,worker,host):
                self.changed.notify_all()
                return
            if method == 'turn/started':
                turn_id = params.get('turn',{}).get('id')
                if turn_id not in worker.completed_turns:
                    worker.active_turn_id = turn_id
                    worker.status = 'stopping' if worker.stop_requested else 'running'
                    if worker.stop_requested:
                        if worker.parent_id and worker.native_parent_thread:
                            parent = self.workers[worker.parent_id]
                            parent.stop_requested = True
                            parent.status = 'stopping'
                        self._client(host).call('turn/interrupt',{'threadId':worker.thread_id,'turnId':turn_id})
            elif method == 'turn/completed':
                turn = params.get('turn',{})
                turn_id = turn.get('id')
                if not turn_id or turn_id in worker.completed_turns:
                    return
                worker.completed_turns.append(turn_id)
                if worker.active_turn_id and worker.active_turn_id != turn_id:
                    self._persist()
                    return  # late terminal from a previous turn cannot stop new work
                worker.active_turn_id = None
                worker.last_completed_turn_id = turn_id
                worker.terminal_received = True
                worker.final_answer = self._answer_from_turn(turn) or worker.final_answer
                worker.error = str(turn['error']) if turn.get('error') else None
                worker.status = {'completed':'completed','interrupted':'stopped' if worker.stop_requested else 'interrupted','failed':'failed'}.get(turn.get('status'),'failed')
                worker.terminal_status = worker.status
                if _leases_enabled() and worker.lease_id:
                    try: _lease_request('DELETE','/leases/'+worker.lease_id)
                    except Exception: pass
                    worker.lease_id = None
                self._close_requests(worker)
                if worker.stop_requested and any(c.status in ACTIVE for c in self._children(worker)):
                    worker.status = 'stopping'
                else:
                    stopping = worker.stop_requested
                    worker.stop_requested = False
                    self._emit(worker, {'kind':'completion','turnId':turn_id}, event_id=f'{worker.id}:{turn_id}:terminal')
                    if stopping:
                        self._cancel_queue(worker)
                    elif worker.send_queue:
                        self._start_next_queued(worker)
                descendant = worker
                while descendant.parent_id and descendant.native_parent_thread:
                    parent = self.workers[descendant.parent_id]
                    if parent.stop_requested and parent.terminal_received and not any(c.status in ACTIVE for c in self._children(parent)):
                        parent.status = parent.terminal_status or 'interrupted'
                        parent.stop_requested = False
                        self._emit(parent, {'kind':'completion','turnId':parent.last_completed_turn_id},event_id=f'{parent.id}:{parent.last_completed_turn_id}:terminal')
                    descendant = parent
            elif method == 'item/completed':
                item = params.get('item',{})
                if item.get('type')=='agentMessage' and params.get('turnId')==worker.active_turn_id:
                    worker.history.append({'turnId':params['turnId'],'itemId':item.get('id'),'text':item.get('text','')})
                    worker.final_answer = '\n'.join(i['text'] for i in worker.history if i.get('turnId')==worker.active_turn_id and 'text' in i)
            elif method == 'error' and not params.get('willRetry'):
                worker.error = str(params.get('error','app-server error'))
                # turn/completed owns terminal state, including interruption.
            worker.updated_at = now_ms()
            if method in {'thread/tokenUsage/updated','thread/tokenUsageUpdated'}:
                self._emit(worker,{'kind':'telemetry','tokenUsage':worker.token_usage},notify=False)
            self._persist()
            self.changed.notify_all()

    @staticmethod
    def _answer_from_turn(turn):
        return '\n'.join(i.get('text','') for i in turn.get('items',[]) if i.get('type')=='agentMessage').strip()

    def _get(self, worker_id):
        if worker_id not in self.workers:
            raise KeyError(f'unknown worker {worker_id}')
        return self.workers[worker_id]

    @staticmethod
    def _require_visible(worker, caller, all_workers=False):
        if worker.owner != caller and (not all_workers or worker.host=='remote'):
            raise PermissionError('worker belongs to another Claude session')

    def _require_mutation_owner(self, worker, caller, allow_other=False):
        self._require_visible(worker,caller,allow_other and worker.host=='local')

    def _command(self, caller, command_id, operation, args, action):
        command_id = command_id or str(uuid.uuid4())
        if not isinstance(command_id,str) or not command_id or len(command_id)>256:
            raise ValueError('invalid clientRequestId')
        fingerprint = hashlib.sha256(json.dumps(args,sort_keys=True,separators=(',',':')).encode()).hexdigest()
        # Persist intent before dispatch. A crash leaves an indeterminate receipt,
        # which refuses automatic replay rather than risking duplicate model work.
        prior = self.store.reserve_command(caller,command_id,operation,fingerprint)
        if prior is not None:
            return prior
        try:
            result = action()
            value = asdict(result) if isinstance(result,Worker) else result
            with self.store.transaction():
                self._persist()
                self.store.finish_command(caller,command_id,value)
                worker_id = value.get('id') if isinstance(value,dict) else None
                if worker_id in self.workers:
                    self._emit(self.workers[worker_id],{'kind':'command','operation':operation,'clientRequestId':command_id},notify=False,event_id=str(uuid.uuid5(uuid.NAMESPACE_URL,caller+':'+command_id+':accepted')))
            return value
        except BaseException as exc:
            self.store.fail_command(caller,command_id,exc)
            raise

    def _drain_host(self,host):
        for note in self._client(host).drain_notifications():
            self._handle_notification(note,host)

    def _resume(self, worker):
        client = self._client(worker.host)
        dead = isinstance(client,AppServerClient) and client.proc is not None and client.proc.poll() is not None
        changed = worker.runtime_generation and worker.runtime_generation!=getattr(client,'generation',None)
        if dead or changed:
            self._recover(worker.host,worker.runtime_generation)
        if worker.needs_resume:
            params = {'threadId':worker.thread_id,'excludeTurns':True,'modelProvider':request_defaults()['modelProvider'],'model':worker.model,'approvalPolicy':worker.approval_policy,'serviceTier':'default','cwd':worker.cwd}
            if 'deferGoalContinuation' in client.schema()['methods'].get('thread/resume',{}).get('properties',{}):
                params['deferGoalContinuation'] = True
            result = client.call('thread/resume',params)
            if result.get('thread',{}).get('id') != worker.thread_id:
                raise RpcError('resume returned a different native thread identity')
            observed_provider = result.get('modelProvider') or result.get('thread',{}).get('modelProvider')
            if observed_provider and observed_provider!=request_defaults()['modelProvider']:
                raise RpcError('resume returned a different provider')
            worker.needs_resume = False
            worker.runtime_generation = getattr(client,'generation',None)
            client = self._client(worker.host)
            worker.pid = getattr(client,'remote_pid',None) if worker.host=='remote' else getattr(getattr(client,'proc',None),'pid',None)
            worker.transport_pid = getattr(getattr(client,'proc',None),'pid',None) if worker.host=='remote' else None
            worker.runtime_uid = getattr(client,'remote_uid',None) if worker.host=='remote' else os.getuid()
            worker.active_turn_id = None
            self._persist()

    def _launch_worker(self, worker, prompt):
        client = self._client(worker.host)
        params = {'cwd':worker.cwd,'modelProvider':request_defaults(worker.model,worker.effort)['modelProvider'],'model':worker.model,'serviceTier':'default','approvalPolicy':worker.approval_policy,'experimentalRawEvents':False}
        if 'historyMode' in client.schema()['methods'].get('thread/start',{}).get('properties',{}):
            params['historyMode'] = 'paginated'
        result = client.call('thread/start',params)
        worker.history_mode = result.get('thread',{}).get('historyMode',params.get('historyMode','legacy'))
        worker.thread_id = result.get('thread',{}).get('id')
        if not worker.thread_id:
            raise RpcError('thread/start returned no thread id')
        worker.pid = getattr(client,'remote_pid',None) if worker.host=='remote' else getattr(getattr(client,'proc',None),'pid',None)
        worker.transport_pid = getattr(getattr(client,'proc',None),'pid',None) if worker.host=='remote' else None
        worker.runtime_uid = getattr(client,'remote_uid',None) if worker.host=='remote' else os.getuid()
        self._persist()
        self._start_turn(worker,prompt)

    def spawn(self,prompt,cwd,model=None,effort=None,owner='',command_id=None,host='local',approval_policy='on-request',pending=False,parent_id=None):
        if approval_policy not in {'on-request','never','untrusted','on-failure'}:
            raise ValueError('unsupported approvalPolicy')
        if host not in {'local','remote'} or not Path(cwd).is_absolute():
            raise ValueError('host must be local or remote; cwd must be absolute on that host')
        defaults = request_defaults(model,effort)
        with self.lock:
            def action():
                lease_id = None
                if _leases_enabled():
                    try: lease = _lease_request('POST','/leases',{'holder':owner or 'codex-subagents','pid':os.getpid(),'host':host,'priority':'lead','ttl':90,'pending':pending})
                    except Exception as exc: raise RpcError('capacity full; retry later or use pending') from exc
                    if not lease.get('granted') and not lease.get('pending'): raise RpcError('capacity full; retry later or use pending')
                    lease_id = lease.get('lease_id') if lease.get('granted') else None
                worker = Worker(str(uuid.uuid4()),None,cwd,str(defaults['model']),str(defaults['effort']),'pending' if _leases_enabled() and not lease_id else 'starting',now_ms(),now_ms(),owner=owner,host=host,parent_id=parent_id,approval_policy=approval_policy,lease_id=lease_id,lease_ticket=(lease.get('ticket') if _leases_enabled() and not lease_id else None))
                self.workers[worker.id] = worker
                self._persist()
                if worker.status == 'pending':
                    worker._pending_prompt = prompt
                    self._emit(worker, {'kind':'lease_pending'}, notify=True)
                    return worker
                try: self._launch_worker(worker,prompt)
                except Exception:
                    if _leases_enabled() and worker.lease_id:
                        try: _lease_request('DELETE','/leases/'+worker.lease_id)
                        except Exception: pass
                        worker.lease_id = None
                    raise
                return worker
            result = self._command(owner,command_id,'spawn',locals_for_command(prompt=prompt,cwd=cwd,model=model,effort=effort,host=host,approval_policy=approval_policy,pending=pending,parentId=parent_id),action)
            return Worker.from_dict(result)

    def _start_turn(self,worker,message):
        worker.status = 'starting'
        worker.active_turn_id = None
        worker.final_answer = None
        worker.error = None
        worker.terminal_received = False
        worker.terminal_status = None
        worker.interrupt_descendants = False
        self._persist()
        result = self._client(worker.host).call('turn/start',{'threadId':worker.thread_id,'input':[{'type':'text','text':message}], 'model':worker.model,'effort':worker.effort,'serviceTier':'default','approvalPolicy':worker.approval_policy})
        worker.active_turn_id = result.get('turn',{}).get('id')
        if not worker.active_turn_id:
            raise RpcError('turn/start returned no turn id')
        worker.status = 'running'
        worker.runtime_generation = getattr(self._client(worker.host),'generation',None)
        self._persist()

    @staticmethod
    def _queue_preview(message, limit=160):
        message = str(message).replace('\n', ' ')
        return message if len(message) <= limit else message[:limit-1] + '…'

    def _worker_view(self, worker):
        value = asdict(worker)
        value['pendingQueueCount'] = len(worker.send_queue)
        value['pendingQueuePreviews'] = [self._queue_preview(item.get('message', '')) for item in worker.send_queue]
        return value

    def _start_next_queued(self, worker):
        """Start the oldest queued message, retaining it until turn/start succeeds."""
        if not worker.send_queue or worker.active_turn_id or worker.stop_requested or worker.status in {'starting', 'stopping'}:
            return False
        item = worker.send_queue[0]
        settled_status = worker.status
        try:
            self._start_turn(worker, item['message'])
        except Exception as exc:
            if isinstance(exc, RpcError) and self._steer_race(exc):
                # A just-finished native turn can briefly reject turn/start too.
                # Drain its terminal notification and retry the same durable item.
                self._drain_host(worker.host)
                try:
                    self._start_turn(worker, item['message'])
                except Exception as retry_exc:
                    exc = retry_exc
                else:
                    worker.send_queue.pop(0)
                    worker.updated_at = now_ms()
                    self._persist()
                    return True
            # The item remains durable and visible for a later explicit retry.
            worker.status = settled_status if settled_status in TERMINAL else 'failed'
            worker.error = f'queued send was not started: {exc}'
            worker.updated_at = now_ms()
            self._persist()
            return False
        worker.send_queue.pop(0)
        worker.updated_at = now_ms()
        self._persist()
        return True

    def _cancel_queue(self, worker, reason='explicit stop'):
        if not worker.send_queue:
            return
        count = len(worker.send_queue)
        dropped_at = now_ms()
        for item in worker.send_queue:
            worker.dropped_queue.append({**item, 'droppedAt': dropped_at, 'dropReason': reason})
        worker.send_queue.clear()
        worker.updated_at = dropped_at
        self._emit(worker, {'kind': 'queue_cancelled', 'count': count, 'reason': reason}, notify=True)

    @staticmethod
    def _steer_race(exc):
        text = str(exc).lower()
        return (
            ('expected' in text and 'turn' in text) or
            'no active turn' in text or
            'turn is no longer active' in text or
            'turn already completed' in text or
            'turn completing' in text or
            'already completing' in text or
            'still completing' in text
        )

    def send(self,worker_id,message,caller='',allow_other=False,command_id=None,mode='steer'):
        if mode not in {'steer', 'queue'}:
            raise ValueError('mode must be steer or queue')
        with self.lock:
            worker = self._get(worker_id)
            self._require_mutation_owner(worker,caller,allow_other)
            self._drain_host(worker.host)
            def action():
                if worker.stop_requested or worker.status == 'stopping':
                    raise ValueError('worker has unsettled work')
                self._resume(worker)
                if mode == 'queue':
                    worker.send_queue.append({'id': str(uuid.uuid4()), 'message': message, 'enqueuedAt': now_ms()})
                    self._persist()
                    if not worker.active_turn_id and worker.status not in {'starting', 'stopping'}:
                        self._start_next_queued(worker)
                elif worker.status=='running' and worker.active_turn_id:
                    expected_turn_id = worker.active_turn_id
                    try:
                        self._client(worker.host).call('turn/steer',{'threadId':worker.thread_id,'expectedTurnId':expected_turn_id,'input':[{'type':'text','text':message}]})
                    except RpcError as exc:
                        if not self._steer_race(exc):
                            raise
                        # Drain a terminal notification that raced our status check,
                        # then submit this command once as a fresh turn.
                        self._drain_host(worker.host)
                        if worker.stop_requested or worker.status == 'stopping':
                            raise ValueError('worker is stopping')
                        if worker.active_turn_id == expected_turn_id:
                            worker.active_turn_id = None
                        worker.send_queue.append({'id': str(uuid.uuid4()), 'message': message, 'enqueuedAt': now_ms()})
                        self._persist()
                        self._start_next_queued(worker)
                else:
                    self._start_turn(worker,message)
                return worker
            return Worker.from_dict(self._command(caller,command_id,'send',locals_for_command(id=worker_id,message=message,allow_other=allow_other,mode=mode),action))

    def _interrupt_tree(self,worker):
        worker.interrupt_descendants = True
        for child in self._children(worker):
            self._interrupt_tree(child)
        if worker.status in ACTIVE:
            worker.stop_requested = True
            worker.status = 'stopping'
            self._persist()
            if not worker.active_turn_id and worker.native_parent_thread:
                client = self._client(worker.host)
                params = {'threadId':worker.thread_id,'excludeTurns':True}
                if 'deferGoalContinuation' in client.schema()['methods'].get('thread/resume',{}).get('properties',{}):
                    params['deferGoalContinuation'] = True
                resumed = client.call('thread/resume',params)
                if resumed.get('thread',{}).get('id')!=worker.thread_id:
                    raise RpcError('descendant resume returned a different native identity')
                page = client.call('thread/turns/list',{'threadId':worker.thread_id,'sortDirection':'desc','itemsView':'notLoaded','limit':1})
                active = next((t for t in page.get('data',[]) if t.get('status')=='inProgress'),None)
                worker.active_turn_id = active.get('id') if active else None
                self._persist()
            if worker.active_turn_id:
                self._client(worker.host).call('turn/interrupt',{'threadId':worker.thread_id,'turnId':worker.active_turn_id})
        elif any(c.status in ACTIVE for c in self._children(worker)):
            worker.stop_requested = True
            worker.status = 'stopping'
            self._persist()

    def stop(self,worker_id,caller='',allow_other=False,command_id=None,timeout_sec=10):
        with self.changed:
            worker = self._get(worker_id)
            self._require_mutation_owner(worker,caller,allow_other)
            self._drain_host(worker.host)
            def action():
                self._cancel_queue(worker)
                self._interrupt_tree(worker)
                deadline = time.monotonic()+max(0,min(timeout_sec,30))
                while worker.status=='stopping' and time.monotonic()<deadline:
                    self.changed.wait(timeout=min(0.1,max(0,deadline-time.monotonic())))
                return worker  # timeout returns stopping, never fabricated stopped
            return Worker.from_dict(self._command(caller,command_id,'stop',locals_for_command(id=worker_id,allow_other=allow_other),action))

    def capabilities(self,host='local'):
        with self.lock:
            schema = self._client(host).schema()
            methods = schema['methods']
            return {'host':host,'fork':'thread/fork' in methods,'rollback':'thread/revert' in methods,
                    'contextHandoff':'thread/inject_items' in methods,'paginatedHistory':'historyMode' in methods.get('thread/start',{}).get('properties',{}),
                    'rollbackMethod':'thread/revert' if 'thread/revert' in methods else None,
                    'rollbackSemantics':'exclude beforeTurnId and all later conversation turns; filesystem changes are preserved'}

    def _require_native(self,worker,method):
        if method not in self._client(worker.host).schema()['methods']:
            raise ValueError(f'unsupported by installed app-server: {method}')

    def _handoff(self,worker,text,source):
        if not text:
            return
        self._require_native(worker,'thread/inject_items')
        worker.history.append({'type':'context_handoff','text':text,'source':source,'state':'dispatching'})
        self._persist()
        self._client(worker.host).call('thread/inject_items',{'threadId':worker.thread_id,'items':[{'type':'message','role':'developer','content':[{'type':'input_text','text':text}]}]})
        worker.history[-1]['state'] = 'delivered'
        self._persist()

    def fork(self,worker_id,caller='',last_turn_id=None,before_turn_id=None,context_handoff=None,command_id=None):
        with self.lock:
            worker = self._get(worker_id)
            self._require_mutation_owner(worker,caller,False)
            self._drain_host(worker.host)
            self._require_native(worker,'thread/fork')
            if last_turn_id and before_turn_id:
                raise ValueError('lastTurnId and beforeTurnId are mutually exclusive')
            if context_handoff:
                self._require_native(worker,'thread/inject_items')
            def action():
                if worker.status in ACTIVE:
                    raise ValueError('fork requires a settled worker')
                self._resume(worker)
                params = {'threadId':worker.thread_id,'excludeTurns':True,'deferGoalContinuation':True}
                if last_turn_id: params['lastTurnId'] = last_turn_id
                if before_turn_id: params['beforeTurnId'] = before_turn_id
                native = self._client(worker.host).call('thread/fork',params).get('thread',{})
                if not native.get('id') or native['id']==worker.thread_id:
                    raise RpcError('native fork did not return a distinct thread')
                child = Worker(str(uuid.uuid4()),native['id'],worker.cwd,worker.model,worker.effort,'idle',now_ms(),now_ms(),owner=worker.owner,host=worker.host,pid=worker.pid,parent_id=worker.id,approval_policy=worker.approval_policy,history_mode=native.get('historyMode',worker.history_mode),transport_pid=worker.transport_pid,runtime_uid=worker.runtime_uid,runtime_generation=worker.runtime_generation)
                child.history.append({'type':'fork','sourceWorkerId':worker.id,'sourceThreadId':worker.thread_id,'lastTurnId':last_turn_id,'beforeTurnId':before_turn_id})
                self.workers[child.id] = child
                self._persist()
                self._handoff(child,context_handoff,worker.id)
                return child
            args = {'id':worker_id,'lastTurnId':last_turn_id,'beforeTurnId':before_turn_id,'contextHandoff':context_handoff}
            return Worker.from_dict(self._command(caller,command_id,'fork',args,action))

    def rollback(self,worker_id,before_turn_id,caller='',allow_other=False,command_id=None,context_handoff=None):
        with self.lock:
            worker = self._get(worker_id)
            self._require_mutation_owner(worker,caller,allow_other)
            self._drain_host(worker.host)
            self._require_native(worker,'thread/revert')
            self._require_native(worker,'thread/turns/list')
            if context_handoff:
                self._require_native(worker,'thread/inject_items')
            def action():
                if worker.status in ACTIVE:
                    raise ValueError('rollback requires a settled worker')
                self._resume(worker)
                client = self._client(worker.host)
                native = client.call('thread/read',{'threadId':worker.thread_id,'includeTurns':False}).get('thread',{})
                if native.get('id')!=worker.thread_id:
                    raise RpcError('native read returned a different thread')
                if native.get('historyMode')!='paginated':
                    raise ValueError('rollback unsupported for legacy native history; native thread/revert requires paginated history')
                cursor, found, seen = None,False,set()
                for _ in range(1000):
                    page = client.call('thread/turns/list',{'threadId':worker.thread_id,'sortDirection':'desc','itemsView':'notLoaded','limit':100,**({'cursor':cursor} if cursor else {})})
                    if any(t.get('id')==before_turn_id for t in page.get('data',[])):
                        found = True; break
                    cursor = page.get('nextCursor')
                    if not cursor: break
                    if cursor in seen: raise RpcError('native turns cursor repeated')
                    seen.add(cursor)
                if not found: raise ValueError('beforeTurnId is not a turn in this worker')
                client.call('thread/revert',{'threadId':worker.thread_id,'beforeTurnId':before_turn_id})
                worker.status = 'idle'
                worker.final_answer = None
                worker.last_completed_turn_id = None
                worker.active_turn_id = None
                worker.terminal_received = False
                worker.error = None
                worker.updated_at = now_ms()
                self._persist()
                self._handoff(worker,context_handoff,worker.id)
                return {'id':worker.id,'threadId':worker.thread_id,'status':'rolled_back','beforeTurnId':before_turn_id,'filesystemReverted':False}
            return self._command(caller,command_id,'rollback',{'id':worker_id,'beforeTurnId':before_turn_id,'contextHandoff':context_handoff,'allowOtherSession':allow_other},action)

    def status(self,worker_id,caller='',all_workers=False):
        with self.lock:
            worker = self._get(worker_id)
            self._require_visible(worker,caller,all_workers)
            value = self._worker_view(worker)
            value['pendingRequests'] = self._pending_refs({worker_id})
            return value

    def result(self,worker_id,caller='',all_workers=False):
        status = self.status(worker_id,caller,all_workers)
        return {'id':worker_id,'status':status['status'],'answer':status['final_answer'],'error':status['error'],'pendingRequests':status['pendingRequests']}

    def list(self,caller,all_workers=False):
        with self.lock:
            return [self._worker_view(w) for w in self.workers.values() if w.owner==caller or (all_workers and w.host=='local')]

    def snapshot(self,caller,all_workers=False):
        with self.lock:
            return {'workers':self.list(caller,all_workers),'cursor':self.store.cursor(),'version':'0.2.0'}

    def _pending_refs(self,wanted):
        wanted = set(wanted)
        stack = list(wanted)
        while stack:
            parent = self.workers[stack.pop()]
            for child in self._children(parent):
                if child.id not in wanted:
                    wanted.add(child.id)
                    stack.append(child.id)
        return [{'requestId':r['id'],'workerId':r['worker_id'],'method':r['method']} for r in self.store.db.execute("SELECT id,worker_id,method FROM requests WHERE status='pending' ORDER BY created_at") if r['worker_id'] in wanted]

    def wait_state(self,ids,caller='',timeout_sec=30,all_workers=False):
        with self.lock:
            finished = self.wait(ids,caller,timeout_sec,all_workers,wake_on_requests=True)
            wanted = {i for i,w in self.workers.items() if w.owner==caller or (all_workers and w.host=='local')} if ids=='any' else set(ids)
            return {'finished':finished,'pendingRequests':self._pending_refs(wanted)}

    def wait(self,ids,caller='',timeout_sec=30,all_workers=False,wake_on_requests=False):
        with self.changed:
            wanted = {i for i,w in self.workers.items() if w.owner==caller or (all_workers and w.host=='local')} if ids=='any' else set(ids)
            for wid in wanted:
                self._require_visible(self._get(wid),caller,all_workers)
            deadline = time.monotonic()+max(0,min(timeout_sec,30))
            while True:
                finished = [self.result(wid,caller,all_workers) for wid in wanted if self.workers[wid].status in TERMINAL]
                if (ids=='any' and finished) or len(finished)==len(wanted) or wake_on_requests and self._pending_refs(wanted) or time.monotonic()>=deadline:
                    return finished
                self.changed.wait(timeout=min(0.1,max(0,deadline-time.monotonic())))

    def claim_events(self,consumer,lease_owner=None,lease_seconds=30):
        with self.lock:
            return self.store.claim_outbox(consumer,lease_owner or consumer,lease_seconds)

    def ack_events(self,consumer,claims):
        with self.lock:
            self.store.ack_outbox(consumer,claims)

    def read_events(self,caller,after=0):
        with self.lock:
            events = self.store.load_events(caller,after)
            high = self.store.cursor()
            return {'events':events,'cursor':events[-1]['seq'] if events else max(after,high),'highWatermark':high}

    def close(self):
        if self._closed:
            return
        self._closed = True
        self.stop_event.set()
        self.monitor.join(timeout=35)
        for client in set(self.apps.values()):
            client.close()
        self.store.close()


def locals_for_command(**kwargs):
    return kwargs


class Handler(BaseHTTPRequestHandler):
    backend: Backend
    token: str

    def log_message(self,*_):
        pass

    def _auth(self):
        return secrets.compare_digest(self.headers.get('Authorization',''),f'Bearer {self.token}')

    def _json(self,status,value):
        body = json.dumps(value).encode()
        self.send_response(status)
        self.send_header('Content-Type','application/json')
        self.send_header('Content-Length',str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if not self._auth():
            return self._json(401,{'error':'unauthorized'})
        try:
            parsed = urlparse(self.path)
            query = parse_qs(parsed.query)
            caller = query.get('session',[''])[0]
            all_workers = query.get('all',['false'])[0]=='true'
            if parsed.path=='/v1/list':
                value = self.backend.snapshot(caller,all_workers)
            elif parsed.path=='/v1/events':
                consumer = query.get('consumer',[''])[0]
                value = {'events':self.backend.claim_events(consumer)}
            elif parsed.path=='/v1/event-log':
                value = self.backend.read_events(caller,int(query.get('after',['0'])[0]))
            elif parsed.path=='/v1/requests':
                value = {'requests':self.backend.pending_requests(query.get('workerId',[None])[0],caller,all_workers)}
            else:
                return self._json(404,{'error':'not found'})
            self._json(200,value)
        except (ValueError,KeyError,PermissionError) as exc:
            self._json(403 if isinstance(exc,PermissionError) else 400,{'error':str(exc)})

    def do_POST(self):
        if not self._auth():
            return self._json(401,{'error':'unauthorized'})
        try:
            length = int(self.headers.get('Content-Length','0'))
            if length>2*1024*1024:
                raise ValueError('request too large')
            p = json.loads(self.rfile.read(length) or b'{}')
            caller = p.get('session','')
            command = p.get('clientRequestId')
            path = self.path
            if path=='/v1/spawn':
                value = asdict(self.backend.spawn(p['prompt'],p['cwd'],p.get('model'),p.get('effort'),caller,command,p.get('host','local'),p.get('approvalPolicy','on-request'),bool(p.get('pending',False)),p.get('parentId')))
            elif path=='/v1/send':
                value = asdict(self.backend.send(p['id'],p['message'],caller,bool(p.get('allowOtherSession')),command,p.get('mode','steer')))
            elif path=='/v1/stop':
                value = asdict(self.backend.stop(p['id'],caller,bool(p.get('allowOtherSession')),command,p.get('timeoutSec',10)))
            elif path=='/v1/wait':
                value = self.backend.wait_state(p.get('ids','any'),caller,p.get('timeoutSec',30),bool(p.get('all')))
            elif path in {'/v1/status','/v1/result'}:
                value = getattr(self.backend,path.rsplit('/',1)[1])(p['id'],caller,bool(p.get('all')))
            elif path=='/v1/ack-events':
                self.backend.ack_events(p['consumer'],p.get('claims',[]))
                value = {'ok':True}
            elif path=='/v1/requests':
                value = {'requests':self.backend.pending_requests(p.get('workerId'),caller,bool(p.get('all')))}
            elif path=='/v1/fork':
                value = asdict(self.backend.fork(p['id'],caller,p.get('lastTurnId'),p.get('beforeTurnId'),p.get('contextHandoff'),command))
            elif path=='/v1/rollback':
                value = self.backend.rollback(p['id'],p['beforeTurnId'],caller,bool(p.get('allowOtherSession')),command,p.get('contextHandoff'))
            elif path=='/v1/capabilities':
                value = self.backend.capabilities(p.get('host','local'))
            elif path=='/v1/answer-request':
                value = self.backend.answer_request(p['requestId'],p['answer'],caller,bool(p.get('allowOtherSession')),command)
            else:
                return self._json(404,{'error':'not found'})
            self._json(200,value)
        except (KeyError,RpcError,ValueError,OSError,PermissionError,TypeError) as exc:
            self._json(403 if isinstance(exc,PermissionError) else 400,{'error':str(exc)})


def run_server(host=None, port=None, state_dir=STATE_DIR):
    """Run the daemon on its private Unix socket.

    ``host``/``port`` are retained only for old test callers that explicitly
    request a port. Normal startup never supplies them and never probes a
    fixed TCP port.
    """
    # Lock before opening state or recovering workers. Concurrent startup must
    # not mistake another daemon's live workers for process-loss recovery.
    import fcntl
    state_dir.mkdir(mode=0o700,parents=True,exist_ok=True)
    with (state_dir/'daemon.lock').open('a') as lock:
        os.chmod(state_dir/'daemon.lock',0o600)
        try:
            fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        except BlockingIOError:
            return
        socket_path, endpoint_path = paths(state_dir)
        try:
            if _remove_stale_socket(socket_path):
                return
        except OSError:
            return
        try:
            unix_server = ThreadingUnixHTTPServer(str(socket_path), Handler)
            os.chmod(socket_path, 0o600)
        except OSError:
            return
        tcp_server = None
        try:
            # Hooks use $.http, which cannot address AF_UNIX. Publish only an
            # OS-assigned loopback port for that compatibility path.
            if port is not None:
                if host not in (None, '127.0.0.1'):
                    raise ValueError('daemon compatibility listener must bind to 127.0.0.1')
                tcp_server = ThreadingHTTPServer(('127.0.0.1', port), Handler)
            else:
                tcp_server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        except OSError:
            unix_server.server_close()
            try:
                socket_path.unlink()
            except FileNotFoundError:
                pass
            return
        backend = None
        try:
            backend = Backend(state_dir)
            Handler.backend,Handler.token = backend,backend.token_file.read_text().strip()
            write_endpoint(state_dir, socket_path, tcp_server.server_port)
            tcp_thread = threading.Thread(target=tcp_server.serve_forever, daemon=True)
            tcp_thread.start()
            unix_server.serve_forever()
        finally:
            unix_server.shutdown()
            unix_server.server_close()
            if tcp_server:
                tcp_server.shutdown()
                tcp_server.server_close()
            try:
                socket_path.unlink()
            except FileNotFoundError:
                pass
            try:
                endpoint_path.unlink()
            except FileNotFoundError:
                pass
            if backend:
                backend.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('serve',nargs='?',default='serve')
    parser.add_argument('--detach',action='store_true')
    parser.add_argument('--state-dir',type=Path,default=STATE_DIR)
    args = parser.parse_args()
    if args.detach and os.name!='nt':
        if os.fork():
            return
        os.setsid()
        fd = os.open(os.devnull,os.O_RDWR)
        for stream in (0,1,2):
            os.dup2(fd,stream)
        os.close(fd)
    run_server(state_dir=args.state_dir)


if __name__=='__main__':
    main()
