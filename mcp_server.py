#!/usr/bin/env python3
"""MCP transport for the installed codex-subagents plugin.

Installed Claude plugins expose MCP servers reliably.  Function-hook modules
remain in hooks/ for --plugin-dir/CLAUDE_CODE_PLUGIN_DIRS development sessions;
this server carries the worker tools for the normal installed-plugin path.
"""

from __future__ import annotations

import json
import os
import subprocess
import threading
import time
import sys
import urllib.error
import urllib.parse
import urllib.request
import uuid
from pathlib import Path
from typing import Any
from backend.protocol import validate
from backend.transport import UnixHTTPConnection, endpoint


ROOT = Path(__file__).resolve().parent
STATE_DIR = Path(os.environ.get("CODEX_SUBAGENTS_STATE_DIR", Path.home() / ".codex-subagents"))
TOKEN_FILE = STATE_DIR / "token"
CONSUMER = os.environ.get("CLAUDE_SESSION_ID") or os.environ.get("CLAUDE_CODE_SESSION_ID") or f"mcp-{uuid.uuid4()}"
MCP_CLIENT_INFO: dict[str, Any] = {}


OUTPUT_LOCK = threading.Lock()
RESOURCE = "codex-subagents://pending-requests"

TOOLS: list[dict[str, Any]] = json.loads((ROOT / 'tools.json').read_text())


def _native_parent_id() -> str | None:
    """Return an explicitly supplied caller-native session/thread id.

    MCP does not standardize a parent thread field. Codex integrations can
    provide CODEX_THREAD_ID (or CODEX_PARENT_THREAD_ID); the Pi extension
    provides PI_SESSION_ID. If neither the environment nor initialize's
    clientInfo contains one, parentage is intentionally left unset.
    """
    for name in ("CODEX_THREAD_ID", "CODEX_PARENT_THREAD_ID", "PI_SESSION_ID"):
        value = os.environ.get(name)
        if value:
            return value
    for name in ("threadId", "thread_id", "parentThreadId", "parent_thread_id", "sessionId", "session_id"):
        value = MCP_CLIENT_INFO.get(name)
        if isinstance(value, str) and value:
            return value
    return None


def _spawn_payload(args: dict[str, Any]) -> dict[str, Any]:
    payload = {**args, "session": CONSUMER}
    parent_id = _native_parent_id()
    if parent_id:
        payload["parentId"] = parent_id
    return payload

def _token() -> str:
    return TOKEN_FILE.read_text(encoding="utf-8").strip()


def _ensure_daemon() -> None:
    try:
        value = _request("GET", "/v1/list?session=" + urllib.parse.quote(CONSUMER))
        if value.get("version")!="0.2.0":
            raise RuntimeError("an older daemon owns the codex-subagents endpoint; finish its workers and restart that daemon before using this plugin")
        return
    except (OSError,ValueError):
        pass
    subprocess.run([sys.executable, str(ROOT / "backend" / "codex_subagents.py"), "--detach"], check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    for _ in range(50):
        try:
            value = _request("GET", "/v1/list?session=" + urllib.parse.quote(CONSUMER))
            if value.get("version")!="0.2.0":
                raise RuntimeError("older worker daemon is still running; restart it after its workers settle")
            return
        except (OSError,ValueError):
            time.sleep(0.1)
    raise RuntimeError("worker daemon failed to start")


def _request(method: str, path: str, payload: dict[str, Any] | None = None) -> Any:
    body = None if payload is None else json.dumps(payload).encode("utf-8")
    headers = {"Authorization": f"Bearer {_token()}", "Content-Type": "application/json"}
    conn = UnixHTTPConnection(endpoint(STATE_DIR)["socket"])
    conn.request(method, path, body=body, headers=headers)
    response = conn.getresponse()
    data = response.read()
    status = response.status
    conn.close()
    if status < 200 or status >= 300:
        raise urllib.error.HTTPError(path, status, data.decode("utf-8", "replace"), None, None)
    return json.loads(data)


def _call_tool(name: str, args: dict[str, Any]) -> Any:
    tool = next((tool for tool in TOOLS if tool['name']==name),None)
    if tool is None:
        raise ValueError(f'unknown tool: {name}')
    validate(args,tool['inputSchema'])
    _ensure_daemon()
    if name == "spawn":
        value = _request("POST", "/v1/spawn", _spawn_payload(args))
        # Keep pool/daemon state out of the agent-facing surface.
        return {"id": value.get("id"), **({"pending": True} if value.get("status") == "pending" else {})}
    if name == "send":
        return _request("POST", "/v1/send", {**args, "session": CONSUMER})
    if name == "wait":
        return _request("POST", "/v1/wait", {**args, "session": CONSUMER})
    if name in {"status", "result", "stop"}:
        return _request("POST", f"/v1/{name}", {**args, "session": CONSUMER})
    if name == "pending_requests":
        query = urllib.parse.urlencode({"session": CONSUMER, "all": str(args.get("all", False)).lower(), **({"workerId": args["workerId"]} if args.get("workerId") else {})})
        return _request("GET", f"/v1/requests?{query}")
    if name in {"fork","rollback","capabilities"}:
        return _request("POST", f"/v1/{name}", {**args, "session": CONSUMER})
    if name == "events":
        return _request("GET", "/v1/event-log?"+urllib.parse.urlencode({"session":CONSUMER,"after":args.get("after",0)}))
    if name == "answer_request":
        return _request("POST", "/v1/answer-request", {**args, "session": CONSUMER})
    if name == "list":
        query = urllib.parse.urlencode({"session": CONSUMER, "all": str(args.get("all", False)).lower()})
        return _request("GET", f"/v1/list?{query}")
    raise ValueError(f"unknown tool: {name}")


def _reply(message_id: Any, result: Any = None, error: dict[str, Any] | None = None) -> None:
    response: dict[str, Any] = {"jsonrpc": "2.0", "id": message_id}
    if error is not None:
        response["error"] = error
    else:
        response["result"] = result
    _write(response)


def _write(message):
    with OUTPUT_LOCK:
        sys.stdout.write(json.dumps(message,separators=(",",":"))+"\n")
        sys.stdout.flush()


def _notification_loop(stopped, subscriptions=None):
    while not stopped.wait(0.5):
        try:
            _ensure_daemon()
            batch = _request("GET","/v1/events?"+urllib.parse.urlencode({"consumer":CONSUMER}))
            for event in batch.get("events",[]):
                data = {k:event[k] for k in ("workerId","status","kind","requestId","seq") if k in event}
                data["nextTool"] = "pending_requests" if event.get("kind")=="request" else "result"
                _write({"jsonrpc":"2.0","method":"notifications/message","params":{"level":"notice","logger":"codex-subagents","data":data}})
                if subscriptions is not None and RESOURCE in subscriptions:
                    _write({"jsonrpc":"2.0","method":"notifications/resources/updated","params":{"uri":RESOURCE}})
                _request("POST","/v1/ack-events",{"consumer":CONSUMER,"claims":[{"eventId":event["eventId"],"leaseId":event["leaseId"]}]})
        except Exception:
            # Lease expiry retries failed delivery. Nothing writes to stdout
            # outside the JSON-RPC serializer.
            continue


def main() -> None:
    stopped = threading.Event()
    notifications = None
    subscriptions = set()
    for line in sys.stdin:
        message_id = None
        try:
            message = json.loads(line)
            method = message.get("method")
            message_id = message.get("id")
            if method == "initialize":
                MCP_CLIENT_INFO.clear()
                MCP_CLIENT_INFO.update(message.get("params", {}).get("clientInfo", {}) or {})
                _reply(message_id, {"protocolVersion": message.get("params", {}).get("protocolVersion", "2024-11-05"), "capabilities": {"tools": {"listChanged": False}, "logging": {}, "resources": {"subscribe": True, "listChanged": False}}, "serverInfo": {"name": "codex-subagents", "version": "0.2.0"}})
            elif method == "notifications/initialized":
                if notifications is None:
                    notifications = threading.Thread(target=_notification_loop,args=(stopped,subscriptions),daemon=True)
                    notifications.start()
            elif method in {"resources/subscribe","resources/unsubscribe"}:
                uri = message.get("params",{}).get("uri")
                if uri!=RESOURCE:
                    raise ValueError("unknown resource")
                if method=="resources/subscribe": subscriptions.add(uri)
                else: subscriptions.discard(uri)
                _reply(message_id,{})
            elif method == "resources/list":
                _reply(message_id,{"resources":[{"uri":RESOURCE,"name":"Pending worker requests","mimeType":"application/json"}]})
            elif method == "resources/read":
                if message.get("params",{}).get("uri")!=RESOURCE:
                    raise ValueError("unknown resource")
                value = _call_tool("pending_requests",{})
                _reply(message_id,{"contents":[{"uri":RESOURCE,"mimeType":"application/json","text":json.dumps(value)}]})
            elif method == "tools/list":
                _reply(message_id, {"tools": TOOLS})
            elif method == "tools/call":
                params = message.get("params", {})
                try:
                    value = _call_tool(params["name"], params.get("arguments", {}))
                    _reply(message_id, {"content": [{"type": "text", "text": json.dumps(value)}], "isError": False})
                except Exception as exc:
                    _reply(message_id, {"content": [{"type": "text", "text": str(exc)}], "isError": True})
            elif message_id is not None:
                _reply(message_id, {})
        except Exception as exc:
            if "message_id" in locals() and message_id is not None:
                _reply(message_id, error={"code": -32603, "message": str(exc)})

    stopped.set()
    if notifications:
        notifications.join(timeout=1)


if __name__ == "__main__":
    main()
