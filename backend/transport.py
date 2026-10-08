"""Local daemon transport and endpoint discovery.

The Unix socket is the authoritative API transport.  A dynamically assigned
loopback listener is also published for Claude's hook HTTP primitive, which
cannot address Unix sockets.
"""
from __future__ import annotations

import http.client
import json
import os
import socket
import socketserver
import stat
import tempfile
from pathlib import Path
from typing import Any

SOCKET_NAME = "api.sock"
ENDPOINT_NAME = "endpoint.json"


def paths(state_dir: Path) -> tuple[Path, Path]:
    return state_dir / SOCKET_NAME, state_dir / ENDPOINT_NAME


def endpoint(state_dir: Path) -> dict[str, Any]:
    socket_path, endpoint_path = paths(Path(state_dir))
    try:
        info = json.loads(endpoint_path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError) as exc:
        raise OSError("codex-subagents endpoint is unavailable") from exc
    if not isinstance(info, dict) or info.get("socket") != str(socket_path):
        raise OSError("codex-subagents endpoint is invalid")
    st = endpoint_path.stat()
    if st.st_uid != os.getuid() or stat.S_IMODE(st.st_mode) & 0o077:
        raise PermissionError("codex-subagents endpoint is not owner-only")
    return info


def _remove_stale_socket(path: Path) -> bool:
    """Remove a dead, owner-owned Unix socket; return whether it is live."""
    try:
        st = path.lstat()
    except FileNotFoundError:
        return False
    if not stat.S_ISSOCK(st.st_mode):
        raise OSError(f"refusing to replace non-socket endpoint {path}")
    if st.st_uid != os.getuid() or stat.S_IMODE(st.st_mode) & 0o077:
        raise PermissionError("refusing to remove a non-owner or non-private socket")
    probe = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        probe.settimeout(0.25)
        probe.connect(str(path))
        return True
    except OSError:
        path.unlink()
        return False
    finally:
        probe.close()


class UnixHTTPConnection(http.client.HTTPConnection):
    """Small HTTPConnection adapter for a Unix domain socket."""

    def __init__(self, socket_path: str | os.PathLike[str], timeout: float = 35):
        super().__init__("localhost", timeout=timeout)
        self.socket_path = str(socket_path)

    def connect(self):  # noqa: D401 - inherited API
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.settimeout(self.timeout)
        try:
            self.sock.connect(self.socket_path)
        except Exception:
            self.sock.close()
            self.sock = None
            raise


class ThreadingUnixHTTPServer(socketserver.ThreadingMixIn, socketserver.UnixStreamServer):
    daemon_threads = True
    request_queue_size = 128
    allow_reuse_address = True


def write_endpoint(state_dir: Path, socket_path: Path, http_port: int) -> None:
    state_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    os.chmod(state_dir, 0o700)
    _, endpoint_path = paths(state_dir)
    payload = {"version": "0.2.0", "socket": str(socket_path), "httpPort": http_port}
    fd, temporary = tempfile.mkstemp(prefix="endpoint.", dir=state_dir)
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            json.dump(payload, stream, separators=(",", ":"))
            stream.write("\n")
        os.replace(temporary, endpoint_path)
        os.chmod(endpoint_path, 0o600)
    finally:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass


def http_request(state_dir: Path, method: str, path: str, body: bytes | None, headers: dict[str, str]):
    info = endpoint(state_dir)
    conn = UnixHTTPConnection(info["socket"])
    conn.request(method, path, body=body, headers=headers)
    response = conn.getresponse()
    data = response.read()
    status = response.status
    conn.close()
    return status, data
