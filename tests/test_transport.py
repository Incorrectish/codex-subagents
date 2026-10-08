import json
import os
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path

from backend.transport import UnixHTTPConnection, endpoint


ROOT = Path(__file__).resolve().parents[1]


class TransportTests(unittest.TestCase):
    def start_daemon(self, state: Path):
        env = {**os.environ, "CODEX_SUBAGENTS_STATE_DIR": str(state)}
        process = subprocess.Popen(
            [sys.executable, str(ROOT / "backend" / "codex_subagents.py")],
            env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        for _ in range(100):
            if (state / "endpoint.json").exists():
                return process
            time.sleep(0.03)
        process.terminate()
        process.wait(timeout=5)
        self.fail("daemon did not publish endpoint")

    @staticmethod
    def request(state: Path):
        info = endpoint(state)
        conn = UnixHTTPConnection(info["socket"])
        conn.request("GET", "/v1/list?session=transport-test", headers={"Authorization": f"Bearer {(state / 'token').read_text().strip()}"})
        response = conn.getresponse()
        value = json.loads(response.read())
        conn.close()
        return response.status, value

    @staticmethod
    def request_with_token(state: Path, token: str):
        info = endpoint(state)
        conn = UnixHTTPConnection(info["socket"])
        conn.request("GET", "/v1/list?session=transport-test", headers={"Authorization": f"Bearer {token}"})
        response = conn.getresponse()
        response.read()
        conn.close()
        return response.status

    def test_endpoint_discovery_and_owner_only_files(self):
        with tempfile.TemporaryDirectory() as directory:
            state = Path(directory)
            process = self.start_daemon(state)
            try:
                info = endpoint(state)
                self.assertTrue(info["socket"].endswith("api.sock"))
                self.assertGreater(info["httpPort"], 0)
                self.assertEqual((state / "endpoint.json").stat().st_mode & 0o777, 0o600)
                self.assertEqual((state / "api.sock").stat().st_mode & 0o777, 0o600)
                self.assertEqual(self.request(state)[0], 200)
                self.assertEqual(self.request_with_token(state, "wrong-token"), 401)
            finally:
                process.terminate()
                process.wait(timeout=5)

    def test_stale_socket_is_replaced(self):
        with tempfile.TemporaryDirectory() as directory:
            state = Path(directory)
            stale = state / "api.sock"
            probe = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            probe.bind(str(stale))
            probe.close()
            stale.chmod(0o600)
            process = self.start_daemon(state)
            try:
                self.assertEqual(self.request(state)[0], 200)
            finally:
                process.terminate()
                process.wait(timeout=5)

    def test_concurrent_clients_share_socket(self):
        with tempfile.TemporaryDirectory() as directory:
            state = Path(directory)
            process = self.start_daemon(state)
            try:
                results = []
                threads = [threading.Thread(target=lambda: results.append(self.request(state)[0])) for _ in range(12)]
                for thread in threads:
                    thread.start()
                for thread in threads:
                    thread.join()
                self.assertEqual(results, [200] * 12)
            finally:
                process.terminate()
                process.wait(timeout=5)


if __name__ == "__main__":
    unittest.main()
