"""Exercise real curl, hashing and resume against a local HTTP server; no Docker daemon."""
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import os
from pathlib import Path
import signal
import socket
import subprocess
import tempfile
import threading
import time
import unittest

SCRIPT = Path(__file__).resolve().parents[1] / "build/download.sh"
PAYLOAD = bytes(range(256)) * 4096
CHECKSUM = hashlib.sha512(PAYLOAD).hexdigest()


class DownloadTests(unittest.TestCase):
    def setUp(self):
        self.requests = []
        self.paused = threading.Event()
        self.release = threading.Event()
        test = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def do_GET(self):
                offset = int(self.headers.get("Range", "bytes=0-").split("=")[1].split("-")[0])
                test.requests.append((self.path, offset))
                if self.path == "/missing":
                    self.send_error(404)
                    return
                if self.path == "/no-range":
                    offset = 0
                data = b"corrupt artifact" if self.path == "/bad" else PAYLOAD
                if offset >= len(data):
                    self.send_error(416)
                    return
                self.send_response(206 if offset else 200)
                self.send_header("Content-Length", str(len(data) - offset))
                if offset:
                    self.send_header("Content-Range", f"bytes {offset}-{len(data)-1}/{len(data)}")
                self.end_headers()
                try:
                    if self.path in ("/disconnect", "/pause") and not offset:
                        self.wfile.write(data[:65536])
                        self.wfile.flush()
                        if self.path == "/pause":
                            test.paused.set()
                            test.release.wait(5)
                        self.connection.shutdown(socket.SHUT_WR)
                        self.close_connection = True
                    else:
                        self.wfile.write(data[offset:])
                except (BrokenPipeError, ConnectionResetError):
                    pass

        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.cache = Path(self.directory.name)
        try:
            self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        except PermissionError as error:
            self.skipTest(f"Execution environment forbids local HTTP sockets: {error}")
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.addCleanup(self.stop_server)
        self.base = f"http://127.0.0.1:{self.server.server_port}"

    def stop_server(self):
        self.release.set()
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)

    @property
    def destination(self):
        return self.cache / CHECKSUM / "binary.tgz"

    def partial(self, path):
        key = hashlib.sha256((self.base + path).encode()).hexdigest()
        return self.cache / CHECKSUM / (key + ".part")

    def command(self, *paths):
        return ["bash", str(SCRIPT), str(self.cache), "binary.tgz", CHECKSUM,
                *(self.base + path for path in paths)]

    def fetch(self, *paths, success=True):
        result = subprocess.run(self.command(*paths), capture_output=True, text=True, timeout=15)
        self.assertEqual(0 if success else 1, result.returncode, result.stdout + result.stderr)
        if success:
            self.assertEqual(PAYLOAD, self.destination.read_bytes())
        else:
            self.assertFalse(self.destination.exists())
        return result

    def test_valid_cache_avoids_network_and_corrupt_cache_is_replaced(self):
        self.fetch("/ok")
        result = self.fetch("/missing")
        self.assertIn("Cache hit", result.stdout)
        self.assertEqual([("/ok", 0)], self.requests)
        self.destination.write_bytes(b"corrupt cache")
        self.fetch("/ok")
        self.assertEqual(2, len(self.requests))

    def test_disconnected_transfer_resumes_at_saved_offset(self):
        self.fetch("/disconnect")
        self.assertEqual([("/disconnect", 0), ("/disconnect", 65536)], self.requests)

    def test_sigterm_retains_partial_and_next_invocation_resumes(self):
        with subprocess.Popen(self.command("/pause"), stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                              text=True, start_new_session=True) as process:
            try:
                self.assertTrue(self.paused.wait(3))
                deadline = time.monotonic() + 3
                partial = self.partial("/pause")
                while time.monotonic() < deadline and (not partial.exists() or partial.stat().st_size == 0):
                    time.sleep(0.01)
                self.assertTrue(partial.exists())
                self.assertGreater(partial.stat().st_size, 0)
                os.killpg(process.pid, signal.SIGTERM)
                process.communicate(timeout=5)
                self.assertEqual(130, process.returncode)
            finally:
                self.release.set()
                if process.poll() is None:
                    os.killpg(process.pid, signal.SIGKILL)
                    process.communicate(timeout=5)
        self.fetch("/pause")
        self.assertGreater(self.requests[-1][1], 0)

    def test_server_ignoring_range_restarts_from_zero(self):
        partial = self.partial("/no-range")
        partial.parent.mkdir(parents=True)
        partial.write_bytes(PAYLOAD[:65536])
        self.fetch("/no-range")
        self.assertEqual([("/no-range", 65536), ("/no-range", 0)], self.requests)

    def test_checksum_mismatch_and_http_error_use_fallback(self):
        result = self.fetch("/bad", "/missing", "/ok")
        self.assertIn("SHA-512 mismatch", result.stderr)
        self.assertFalse(self.partial("/bad").exists())
        self.assertEqual([("/bad", 0), ("/missing", 0), ("/ok", 0)], self.requests)

    def test_all_sources_fail_without_publishing_binary(self):
        self.fetch("/bad", "/missing", success=False)

    def test_oversized_partial_is_restarted_after_http_416(self):
        partial = self.partial("/ok")
        partial.parent.mkdir(parents=True)
        partial.write_bytes(PAYLOAD + b"extra")
        self.fetch("/ok")
        self.assertEqual([("/ok", len(PAYLOAD) + 5), ("/ok", 0)], self.requests)


class CachedDownloadTests(unittest.TestCase):
    """Hashing and atomic publication can also be checked without network sockets."""
    def test_valid_binary_cache_never_contacts_unavailable_source(self):
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / CHECKSUM / "binary.tgz"
            target.parent.mkdir()
            target.write_bytes(PAYLOAD)
            result = subprocess.run(["bash", str(SCRIPT), directory, "binary.tgz", CHECKSUM,
                                     "invalid-protocol://unreachable"], capture_output=True, text=True, timeout=5)
            self.assertEqual(0, result.returncode, result.stderr)
            self.assertIn("Cache hit", result.stdout)
            self.assertEqual(PAYLOAD, target.read_bytes())

    def test_complete_partial_is_published_without_network(self):
        with tempfile.TemporaryDirectory() as directory:
            url = "invalid-protocol://unreachable"
            target = Path(directory) / CHECKSUM / "binary.tgz"
            target.parent.mkdir()
            partial = target.parent / (hashlib.sha256(url.encode()).hexdigest() + ".part")
            partial.write_bytes(PAYLOAD)
            result = subprocess.run(["bash", str(SCRIPT), directory, "binary.tgz", CHECKSUM, url],
                                    capture_output=True, text=True, timeout=5)
            self.assertEqual(0, result.returncode, result.stderr)
            self.assertEqual(PAYLOAD, target.read_bytes())
            self.assertFalse(partial.exists())

    def test_corrupt_cache_is_rejected_when_sources_fail(self):
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / CHECKSUM / "binary.tgz"
            target.parent.mkdir()
            target.write_bytes(b"corrupt cache")
            result = subprocess.run(["bash", str(SCRIPT), directory, "binary.tgz", CHECKSUM,
                                     "invalid-protocol://unreachable"], capture_output=True, text=True, timeout=5)
            self.assertEqual(1, result.returncode, result.stderr)
            self.assertFalse(target.exists())
            self.assertIn("Discarding corrupt cached binary", result.stderr)


if __name__ == "__main__":
    unittest.main()
