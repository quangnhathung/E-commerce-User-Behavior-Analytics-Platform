"""Real host file/lock boundaries and container observation interpretation."""
import hashlib
import importlib.util
from pathlib import Path
import subprocess
import sys
import tempfile
import types
import unittest
from unittest import mock
import uuid

from helpers import APP


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class HostTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.manage = load("streaming_host_tests", APP / "streaming_manage.py")
        cls.monitor = load("streaming_monitor_tests", APP / "streaming_monitor.py")

    def test_failed_replay_fetch_keeps_previous_artifact(self):
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / "events.jsonl"
            target.write_bytes(b"previous verified source\n")

            def failed(*args, **kwargs):
                kwargs["stdout"].write(b"partial")
                return types.SimpleNamespace(returncode=1)

            with mock.patch.object(self.manage.subprocess, "run", side_effect=failed):
                with self.assertRaises(self.manage.infra.InfraError):
                    self.manage.fetch_replay("/tmp/owned", target, "wrong")
            self.assertEqual(b"previous verified source\n", target.read_bytes())
            self.assertEqual([target], list(Path(directory).iterdir()))

    def test_large_control_spec_uses_stdin_instead_of_argv(self):
        spec = {"reference": "x" * 250000}
        with mock.patch.object(self.manage.subprocess, "run", return_value=types.SimpleNamespace(returncode=0)) as execute:
            self.manage.upload_spec("/tmp/owned-spec.json", spec)
        args, kwargs = execute.call_args
        self.assertLess(max(len(argument) for argument in args[0]), 1024)
        self.assertGreater(len(kwargs["input"]), 250000)
        self.assertIn("O_EXCL", args[0][-2])

    def test_producer_lock_excludes_another_process_without_driver_lock(self):
        with tempfile.TemporaryDirectory() as directory, mock.patch.object(self.manage, "ARTIFACTS", Path(directory)):
            with self.manage.producer_lock():
                command = ("import fcntl,sys; f=open(sys.argv[1],'w'); "
                           "fcntl.flock(f,fcntl.LOCK_EX|fcntl.LOCK_NB)")
                result = subprocess.run([sys.executable, "-c", command, str(Path(directory) / "producer.lock")],
                                        capture_output=True, timeout=5)
                self.assertNotEqual(0, result.returncode)
            result = subprocess.run([sys.executable, "-c", command, str(Path(directory) / "producer.lock")],
                                    capture_output=True, timeout=5)
            self.assertEqual(0, result.returncode, result.stderr)

    def test_cancel_before_wrapper_start_prevents_child_launch(self):
        token = uuid.uuid4().hex
        marker = Path(f"/tmp/ecommerce-cancel-{token}")
        try:
            cancelled = subprocess.run([sys.executable, str(APP / "run_job.py"), "cancel", token],
                                       capture_output=True, timeout=5)
            self.assertEqual(0, cancelled.returncode, cancelled.stderr)
            result = subprocess.run([sys.executable, str(APP / "run_job.py"), "run", token,
                                     sys.executable, "-c", "print('unexpected child')"], capture_output=True, timeout=5)
            self.assertEqual(130, result.returncode, result.stderr)
            self.assertEqual(b"", result.stdout)
            self.assertFalse(marker.exists())
        finally:
            marker.unlink(missing_ok=True)

    def test_unconfirmed_driver_guard_survives_local_lock_release(self):
        with tempfile.TemporaryDirectory() as directory, mock.patch.object(self.manage.infra, "ARTIFACTS", Path(directory)):
            marker = Path(directory) / "driver-unconfirmed.json"
            marker.write_text('{"token":"recorded-run"}')
            with self.assertRaisesRegex(self.manage.infra.InfraError, "not confirmed stopped"):
                with self.manage.infra.driver_lock():
                    self.fail("Unconfirmed remote driver must block another managed Spark job")
            marker.unlink()
            with self.manage.infra.driver_lock():
                pass

    def test_resource_monitor_detects_restart_even_if_restart_count_resets(self):
        before = {"id": {"memory_limit": 1024, "oom_killed": False, "restart_count": 0, "started_at": "first"}}
        after = {"id": {**before["id"], "started_at": "second"}}
        with mock.patch.object(self.monitor, "states", side_effect=[before, after]):
            observer = self.monitor.Monitor()
            observer.samples = [{"containers": []}]
            observer.thread = mock.Mock()
            result = observer.finish()
        self.assertFalse(result["no_oom_or_restart"])

    def test_resource_monitor_rejects_old_container_limits(self):
        with mock.patch.object(self.monitor, "states", return_value={"old": {"memory_limit": 8 * 1024 ** 3}}):
            with self.assertRaisesRegex(self.manage.infra.InfraError, "7 GiB"):
                self.monitor.Monitor()


if __name__ == "__main__":
    unittest.main()
