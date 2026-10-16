import importlib.util
from pathlib import Path
import select
import signal
import subprocess
import sys
import tempfile
import unittest
from unittest import mock
import uuid

from helpers import APP


class JobControlTests(unittest.TestCase):
    def test_remote_wrapper_preserves_exit_code_and_cleans_record(self):
        token = uuid.uuid4().hex
        result = subprocess.run([sys.executable, str(APP / "run_job.py"), "run", token,
                                 sys.executable, "-c", "raise SystemExit(7)"], capture_output=True, timeout=5)
        self.assertEqual(7, result.returncode, result.stderr)
        self.assertFalse(Path(f"/tmp/ecommerce-job-{token}.json").exists())

    def test_cancellation_stops_real_child_process_group(self):
        token = uuid.uuid4().hex
        wrapper = APP / "run_job.py"
        process = subprocess.Popen([sys.executable, str(wrapper), "run", token, sys.executable, "-u", "-c",
                                    "import os,time; print(os.getpid(),flush=True); time.sleep(30)"],
                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        try:
            ready, _, _ = select.select([process.stdout], [], [], 5)
            self.assertTrue(ready, "Child process did not start")
            child_pid = int(process.stdout.readline())
            result = subprocess.run([sys.executable, str(wrapper), "cancel", token], capture_output=True, timeout=25)
            self.assertEqual(0, result.returncode, result.stderr)
            process.communicate(timeout=5)
            self.assertEqual(130, process.returncode)
            self.assertFalse(Path(f"/proc/{child_pid}").exists())
            self.assertFalse(Path(f"/tmp/ecommerce-job-{token}.json").exists())
        finally:
            if process.poll() is None:
                process.send_signal(signal.SIGTERM)
                process.communicate(timeout=20)
            process.stdout.close()
            process.stderr.close()

    def test_report_replace_failure_preserves_previous_json(self):
        spec = importlib.util.spec_from_file_location("application_manage", APP / "manage.py")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        with tempfile.TemporaryDirectory() as directory:
            report = Path(directory) / "quality.json"
            report.write_text('{"previous":true}\n')
            with mock.patch.object(module.os, "replace", side_effect=OSError("disk full")):
                with self.assertRaises(OSError):
                    module.atomic_json(report, {"next": True})
            self.assertEqual('{"previous":true}\n', report.read_text())
            self.assertEqual([report], list(Path(directory).iterdir()))


if __name__ == "__main__":
    unittest.main()
