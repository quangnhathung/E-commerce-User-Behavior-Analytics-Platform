"""Host publication and shared launcher regression at real file boundaries."""
import io
import json
from pathlib import Path
import tempfile
import types
import unittest
from unittest import mock

from helpers import APP
from test_streaming_host import load


class MLHostTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.host = load("ml_host_tests", APP / "ml_manage.py")
        cls.launcher = load("ml_shared_launcher_tests", APP / "streaming_manage.py")

    def args(self, action, **values):
        return types.SimpleNamespace(action=action, as_of=self.host.DEFAULT_AS_OF,
            feature_snapshot_id=None, model_version=None, visitor_id=None, **values)

    def test_feature_request_and_pointer_advance_only_after_resource_acceptance(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "artifacts/data/latest-ingest.json"
            source.parent.mkdir(parents=True)
            source.write_text(json.dumps({"quality_status": "passed", "dataset_version": "a" * 64, "silver_path": "/silver"}))
            artifacts = root / "artifacts/ml"
            artifacts.mkdir()
            pointer = artifacts / "latest-features.json"
            pointer.write_text('{"feature_snapshot_id":"previous"}')
            captured = []
            result = {"passed": True, "feature_snapshot_id": "b" * 64, "reused": False,
                      "resources": {"no_oom_or_restart": True, "memory_observations_complete": False}}
            def run(action, spec, destination):
                captured.append(spec("code-sha"))
                return result
            with mock.patch.object(self.host, "ROOT", root), mock.patch.object(self.host, "ARTIFACTS", artifacts), \
                    mock.patch.object(self.host, "run", side_effect=run):
                with self.assertRaisesRegex(self.host.infra.InfraError, "pointer"):
                    self.host.dispatch(self.args("build-features"))
                self.assertEqual("previous", json.loads(pointer.read_text())["feature_snapshot_id"])
                result["resources"]["memory_observations_complete"] = True
                self.host.dispatch(self.args("build-features"))
            self.assertEqual("b" * 64, json.loads(pointer.read_text())["feature_snapshot_id"])
            self.assertEqual("code-sha", captured[0]["request"]["code_sha256"])
            self.assertEqual(self.host.DEFAULT_AS_OF, captured[0]["request"]["as_of"])

    def test_train_pins_saved_features_and_never_auto_builds(self):
        with tempfile.TemporaryDirectory() as directory, mock.patch.object(self.host, "ARTIFACTS", Path(directory)):
            with mock.patch.object(self.host, "run") as run:
                with self.assertRaises(FileNotFoundError):
                    self.host.dispatch(self.args("train-segments"))
                run.assert_not_called()
            snapshot = "a" * 64
            (Path(directory) / "latest-features.json").write_text(json.dumps({"feature_snapshot_id": snapshot}))
            folder = Path(directory) / snapshot
            folder.mkdir()
            (folder / "report.json").write_text(json.dumps({"passed": True, "feature_snapshot_id": snapshot,
                "feature_path": "/features", "manifest_sha256": "c" * 64}))
            captured = []
            def run(action, spec, destination):
                captured.append(spec("code-sha"))
                raise self.host.infra.InfraError("failed")
            with mock.patch.object(self.host, "run", side_effect=run), self.assertRaises(self.host.infra.InfraError):
                self.host.dispatch(self.args("train-segments"))
            request = captured[0]["request"]
            self.assertNotIn("as_of", request)
            self.assertEqual("c" * 64, request["feature_manifest_sha256"])
            self.assertEqual(snapshot, request["feature_snapshot_id"])
            self.assertFalse((Path(directory) / "latest-model.json").exists())

    def test_status_uses_explicit_id_and_keeps_visitor_string(self):
        with tempfile.TemporaryDirectory() as directory, mock.patch.object(self.host, "ARTIFACTS", Path(directory)), \
                mock.patch.object(self.host, "run", return_value={"status": "published"}) as run:
            args = self.args("segment-status")
            args.model_version, args.visitor_id = "a" * 64, "001"
            self.host.dispatch(args)
            self.assertEqual({"model_version": "a" * 64, "visitor_id": "001"}, run.call_args.args[1])

    def test_ml_launcher_options_and_incomplete_test_resources_fail(self):
        with mock.patch.object(self.host, "launch", return_value={"passed": True, "resources": {
                "no_oom_or_restart": True, "memory_observations_complete": False}}) as launch:
            with self.assertRaisesRegex(self.host.infra.InfraError, "resource"):
                self.host.run("test-ml", {}, Path("unused"))
            self.assertEqual({"profile": "ml", "entry": "ml.py", "result_prefix": "ML_RESULT=",
                              "spark_job": True, "monitor": True}, launch.call_args.kwargs)
            self.host.run("segment-status", {}, Path("unused"))
            self.assertFalse(launch.call_args.kwargs["spark_job"])
            self.assertFalse(launch.call_args.kwargs["monitor"])

    def launch_fixture(self, action, root, *, failure=False, **options):
        process = mock.Mock()
        process.stdout = io.StringIO("" if failure else options.get("result_prefix", "STREAMING_RESULT=") + '{"passed":true}\n')
        process.wait.return_value = 1 if failure else 0
        process.poll.return_value = process.wait.return_value
        def execute(args, **kwargs):
            if "/opt/app/package_code.py" in args:
                return '{"sha256":"code-sha"}'
            if "cancel" in args and failure:
                raise self.launcher.infra.InfraError("cancel unconfirmed")
            return ""
        with mock.patch.object(self.launcher, "ROOT", root), mock.patch.object(self.launcher.infra, "ARTIFACTS", root / "infra"), \
             mock.patch.object(self.launcher.infra, "execute", side_effect=execute) as commands, \
             mock.patch.object(self.launcher, "upload_spec"), mock.patch.object(self.launcher.subprocess, "Popen", return_value=process) as popen:
            if failure:
                with self.assertRaisesRegex(self.launcher.infra.InfraError, "unconfirmed"):
                    self.launcher.launch(action, {}, root / "attempts", quiet=True, **options)
            else:
                self.assertTrue(self.launcher.launch(action, {}, root / "attempts", quiet=True, **options)["passed"])
        return popen.call_args.args[0], [call.args[0] for call in commands.call_args_list]

    def test_shared_launcher_defaults_and_ml_spark_status_branches(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            command, calls = self.launch_fixture("prepare-demo", root)
            self.assertIn("spark-submit", command)
            self.assertIn("/opt/app/jobs/streaming.py", command)
            self.assertIn("streaming", calls[0])
            command, calls = self.launch_fixture("train-segments", root, profile="ml", entry="ml.py",
                result_prefix="ML_RESULT=", spark_job=True, monitor=False)
            self.assertIn("spark-submit", command)
            self.assertIn("/opt/app/jobs/ml.py", command)
            self.assertIn("ml", calls[0])
            command, _ = self.launch_fixture("segment-status", root, profile="ml", entry="ml.py",
                result_prefix="ML_RESULT=", spark_job=False, monitor=False)
            self.assertNotIn("spark-submit", command)

    def test_ml_unconfirmed_cancel_uses_global_driver_guard_and_preserves_remote_files(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            _, calls = self.launch_fixture("train-segments", root, failure=True, profile="ml", entry="ml.py",
                result_prefix="ML_RESULT=", spark_job=True, monitor=False)
            marker = json.loads((root / "infra/driver-unconfirmed.json").read_text())
            self.assertEqual("train-segments", marker["action"])
            self.assertTrue(marker["archive"].endswith(".zip"))
            self.assertFalse(any("rm" in command for command in calls))
