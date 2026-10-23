"""Host resource gate, pinned selectors and shared producer cancellation action."""
import json
from pathlib import Path
import tempfile
import types
import unittest
from unittest import mock

from helpers import APP
from test_streaming_host import load
from test_streaming_ml import request
from ecommerce_analytics.contracts import sha256_file


class StreamingMLHostTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.host = load("streaming_ml_host_tests", APP / "streaming_ml_manage.py")

    def test_resource_failure_is_not_accepted_and_status_uses_no_spark(self):
        with mock.patch.object(self.host, "launch", return_value={"passed": True,
                "resources": {"no_oom_or_restart": True, "memory_observations_complete": False}}) as launch:
            with self.assertRaises(self.host.infra.InfraError): self.host.run("stream", {}, Path("/unused"))
            self.assertEqual("streaming-ml", launch.call_args.kwargs["profile"])
            self.host.run("status", {}, Path("/unused"))
            self.assertFalse(launch.call_args.kwargs["spark_job"]); self.assertFalse(launch.call_args.kwargs["monitor"])

    def test_prepare_selectors_are_read_once_and_failed_run_keeps_latest(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder); artifacts = root / "artifacts"
            for relative, content in {"data/latest-ingest.json": {"dataset_version": "a"*64, "silver_path": "/silver", "quality_status": "passed"},
                    "ml/latest-model.json": {"model_version": "b"*64}, "prediction/latest-purchase-model.json": {"model_version": "c"*64},
                    "prediction/policies/latest-policy.json": {"policy_version": "d"*64},
                    "streaming-ml/latest-run.json": {"run_id": "previous"}}.items():
                p = artifacts / relative; p.parent.mkdir(parents=True, exist_ok=True); p.write_text(json.dumps(content))
            args = types.SimpleNamespace(run_id="new", start=request()["start"], end=request()["end"], rate=10,
                demo_source="replay", segment_model_version=None, purchase_model_version=None, policy_version=None)
            captured=[]
            def failure(action, spec, directory):
                value = spec("f" * 64)["request"]
                captured.append(value)
                self.assertEqual("b"*64, value["segment_model_version"])
                self.assertEqual("c"*64, value["purchase_model_version"])
                self.assertEqual("d"*64, value["policy_version"])
                raise self.host.infra.InfraError("binding rejected")
            with mock.patch.object(self.host, "ROOT", root), mock.patch.object(self.host, "ARTIFACTS", artifacts / "streaming-ml"), \
                    mock.patch.object(self.host, "run", side_effect=failure):
                with self.assertRaisesRegex(self.host.infra.InfraError, "binding rejected"): self.host.prepare_demo(args)
            self.assertEqual({"run_id":"previous"}, json.loads((artifacts / "streaming-ml/latest-run.json").read_text()))
            # A prepared run must not consult moved/deleted latest pointers on resume.
            directory=artifacts/'streaming-ml/new'; directory.mkdir(parents=True,exist_ok=True)
            (directory/'config.json').write_text(json.dumps({'request':captured[0]}))
            for p in artifacts.rglob('latest-*.json'):
                if p.parent!=artifacts/'streaming-ml':p.unlink()
            args.start=args.end=args.rate=args.demo_source=None
            with mock.patch.object(self.host,"ROOT",root), mock.patch.object(self.host,"ARTIFACTS",artifacts/'streaming-ml'), \
                    mock.patch.object(self.host,"run",side_effect=failure):
                with self.assertRaisesRegex(self.host.infra.InfraError,"binding rejected"):self.host.prepare_demo(args)
            self.assertEqual(captured[0],captured[1])

    def test_simulator_uses_pinned_config_ack_cursor_and_canonical_replay_action(self):
        with tempfile.TemporaryDirectory() as folder:
            directory = Path(folder) / "ml-test"; directory.mkdir()
            source = directory / "events.jsonl"; source.write_text('one\ntwo\nthree\n')
            config = {**request(), "replay_sha256": sha256_file(source)}
            (directory / "config.json").write_text(json.dumps(config))
            args = types.SimpleNamespace(action="simulate-ml", run_id="ml-test", visitor_id=None)
            def run(action, spec, directory, **kw):
                self.assertEqual(config, spec["config"])
                if action == "status": return {"producer":{"last_seq":0}}
                self.assertEqual("replay", action)
                self.assertEqual(b'two\nthree\n',kw["stdin"].read())
                return {"passed":True}
            with mock.patch.object(self.host, "ARTIFACTS", directory.parent), mock.patch.object(self.host, "run", side_effect=run):
                self.host.dispatch(args)
                args.action="replay-ml"
                with self.assertRaisesRegex(ValueError,"matching producer"): self.host.dispatch(args)

    def test_status_has_its_own_namespace_and_keeps_visitor_string(self):
        with tempfile.TemporaryDirectory() as folder:
            directory=Path(folder)/"ml-test"; directory.mkdir()
            (directory/'config.json').write_text(json.dumps(request()))
            args=types.SimpleNamespace(action="stream-ml-status",run_id="ml-test",visitor_id="001")
            with mock.patch.object(self.host,"ARTIFACTS",directory.parent), mock.patch.object(self.host,"run",return_value={}) as run:
                self.host.dispatch(args)
                self.assertEqual("001",run.call_args.args[1]['visitor_id'])
                self.assertTrue((directory/'status.json').exists())
