"""Wire boundaries, simulator chronology and frozen archive compatibility."""
import copy
import json
from pathlib import Path
import tempfile
import unittest

from helpers import APP
from package_code import package_code
from ecommerce_ml.contracts import reference
from ecommerce_streaming_ml.contracts import (DEFAULT_START, DEFAULT_END, PIPELINE_VERSION,
    cutoff, frozen_hashes, normalize_message, prediction_fingerprint, simulator_events, validate_request)


def request(**overrides):
    return {"run_id": "ml-test", "start": DEFAULT_START, "end": DEFAULT_END, "rate": 10,
        "demo_source": "simulator", "pipeline_version": PIPELINE_VERSION, "behavior_version": "behavior-v1",
        "code_sha256": "a" * 64, "dataset_version": "b" * 64, "base": "/ecommerce",
        "topic": "raw", "dlq_topic": "dlq", "silver_path": "/silver",
        "segment_model_version": "c" * 64, "purchase_model_version": "d" * 64, "policy_version": "e" * 64,
        **overrides}


def bindings():
    return {"segment_model_version": "c" * 64, "purchase_model_version": "d" * 64,
            "policy_version": "e" * 64, "selected_k": 2, "threshold": .96, "original_threshold": .93}


def predictions(count=121):
    source = []
    for index in range(count):
        source.append({"visitor_id": "001" if index == count - 1 else f"{index:04d}", "item_id": "10",
            "event_type": "view", "transaction_id": None, "event_time": DEFAULT_START})
    as_of = cutoff(DEFAULT_START)
    return [{**row, "cluster_id": 0, "purchase_score": .96, "original_predicted_label": 1, "predicted_label": 1,
        "original_threshold": .93, "threshold": .96, "feature_version": "features-v1", "scored_at": DEFAULT_START,
        **{key: value for key, value in bindings().items() if key.endswith("version")}} for row in reference(source, as_of)]


class StreamingMLContracts(unittest.TestCase):
    def test_old_archives_are_unchanged_and_reconstructed_from_new_archive(self):
        expected = {"ingest": "ae42a5981bc20e1295df1dabbfab7e553af0f133a39c97fb52fa795ceafcb554",
            "streaming": "009c7d7d1d411e3f1810096f9c2954235ee6b92c7a3789514fe026b9af3516ac",
            "ml": "b3991c1379b84c0a440ce9ad023c411221e357dca6750cbd86bf917c6af831a9",
            "prediction": "a8b1243293bd1ed091ccb1cbcf1673ca8437d4ef48f0e50b1749d72ec3c7e4e0",
            "purchase-policy": "fccbd899349a98bf9439e1f287d86f459a4006ddab0453784247e7ad229f81b4"}
        with tempfile.TemporaryDirectory() as folder:
            archive = package_code(Path(folder) / "live.zip", profile="streaming-ml")
            self.assertEqual(expected, frozen_hashes(archive["archive"]))
            for profile, checksum in expected.items():
                self.assertEqual(checksum, package_code(Path(folder) / (profile + ".zip"), profile=profile)["sha256"])

    def test_request_rejects_invalid_source_rate_identity_and_range(self):
        validate_request(request())
        for change in [{"demo_source": "other"}, {"rate": 11}, {"rate": True}, {"rate": float("nan")},
                {"policy_version": "latest"}, {"start": DEFAULT_END}, {"end": "2015-09-18T00:01:00.000Z"},
                {"pipeline_version": "stream-v2"}, {"base": "/ecommerce/../other"}]:
            with self.subTest(change=change), self.assertRaises(ValueError): validate_request(request(**change))

    def test_simulator_is_repeatable_isolated_and_preserves_zero_prefix(self):
        events = simulator_events(request())
        self.assertEqual(events, simulator_events(request()))
        self.assertEqual(9, len(events)); self.assertEqual(9, len({value["event_id"] for value in events}))
        self.assertEqual(3, len({value["visitor_id"] for value in events}))
        self.assertTrue(any(value["visitor_id"].startswith("0") for value in events))
        self.assertNotEqual(events[0]["event_id"], simulator_events(request(run_id="another"))[0]["event_id"])
        self.assertTrue(all(value["source"] == "simulator" for value in events))

    def message(self, payload):
        return {"key": b"key", "value": json.dumps(payload).encode(), "topic": "raw", "partition": 0,
                "offset": 0, "timestamp": None}

    def test_simulator_uses_all_wire_checks_and_rejects_retailrocket_source(self):
        config = request(); payload = {**simulator_events(config)[0], "run_id": config["run_id"], "emitted_at": DEFAULT_START}
        value = normalize_message(self.message(payload), config, "worker")
        self.assertFalse(value["error_codes"]); self.assertIsNotNone(value["business_sha256"])
        for change, reason in [({"source": "retailrocket"}, "INVALID_SOURCE"), ({"source": "other"}, "INVALID_SOURCE"),
                ({"event_id": "bad"}, "INVALID_EVENT_ID"), ({"visitor_id": 1}, "INVALID_VISITOR_ID"),
                ({"event_time": DEFAULT_END}, "OUTSIDE_REPLAY_RANGE")]:
            value = normalize_message(self.message({**payload, **change}), config, "worker")
            self.assertIn(reason, value["error_codes"])
        foreign = normalize_message(self.message({**payload, "run_id": "foreign"}), config, "worker")
        self.assertTrue(foreign["foreign_run"])

    def test_oracle_window_boundaries_zero_views_and_no_future(self):
        events = simulator_events(request())
        first = reference(events, cutoff(events[2]["event_time"]))
        zero = next(row for row in first if row["visitor_id"].startswith("0"))
        self.assertEqual(0, zero["view_count_7d"]); self.assertEqual(0, zero["cart_view_ratio_7d"])
        self.assertEqual(1, zero["cart_count_7d"])
        # No future phase contributes transactions until Kafka has accepted that phase.
        self.assertEqual(0, sum(row["transaction_count_7d"] for row in first))
        final = reference(events, cutoff(events[-1]["event_time"]))
        self.assertEqual(2, sum(row["transaction_count_7d"] for row in final))

    def test_prediction_fingerprint_checks_models_labels_dates_and_unique_strings(self):
        values = predictions(); as_of = cutoff(DEFAULT_START)
        expected = prediction_fingerprint(values, bindings(), as_of, DEFAULT_START)
        self.assertEqual(121, expected["rows"]); self.assertEqual({"0": 121, "1": 0}, expected["clusters"])
        for field, invalid in [("predicted_label", 0), ("original_threshold", .5), ("purchase_score", float("nan")),
                ("cluster_id", 2), ("policy_version", "f" * 64), ("as_of", DEFAULT_START),
                ("scored_at", DEFAULT_END), ("view_count_7d", -1)]:
            changed = copy.deepcopy(values); changed[0][field] = invalid
            with self.subTest(field=field), self.assertRaises(ValueError):
                prediction_fingerprint(changed, bindings(), as_of, DEFAULT_START)
        with self.assertRaises(ValueError): prediction_fingerprint([values[0], values[0]], bindings(), as_of, DEFAULT_START)
        self.assertEqual(0, prediction_fingerprint([], bindings(), None, DEFAULT_START)["rows"])
