"""Offline streaming contract, delivery and compatibility regression tests."""
from datetime import datetime, timezone
import hashlib
import importlib.util
import io
import json
from pathlib import Path
import tempfile
import types
import unittest
from unittest import mock
import zipfile

from helpers import APP
from ecommerce_analytics.contracts import canonical_json
from ecommerce_streaming.contracts import (BUSINESS_FIELDS, DEFAULT_END, DEFAULT_START, PIPELINE_VERSION,
    business_hash, dlq_record, drained, instant, normalize_message, reference_metrics, source_offsets, validate_request)
from ecommerce_streaming.kafka_io import replay_records, send_confirmed


def event(index=0, minute=0, **updates):
    return {"schema_version": 1, "dataset_version": "a" * 64, "run_id": "demo-001",
            "event_id": hashlib.sha256(str(index).encode()).hexdigest(), "source": "retailrocket",
            "visitor_id": "001", "item_id": "0", "event_type": "view", "transaction_id": None,
            "event_time": f"2015-09-10T10:{minute:02d}:00.000Z", "emitted_at": "2026-10-16T10:00:00.000Z", **updates}


def config():
    return {"run_id": "demo-001", "dataset_version": "a" * 64, "code_sha256": "b" * 64,
            "pipeline_version": PIPELINE_VERSION, "start": DEFAULT_START, "end": DEFAULT_END,
            "rate": 10, "event_count": 2, "topic": "ecommerce.events.raw"}


def normalize(payload, current=None):
    value = payload if isinstance(payload, bytes) else json.dumps(payload).encode()
    return normalize_message({"key": b"001", "value": value, "topic": "raw", "partition": 0,
                              "offset": 7, "timestamp": datetime.now(timezone.utc)}, current or config(), "worker")


class StreamingContractTests(unittest.TestCase):
    def test_wire_normalization_preserves_ids_and_milliseconds(self):
        original = event(event_time="2015-09-10T10:00:00.384Z")
        result = normalize(original)
        self.assertEqual([], result["error_codes"])
        self.assertEqual("001", result["visitor_id"])
        self.assertEqual("0", result["item_id"])
        self.assertIsNone(result["transaction_id"])
        self.assertEqual(384000, result["event_time"].microsecond)
        self.assertEqual(business_hash(original), result["business_sha256"])

    def test_bad_json_and_wrong_types_are_quarantined(self):
        for value in (b"{", b"\xff", b"[]", b"null", b"123"):
            with self.subTest(value=value):
                self.assertEqual(["MALFORMED_JSON"], normalize(value)["error_codes"])
        bad = normalize(event(schema_version=True, visitor_id=4, transaction_id=[], event_type="unexpected"))
        self.assertIn("INVALID_SCHEMA_VERSION", bad["error_codes"])
        self.assertIn("INVALID_VISITOR_ID", bad["error_codes"])
        self.assertIn("INVALID_TRANSACTION_ID", bad["error_codes"])
        self.assertIn("UNKNOWN_EVENT_TYPE", bad["error_codes"])
        self.assertIsNone(bad["visitor_id"])
        self.assertIsNone(bad["business_sha256"])

    def test_purchase_provenance_and_range_errors(self):
        bad = normalize(event(event_type="purchase", dataset_version="wrong", source="other",
                              event_time=DEFAULT_END, event_id="invalid"))
        self.assertEqual({"MISSING_TRANSACTION_ID", "INVALID_DATASET_VERSION", "INVALID_SOURCE",
                          "OUTSIDE_REPLAY_RANGE", "INVALID_EVENT_ID"}, set(bad["error_codes"]))

    def test_foreign_run_is_ignored_before_content_validation(self):
        result = normalize({"run_id": "another-run"})
        self.assertTrue(result["foreign_run"])
        self.assertEqual([], result["error_codes"])
        self.assertIsNone(result["business_sha256"])

    def test_timestamps_are_strict_utc_and_lower_boundary_is_inclusive(self):
        self.assertEqual([], normalize(event(event_time=DEFAULT_START))["error_codes"])
        for timestamp in (1430622004384, "2015-09-10T10:00:00Z", "2015-09-10T10:00:00.000+00:00", "2015-02-30T10:00:00.000Z"):
            with self.subTest(timestamp=timestamp):
                self.assertIn("INVALID_EVENT_TIME", normalize(event(event_time=timestamp))["error_codes"])

    def test_business_identity_excludes_delivery_attempt_metadata(self):
        original = event()
        self.assertEqual(business_hash(original), business_hash({**original, "run_id": "different", "emitted_at": "later"}))
        self.assertNotEqual(business_hash(original), business_hash({**original, "item_id": "1"}))

    def test_run_requests_reject_path_escape_rate_and_range_errors(self):
        validate_request(config())
        for changes in ({"run_id": "../escape"}, {"rate": float("nan")}, {"rate": float("inf")},
                        {"rate": 0}, {"rate": 11}, {"end": DEFAULT_START}):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                validate_request({**config(), **changes})

    def test_offset_drain_requires_every_partition(self):
        initial = {"0": 10, "1": 20, "2": 30}
        actual = source_offsets([{"partition": 1, "maximum": 24}], initial)
        self.assertEqual({"0": 10, "1": 25, "2": 30}, actual)
        self.assertFalse(drained({"0": 10, "1": 25}, initial))
        self.assertTrue(drained(actual, initial))
        self.assertEqual(initial, {"0": 10, "1": 20, "2": 30})

    def test_dlq_id_is_stable_and_retains_arbitrary_raw_bytes(self):
        row = types.SimpleNamespace(source_topic="raw", source_partition=2, source_offset=19,
                                    error_codes=["MALFORMED_JSON"], raw_key=None, raw_value=b"\xff")
        first = dlq_record(row, "run-one", 0)
        second = dlq_record(row, "run-two", 8)
        self.assertEqual(first["dlq_id"], second["dlq_id"])
        self.assertEqual("/w==", first["raw_value_base64"])
        row.source_offset = 20
        self.assertNotEqual(first["dlq_id"], dlq_record(row, "run-one", 0)["dlq_id"])

    def test_reference_distinct_transactions_and_active_window(self):
        rows = [event(0, 0), event(1, 2, visitor_id="2"),
                event(2, 6, event_type="purchase", transaction_id="900"),
                event(3, 7, item_id="11", event_type="purchase", transaction_id="900")]
        result = reference_metrics(rows)
        self.assertEqual(4, result["overview"]["processed_events"])
        self.assertEqual(2, result["overview"]["visitors"])
        self.assertEqual(1, result["overview"]["buyers"])
        self.assertEqual(1, result["overview"]["transactions"])
        self.assertEqual(1, result["overview"]["active_visitors"])
        self.assertEqual(.5, result["overview"]["buyer_rate"])
        self.assertEqual(2, sum(row["transactions"] for row in result["minutes"]))
        self.assertEqual(0.0, reference_metrics([])["overview"]["buyer_rate"])


class ReplayTests(unittest.TestCase):
    def test_rate_order_ack_cursor_and_resume(self):
        sends, saved = [], []
        clock = [0.0]

        def send(topic, key, value):
            sends.append((clock[0], json.loads(value)))
            return "0", len(sends)

        cursor = {"last_seq": -1, "end_offsets": {"0": 0, "1": 0, "2": 0}}
        result = replay_records([json.dumps(event(0)), json.dumps(event(1))], config(), cursor, send,
            lambda state, status: saved.append((state, status)), clock=lambda: clock[0],
            sleep=lambda seconds: clock.__setitem__(0, clock[0] + seconds), now=lambda: "2026-10-17T00:00:00.000Z")
        self.assertEqual([0, .1], [row[0] for row in sends])
        self.assertEqual([event(0)["event_id"], event(1)["event_id"]], [row[1]["event_id"] for row in sends])
        self.assertEqual(event(0)["event_time"], sends[0][1]["event_time"])
        self.assertEqual("2026-10-17T00:00:00.000Z", sends[0][1]["emitted_at"])
        self.assertEqual("completed", saved[-1][1])
        self.assertEqual(1, result["last_seq"])
        self.assertEqual(-1, cursor["last_seq"])
        resumed = replay_records([json.dumps(event(1))], config(), saved[0][0], send, lambda *_: None,
                                 sleep=lambda _: None)
        self.assertEqual(1, resumed["last_seq"])

    def test_delivery_failure_never_advances_cursor(self):
        saved = []
        cursor = {"last_seq": -1, "end_offsets": {"0": 0}}
        with self.assertRaisesRegex(RuntimeError, "unconfirmed"):
            replay_records([json.dumps(event())], config(), cursor,
                mock.Mock(side_effect=RuntimeError("unconfirmed")), lambda *state: saved.append(state), sleep=lambda _: None)
        self.assertEqual([], saved)
        self.assertEqual(-1, cursor["last_seq"])

    def test_cursor_storage_failure_preserves_business_identity_on_retry(self):
        delivered = []

        def send(topic, key, value):
            delivered.append(json.loads(value))
            return "0", len(delivered)

        cursor = {"last_seq": -1, "end_offsets": {"0": 0}}
        current = {**config(), "event_count": 1}
        with self.assertRaises(OSError):
            replay_records([json.dumps(event())], current, cursor, send,
                mock.Mock(side_effect=OSError("store unavailable")), sleep=lambda _: None)
        replay_records([json.dumps(event())], current, cursor, send, lambda *_: None, sleep=lambda _: None)
        self.assertEqual(business_hash(delivered[0]), business_hash(delivered[1]))

    def test_short_input_never_marks_producer_completed(self):
        statuses = []
        with self.assertRaisesRegex(ValueError, "ended before"):
            replay_records([json.dumps(event())], config(), {"last_seq": -1, "end_offsets": {}},
                           lambda *_: ("0", 1), lambda state, status: statuses.append(status), sleep=lambda _: None)
        self.assertEqual(["running"], statuses)

    def test_real_delivery_callback_errors_and_outstanding_queue_fail(self):
        class Client:
            def __init__(self, error=None, outstanding=0):
                self.error, self.outstanding = error, outstanding
            def produce(self, topic, **kwargs):
                kwargs["on_delivery"](self.error, types.SimpleNamespace(partition=lambda: 2, offset=lambda: 41))
            def flush(self, timeout):
                return self.outstanding
        self.assertEqual(("2", 42), send_confirmed(Client(), "raw", "key", b"value"))
        for client in (Client(error="rejected"), Client(outstanding=1)):
            with self.assertRaises(RuntimeError):
                send_confirmed(client, "raw", "key", b"value")


class StreamingPackagingTests(unittest.TestCase):
    def test_ingest_bytes_unchanged_and_streaming_is_separate(self):
        spec = importlib.util.spec_from_file_location("stream_package_test", APP / "package_code.py")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        with tempfile.TemporaryDirectory() as directory:
            original = module.package_code(Path(directory) / "ingest.zip")
            first = module.package_code(Path(directory) / "stream-one.zip", profile="streaming")
            second = module.package_code(Path(directory) / "stream-two.zip", profile="streaming")
            self.assertEqual("ae42a5981bc20e1295df1dabbfab7e553af0f133a39c97fb52fa795ceafcb554", original["sha256"])
            self.assertEqual(first["sha256"], second["sha256"])
            with zipfile.ZipFile(original["archive"]) as archive:
                self.assertFalse(any(name.startswith("ecommerce_streaming/") for name in archive.namelist()))
            with zipfile.ZipFile(first["archive"]) as archive:
                self.assertIn("ecommerce_streaming/batch.py", archive.namelist())
                self.assertTrue(all(name.endswith(".py") for name in archive.namelist()))


if __name__ == "__main__":
    unittest.main()
