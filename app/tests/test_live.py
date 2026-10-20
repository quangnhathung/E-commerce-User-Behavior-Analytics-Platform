"""Live generator, wire compatibility, bounded telemetry, fencing and ACK resume."""
import copy
import json
import itertools
from pathlib import Path
import tempfile
import types
import unittest
from unittest import mock

from helpers import APP
from package_code import package_code
from test_streaming_ml import request as ml_request
from test_streaming_store import Collection as BaseCollection, Database as BaseDatabase
from ecommerce_live.contracts import VERSION, acknowledged_total, child_request, events, validate
from ecommerce_live.producer import emit
from ecommerce_live.store import LiveStore
from ecommerce_streaming_ml.contracts import normalize_message


def request(**extra):
    return {**ml_request(), "live_id": "live-test", "live_version": VERSION, "seed": "1" * 32,
        "events_per_segment": 1000, "visitors": 200, "items": 100, "max_segments": 0,
        "minimum_disk_gib": 30, **extra}


class Collection(BaseCollection):
    def update_one(self, query, changes, upsert=False):
        row = self.documents.get(query["_id"])
        if row is not None and any(row.get(key) != value for key, value in query.items()):
            return types.SimpleNamespace(matched_count=0)
        result = super().update_one(query, changes, upsert)
        if result.matched_count:
            for key, value in changes.get("$inc", {}).items():
                self.documents[query["_id"]][key] += value
        return result

    def delete_many(self, query):
        for key, value in list(self.documents.items()):
            if value["live_id"] == query["live_id"] and value["sequence"] <= query["sequence"]["$lte"]:
                del self.documents[key]


class Database(BaseDatabase):
    def __getitem__(self, name):
        return self.collections.setdefault(name, Collection())


class LiveContracts(unittest.TestCase):
    def test_large_varied_repeatable_source_and_disjoint_segments(self):
        req = request(); validate(req)
        source = events(child_request(req, 1))
        self.assertEqual(1000, len(source)); self.assertEqual(200, len({row["visitor_id"] for row in source}))
        self.assertEqual(source, events(child_request(req, 1)))
        self.assertEqual(1000, len({row["event_id"] for row in source}))
        self.assertEqual({"view", "add_to_cart", "purchase"}, {row["event_type"] for row in source})
        self.assertTrue(any(row["visitor_id"].startswith("0000") for row in source))
        self.assertGreater(len({row["item_id"] for row in source}), 50)
        self.assertEqual(sorted(row["event_time"] for row in source), [row["event_time"] for row in source])
        other = events(child_request(req, 2))
        self.assertFalse({row["visitor_id"] for row in source} & {row["visitor_id"] for row in other})
        self.assertNotEqual(source, events(child_request(request(seed="2"*32), 1)))

    def test_generated_records_pass_existing_wire_validation(self):
        config = child_request(request(events_per_segment=30, visitors=6), 1)
        for event in events(config):
            payload = {**event, "run_id": config["run_id"], "emitted_at": config["start"]}
            row = normalize_message({"value": json.dumps(payload).encode(), "key": b"key", "topic": "raw",
                "partition": 0, "offset": 0, "timestamp": None}, config, "spark-worker-1")
            self.assertEqual([], row["error_codes"])

    def test_rejects_oversized_bad_and_nonfinite_parameters(self):
        for change in [{"rate":11}, {"events_per_segment":1001}, {"visitors":201}, {"items":101},
                {"seed":"bad"}, {"live_id":"../bad"}, {"events_per_segment":12,"visitors":200},
                {"minimum_disk_gib":float('nan')}, {"max_segments":True}]:
            with self.subTest(change=change), self.assertRaises(ValueError): validate(request(**change))

    def test_six_archives_stay_frozen(self):
        with tempfile.TemporaryDirectory() as folder:
            frozen = package_code(Path(folder)/"old.zip", profile="streaming-ml")
            self.assertEqual("97c5e4715359ee334fdd8fec6354dc22f7a6b85bfbdd60d177f1afc06328302c", frozen["sha256"])
            newer = package_code(Path(folder)/"live.zip", profile="streaming-live")
            self.assertNotEqual(frozen["sha256"], newer["sha256"])


class LiveStoreTests(unittest.TestCase):
    def setUp(self):
        self.store = LiveStore(Database())
        self.request = request()
        self.store.ensure_session(self.request, "owner")
        self.config = {**child_request(self.request, 1), "initial_offsets": {"0":0}}
        self.store.ensure_run(self.config)
        self.store.state("live-test", "owner", active_run_id=self.config["run_id"], segment_index=1)

    def test_seed_is_pinned_and_stale_owner_cannot_stop_new_owner(self):
        with self.assertRaises(ValueError): self.store.ensure_session(request(seed="2"*32), "owner")
        self.store.ensure_session(self.request, "new-owner")
        with self.assertRaises(ValueError): self.store.request_stop("live-test", "owner")
        self.store.request_stop("live-test", "new-owner")
        self.assertTrue(self.store.session("live-test")["stop_requested"])

    def test_ack_retry_preserves_first_ack_and_does_not_double_count(self):
        value = {**events(self.config)[0], "emitted_at": self.config["start"]}
        self.store.ack(self.config, "owner", 0, value, "0", 1, {"last_seq":0,"end_offsets":{"0":1}})
        self.store.ack(self.config, "owner", 0, value, "0", 2, {"last_seq":0,"end_offsets":{"0":2}})
        self.assertEqual(1, len(self.store.db.live_events.documents))
        self.assertEqual(1, self.store.db.live_events.documents['live-test:0']["next_offset"])
        self.assertEqual(1, self.store.session("live-test")["acknowledged_events"])

    def test_buffer_is_bounded(self):
        value = {**events(self.config)[0], "emitted_at": self.config["start"]}
        for seq in range(2005):
            self.store.ack(self.config, "owner", seq, value, "0", seq+1, {"last_seq":seq,"end_offsets":{"0":seq+1}})
        self.assertEqual(2000, len(self.store.db.live_events.documents))
        self.assertNotIn('live-test:4', self.store.db.live_events.documents)

    def test_segment_completion_is_idempotent_and_cursor_total_does_not_reset(self):
        report = {"overview":{"processed_events":1000}}
        self.store.finish_segment(self.config, "owner", report)
        self.store.finish_segment(self.config, "owner", report)
        row = self.store.session("live-test")
        self.assertEqual(1000,row["completed_events"]); self.assertEqual(1,row["completed_segments"])
        self.assertEqual(1012, acknowledged_total(1,1000,2,11))
        self.assertEqual(1000, acknowledged_total(1,1000,1,999))

    def test_resume_skips_acked_records_and_pause_retains_cursor(self):
        config = {**self.config, "events_per_segment":12, "event_count":12}
        self.store.ensure_run = mock.Mock()
        self.store.load = mock.Mock(return_value={"producer":{"status":"paused","last_seq":8,"end_offsets":{"0":9}}})
        self.store.status = mock.Mock(return_value={"consumer":{"status":"running"},"backlog":0})
        self.store.producer_progress = mock.Mock(); self.store.ack = mock.Mock()
        clock = mock.Mock(side_effect=itertools.count())
        sent=[]
        def send(client, topic, key, payload):
            sent.append(json.loads(payload)); return "0",len(sent)+9
        emit(config, events(config), self.store, "owner", stopping=lambda:False, clock=clock,
             make_producer=mock.Mock, send=send)
        self.assertEqual(3,len(sent));self.assertEqual(9,self.store.ack.call_args_list[0].args[2])
        self.assertEqual("completed",self.store.producer_progress.call_args.args[2])
        self.store.ack.reset_mock()
        emit(config, events(config), self.store, "owner", stopping=lambda:True, clock=clock, make_producer=mock.Mock, send=send)
        self.store.ack.assert_not_called()
        self.assertEqual("paused",self.store.producer_progress.call_args.args[2])

    def test_backpressure_waits_and_failure_retains_cursor(self):
        config={**self.config,"events_per_segment":12,"event_count":12}
        self.store.load=mock.Mock(return_value={"producer":{"status":"running","last_seq":10,"end_offsets":{"0":11}}})
        self.store.status=mock.Mock(side_effect=[{"consumer":{"status":"running"},"backlog":200},
            {"consumer":{"status":"failed"},"backlog":200}])
        self.store.producer_progress=mock.Mock();self.store.ack=mock.Mock()
        send=mock.Mock();sleep=mock.Mock()
        with self.assertRaisesRegex(RuntimeError,"consumer stopped"):
            emit(config,events(config),self.store,"owner",stopping=lambda:False,
                 make_producer=mock.Mock,send=send,sleep=sleep)
        sleep.assert_called_once();send.assert_not_called();self.store.ack.assert_not_called()


class LiveProducerPacingTests(unittest.TestCase):
    def setUp(self):
        self.now = 0.0
        self.wake_delay = 0.0
        self.waits = []
        self.sent = []
        self.client = mock.Mock()
        self.store = mock.Mock()
        self.store.load.return_value = {"producer": {
            "status": "running", "last_seq": -1, "end_offsets": {"0": 0}}}
        self.store.status.return_value = {"consumer": {"status": "running"}, "backlog": 0}
        self.source = [{"visitor_id": str(index)} for index in range(2)]

    def sleep(self, seconds):
        if seconds < 0:
            raise ValueError("sleep length must be non-negative")
        self.waits.append(seconds)
        self.now += seconds + self.wake_delay

    def send(self, client, topic, key, payload):
        self.sent.append((self.now, json.loads(payload)))
        return "0", len(self.sent)

    def emit(self, *, rate=5, stopping=lambda: False):
        emit({"run_id": "pacing-test", "topic": "raw", "rate": rate}, self.source,
             self.store, "owner", stopping=stopping, clock=lambda: self.now,
             sleep=self.sleep, make_producer=lambda: self.client, send=self.send)

    def test_deadline_crossed_while_checking_stop_never_sleeps_negative(self):
        def status(config):
            if self.sent:
                self.now = .2 - .000001  # Metadata lookup returns just before the next send.
            return {"consumer": {"status": "running"}, "backlog": 0}

        def stopping():
            if self.now > .19:
                self.now += .000002  # Control lookup crosses the deadline.
            return False

        self.store.status.side_effect = status
        self.emit(stopping=stopping)
        self.assertEqual(["0", "1"], [value["visitor_id"] for _, value in self.sent])
        self.assertEqual([], self.waits)
        self.assertEqual(2, self.store.ack.call_count)
        self.assertEqual("completed", self.store.producer_progress.call_args.args[2])
        self.client.flush.assert_called_once_with(10)

    def test_deadline_equality_does_not_sleep(self):
        def status(config):
            if self.sent:
                self.now = .2
            return {"consumer": {"status": "running"}, "backlog": 0}

        self.store.status.side_effect = status
        self.emit()
        self.assertEqual([0, .2], [at for at, _ in self.sent])
        self.assertEqual([], self.waits)

    def test_supported_rates_and_late_wakeups_preserve_order_and_acks(self):
        for rate in (1, 5, 10):
            for wake_delay in (0, .35):
                with self.subTest(rate=rate, wake_delay=wake_delay):
                    self.setUp()
                    self.source = [{"visitor_id": str(index)} for index in range(4)]
                    self.wake_delay = wake_delay
                    self.emit(rate=rate)
                    self.assertEqual(["0", "1", "2", "3"], [v["visitor_id"] for _, v in self.sent])
                    self.assertTrue(self.waits)
                    self.assertTrue(all(0 < seconds <= .1 for seconds in self.waits))
                    self.assertEqual([0, 1, 2, 3], [call.args[2] for call in self.store.ack.call_args_list])
                    if wake_delay == 0:
                        for index, (at, _) in enumerate(self.sent):
                            self.assertAlmostEqual(index / rate, at)
                    self.assertEqual("completed", self.store.producer_progress.call_args.args[2])
                    self.client.flush.assert_called_once_with(10)

    def test_stop_during_wait_retains_ack_cursor_and_resume_skips_it(self):
        self.emit(stopping=lambda: bool(self.waits))
        cursor = {"last_seq": 0, "end_offsets": {"0": 1}}
        self.assertEqual(["0"], [value["visitor_id"] for _, value in self.sent])
        self.assertEqual((cursor, "paused"), self.store.producer_progress.call_args.args[1:])
        self.assertEqual(1, self.store.ack.call_count)
        self.client.flush.assert_called_once_with(10)

        self.store.load.return_value = {"producer": {**cursor, "status": "paused"}}
        self.emit()
        self.assertEqual(["0", "1"], [value["visitor_id"] for _, value in self.sent])
        self.assertEqual([0, 1], [call.args[2] for call in self.store.ack.call_args_list])
        self.assertEqual("completed", self.store.producer_progress.call_args.args[2])
