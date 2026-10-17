"""Kafka topic readiness regressions without container-only dependencies."""
import types
import unittest
from unittest import mock

from helpers import APP
from ecommerce_streaming import kafka_io


class KafkaError:
    UNKNOWN_TOPIC_OR_PART = 3
    LEADER_NOT_AVAILABLE = 5
    NOT_LEADER_FOR_PARTITION = 6
    REQUEST_TIMED_OUT = 7
    INVALID_TOPIC = 17
    TOPIC_AUTHORIZATION_FAILED = 29
    _UNKNOWN_PARTITION = -190
    _UNKNOWN_TOPIC = -188
    _ALL_BROKERS_DOWN = -187
    _TIMED_OUT = -185
    _TRANSPORT = -195

    def __init__(self, code, message="broker response"):
        self._code, self.message = code, message

    def code(self):
        return self._code

    def __str__(self):
        return f"{self.message} (code={self._code})"


class KafkaException(Exception):
    pass


def metadata(topic="raw", partitions=3, *, leader=1, error=None, partition_error=None):
    details = types.SimpleNamespace(error=error, partitions={
        index: types.SimpleNamespace(leader=leader, error=partition_error)
        for index in reversed(range(partitions))})
    return types.SimpleNamespace(topics={topic: details})


class KafkaReadinessTests(unittest.TestCase):
    def setUp(self):
        self.now = 0.0
        self.sleeps = []
        self.client = mock.Mock()
        self.client.list_topics.return_value = metadata()
        self.client.get_watermark_offsets.side_effect = lambda partition, **_: (0, partition.partition + 10)
        kafka = types.ModuleType("confluent_kafka")
        kafka.KafkaError, kafka.KafkaException = KafkaError, KafkaException
        kafka.Consumer = mock.Mock(return_value=self.client)
        kafka.TopicPartition = lambda topic, partition: types.SimpleNamespace(topic=topic, partition=partition)
        self.consumer = kafka.Consumer
        for patch in (mock.patch.dict("sys.modules", {"confluent_kafka": kafka}),
                      mock.patch.dict("os.environ", {"KAFKA_BOOTSTRAP_SERVERS": "kafka:9092"}),
                      mock.patch.object(kafka_io.time, "monotonic", lambda: self.now),
                      mock.patch.object(kafka_io.time, "sleep", self.sleep)):
            patch.start()
            self.addCleanup(patch.stop)

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.now += seconds

    def test_ready_topic_reads_all_uncached_offsets_in_partition_order(self):
        self.assertEqual({"0": 10, "1": 11, "2": 12}, kafka_io.end_offsets("raw"))
        self.assertEqual([0, 1, 2], [call.args[0].partition for call in self.client.get_watermark_offsets.call_args_list])
        self.assertTrue(all(call.kwargs["cached"] is False for call in self.client.get_watermark_offsets.call_args_list))
        self.assertFalse(self.consumer.call_args.args[0]["enable.auto.commit"])
        self.client.subscribe.assert_not_called()
        self.client.commit.assert_not_called()
        self.assertEqual([], self.sleeps)
        self.client.close.assert_called_once_with()

    def test_stale_leader_discards_partial_offsets_and_refreshes_metadata(self):
        stale = KafkaException(KafkaError(KafkaError.NOT_LEADER_FOR_PARTITION, "stale leader"))
        self.client.get_watermark_offsets.side_effect = [(0, 11), stale, (0, 21), (0, 22), (0, 23)]
        self.assertEqual({"0": 21, "1": 22, "2": 23}, kafka_io.end_offsets("raw"))
        self.assertEqual(2, self.client.list_topics.call_count)
        self.assertEqual([0, 1, 0, 1, 2], [call.args[0].partition for call in self.client.get_watermark_offsets.call_args_list])
        self.assertEqual([.25], self.sleeps)
        self.client.close.assert_called_once_with()

    def test_missing_topic_and_unready_leaders_are_polled_until_ready(self):
        self.client.list_topics.side_effect = [
            types.SimpleNamespace(topics={}),
            metadata(error=KafkaError(KafkaError.UNKNOWN_TOPIC_OR_PART)),
            metadata(leader=-1),
            metadata(partition_error=KafkaError(KafkaError.LEADER_NOT_AVAILABLE)),
            metadata()]
        self.assertEqual({"0": 10, "1": 11, "2": 12}, kafka_io.end_offsets("raw"))
        self.assertEqual([.25, .5, 1, 1], self.sleeps)
        self.assertEqual(3, self.client.get_watermark_offsets.call_count)
        self.client.close.assert_called_once_with()

    def test_transient_kafka_errors_at_metadata_and_watermarks_can_recover(self):
        codes = (KafkaError.UNKNOWN_TOPIC_OR_PART, KafkaError.LEADER_NOT_AVAILABLE,
                 KafkaError.NOT_LEADER_FOR_PARTITION, KafkaError.REQUEST_TIMED_OUT,
                 KafkaError._UNKNOWN_TOPIC, KafkaError._UNKNOWN_PARTITION,
                 KafkaError._TIMED_OUT, KafkaError._TRANSPORT, KafkaError._ALL_BROKERS_DOWN)
        for code in codes:
            for stage in ("metadata", "watermark"):
                with self.subTest(code=code, stage=stage):
                    self.client.reset_mock()
                    error = KafkaException(KafkaError(code))
                    self.client.list_topics.side_effect = [error, metadata()] if stage == "metadata" else None
                    self.client.get_watermark_offsets.side_effect = (
                        [error, (0, 10), (0, 11), (0, 12)] if stage == "watermark"
                        else lambda partition, **_: (0, partition.partition + 10))
                    self.assertEqual({"0": 10, "1": 11, "2": 12}, kafka_io.end_offsets("raw"))
                    self.client.close.assert_called_once_with()

    def test_retry_deadline_preserves_last_error_and_closes_consumer(self):
        error = KafkaException(KafkaError(KafkaError.NOT_LEADER_FOR_PARTITION, "still not leader"))
        self.client.get_watermark_offsets.side_effect = error
        with self.assertRaisesRegex(TimeoutError, "raw.*1.5.*still not leader") as raised:
            kafka_io.end_offsets("raw", timeout=1.5)
        self.assertIs(error, raised.exception.__cause__)
        self.assertEqual(1.5, self.now)
        self.assertEqual([.25, .5, .75], self.sleeps)
        self.client.close.assert_called_once_with()

    def test_each_broker_call_uses_the_remaining_shared_deadline(self):
        timeouts = []

        def offsets(partition, *, timeout, cached):
            timeouts.append(timeout)
            self.now += .4
            return 0, 10

        self.client.get_watermark_offsets.side_effect = offsets
        with self.assertRaisesRegex(TimeoutError, "raw"):
            kafka_io.end_offsets("raw", timeout=1)
        self.assertAlmostEqual(1, self.client.list_topics.call_args.kwargs["timeout"])
        self.assertEqual(3, len(timeouts))
        for actual, expected in zip(timeouts, [1, .6, .2]):
            self.assertAlmostEqual(expected, actual)
        self.client.close.assert_called_once_with()

    def test_fatal_errors_are_not_retried_at_any_stage(self):
        for code in (KafkaError.TOPIC_AUTHORIZATION_FAILED, KafkaError.INVALID_TOPIC):
            for stage in ("request", "topic", "partition", "watermark"):
                with self.subTest(code=code, stage=stage):
                    self.client.reset_mock()
                    self.sleeps.clear()
                    error = KafkaError(code, "request refused")
                    exception = KafkaException(error)
                    self.client.list_topics.side_effect = exception if stage == "request" else None
                    self.client.list_topics.return_value = metadata(
                        error=error if stage == "topic" else None,
                        partition_error=error if stage == "partition" else None)
                    self.client.get_watermark_offsets.side_effect = exception if stage == "watermark" else None
                    with self.assertRaises(KafkaException) as raised:
                        kafka_io.end_offsets("raw")
                    self.assertIs(error, raised.exception.args[0])
                    self.assertEqual([], self.sleeps)
                    self.client.close.assert_called_once_with()

    def test_wrong_partition_layout_fails_immediately(self):
        for layout in ({}, {0: mock.Mock()}, {0: mock.Mock(), 1: mock.Mock(), 3: mock.Mock()}):
            with self.subTest(partitions=list(layout)):
                self.client.reset_mock()
                self.client.list_topics.return_value = types.SimpleNamespace(topics={
                    "raw": types.SimpleNamespace(error=None, partitions=layout)})
                with self.assertRaisesRegex(ValueError, "raw.*3.*partition"):
                    kafka_io.end_offsets("raw")
                self.client.get_watermark_offsets.assert_not_called()
                self.assertEqual([], self.sleeps)
                self.client.close.assert_called_once_with()

    def test_single_partition_dlq_uses_the_same_readiness_check(self):
        self.client.list_topics.return_value = metadata("dlq", partitions=1)
        self.assertEqual({"0": 10}, kafka_io.end_offsets("dlq", partitions=1))
        self.assertEqual(1, self.client.get_watermark_offsets.call_count)
        self.client.close.assert_called_once_with()

    def test_invalid_readiness_budget_is_refused_before_opening_consumer(self):
        for options in ({"timeout": 0}, {"timeout": -1}, {"timeout": float("inf")},
                        {"timeout": float("nan")}, {"partitions": 0}, {"partitions": 1.5}):
            with self.subTest(options=options), self.assertRaises(ValueError):
                kafka_io.end_offsets("raw", **options)
        self.consumer.assert_not_called()

    def test_create_topics_waits_for_both_futures_then_raw_and_dlq_readiness(self):
        order = []
        admin = types.ModuleType("confluent_kafka.admin")
        client = mock.Mock()
        admin.AdminClient = mock.Mock(return_value=client)
        admin.NewTopic = lambda topic, **kwargs: types.SimpleNamespace(topic=topic, **kwargs)
        client.create_topics.return_value = {
            name: mock.Mock(result=mock.Mock(side_effect=lambda timeout, name=name: order.append(name)))
            for name in ("raw-created", "dlq-created")}
        with mock.patch.dict("sys.modules", {"confluent_kafka.admin": admin}), mock.patch.object(
                kafka_io, "end_offsets", side_effect=lambda topic, **_: order.append(topic)) as ready:
            kafka_io.create_test_topics("raw", "dlq")
        self.assertEqual(["raw-created", "dlq-created", "raw", "dlq"], order)
        self.assertEqual([mock.call("raw"), mock.call("dlq", partitions=1)], ready.call_args_list)

    def test_create_failure_waits_for_other_future_without_checking_readiness(self):
        admin = types.ModuleType("confluent_kafka.admin")
        client = mock.Mock()
        admin.AdminClient = mock.Mock(return_value=client)
        admin.NewTopic = lambda topic, **kwargs: types.SimpleNamespace(topic=topic, **kwargs)
        error = KafkaException(KafkaError(KafkaError.TOPIC_AUTHORIZATION_FAILED))
        failed, completed = mock.Mock(), mock.Mock()
        failed.result.side_effect = error
        client.create_topics.return_value = {"raw": failed, "dlq": completed}
        with mock.patch.dict("sys.modules", {"confluent_kafka.admin": admin}), mock.patch.object(kafka_io, "end_offsets") as ready:
            with self.assertRaises(KafkaException) as raised:
                kafka_io.create_test_topics("raw", "dlq")
        self.assertIs(error, raised.exception)
        completed.result.assert_called_once_with(timeout=40)
        ready.assert_not_called()


if __name__ == "__main__":
    unittest.main()
