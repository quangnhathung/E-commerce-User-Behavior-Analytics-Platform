"""Kafka helper lifetime/error checks without container-only dependencies."""
from concurrent.futures import Future, TimeoutError
import gc
import importlib.util
from pathlib import Path
import types
import unittest
from unittest import mock
import weakref


class PendingFuture(Future):
    def __init__(self, owner, waits, error):
        super().__init__()
        self.owner = weakref.ref(owner)
        self.waits = waits
        if error is None:
            self.set_result(None)
        else:
            self.set_exception(error)

    def result(self, timeout=None):
        gc.collect()
        if self.owner() is None:
            raise AssertionError("AdminClient was released before waiting for its Future")
        self.waits.append(timeout)
        return super().result(timeout=timeout)


class FakeAdmin:
    def __init__(self, calls, waits, error):
        self.calls, self.waits, self.error = calls, waits, error

    def create_topics(self, topics, **kwargs):
        self.calls.append(("create", topics, kwargs))
        return {topic.topic: PendingFuture(self, self.waits, self.error) for topic in topics}

    def delete_topics(self, topics, **kwargs):
        self.calls.append(("delete", topics, kwargs))
        return {topic: PendingFuture(self, self.waits, self.error) for topic in topics}


class KafkaAdminTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        kafka = types.ModuleType("confluent_kafka")
        kafka.Consumer, kafka.Producer = mock.Mock(), mock.Mock()
        admin = types.ModuleType("confluent_kafka.admin")
        admin.AdminClient = mock.Mock()
        admin.NewTopic = lambda topic, **kwargs: types.SimpleNamespace(topic=topic, **kwargs)
        mongo = types.ModuleType("pymongo")
        mongo.MongoClient = mock.Mock()
        path = Path(__file__).resolve().parents[1] / "smoke/common.py"
        spec = importlib.util.spec_from_file_location("infra_smoke_common_test", path)
        cls.common = importlib.util.module_from_spec(spec)
        with mock.patch.dict("sys.modules", {"confluent_kafka": kafka,
                                            "confluent_kafka.admin": admin, "pymongo": mongo}):
            spec.loader.exec_module(cls.common)

    def setUp(self):
        self.calls, self.waits = [], []
        self.error = None
        # Create a fresh client per call; the test must not retain it itself.
        self.patch = mock.patch.object(self.common, "kafka", side_effect=self.new_admin)
        self.patch.start()
        self.addCleanup(self.patch.stop)

    def new_admin(self):
        return FakeAdmin(self.calls, self.waits, self.error)

    def test_create_keeps_client_alive_until_future_result(self):
        self.common.create_topic("infra-smoke-lifetime", partitions=1)
        operation, topics, options = self.calls[0]
        self.assertEqual("create", operation)
        self.assertEqual("infra-smoke-lifetime", topics[0].topic)
        self.assertEqual(1, topics[0].num_partitions)
        self.assertEqual(1, topics[0].replication_factor)
        self.assertEqual({"retention.ms": "3600000"}, topics[0].config)
        self.assertEqual({"request_timeout": 30}, options)
        self.assertEqual([40], self.waits)

    def test_delete_keeps_client_alive_until_future_result(self):
        self.common.delete_topic("infra-smoke-lifetime")
        self.assertEqual([("delete", ["infra-smoke-lifetime"], {"request_timeout": 30})], self.calls)
        self.assertEqual([40], self.waits)

    def test_broker_errors_are_propagated(self):
        self.error = RuntimeError("broker rejected request")
        for operation in (self.common.create_topic, self.common.delete_topic):
            with self.subTest(operation=operation.__name__):
                with self.assertRaises(RuntimeError) as raised:
                    operation("infra-smoke-error")
                self.assertIs(self.error, raised.exception)

    def test_timeouts_are_propagated(self):
        self.error = TimeoutError("admin request timed out")
        for operation in (self.common.create_topic, self.common.delete_topic):
            with self.subTest(operation=operation.__name__):
                with self.assertRaises(TimeoutError) as raised:
                    operation("infra-smoke-timeout")
                self.assertIs(self.error, raised.exception)


if __name__ == "__main__":
    unittest.main()
