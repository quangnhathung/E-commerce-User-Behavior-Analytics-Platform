"""Small integration-test helpers. No business topics or user data are modified."""
import json
import os
import time

from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic
from pymongo import MongoClient


def mongo_settings():
    return {"host": "mongodb", "username": os.environ["MONGO_USERNAME"],
            "password": os.environ["MONGO_PASSWORD"], "authSource": os.environ["MONGO_DATABASE"],
            "serverSelectionTimeoutMS": 5000}


def mongo():
    return MongoClient(**mongo_settings())


def kafka():
    return AdminClient({"bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
                        "socket.timeout.ms": 10000})


def create_topic(topic, partitions=3):
    futures = kafka().create_topics([NewTopic(topic, num_partitions=partitions, replication_factor=1,
                                            config={"retention.ms": "3600000"})], request_timeout=30)
    futures[topic].result(timeout=40)


def delete_topic(topic):
    kafka().delete_topics([topic], request_timeout=30)[topic].result(timeout=40)


def publish(topic, records, partitions=3):
    failures = []
    producer = Producer({"bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
                         "enable.idempotence": True, "acks": "all", "message.timeout.ms": 30000})
    for index, record in enumerate(records):
        producer.produce(topic, key=str(index), value=json.dumps(record).encode(),
                         partition=index % partitions,
                         on_delivery=lambda error, message: failures.append(str(error)) if error else None)
    outstanding = producer.flush(35)
    if outstanding or failures:
        raise RuntimeError(f"Kafka delivery failed: {outstanding} outstanding, {len(failures)} errors")


def consume(topic, expected_count):
    client = Consumer({"bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
                       "group.id": topic + "-verify", "auto.offset.reset": "earliest",
                       "enable.auto.commit": False})
    records = []
    try:
        client.subscribe([topic])
        deadline = time.monotonic() + 40
        while len(records) < expected_count and time.monotonic() < deadline:
            message = client.poll(1)
            if message is None:
                continue
            if message.error():
                raise RuntimeError(f"Kafka consumer error: {message.error()}")
            records.append(json.loads(message.value()))
    finally:
        client.close()
    if len(records) != expected_count:
        raise RuntimeError(f"Expected {expected_count} Kafka records, got {len(records)}")
    return records
