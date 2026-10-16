"""Bounded Kafka delivery and replay; dependencies are imported only at runtime."""
import json
import os
import time

from ecommerce_analytics.contracts import canonical_json
from .contracts import BUSINESS_FIELDS, utc_now


def broker_config():
    return {"bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"], "socket.timeout.ms": 10000}


def end_offsets(topic):
    from confluent_kafka import Consumer, TopicPartition
    client = Consumer({**broker_config(), "group.id": "replay-offset-inspection", "enable.auto.commit": False})
    try:
        metadata = client.list_topics(topic=topic, timeout=10)
        details = metadata.topics[topic]
        if details.error is not None or len(details.partitions) != 3:
            raise ValueError("Replay input must have exactly three healthy partitions")
        return {str(partition): client.get_watermark_offsets(TopicPartition(topic, partition), timeout=10, cached=False)[1]
                for partition in sorted(details.partitions)}
    finally:
        client.close()


def producer():
    from confluent_kafka import Producer
    return Producer({**broker_config(), "enable.idempotence": True, "acks": "all", "retries": 3,
                     "max.in.flight.requests.per.connection": 1, "message.timeout.ms": 30000,
                     "queue.buffering.max.messages": 1000, "queue.buffering.max.kbytes": 16384})


def send_confirmed(client, topic, key, value):
    deliveries = []
    client.produce(topic, key=key, value=value, on_delivery=lambda error, message: deliveries.append((error, message)))
    if client.flush(35) or len(deliveries) != 1:
        raise RuntimeError("Kafka delivery not confirmed; cursor was not advanced")
    error, message = deliveries[0]
    if error is not None:
        raise RuntimeError(f"Kafka rejected record: {error}")
    return str(message.partition()), message.offset() + 1


def replay_records(lines, config, cursor, send, save, *, clock=time.monotonic, sleep=time.sleep, now=utc_now):
    """Input starts at cursor+1. One ACK at a time preserves replay order on retry."""
    cursor = {"last_seq": int(cursor["last_seq"]), "end_offsets": dict(cursor["end_offsets"])}
    deadline = clock()
    expected = config["event_count"]
    for line in lines:
        sequence = cursor["last_seq"] + 1
        if sequence >= expected:
            raise ValueError("Replay source has more records than its manifest")
        event = json.loads(line)
        if any(key not in event for key in BUSINESS_FIELDS):
            raise ValueError("Incomplete replay artifact")
        # Rate limits use monotonic time; event_time remains historical.
        sleep(max(0, deadline - clock()))
        event.update(run_id=config["run_id"], emitted_at=now())
        partition, offset = send(config["topic"], event["visitor_id"], canonical_json(event).encode())
        cursor = {"last_seq": sequence, "end_offsets": {**cursor["end_offsets"], partition: offset}}
        save(cursor, "running")  # Persist only after ACK. Failure safely resends this record.
        deadline = max(deadline + 1 / config["rate"], clock())
    if cursor["last_seq"] + 1 != expected:
        raise ValueError("Replay source ended before its manifest count")
    save(cursor, "completed")
    return cursor


def replay(lines, config, store):
    run = store.load(config)
    if run["producer"]["status"] == "completed":
        return {"run_id": config["run_id"], "reused": True, "producer": run["producer"]}
    client = producer()
    store.producer_progress(config, {key: run["producer"][key] for key in ("last_seq", "end_offsets")}, "running")

    def save(cursor, status):
        store.producer_progress(config, cursor, status)
        print("REPLAY_PROGRESS=" + json.dumps({**cursor, "status": status}), flush=True)

    result = replay_records(lines, config, run["producer"],
                            lambda topic, key, value: send_confirmed(client, topic, key, value), save)
    return {"run_id": config["run_id"], "reused": False, "producer": {**result, "status": "completed"}}


def send_dlq(records, topic):
    client = None
    for record in records:
        if client is None:
            client = producer()
        send_confirmed(client, topic, record["dlq_id"], canonical_json(record).encode())


def create_test_topics(raw, dlq):
    from confluent_kafka.admin import AdminClient, NewTopic
    admin = AdminClient(broker_config())
    futures = admin.create_topics([NewTopic(raw, num_partitions=3, replication_factor=1,
                                            config={"retention.ms": "3600000"}),
                                   NewTopic(dlq, num_partitions=1, replication_factor=1,
                                            config={"retention.ms": "3600000"})], request_timeout=30)
    errors = []
    for future in futures.values():
        try:
            future.result(timeout=40)
        except Exception as error:
            errors.append(error)
    if errors:
        raise errors[0]


def delete_test_topics(topics):
    from confluent_kafka import KafkaError, KafkaException
    from confluent_kafka.admin import AdminClient
    admin = AdminClient(broker_config())
    errors = []
    for future in admin.delete_topics(topics, request_timeout=30).values():
        try:
            future.result(timeout=40)
        except KafkaException as error:
            if error.args[0].code() != KafkaError.UNKNOWN_TOPIC_OR_PART:
                errors.append(error)
        except Exception as error:
            errors.append(error)
    if errors:
        raise errors[0]
