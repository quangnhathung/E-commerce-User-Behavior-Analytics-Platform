"""Rate-limited ACK telemetry, bounded backlog and resumable graceful pause."""
import time

from ecommerce_analytics.contracts import canonical_json
from ecommerce_streaming.contracts import utc_now
from ecommerce_streaming.kafka_io import producer, send_confirmed
from .contracts import MAX_INFLIGHT


def emit(config, source, store, owner, *, stopping, clock=time.monotonic, sleep=time.sleep,
         make_producer=producer, send=send_confirmed):
    state = store.load(config)
    if state["producer"]["status"] == "completed":
        return
    cursor = {key: state["producer"][key] for key in ("last_seq", "end_offsets")}
    store.producer_progress(config, cursor, "running")
    client = make_producer()
    deadline = clock()
    try:
        for seq in range(cursor["last_seq"] + 1, len(source)):
            wait_started = clock()
            while True:
                if stopping():
                    store.producer_progress(config, cursor, "paused")
                    return
                current = store.status(config)
                if current["consumer"]["status"] in ("failed", "interrupted"):
                    raise RuntimeError("Live consumer stopped; ACK cursor retained")
                if current["backlog"] < MAX_INFLIGHT:
                    break
                if clock() - wait_started > 180:
                    raise TimeoutError("Live backlog did not drain within 180 seconds")
                sleep(.2)
            while clock() < deadline:
                if stopping():
                    store.producer_progress(config, cursor, "paused")
                    return
                sleep(min(.1, deadline - clock()))
            value = {**source[seq], "run_id": config["run_id"], "emitted_at": utc_now()}
            partition, offset = send(client, config["topic"], value["visitor_id"], canonical_json(value).encode())
            cursor = {"last_seq": seq, "end_offsets": {**cursor["end_offsets"], partition: offset}}
            store.ack(config, owner, seq, value, partition, offset, cursor)
            deadline = max(deadline + 1 / config["rate"], clock())
        store.producer_progress(config, cursor, "completed")
    finally:
        client.flush(10)
