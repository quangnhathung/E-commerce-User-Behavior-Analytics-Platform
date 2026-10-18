"""ACK-based producer, with simulator phases gated by committed ML snapshots."""
import json
import time

from ecommerce_streaming.contracts import drained
from ecommerce_streaming.kafka_io import producer, replay_records, send_confirmed


def wait_phase(config, store, cursor, *, timeout=180, clock=time.monotonic, sleep=time.sleep):
    deadline = clock() + timeout
    while True:
        state = store.status(config)
        if state["consumer"]["status"] in ("failed", "interrupted"):
            raise RuntimeError("Streaming consumer stopped; restart stream-ml before continuing simulator")
        if (state["batch"] is not None and drained(state["batch"]["processed_offsets"], cursor["end_offsets"]) and
                state["scoring"]["status"] == "ready"):
            print("SIMULATOR_PHASE_COMMITTED=" + json.dumps({"phase": (cursor["last_seq"] + 1) // 3,
                "batch_id": state["committed_batch_id"], "scoring": state["scoring"]}), flush=True)
            return
        if clock() >= deadline:
            raise TimeoutError("Simulator phase did not publish within its 180-second budget; retain the run and retry")
        sleep(min(1, max(0, deadline - clock())))


def replay(lines, config, store):
    run = store.load(config)
    if run["producer"]["status"] == "completed":
        return {"passed": True, "run_id": config["run_id"], "reused": True, "producer": run["producer"]}
    client = producer()
    initial = {key: run["producer"][key] for key in ("last_seq", "end_offsets")}
    store.producer_progress(config, initial, "running")
    if config["demo_source"] == "simulator" and initial["last_seq"] in (2, 5, 8):
        wait_phase(config, store, initial)

    def save(cursor, status):
        store.producer_progress(config, cursor, status)
        print("REPLAY_PROGRESS=" + json.dumps({**cursor, "status": status}), flush=True)
        if config["demo_source"] == "simulator" and status == "running" and cursor["last_seq"] in (2, 5, 8):
            wait_phase(config, store, cursor)

    result = replay_records(lines, config, initial,
        lambda topic, key, value: send_confirmed(client, topic, key, value), save)
    return {"passed": True, "run_id": config["run_id"], "reused": False, "producer": {**result, "status": "completed"}}
