"""Generated bounded documents; fake Mongo is only an offline boundary adapter."""
import copy
from collections import defaultdict


def field(row, key):
    for part in key.split("."):
        if not isinstance(row, dict): return None
        row = row.get(part)
    return row


def matches(row, query):
    for key, value in query.items():
        if key == "$or":
            if not any(matches(row, part) for part in value): return False
        elif isinstance(value, dict):
            for op, wanted in value.items():
                actual = field(row, key)
                if actual is None: return False
                if op == "$lt" and not actual < wanted: return False
                if op == "$lte" and not actual <= wanted: return False
                if op == "$gte" and not actual >= wanted: return False
                if op == "$gt" and not actual > wanted: return False
                if op not in ("$lt", "$lte", "$gte", "$gt"): raise AssertionError(op)
        elif field(row, key) != value:
            return False
    return True


class Cursor(list):
    def sort(self, keys):
        for key, order in reversed(keys):
            super().sort(key=lambda r: field(r, key), reverse=order < 0)
        return self

    def limit(self, n):
        return Cursor(self[:n])

    def max_time_ms(self, n):
        return self


class Collection:
    def __init__(self): self.documents = []

    def insert_many(self, values): self.documents.extend(copy.deepcopy(values))

    def insert_one(self, value): self.insert_many([value])

    def find(self, query, projection=None):
        rows = [copy.deepcopy(v) for v in self.documents if matches(v, query)]
        for r in rows:
            for k, included in (projection or {}).items():
                if not included: r.pop(k, None)
        return Cursor(rows)

    def find_one(self, query, projection=None, **kw):
        return next(iter(self.find(query, projection)), None)

    def count_documents(self, query, **kw): return len(self.find(query))

    def update_one(self, query, change):
        row = next(v for v in self.documents if matches(v, query))
        for k, value in change["$set"].items():
            target = row
            *parts, leaf = k.split(".")
            for part in parts: target = target[part]
            target[leaf] = copy.deepcopy(value)

    def delete_one(self, query):
        row = next(v for v in self.documents if matches(v, query))
        self.documents.remove(row)

    def aggregate(self, pipeline, **kw):
        values = self.find(pipeline[0]["$match"])
        group = pipeline[1]["$group"]
        groups = defaultdict(list)
        for v in values: groups[field(v, group["_id"][1:])].append(v)
        return [{"_id": key, "count": len(rows),
            **{k: sum(r[op["$avg"][1:]] for r in rows) / len(rows)
               for k, op in group.items() if isinstance(op, dict) and "$avg" in op}} for key, rows in groups.items()]


class Database:
    def __init__(self): self.collections = defaultdict(Collection)
    def __getitem__(self, key): return self.collections[key]
    def command(self, name): return {"ok": 1}


def seed(db, run_id="web-fixture"):
    from ecommerce_ml.contracts import reference, fingerprint, digest
    from ecommerce_streaming.contracts import reference_metrics
    from ecommerce_streaming_ml.contracts import simulator_events, prediction_fingerprint
    config = {"run_id": run_id, "base": "/ecommerce", "pipeline_version": "stream-ml-v1", "demo_source": "simulator",
              "start": "2015-09-18T00:00:00.000Z", "end": "2015-09-18T03:00:00.000Z", "dataset_version": "a"*64,
              "event_count": 9, "initial_offsets": {"0": 0, "1": 0, "2": 0}}
    bindings = {"selected_k": 3, "original_threshold": .5, "threshold": .7, "feature_version": "features-v1",
                "evaluation_kind": "posthoc_threshold_reassessment", "fresh_holdout": False}
    for collection, key in [("segment_models", "segment_model_version"), ("purchase_models", "purchase_model_version"), ("purchase_policies", "policy_version")]:
        identity = {"fixture": collection}
        version = digest(identity)
        bindings[key] = version
        report = {"selected_k": 3} if collection == "segment_models" else {"evaluation": {"test": {"precision": .75, "recall": .15}}}
        db[collection].insert_one({"_id": version, "identity": identity, "status": "published", "report": report})
    config["bindings"] = bindings
    events = simulator_events(config)
    config["expected"] = reference_metrics(events)
    checksum = digest(config)
    run = {"_id": run_id, "run_id": run_id, "config": config, "config_sha256": checksum, "created_at": "2026-10-20T00:00:00.000Z",
           "committed_batch_id": 1, "worker_hosts": ["spark-worker-1", "spark-worker-2"],
           "consumer": {"status": "completed"}, "producer": {"status": "completed", "last_seq": 8,
            "end_offsets": {"0": 3, "1": 3, "2": 3}, "started_at": "2026-10-20T00:00:00.000Z",
            "updated_at": "2026-10-20T00:00:30.000Z"}}
    db["stream_runs"].insert_one(run)
    features = reference(events, "2015-09-18T00:02:02.001Z")
    predictions = []
    for i, f in enumerate(features):
        score = .8 if i < 2 else .2
        predictions.append({**f, "cluster_id": i % 2, "purchase_score": score, "original_predicted_label": int(score >= .5),
            "predicted_label": int(score >= .7), "original_threshold": .5, "threshold": .7, "feature_version": "features-v1",
            **{k: bindings[k] for k in ("segment_model_version", "purchase_model_version", "policy_version")},
            "scored_at": "2026-10-20T00:00:30.000Z"})
    quality = {"status": "ready", "as_of": features[0]["as_of"], "scored_at": predictions[0]["scored_at"],
               "carried_forward": False, "feature_fingerprint": fingerprint(features),
               "fingerprint": prediction_fingerprint(predictions, bindings, features[0]["as_of"], predictions[0]["scored_at"])}
    for batch_id in (1, 2):  # Batch 2 is deliberately prepared ahead of the pointer.
        key = f"{run_id}:{batch_id}"
        db["stream_batches"].insert_one({"_id": key, "run_id": run_id, "batch_id": batch_id, "ml": quality,
            "counts": {"accepted": 9, "duplicate": 0, "invalid": 0, "late": 0, "foreign": 0},
            "batch_seconds": 12.3, "latency_seconds": [15., 20.], "processed_offsets": {"0": 3, "1": 3, "2": 3}})
        db["stream_ml_snapshots"].insert_one({"_id": key, "status": "published", "config_sha256": checksum, "quality": quality})
        db["kpi_overview"].insert_one({"_id": key, "run_id": run_id, "batch_id": batch_id, **config["expected"]["overview"]})
        for row in config["expected"]["minutes"]:
            db["kpi_minute"].insert_one({"_id": key+":"+row["window_start"], "run_id": run_id, "batch_id": batch_id, **row})
        db["funnel_overview"].insert_one({"_id": key, "run_id": run_id, "batch_id": batch_id,
            "view_sessions": 2, "cart_sessions": 1, "purchase_sessions": 1, "event_count": 9, "product_count": 1})
        db["funnel_products"].insert_one({"_id": key+":10", "snapshot_id": key, "item_id": "10", "view_sessions": 1})
        for row in predictions:
            db["stream_ml_predictions"].insert_one({"_id": key+":"+row["visitor_id"], "run_id": run_id, "batch_id": batch_id, **row})
    return config
