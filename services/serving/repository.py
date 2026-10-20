"""Bounded Mongo reads pinned to the existing atomic committed pointer."""
import math
import re

from ecommerce_ml.contracts import FEATURES
from ecommerce_streaming.contracts import digest
from ecommerce_streaming_ml.contracts import normalized_prediction
from .contracts import ServingError, cursor_decode, cursor_encode, now, percentile95

RUN_QUERY = {"config.pipeline_version": "stream-ml-v1", "config.base": "/ecommerce"}
TIMEOUT_MS = 2000


def consumer_state(value):
    result = {k: value[k] for k in ("status", "updated_at", "startup_seconds", "error_type") if k in value}
    if value.get("reason"):
        result["reason"] = "Consumer gặp lỗi; xem log của run để chẩn đoán"
    return result


def unavailable(message="Snapshot đã công bố thiếu hoặc sai dữ liệu"):
    raise ServingError(503, "SNAPSHOT_INCOMPLETE", message)


class Repository:
    def __init__(self, database):
        self.db = database

    def one(self, name, query):
        return self.db[name].find_one(query, {"_id": 0}, max_time_ms=TIMEOUT_MS)

    def rows(self, name, query, sort, limit=10001):
        return self.db[name].find(query, {"_id": 0}).sort(sort).limit(limit).max_time_ms(TIMEOUT_MS)

    def runs(self, limit, after=None):
        query = dict(RUN_QUERY)
        if after:
            position = cursor_decode(after)
            if set(position) != {"created_at", "run_id", "kind"} or position["kind"] != "runs":
                raise ServingError(422, "INVALID_CURSOR", "Cursor không thuộc danh sách run")
            if not all(isinstance(position[k], str) for k in ("created_at", "run_id")):
                raise ServingError(422, "INVALID_CURSOR", "Cursor không hợp lệ")
            query["$or"] = [{"created_at": {"$lt": position["created_at"]}},
                {"created_at": position["created_at"], "run_id": {"$gt": position["run_id"]}}]
        values = list(self.rows("stream_runs", query, [("created_at", -1), ("run_id", 1)], limit + 1))
        page = values[:limit]
        data = [{"run_id": v["run_id"], "demo_source": v["config"]["demo_source"],
            "start": v["config"]["start"], "end": v["config"]["end"], "created_at": v["created_at"],
            "consumer": consumer_state(v["consumer"]), "producer_status": v["producer"]["status"],
            "committed_batch_id": v["committed_batch_id"]} for v in page]
        following = cursor_encode({"kind": "runs", "created_at": page[-1]["created_at"],
                                   "run_id": page[-1]["run_id"]}) if len(values) > limit else None
        return {"data": data, "next_cursor": following, "observed_at": now()}

    def context(self, run_id, batch_id=None):
        run = self.one("stream_runs", {"_id": run_id, **RUN_QUERY})
        if run is None:
            raise ServingError(404, "RUN_NOT_FOUND", "Không tìm thấy run ML nghiệp vụ")
        config = run["config"]
        if run.get("config_sha256") != digest(config) or not 0 <= config["event_count"] <= 10000:
            unavailable("Cấu hình run không hợp lệ")
        current = run["committed_batch_id"]
        selected = current if batch_id is None else batch_id
        if selected > current:
            raise ServingError(409, "SNAPSHOT_NOT_COMMITTED", "Batch chưa được công bố")
        batch = overview = funnel = quality = None
        if selected >= 0:
            key = f"{run_id}:{selected}"
            batch = self.one("stream_batches", {"_id": key})
            overview = self.one("kpi_overview", {"_id": key})
            funnel = self.one("funnel_overview", {"_id": key})
            registry = self.one("stream_ml_snapshots", {"_id": key})
            if any(v is None for v in (batch, overview, funnel, registry)):
                unavailable()
            quality = batch.get("ml")
            if (quality is None or registry.get("status") != "published" or registry.get("quality") != quality
                    or registry.get("config_sha256") != run["config_sha256"]):
                unavailable()
            count = self.db["stream_ml_predictions"].count_documents({"run_id": run_id, "batch_id": selected},
                                                                 maxTimeMS=TIMEOUT_MS)
            if count != quality["fingerprint"]["rows"] or count != overview["visitors"]:
                unavailable("Số prediction không khớp snapshot KPI/ML")
            product_count = self.db["funnel_products"].count_documents({"snapshot_id": key}, maxTimeMS=TIMEOUT_MS)
            if product_count != funnel["product_count"]:
                unavailable("Số sản phẩm không khớp snapshot funnel")
        failed = run["consumer"]["status"] in ("failed", "interrupted")
        status = quality["status"] if quality else ("not_ready" if failed else "waiting_for_events")
        return {"run": run, "batch_id": selected, "batch": batch, "overview": overview,
                "funnel": funnel, "quality": quality, "status": status}

    def response(self, ctx, data):
        return {"run_id": ctx["run"]["run_id"], "batch_id": ctx["batch_id"],
                "as_of_event_time": ctx["quality"]["as_of"] if ctx["quality"] else None,
                "observed_at": now(), "status": ctx["status"], "data": data}

    def overview(self, ctx):
        return self.response(ctx, self.clean(ctx["overview"]))

    @staticmethod
    def clean(value):
        if value is None:
            return None
        return {k: v for k, v in value.items() if k not in ("_id", "run_id", "batch_id", "snapshot_id")}

    def timeseries(self, ctx, start=None, end=None):
        query = {"run_id": ctx["run"]["run_id"], "batch_id": ctx["batch_id"]}
        bounds = {}
        if start: bounds["$gte"] = start
        if end: bounds["$lt"] = end
        if bounds: query["window_start"] = bounds
        # A 24-hour interval with non-minute-aligned edges can touch 1,441 buckets.
        values = [self.clean(v) for v in self.rows("kpi_minute", query, [("window_start", 1)], 1442)] if ctx["batch"] else None
        if values is not None and len(values) > 1441:
            unavailable("Timeseries vượt cửa sổ demo 24 giờ")
        return self.response(ctx, values)

    def funnel(self, ctx, limit=20):
        if not ctx["batch"]:
            return self.response(ctx, None)
        key = f"{ctx['run']['run_id']}:{ctx['batch_id']}"
        products = list(self.rows("funnel_products", {"snapshot_id": key},
                                  [("view_sessions", -1), ("item_id", 1)], limit))
        return self.response(ctx, {"overview": self.clean(ctx["funnel"]),
            "products": [self.clean(p) for p in products],
            "is_final": ctx["run"]["consumer"]["status"] == "completed" and
                        ctx["batch_id"] == ctx["run"]["committed_batch_id"]})

    def validate_prediction(self, ctx, row):
        try:
            return normalized_prediction(row, ctx["run"]["config"]["bindings"],
                                         ctx["quality"]["as_of"], ctx["quality"]["scored_at"])
        except (ValueError, KeyError, TypeError):
            unavailable("Prediction không khớp feature/model/policy đã khóa")

    def predictions(self, ctx, limit, after=None, cluster_id=None, predicted_label=None):
        query = {"run_id": ctx["run"]["run_id"], "batch_id": ctx["batch_id"]}
        if cluster_id is not None:
            if cluster_id >= ctx["run"]["config"]["bindings"]["selected_k"]:
                raise ServingError(422, "INVALID_CLUSTER", "Cluster không thuộc model của run")
            query["cluster_id"] = cluster_id
        if predicted_label is not None: query["predicted_label"] = predicted_label
        selection = {"run_id": ctx["run"]["run_id"], "batch_id": ctx["batch_id"],
                     "cluster_id": cluster_id, "predicted_label": predicted_label}
        if after:
            position = cursor_decode(after)
            if (position.get("selection") != selection or not isinstance(position.get("visitor_id"), str)
                    or not re.fullmatch(r"[0-9]+", position["visitor_id"])
                    or type(position.get("score")) not in (int, float)
                    or not math.isfinite(position["score"]) or not 0 <= position["score"] <= 1):
                raise ServingError(422, "INVALID_CURSOR", "Cursor không thuộc run, snapshot hoặc bộ lọc")
            query["$or"] = [{"purchase_score": {"$lt": position["score"]}},
                {"purchase_score": position["score"], "visitor_id": {"$gt": position["visitor_id"]}}]
        values = list(self.rows("stream_ml_predictions", query,
                      [("purchase_score", -1), ("visitor_id", 1)], limit + 1)) if ctx["quality"] else []
        page = [self.validate_prediction(ctx, r) for r in values[:limit]]
        following = cursor_encode({"selection": selection, "score": page[-1]["purchase_score"],
                    "visitor_id": page[-1]["visitor_id"]}) if len(values) > limit else None
        return {**self.response(ctx, page if ctx["quality"] else None), "next_cursor": following}

    def visitor(self, ctx, visitor_id):
        if not ctx["quality"]:
            return self.response(ctx, None)
        row = self.one("stream_ml_predictions", {"_id": f"{ctx['run']['run_id']}:{ctx['batch_id']}:{visitor_id}"})
        if row is None:
            raise ServingError(404, "VISITOR_NOT_FOUND", "Visitor chưa thuộc snapshot của run")
        return self.response(ctx, self.validate_prediction(ctx, row))

    def segments(self, ctx):
        if not ctx["quality"] or ctx["status"] != "ready":
            return self.response(ctx, None)
        group = {"_id": "$cluster_id", "count": {"$sum": 1},
                 **{k: {"$avg": "$" + k} for k in FEATURES}}
        values = {v["_id"]: v for v in self.db["stream_ml_predictions"].aggregate([
            {"$match": {"run_id": ctx["run"]["run_id"], "batch_id": ctx["batch_id"]}},
            {"$group": group}], maxTimeMS=TIMEOUT_MS, allowDiskUse=False)}
        expected = ctx["quality"]["fingerprint"]["clusters"]
        if any(values.get(int(k), {}).get("count", 0) != n for k, n in expected.items()) or set(values) - {int(k) for k in expected}:
            unavailable("Số visitor theo cluster không khớp snapshot ML")
        total = ctx["quality"]["fingerprint"]["rows"]
        return self.response(ctx, [{"cluster_id": int(k), "count": n, "share": n / total if total else 0,
            "raw_feature_means": {f: values[int(k)][f] for f in FEATURES} if n else None}
            for k, n in sorted(expected.items(), key=lambda p: int(p[0]))])

    def models(self, ctx):
        bindings = ctx["run"]["config"]["bindings"]
        data = {"bindings": bindings}
        for label, collection, key in [("segments", "segment_models", "segment_model_version"),
                                      ("purchase", "purchase_models", "purchase_model_version"),
                                      ("policy", "purchase_policies", "policy_version")]:
            row = self.one(collection, {"_id": bindings[key], "status": "published"})
            if row is None or digest(row["identity"]) != bindings[key]:
                unavailable("Model/policy đã khóa chưa sẵn sàng hoặc sai identity")
            data[label] = {"version": bindings[key], "report": row["report"]}
        return self.response(ctx, data)

    def pipeline(self, ctx):
        run, batch = ctx["run"], ctx["batch"]
        history = list(self.rows("stream_batches", {"run_id": run["run_id"], "batch_id": {"$lte": ctx["batch_id"]}},
                                [("batch_id", 1)])) if batch else []
        if len(history) > 10000: unavailable("Batch history vượt giới hạn demo")
        totals = {k: sum(v["counts"][k] for v in history) for k in ("accepted", "duplicate", "invalid", "late", "foreign")}
        samples = [x for v in history[1:] for x in v.get("latency_seconds", [])]
        processed = batch["processed_offsets"] if batch else run["config"]["initial_offsets"]
        producer = run["producer"]
        backlog = sum(max(0, n - processed.get(p, 0)) for p, n in producer["end_offsets"].items())
        rate = None
        if producer.get("started_at") and producer.get("updated_at"):
            from datetime import datetime
            elapsed = (datetime.fromisoformat(producer["updated_at"].replace("Z", "+00:00")) -
                       datetime.fromisoformat(producer["started_at"].replace("Z", "+00:00"))).total_seconds()
            if elapsed > 0: rate = (producer["last_seq"] + 1) / elapsed
        return self.response(ctx, {"producer": producer, "consumer": consumer_state(run["consumer"]), "backlog": backlog,
            "backlog_basis": "acknowledged_end_offsets_minus_committed_processed_offsets",
            "processed_offsets": processed, "counts": totals if batch else None,
            "acknowledged_rate_eps": rate, "batch_seconds": batch["batch_seconds"] if batch else None,
            "batch_history": [{"batch_id": v["batch_id"], "batch_seconds": v["batch_seconds"],
                               "counts": v["counts"]} for v in history],
            "steady_state_latency_p95_seconds": percentile95(samples), "latency_samples": len(samples),
            "worker_hosts": run.get("worker_hosts", []), "scoring": ctx["quality"]})
