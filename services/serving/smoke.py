"""Read-only HTTP/Mongo reconciliation of one completed source run."""
import argparse
import json
import os
import time
from urllib.parse import urlencode
from urllib.request import urlopen

from ecommerce_streaming_ml.contracts import prediction_fingerprint
from .contracts import now, percentile95


def check(condition, message):
    if not condition: raise RuntimeError(message)


def run(run_id):
    from pymongo import MongoClient
    def request(endpoint, **extra):
        with urlopen("http://127.0.0.1:"+os.getenv("API_HOST_PORT","8000")+"/api/v1/" + endpoint + "?" + urlencode({"run_id": run_id, **extra}), timeout=5) as response:
            return json.load(response)

    with MongoClient(os.getenv("MONGO_HOST","127.0.0.1"),port=int(os.getenv("MONGO_PORT","27018")),
                     username=os.environ["MONGO_USERNAME"], password=os.environ["MONGO_PASSWORD"],
                     authSource=os.environ["MONGO_DATABASE"], serverSelectionTimeoutMS=3000) as mongo:
        db = mongo[os.environ["MONGO_DATABASE"]]
        registry = db.stream_runs.find_one({"_id": run_id})
        check(registry is not None, "Run not found")
        check(registry["consumer"]["status"] == "completed", "Wait for the consumer to complete before final smoke")
        config = registry["config"]
        pipeline = request("pipeline")
        batch_id = pipeline["batch_id"]
        check(batch_id == registry["committed_batch_id"], "HTTP/Mongo committed pointers differ")
        check(pipeline["data"]["backlog"] == 0, "Acknowledged backlog has not drained")
        values = {name: request(name, batch_id=batch_id) for name in ("overview", "timeseries", "funnel", "segments", "models")}
        check(all(v["batch_id"] == batch_id for v in values.values()), "Mixed HTTP snapshots")
        check(values["overview"]["data"] == config["expected"]["overview"], "API KPI differs from immutable source reference")
        check(values["timeseries"]["data"] == config["expected"]["minutes"], "API minute KPI differs from source")
        check(values["models"]["data"]["bindings"] == config["bindings"], "API changed pinned models/policy")
        check(values["funnel"]["data"]["overview"] == config["expected_behavior"], "HTTP funnel differs from source reference")
        predictions, cursor = [], None
        while True:
            page = request("predictions", batch_id=batch_id, limit=200, **({"after": cursor} if cursor else {}))
            predictions.extend(page["data"])
            check(len(predictions) <= 10000, "HTTP predictions exceed bounded demo")
            cursor = page["next_cursor"]
            if cursor is None: break
        quality = pipeline["data"]["scoring"]
        actual = prediction_fingerprint(sorted(predictions, key=lambda r: r["visitor_id"]), config["bindings"],
                                        quality["as_of"], quality["scored_at"])
        check(actual == quality["fingerprint"], "HTTP prediction fingerprint differs from committed ML quality")
        check({str(r["cluster_id"]): r["count"] for r in values["segments"]["data"]} == actual["clusters"], "HTTP clusters differ")
        visitor_id = "000099000000003" if config["demo_source"] == "simulator" else predictions[0]["visitor_id"]
        check(request("visitors/"+visitor_id,batch_id=batch_id)["data"] ==
              next(r for r in predictions if r["visitor_id"] == visitor_id), "HTTP visitor differs from ranked prediction")
        latencies = []
        for _ in range(100):
            started = time.monotonic()
            check(request("overview",batch_id=batch_id)["data"] == values["overview"]["data"], "Snapshot changed during benchmark")
            latencies.append(time.monotonic()-started)
        p95 = percentile95(latencies)
        return {"passed": p95 <= 1, "run_id": run_id, "batch_id": batch_id,
            "processed_events": values["overview"]["data"]["processed_events"], "prediction_count": len(predictions),
            "fingerprint": actual, "batch_reconciled": True, "api_response_p95_seconds": round(p95,6),
            "api_performance_target_met": p95 <= 1, "benchmark_requests": 100, "completed_at": now()}


if __name__ == "__main__":
    parser=argparse.ArgumentParser()
    parser.add_argument("--run-id", required=True)
    args=parser.parse_args()
    if not args.run_id: parser.error("RUN_ID is required")
    result=run(args.run_id)
    print("SERVING_RESULT="+json.dumps(result,sort_keys=True))
    raise SystemExit(not result["passed"])
