"""Checked temporal labels, resumable daily snapshots and a final composite index."""
from datetime import timedelta

from ecommerce_analytics.hdfs import Hdfs
from ecommerce_analytics.snapshots import materialize, write_json
from ecommerce_ml.features import checked, feature_frame, validate_frame
from .contracts import (AUDIT, BASELINE_COUNTS, DATA_VERSION, FEATURES, FEATURE_VERSION, LABEL, chunks,
                        composite, coverage, default_schedule, digest, identifier, instant, iso,
                        row_fingerprint, validate_schedule)

KNOWN_SOURCE = "aef4779c2d9008f4a51f3d12f229c9dbb6718288ea4c380dbbebaa6b2eb24d2a"


def fire(fault, stage):
    if fault:
        fault(stage)


def source(spark, storage, request, expected_sha=None):
    from ecommerce_analytics.ingestion import silver_schema
    root = request["silver_path"]
    manifest = checked(storage, root)
    if any(manifest["identity"].get(key) != value for key, value in {
            "kind": "silver", "source": "retailrocket", "dataset_version": request["dataset_version"],
            "schema_version": 1, "transform_version": "events-v1"}.items()):
        raise ValueError("Purchase source differs from historical Silver")
    checksum = storage.sha256(root + "/manifest.json")
    if expected_sha is not None and checksum != expected_sha:
        raise ValueError("Purchase Silver manifest changed")
    frame = spark.read.parquet(root + "/events")
    if {f.name: f.dataType.simpleString() for f in frame.schema} != {
            f.name: f.dataType.simpleString() for f in silver_schema()}:
        raise ValueError("Purchase Silver schema differs from events-v1")
    return frame, manifest, checksum


def prepare(spark, request):
    storage = Hdfs(spark)
    _, manifest, checksum = source(spark, storage, request)
    schedule = validate_schedule(request.get("schedule", default_schedule()))
    quality = manifest["metadata"]["quality"]
    for entry in schedule:
        coverage(entry["as_of"], quality["min_event_time"], quality["max_event_time"])
        if instant(entry["as_of"]) + timedelta(days=1) > instant(quality["max_event_time"]) + timedelta(milliseconds=1):
            raise ValueError("Purchase label window is not fully observed")
    identity = {"kind": "purchase-data", "data_version": DATA_VERSION, "feature_version": FEATURE_VERSION,
        "label": LABEL, "lookback_days": 7, "label_hours": 24, "schedule": schedule,
        "dataset_version": request["dataset_version"], "silver_path": request["silver_path"],
        "silver_manifest_sha256": checksum, "code_sha256": request["code_sha256"]}
    dataset_id = digest(identity)
    config = {"purchase_dataset_id": dataset_id, "identity": identity,
              "root": request["base"].rstrip("/") + "/gold/purchase-data/" + dataset_id,
              "base": request["base"].rstrip("/")}
    return {"passed": True, "config": config, "chunks": chunks(schedule), "days": len(schedule)}


def config_check(config):
    identifier(config["purchase_dataset_id"])
    identity = config["identity"]
    if digest(identity) != config["purchase_dataset_id"] or identity.get("data_version") != DATA_VERSION or identity.get("feature_version") != FEATURE_VERSION:
        raise ValueError("Purchase dataset identity/version mismatch")
    validate_schedule(identity["schedule"])


def day_path(config, entry):
    return config["root"] + "/days/" + entry["as_of"][:10]


def day_identity(config, entry):
    return {"kind": "purchase-day", "purchase_dataset_id": config["purchase_dataset_id"], **entry}


def labeled_frame(events, as_of):
    from pyspark.sql import functions as F
    frame, _ = feature_frame(events, as_of)
    start = instant(as_of)
    buyers = (events.where((F.col("event_date") == F.lit(start.date())) &
        (F.col("event_time") >= F.lit(start)) & (F.col("event_time") < F.lit(start + timedelta(days=1))) &
        (F.col("event_type") == "purchase") & F.col("transaction_id").isNotNull())
        .select("visitor_id").distinct().withColumn(LABEL, F.lit(1).cast("int")))
    return frame.join(buyers, "visitor_id", "left").fillna({LABEL: 0}).select(*AUDIT, *FEATURES, LABEL)


def ordered_rows(frame):
    for row in frame.orderBy("as_of", "visitor_id").toLocalIterator():
        yield row.asDict()


def day_quality(frame, as_of):
    from pyspark.sql import functions as F
    validate_frame(frame.select(*AUDIT, *FEATURES), as_of)
    if frame.schema[LABEL].dataType.simpleString() != "int" or frame.where(
            F.col(LABEL).isNull() | ~F.col(LABEL).isin(0, 1)).limit(1).count():
        raise ValueError("Purchase label schema/values are invalid")
    return row_fingerprint(ordered_rows(frame), labeled=True)


def load_day(spark, storage, config, entry, *, validate=True):
    root = day_path(config, entry)
    manifest = checked(storage, root, day_identity(config, entry))
    frame = spark.read.parquet(root + "/samples")
    if storage.read_json(root + "/quality.json") != manifest["metadata"]:
        raise ValueError("Daily purchase quality report differs from manifest")
    if validate and day_quality(frame, entry["as_of"]) != manifest["metadata"]["fingerprint"]:
        raise ValueError("Daily purchase fingerprint differs from saved data")
    return frame, manifest


def build_chunk(spark, config, entries, *, fault=None):
    from pyspark import StorageLevel
    from pyspark.sql import functions as F
    config_check(config)
    if entries not in chunks(config["identity"]["schedule"]):
        raise ValueError("Chunk differs from the immutable dataset schedule")
    storage = Hdfs(spark)
    events, _, _ = source(spark, storage, config["identity"], config["identity"]["silver_manifest_sha256"])
    left, right = instant(entries[0]["as_of"]) - timedelta(days=7), instant(entries[-1]["as_of"]) + timedelta(days=1)
    events = events.where((F.col("event_date") >= F.lit(left.date())) & (F.col("event_date") < F.lit(right.date())))
    events = events.persist(StorageLevel.DISK_ONLY)
    results = []
    try:
        for entry in entries:
            def build(staging):
                frame = labeled_frame(events, entry["as_of"]).persist(StorageLevel.DISK_ONLY)
                try:
                    report = {"passed": True, **entry, "fingerprint": day_quality(frame, entry["as_of"])}
                    frame.write.mode("errorifexists").parquet(staging + "/samples")
                    write_json(storage, staging + "/quality.json", report)
                    return report
                finally:
                    frame.unpersist()
            root = day_path(config, entry)
            manifest, reused = materialize(storage, root, day_identity(config, entry), build,
                before_publish=lambda: fire(fault, "before_purchase_day"))
            fire(fault, "after_purchase_day")
            load_day(spark, storage, config, entry)
            result = {**entry, "path": root, "manifest_sha256": storage.sha256(root + "/manifest.json"),
                      "fingerprint": manifest["metadata"]["fingerprint"], "reused": reused}
            results.append(result)
            print("PURCHASE_DAY=" + __import__("json").dumps(result, sort_keys=True), flush=True)
    finally:
        events.unpersist()
    return {"passed": True, "days": results, "reused": all(value["reused"] for value in results)}


def finalize(spark, config, *, fault=None):
    config_check(config)
    storage = Hdfs(spark)
    entries = []
    for entry in config["identity"]["schedule"]:
        _, manifest = load_day(spark, storage, config, entry, validate=False)
        entries.append({**entry, "path": day_path(config, entry), "fingerprint": manifest["metadata"]["fingerprint"],
                        "manifest_sha256": storage.sha256(day_path(config, entry) + "/manifest.json")})
    totals = {name: composite([entry for entry in entries if entry["split"] == name]) for name in ("train", "validation", "test")}
    if any(not 0 < value["positives"] < value["rows"] for value in totals.values()):
        raise ValueError("Each complete temporal split requires both classes")
    if config["identity"]["dataset_version"] == KNOWN_SOURCE and config["identity"]["schedule"] == default_schedule():
        if any(any(totals[name][key] != count for key, count in expected.items()) for name, expected in BASELINE_COUNTS.items()):
            raise ValueError("Full-source purchase cohorts differ from the independent CSV baseline")
    identity = {"kind": "purchase-index", "config": config}
    report = {"passed": True, "days": entries, "splits": totals, "feature_order": list(FEATURES)}
    def build(staging):
        write_json(storage, staging + "/index.json", report)
        return report
    manifest, reused = materialize(storage, config["root"] + "/index", identity, build,
        before_publish=lambda: fire(fault, "before_purchase_index"))
    if manifest["metadata"] != report:
        raise ValueError("Purchase dataset index changed")
    return {"passed": True, "purchase_dataset_id": config["purchase_dataset_id"], "config": config,
        "data_path": config["root"] + "/index", "manifest": manifest,
        "manifest_sha256": storage.sha256(config["root"] + "/index/manifest.json"),
        "splits": totals, "reused": reused, "minimum_block_replicas": 2}


def load_dataset(spark, storage, report, code_sha256):
    config = report["config"]
    config_check(config)
    if config["identity"]["code_sha256"] != code_sha256:
        raise ValueError("Prediction code changed; rebuild the purchase dataset first")
    root = report["data_path"]
    if root != config["root"] + "/index" or report["purchase_dataset_id"] != config["purchase_dataset_id"]:
        raise ValueError("Purchase index path/ID mismatch")
    manifest = checked(storage, root, {"kind": "purchase-index", "config": config})
    if storage.sha256(root + "/manifest.json") != report["manifest_sha256"] or storage.read_json(root + "/index.json") != manifest["metadata"]:
        raise ValueError("Purchase index checksum/metadata mismatch")
    entries = manifest["metadata"]["days"]
    if [{"split": value["split"], "as_of": value["as_of"]} for value in entries] != config["identity"]["schedule"]:
        raise ValueError("Purchase index does not cover its exact schedule")
    for entry in entries:
        expected = {"split": entry["split"], "as_of": entry["as_of"]}
        _, day = load_day(spark, storage, config, expected, validate=False)
        if storage.sha256(entry["path"] + "/manifest.json") != entry["manifest_sha256"] or day["metadata"]["fingerprint"] != entry["fingerprint"]:
            raise ValueError("Purchase index child snapshot changed")
    return config, manifest["metadata"]


def split_frame(spark, index, name):
    return spark.read.parquet(*[entry["path"] + "/samples" for entry in index["days"] if entry["split"] == name])
