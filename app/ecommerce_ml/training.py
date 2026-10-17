"""Sequential, disk-cached Spark K-Means selection and portable model verification."""
import math
import json
import time

from ecommerce_analytics.hdfs import Hdfs
from ecommerce_analytics.snapshots import materialize, write_json
from .contracts import FEATURES, COUNTS, MODEL_VERSION, PARAMS, choose, digest, fingerprint, identifier
from .features import checked, load_features, rows, validate_frame
from .native import NativeML


def preprocessor(spark):
    native = NativeML(spark)
    expressions = [f"LOG1P(`{key}`) AS `ml_{key}`" for key in COUNTS]
    columns = ["ml_" + key if key in COUNTS else key for key in FEATURES]
    return native.pipeline([
        native.ml.feature.SQLTransformer().setStatement("SELECT *, " + ", ".join(expressions) + " FROM __THIS__"),
        native.ml.feature.VectorAssembler().setInputCols(native.strings(columns)).setOutputCol(
            "unscaled_features").setHandleInvalid("error"),
        native.ml.feature.StandardScaler().setInputCol("unscaled_features").setOutputCol(
            "scaled_features").setWithMean(True).setWithStd(True)])


def profiles(frame, k):
    from pyspark.sql import functions as F
    result = []
    for row in frame.groupBy("cluster_id").agg(F.count("*").alias("count"),
            *[F.mean(key).alias(key) for key in FEATURES]).orderBy("cluster_id").collect():
        value = row.asDict()
        result.append({"cluster_id": value["cluster_id"], "label": "Cụm " + str(value["cluster_id"]),
                       "count": value["count"], "raw_feature_means": {key: value[key] for key in FEATURES}})
    if [value["cluster_id"] for value in result] != list(range(k)):
        raise ValueError("Assignments have missing or unexpected clusters")
    total = sum(value["count"] for value in result)
    for value in result:
        value["share"] = value["count"] / total
        if not all(math.isfinite(mean) and mean >= 0 for mean in value["raw_feature_means"].values()):
            raise ValueError("Invalid raw cluster profile")
    return result


def assignment_rows(frame):
    return rows(frame.select("visitor_id", "as_of", "last_event_time", *FEATURES, "cluster_id"))


def fit(spark, features, storage, staging):
    from pyspark import StorageLevel
    native = NativeML(spark)
    transformer = preprocessor(spark).fit(features._jdf)
    scaled = native.transform(transformer, features).persist(StorageLevel.DISK_ONLY)
    try:
        count = scaled.count()
        distinct = scaled.select(native.vector_array(scaled).alias("vector")).distinct().count()
        evaluator = (native.ml.evaluation.ClusteringEvaluator().setFeaturesCol("scaled_features")
            .setPredictionCol("cluster_id").setMetricName("silhouette").setDistanceMeasure(PARAMS["silhouette_distance"]))
        candidates, models = [], {}
        for k in PARAMS["candidates"]:
            started = time.monotonic()
            metric = {"k": k, "valid": False}
            if count <= k or distinct < k:
                metric["reason"] = "insufficient rows or distinct scaled vectors"
            else:
                estimator = native.ml.clustering.KMeans().setK(k).setFeaturesCol("scaled_features").setPredictionCol("cluster_id")
                for key in ("seed", "initMode", "initSteps", "maxIter", "tol", "distanceMeasure", "solver"):
                    estimator = getattr(estimator, "set" + key[0].upper() + key[1:])(PARAMS[key])
                model = estimator.fit(scaled._jdf)
                summary = model.summary()
                sizes = [int(value) for value in summary.clusterSizes()]
                cost = float(summary.trainingCost())
                metric.update(cluster_sizes=sizes, training_cost=cost, iterations=int(summary.numIter()))
                if len(sizes) != k or min(sizes) == 0:
                    metric["reason"] = "empty cluster"
                else:
                    silhouette = float(evaluator.evaluate(model.transform(scaled._jdf)))
                    if not math.isfinite(silhouette) or not -1 <= silhouette <= 1 or not math.isfinite(cost):
                        metric["reason"] = "nonfinite or invalid metric"
                    else:
                        metric.update(valid=True, silhouette=silhouette)
                        models[k] = model
                # Invalid floating values cannot be written to strict JSON reports.
                if not math.isfinite(cost):
                    metric["training_cost"] = None
            metric["duration_seconds"] = round(time.monotonic() - started, 3)
            candidates.append(metric)
            print("ML_CANDIDATE=" + json.dumps(metric, sort_keys=True, allow_nan=False), flush=True)
        selected = choose(candidates)
        winner = models[selected["k"]]
        # All stages are fitted Transformers: Pipeline.fit only assembles this model.
        pipeline = native.pipeline([*transformer.stages(), winner]).fit(features._jdf)
        assigned = native.transform(winner, scaled).select("visitor_id", "as_of", "last_event_time", *FEATURES, "cluster_id")
        expected = fingerprint(assignment_rows(assigned), assignments=True)
        summaries = profiles(assigned, selected["k"])
        centers = [[float(value) for value in center.toArray()] for center in winner.clusterCenters()]
        if any(not math.isfinite(value) for center in centers for value in center):
            raise ValueError("Nonfinite standardized centroid")
        pipeline.write().save(staging + "/pipeline")
        reloaded = native.load(staging + "/pipeline")
        if fingerprint(assignment_rows(native.transform(reloaded, features)), assignments=True) != expected:
            raise ValueError("Reloaded model assignments differ before publication")
        assigned.write.mode("errorifexists").parquet(staging + "/assignments")
        report = {"passed": True, "selected_k": selected["k"], "candidates": candidates,
                  "assignment_fingerprint": expected, "profiles": summaries,
                  "standardized_centroids": centers, "feature_order": list(FEATURES),
                  "rows": count, "distinct_scaled_vectors": distinct, "reload_verified": True}
        write_json(storage, staging + "/quality.json", report)
        return report
    finally:
        scaled.unpersist()


def verify_model(spark, storage, root, identity, features):
    native = NativeML(spark)
    manifest = checked(storage, root, identity)
    report = manifest["metadata"]
    if storage.read_json(root + "/quality.json") != report or choose(report["candidates"])["k"] != report["selected_k"]:
        raise ValueError("Model report/selection differs from manifest")
    assigned = spark.read.parquet(root + "/assignments")
    source = validate_frame(features, identity["as_of"])
    actual = validate_frame(assigned.select("visitor_id", "as_of", "last_event_time", *FEATURES), identity["as_of"])
    if source != actual:
        raise ValueError("Assignments no longer cover the exact feature cohort")
    if fingerprint(assignment_rows(assigned), assignments=True) != report["assignment_fingerprint"]:
        raise ValueError("Saved assignment fingerprint mismatch")
    actual_profiles = profiles(assigned, report["selected_k"])
    expected_profiles = report["profiles"]
    if len(actual_profiles) != len(expected_profiles) or any(
        any(actual[key] != expected[key] for key in ("cluster_id", "label", "count", "share")) or
        any(not math.isclose(actual["raw_feature_means"][key], expected["raw_feature_means"][key],
                             rel_tol=1e-12, abs_tol=1e-12) for key in FEATURES)
        for actual, expected in zip(actual_profiles, expected_profiles)):
        raise ValueError("Saved profiles differ from assignments")
    model = native.load(root + "/pipeline")
    if fingerprint(assignment_rows(native.transform(model, features)), assignments=True) != report["assignment_fingerprint"]:
        raise ValueError("Reloaded model assignments differ from saved assignments")
    return manifest, assigned


def train_segments(spark, store, request, *, fault=None):
    storage = Hdfs(spark)
    snapshot_id = identifier(request["feature_snapshot_id"])
    features, source = load_features(spark, storage, request["feature_path"], snapshot_id=snapshot_id)
    if request["feature_manifest_sha256"] != storage.sha256(request["feature_path"] + "/manifest.json"):
        raise ValueError("Feature manifest changed since the host selected it")
    if source["identity"]["code_sha256"] != request["code_sha256"]:
        raise ValueError("ML code changed; build a feature snapshot with the current code first")
    identity = {"kind": "segments", "model_version": MODEL_VERSION, "feature_snapshot_id": snapshot_id,
        "feature_manifest_sha256": request["feature_manifest_sha256"], "feature_path": request["feature_path"],
        "code_sha256": request["code_sha256"], "spark_version": spark.version,
        "as_of": source["identity"]["as_of"], "parameters": PARAMS}
    model_version = digest(identity)
    destination = request["base"].rstrip("/") + "/models/segments/" + model_version
    store.prepare(model_version, identity, destination, storage.exists(destination))
    manifest, reused = materialize(storage, destination, identity,
        lambda staging: fit(spark, features, storage, staging),
        before_publish=(lambda: fault("before_model_gold")) if fault else None)
    if fault:
        fault("after_model_gold")
    manifest, assigned = verify_model(spark, storage, destination, identity, features)
    store.publish(model_version, identity, destination, manifest["metadata"], assignment_rows(assigned), fault=fault)
    return {"passed": True, "model_version": model_version, "model_path": destination,
        "feature_snapshot_id": snapshot_id, "manifest": manifest, "quality": manifest["metadata"],
        "reused": reused, "minimum_block_replicas": 2}
