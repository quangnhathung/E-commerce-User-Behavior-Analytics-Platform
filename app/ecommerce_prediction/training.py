"""Sequential native Spark RF candidates, validation-only selection and locked test."""
import json
import math
import time

from ecommerce_analytics.hdfs import Hdfs
from ecommerce_analytics.snapshots import materialize, write_json
from ecommerce_ml.features import checked
from ecommerce_ml.native import NativeML
from .contracts import (AUDIT, CANDIDATES, FEATURES, LABEL, METRIC_VERSION, MODEL_VERSION, RF_PARAMS,
                        SAMPLE_VERSION, THRESHOLDS, choose_candidate, choose_threshold, composite,
                        digest, instant, row_fingerprint)
from .data import fire, load_dataset, ordered_rows, split_frame
from .metrics import auc, baselines, evaluate, threshold_metrics, validate_scores


def data_reference(report):
    return {key: report[key] for key in ("purchase_dataset_id", "config", "data_path", "manifest_sha256")}


def sample_training(spark, data, code_sha256):
    from pyspark import StorageLevel
    from pyspark.sql import functions as F
    storage = Hdfs(spark)
    config, index = load_dataset(spark, storage, data, code_sha256)
    identity = {"kind": "purchase-sample", "purchase_dataset_id": data["purchase_dataset_id"],
                "data_manifest_sha256": data["manifest_sha256"], "sample_version": SAMPLE_VERSION,
                "seed": 42, "maximum_negative_ratio": 20}
    path = config["base"] + "/gold/purchase-samples/" + digest(identity)
    totals = index["splits"]["train"]
    expected_negatives = min(totals["rows"] - totals["positives"], 20 * totals["positives"])
    def build(staging):
        train = split_frame(spark, index, "train")
        as_of = F.date_format("as_of", "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
        key = F.sha2(F.to_json(F.array(F.lit(SAMPLE_VERSION), F.lit(config["identity"]["dataset_version"]),
                                     as_of, F.col("visitor_id"), F.lit("42"))), 256)
        negative = (train.where(F.col(LABEL) == 0).withColumn("sample_key", key)
            .orderBy("sample_key", "as_of", "visitor_id").limit(expected_negatives).drop("sample_key"))
        sampled = train.where(F.col(LABEL) == 1).unionByName(negative).persist(StorageLevel.DISK_ONLY)
        try:
            report = {"passed": True, "fingerprint": row_fingerprint(ordered_rows(sampled), labeled=True),
                      "original_train": totals, "prevalence": totals["positives"] / totals["rows"],
                      "negatives": expected_negatives}
            sampled.write.mode("errorifexists").parquet(staging + "/samples")
            write_json(storage, staging + "/quality.json", report)
            return report
        finally:
            sampled.unpersist()
    manifest, reused = materialize(storage, path, identity, build)
    actual = row_fingerprint(ordered_rows(spark.read.parquet(path + "/samples")), labeled=True)
    if actual != manifest["metadata"]["fingerprint"] or actual["positives"] != totals["positives"] or actual["rows"] != totals["positives"] + expected_negatives:
        raise ValueError("Purchase training sample lost positives or differs from its exact hash sample")
    return {"path": path, "manifest_sha256": storage.sha256(path + "/manifest.json"),
            "manifest": manifest, "reused": reused}, config, index


def prepare_training(spark, store, data, code_sha256):
    sample, config, _ = sample_training(spark, data, code_sha256)
    identity = {"kind": "purchase-model", "model_contract": MODEL_VERSION, "feature_order": list(FEATURES),
        "purchase_dataset_id": data["purchase_dataset_id"], "data_manifest_sha256": data["manifest_sha256"],
        "sample_manifest_sha256": sample["manifest_sha256"], "sample_version": SAMPLE_VERSION,
        "code_sha256": code_sha256, "spark_version": spark.version, "rf_parameters": RF_PARAMS,
        "candidates": CANDIDATES, "thresholds": THRESHOLDS, "metric_version": METRIC_VERSION,
        "selection": "validation_pr_auc_then_fewer_trees_then_shallower_within_1e-12",
        "threshold_selection": "validation_f1_then_precision_then_higher_threshold"}
    version = digest(identity)
    training = {"model_version": version, "identity": identity, "base": config["base"],
        "model_path": config["base"] + "/models/purchases/" + version,
        "sample": {key: sample[key] for key in ("path", "manifest_sha256", "manifest")},
        "data": data_reference(data)}
    storage = Hdfs(spark)
    store.prepare_model(training, storage.exists(training["model_path"]))
    return {"passed": True, "training": training, "sample_reused": sample["reused"]}


def training_check(spark, storage, training, code_sha256):
    if digest(training["identity"]) != training["model_version"] or training["identity"]["model_contract"] != MODEL_VERSION:
        raise ValueError("Purchase model identity mismatch")
    if training["identity"]["code_sha256"] != code_sha256 or training["identity"]["spark_version"] != spark.version:
        raise ValueError("Purchase model code/Spark version changed")
    if (training["identity"]["rf_parameters"] != RF_PARAMS or training["identity"]["candidates"] != CANDIDATES or
            training["identity"]["thresholds"] != THRESHOLDS or training["identity"]["feature_order"] != list(FEATURES)):
        raise ValueError("Purchase experiment parameters changed")
    sample = training["sample"]
    checked(storage, sample["path"], sample["manifest"]["identity"])
    if (storage.sha256(sample["path"] + "/manifest.json") != sample["manifest_sha256"] or
            sample["manifest_sha256"] != training["identity"]["sample_manifest_sha256"]):
        raise ValueError("Purchase sample manifest changed")
    if (training["data"]["manifest_sha256"] != training["identity"]["data_manifest_sha256"] or
            training["data"]["purchase_dataset_id"] != training["identity"]["purchase_dataset_id"]):
        raise ValueError("Purchase model data reference changed")
    return load_dataset(spark, storage, training["data"], code_sha256)


def candidate_location(training, parameters):
    if parameters not in CANDIDATES:
        raise ValueError("Unexpected RF candidate")
    identity = {"kind": "purchase-candidate", "model_version": training["model_version"], "parameters": parameters}
    return training["base"] + "/models/purchase-candidates/" + training["model_version"] + "/" + digest(identity), identity


def fit_pipeline(spark, sampled, parameters):
    native = NativeML(spark)
    assembler = native.ml.feature.VectorAssembler().setInputCols(native.strings(list(FEATURES))).setOutputCol("rf_features").setHandleInvalid("error")
    rf = (native.ml.classification.RandomForestClassifier().setLabelCol(LABEL).setFeaturesCol("rf_features")
          .setPredictionCol("rf_prediction").setProbabilityCol("rf_probability").setRawPredictionCol("rf_raw_prediction"))
    for key, value in {**RF_PARAMS, **parameters}.items():
        rf = getattr(rf, "set" + key[0].upper() + key[1:])(value)
    return native.pipeline([assembler, rf]).fit(sampled._jdf)


def score_frame(spark, model, frame, *, labeled=False, threshold=None):
    from pyspark.sql import functions as F
    native = NativeML(spark)
    scored = native.transform(model, frame)
    scored = scored.withColumn("purchase_score", native.vector_array(scored, "rf_probability").getItem(1))
    columns = [*AUDIT, *FEATURES, *([LABEL] if labeled else []), "purchase_score"]
    if threshold is not None:
        scored = scored.withColumn("predicted_label", (F.col("purchase_score") >= F.lit(threshold)).cast("int"))
        columns.append("predicted_label")
    # Drop vector UDTs before Python ever reads the result schema.
    return scored.select(*columns)


def fingerprints(frame, schedule, *, labeled=True, threshold=None):
    from pyspark.sql import functions as F
    days = []
    for entry in schedule:
        selected = frame.where(F.col("as_of") == F.lit(instant(entry["as_of"])))
        if "cutoff_date" in frame.columns:
            selected = selected.where(F.col("cutoff_date") == F.lit(instant(entry["as_of"]).date()))
        value = row_fingerprint(ordered_rows(selected), labeled=labeled, scored=True, threshold=threshold)
        if not value["rows"]:
            raise ValueError("Missing day in purchase predictions")
        days.append({"as_of": entry["as_of"], "fingerprint": value})
    return {"days": days, "summary": composite(days)}


def write_scores(frame, path):
    from pyspark.sql import functions as F
    frame.withColumn("cutoff_date", F.to_date("as_of")).write.mode("errorifexists").partitionBy("cutoff_date").parquet(path)


def train_candidate(spark, training, parameters, code_sha256, *, fault=None):
    from pyspark import StorageLevel
    storage = Hdfs(spark)
    _, index = training_check(spark, storage, training, code_sha256)
    root, identity = candidate_location(training, parameters)
    schedule = [entry for entry in index["days"] if entry["split"] == "validation"]
    def build(staging):
        started = time.monotonic()
        sampled = spark.read.parquet(training["sample"]["path"] + "/samples")
        model = fit_pipeline(spark, sampled, parameters)
        validation = score_frame(spark, model, split_frame(spark, index, "validation"), labeled=True).persist(StorageLevel.DISK_ONLY)
        try:
            validate_scores(validation)
            report = {"passed": True, "parameters": parameters, "metrics": auc(spark, validation),
                      "validation_fingerprint": fingerprints(validation, schedule)}
            importance = [float(value) for value in model.stages()[-1].featureImportances().toArray()]
            if len(importance) != 8 or any(not math.isfinite(value) or value < 0 for value in importance):
                raise ValueError("Invalid RF feature importance")
            report["feature_importance"] = dict(zip(FEATURES, importance))
            model.write().save(staging + "/pipeline")
            native = NativeML(spark)
            loaded = native.load(staging + "/pipeline")
            first = row_fingerprint(ordered_rows(score_frame(spark, model, sampled, labeled=True)), labeled=True, scored=True)
            second = row_fingerprint(ordered_rows(score_frame(spark, loaded, sampled, labeled=True)), labeled=True, scored=True)
            if first != second:
                raise ValueError("Reloaded candidate changes training sample scores")
            write_scores(validation, staging + "/validation_scores")
            report["duration_seconds"] = round(time.monotonic() - started, 3)
            write_json(storage, staging + "/quality.json", report)
            print("PURCHASE_CANDIDATE=" + json.dumps(report, sort_keys=True), flush=True)
            return report
        finally:
            validation.unpersist()
    manifest, reused = materialize(storage, root, identity, build,
        before_publish=lambda: fire(fault, "before_purchase_candidate"))
    fire(fault, "after_purchase_candidate")
    report = manifest["metadata"]
    saved = spark.read.parquet(root + "/validation_scores")
    if fingerprints(saved, schedule) != report["validation_fingerprint"]:
        raise ValueError("Candidate validation score snapshot changed")
    if reused:
        loaded = NativeML(spark).load(root + "/pipeline")
        actual = score_frame(spark, loaded, split_frame(spark, index, "validation"), labeled=True)
        if fingerprints(actual, schedule) != report["validation_fingerprint"]:
            raise ValueError("Reloaded candidate changes validation scores")
    return {"passed": True, "candidate_path": root, "parameters": parameters, "quality": report,
            "manifest_sha256": storage.sha256(root + "/manifest.json"), "reused": reused}


def select_model(spark, training, index, *, fault=None):
    storage, native = Hdfs(spark), NativeML(spark)
    candidates = []
    for parameters in CANDIDATES:
        path, identity = candidate_location(training, parameters)
        manifest = checked(storage, path, identity)
        candidates.append({**manifest["metadata"], "candidate_path": path,
                           "manifest_sha256": storage.sha256(path + "/manifest.json")})
    winner = choose_candidate(candidates)
    schedule = [entry for entry in index["days"] if entry["split"] == "validation"]
    validation = spark.read.parquet(winner["candidate_path"] + "/validation_scores")
    if fingerprints(validation, schedule) != winner["validation_fingerprint"]:
        raise ValueError("Winning validation scores changed")
    def build(staging):
        grid = threshold_metrics(validation)
        decision = choose_threshold(grid)
        report = {"passed": True, "candidates": candidates, "selected_parameters": winner["parameters"],
            "winner": winner, "threshold": decision["threshold"], "threshold_grid": grid,
            "validation_metrics": evaluate(spark, validation, decision["threshold"]),
            "validation_baselines": baselines(spark, validation, training["sample"]["manifest"]["metadata"]["prevalence"]),
            "feature_order": list(FEATURES), "feature_importance": winner["feature_importance"]}
        native.load(winner["candidate_path"] + "/pipeline").write().save(staging + "/pipeline")
        write_json(storage, staging + "/selection.json", report)
        return report
    manifest, reused = materialize(storage, training["model_path"], training["identity"], build,
        before_publish=lambda: fire(fault, "before_purchase_model"))
    fire(fault, "after_purchase_model")
    report = manifest["metadata"]
    if (storage.read_json(training["model_path"] + "/selection.json") != report or
            report["candidates"] != candidates or report["winner"] != winner or
            choose_threshold(report["threshold_grid"])["threshold"] != report["threshold"]):
        raise ValueError("Locked purchase selection changed")
    loaded = native.load(training["model_path"] + "/pipeline")
    predicted = score_frame(spark, loaded, split_frame(spark, index, "validation"), labeled=True)
    if fingerprints(predicted, schedule) != winner["validation_fingerprint"]:
        raise ValueError("Reloaded locked model changes validation predictions")
    return manifest, reused


def finish_training(spark, store, training, code_sha256, *, fault=None):
    from pyspark import StorageLevel
    storage = Hdfs(spark)
    _, index = training_check(spark, storage, training, code_sha256)
    store.prepare_model(training, storage.exists(training["model_path"]))
    model_manifest, model_reused = select_model(spark, training, index, fault=fault)
    selection = model_manifest["metadata"]
    model_hash = storage.sha256(training["model_path"] + "/manifest.json")
    identity = {"kind": "purchase-evaluation", "model_version": training["model_version"],
        "model_manifest_sha256": model_hash, "data_manifest_sha256": training["data"]["manifest_sha256"],
        "metric_version": METRIC_VERSION}
    evaluation_path = training["base"] + "/gold/purchase-evaluations/" + digest(identity)
    schedule = [entry for entry in index["days"] if entry["split"] == "test"]
    threshold = selection["threshold"]
    def predictions():
        model = NativeML(spark).load(training["model_path"] + "/pipeline")
        return score_frame(spark, model, split_frame(spark, index, "test"), labeled=True, threshold=threshold)
    def build(staging):
        predicted = predictions().persist(StorageLevel.DISK_ONLY)
        try:
            report = {"passed": True, "test": evaluate(spark, predicted, threshold),
                "baselines": baselines(spark, predicted, training["sample"]["manifest"]["metadata"]["prevalence"]),
                "test_fingerprint": fingerprints(predicted, schedule, threshold=threshold),
                "split_counts": index["splits"], "sampling": training["sample"]["manifest"]["metadata"],
                "reload_verified": True}
            report["beats_prevalence_pr_auc"] = report["test"]["pr_auc"] > report["baselines"]["train_prevalence"]["pr_auc"]
            write_scores(predicted, staging + "/test_scores")
            write_json(storage, staging + "/evaluation.json", report)
            return report
        finally:
            predicted.unpersist()
    evaluation, eval_reused = materialize(storage, evaluation_path, identity, build,
        before_publish=lambda: fire(fault, "before_purchase_evaluation"))
    report = evaluation["metadata"]
    if storage.read_json(evaluation_path + "/evaluation.json") != report:
        raise ValueError("Purchase evaluation metadata changed")
    saved = spark.read.parquet(evaluation_path + "/test_scores")
    if fingerprints(saved, schedule, threshold=threshold) != report["test_fingerprint"]:
        raise ValueError("Saved test scores differ from evaluation")
    if eval_reused and fingerprints(predictions(), schedule, threshold=threshold) != report["test_fingerprint"]:
        raise ValueError("Reloaded model changes held-out scores")
    fire(fault, "after_purchase_evaluation")
    publication = {"selection": selection, "evaluation": report,
                   "model_manifest_sha256": model_hash, "evaluation_path": evaluation_path,
                   "evaluation_manifest_sha256": storage.sha256(evaluation_path + "/manifest.json")}
    store.publish_model(training, publication, fault=fault)
    return {"passed": True, "model_version": training["model_version"], "training": training,
            "model_path": training["model_path"], "model_manifest": model_manifest, "report": publication,
            "reused": model_reused and eval_reused, "minimum_block_replicas": 2}
