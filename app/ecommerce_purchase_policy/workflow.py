"""Choose on saved validation scores, freeze, reassess test, then relabel batches."""
from ecommerce_analytics.hdfs import Hdfs
from ecommerce_analytics.snapshots import materialize, write_json
from ecommerce_ml.features import checked
from ecommerce_prediction.contracts import FEATURES, METRIC_VERSION, MODEL_VERSION, SCORE_VERSION, digest, row_fingerprint
from ecommerce_prediction.data import config_check, fire, ordered_rows
from ecommerce_prediction.metrics import threshold_metrics, validate_scores
from ecommerce_prediction.training import fingerprints
from .contracts import (EVALUATION_KIND, POLICY_CONTRACT, RULE, SCORE_CONTRACT, TARGETS,
                        evaluation_identity, meets, policy_identity, select, selection_identity)


def snapshot(storage, path, payload, *, identity=None, checksum=None):
    manifest = checked(storage, path, identity)
    if checksum is not None and storage.sha256(path + "/manifest.json") != checksum:
        raise ValueError("Policy source manifest changed: " + path)
    if storage.read_json(path + "/" + payload) != manifest["metadata"]:
        raise ValueError("Policy source metadata differs from its manifest: " + path)
    return manifest


def model_source(spark, storage, store, version):
    record = store.model_status(version)
    identity, publication = record["identity"], record["report"]
    if identity.get("model_contract") != MODEL_VERSION or identity.get("feature_order") != list(FEATURES):
        raise ValueError("Unsupported purchase model contract/features")
    if identity["spark_version"] != spark.version:
        raise ValueError("Purchase policy requires the model's Spark version")
    model = snapshot(storage, record["model_path"], "selection.json", identity=identity,
                     checksum=publication["model_manifest_sha256"])
    if model["metadata"] != publication["selection"]:
        raise ValueError("Published purchase selection differs from HDFS")
    data = snapshot(storage, record["data_path"], "index.json", checksum=identity["data_manifest_sha256"])
    if data["identity"].get("kind") != "purchase-index":
        raise ValueError("Policy requires a purchase dataset index")
    config = data["identity"]["config"]
    config_check(config)
    if config["purchase_dataset_id"] != identity["purchase_dataset_id"] or record["data_path"] != config["root"] + "/index":
        raise ValueError("Purchase policy model/dataset mismatch")
    winner = publication["selection"]["winner"]
    candidate = snapshot(storage, winner["candidate_path"], "quality.json", identity={
        "kind": "purchase-candidate", "model_version": version, "parameters": winner["parameters"]},
        checksum=winner["manifest_sha256"])
    expected = {key: value for key, value in winner.items() if key not in ("candidate_path", "manifest_sha256")}
    if candidate["metadata"] != expected:
        raise ValueError("Policy winning candidate changed")
    return record, config, data["metadata"]


def split_schedule(index, name):
    return [value for value in index["days"] if value["split"] == name]


def comparison(original, new_counts):
    # Ranking scores are unchanged: AUC and top-1% remain the original verified values.
    return {**original, **new_counts}


def tune(spark, store, version, code_sha256, *, fault=None):
    from pyspark import StorageLevel
    storage = Hdfs(spark)
    record, config, index = model_source(spark, storage, store, version)
    identity = policy_identity(record, code_sha256)
    key = digest(identity)
    root = config["base"] + "/models/purchase-policies/" + key
    selection_path, evaluation_path = root + "/selection", root + "/evaluation"
    store.prepare_policy(identity, selection_path, evaluation_path,
                         storage.exists(selection_path) and storage.exists(evaluation_path))
    original = record["report"]["selection"]
    def build_selection(staging):
        frame = spark.read.parquet(original["winner"]["candidate_path"] + "/validation_scores").persist(StorageLevel.DISK_ONLY)
        try:
            validate_scores(frame)
            actual = fingerprints(frame, split_schedule(index, "validation"))
            if actual != original["winner"]["validation_fingerprint"]:
                raise ValueError("Policy validation predictions changed")
            grid = threshold_metrics(frame)
            if grid != original["threshold_grid"]:
                raise ValueError("Policy validation grid differs from the original saved report")
            choice = select(grid)
            old = next(row for row in grid if row["threshold"] == original["threshold"])
            if any(original["validation_metrics"][name] != value for name, value in old.items()):
                raise ValueError("Original validation threshold metrics changed")
            report = {"passed": True, "policy_version": key, "targets": TARGETS, "rule": RULE,
                "old_threshold": original["threshold"], "grid": grid, "choice": choice,
                "validation_original": original["validation_metrics"], "validation_fingerprint": actual,
                "validation": comparison(original["validation_metrics"], choice["validation"]) if choice["validation"] else None}
            write_json(storage, staging + "/selection.json", report)
            return report
        finally:
            frame.unpersist()
    selected, selection_reused = materialize(storage, selection_path, selection_identity(identity), build_selection,
        before_publish=lambda: fire(fault, "before_policy_selection"))
    selection = selected["metadata"]
    if storage.read_json(selection_path + "/selection.json") != selection or select(selection["grid"]) != selection["choice"]:
        raise ValueError("Frozen policy selection changed")
    fire(fault, "after_policy_selection")
    selection_sha = storage.sha256(selection_path + "/manifest.json")
    choice = selection["choice"]
    def build_evaluation(staging):
        report = {"passed": True, "policy_version": key, "model_version": version,
            "threshold": choice["threshold"], "old_threshold": selection["old_threshold"],
            "targets": TARGETS, "validation_targets_met": choice["validation_targets_met"],
            "validation_original": selection["validation_original"], "validation": selection["validation"],
            "selection_manifest_sha256": selection_sha, "evaluation_kind": EVALUATION_KIND,
            "fresh_holdout": False, "test_targets_met": False, "eligible": False,
            "test_original": None, "test": None, "test_skipped": not choice["validation_targets_met"],
            "reason": choice["reason"], "ranking_metrics_reused": ["pr_auc", "roc_auc", "precision_at_1pct", "top_1pct"]}
        if choice["validation_targets_met"]:
            # No test payload is accessed until the selection snapshot is committed.
            publication = record["report"]
            test_path = publication["evaluation_path"]
            test_manifest = snapshot(storage, test_path, "evaluation.json", checksum=publication["evaluation_manifest_sha256"],
                identity={"kind": "purchase-evaluation", "model_version": version,
                    "model_manifest_sha256": publication["model_manifest_sha256"],
                    "data_manifest_sha256": identity["data_manifest_sha256"], "metric_version": METRIC_VERSION})
            if test_manifest["metadata"] != publication["evaluation"]:
                raise ValueError("Original purchase evaluation changed")
            frame = spark.read.parquet(test_path + "/test_scores").persist(StorageLevel.DISK_ONLY)
            try:
                validate_scores(frame)
                actual = fingerprints(frame, split_schedule(index, "test"), threshold=selection["old_threshold"])
                if actual != publication["evaluation"]["test_fingerprint"]:
                    raise ValueError("Policy test predictions changed")
                old, new = threshold_metrics(frame, [selection["old_threshold"], choice["threshold"]])
                baseline = publication["evaluation"]["test"]
                if any(baseline[name] != value for name, value in old.items()):
                    raise ValueError("Original test threshold metrics changed")
                report.update({"test_original": baseline, "test": comparison(baseline, new),
                    "test_source_fingerprint": actual, "test_targets_met": meets(new), "eligible": meets(new),
                    "reason": "targets_met_on_validation_and_reassessed_test" if meets(new) else "test_targets_not_met"})
            finally:
                frame.unpersist()
        write_json(storage, staging + "/evaluation.json", report)
        return report
    evaluated, evaluation_reused = materialize(storage, evaluation_path, evaluation_identity(identity, selection_sha), build_evaluation,
        before_publish=lambda: fire(fault, "before_policy_evaluation"))
    report = evaluated["metadata"]
    if storage.read_json(evaluation_path + "/evaluation.json") != report:
        raise ValueError("Policy evaluation metadata changed")
    evaluation_sha = storage.sha256(evaluation_path + "/manifest.json")
    publication = {"evaluation": report, "selection_manifest_sha256": selection_sha, "evaluation_manifest_sha256": evaluation_sha}
    store.publish_policy(identity, selection_path, evaluation_path, publication, fault=fault)
    return {"passed": True, "policy_version": key, "identity": identity, "threshold": report["threshold"],
        "eligible": report["eligible"], "selection_path": selection_path, "evaluation_path": evaluation_path,
        "report": publication, "reused": selection_reused and evaluation_reused, "minimum_block_replicas": 2}


def load_policy(spark, storage, store, key, code_sha256):
    policy = store.policy_status(key)
    identity, publication = policy["identity"], policy["report"]
    if (identity.get("policy_contract") != POLICY_CONTRACT or identity.get("targets") != TARGETS or
            identity.get("rule") != RULE or identity.get("policy_code_sha256") != code_sha256 or
            identity.get("spark_version") != spark.version):
        raise ValueError("Policy contract/code/Spark version changed")
    selected = snapshot(storage, policy["selection_path"], "selection.json", identity=selection_identity(identity),
                        checksum=publication["selection_manifest_sha256"])
    evaluated = snapshot(storage, policy["evaluation_path"], "evaluation.json",
        identity=evaluation_identity(identity, publication["selection_manifest_sha256"]), checksum=publication["evaluation_manifest_sha256"])
    if (evaluated["metadata"] != publication["evaluation"] or select(selected["metadata"]["grid"]) != selected["metadata"]["choice"] or
            selected["metadata"]["choice"]["threshold"] != evaluated["metadata"]["threshold"]):
        raise ValueError("Published policy selection/evaluation changed")
    report = evaluated["metadata"]
    eligible = report["validation_targets_met"] and report["test_targets_met"] and meets(report["validation"]) and meets(report["test"])
    if not report["eligible"] or not eligible:
        raise ValueError("Purchase policy is not eligible: " + report["reason"])
    return policy, report


def apply(spark, store, policy_version, source_run_id, code_sha256, *, fault=None):
    from pyspark.sql import functions as F
    storage = Hdfs(spark)
    policy, evaluation = load_policy(spark, storage, store, policy_version, code_sha256)
    source = store.score_status(source_run_id)
    source_identity = source["identity"]
    if source_identity.get("kind") != "purchase-score" or source_identity.get("score_version") != SCORE_VERSION:
        raise ValueError("Apply requires an original purchase score run; policies cannot be nested")
    if (source["model_version"] != policy["model_version"] or
            source_identity["model_manifest_sha256"] != policy["identity"]["model_manifest_sha256"]):
        raise ValueError("Policy and source score model mismatch")
    record = store.model_status(source["model_version"])
    snapshot(storage, record["model_path"], "selection.json", identity=record["identity"],
             checksum=policy["identity"]["model_manifest_sha256"])
    if (source_identity["code_sha256"] != policy["identity"]["original_prediction_code_sha256"] or
            source["report"]["threshold"] != evaluation["old_threshold"]):
        raise ValueError("Policy source score code/threshold mismatch")
    source_manifest = snapshot(storage, source["score_path"], "quality.json", identity=source_identity)
    if source_manifest["metadata"] != source["report"]:
        raise ValueError("Published source score metadata changed")
    old_threshold, threshold = evaluation["old_threshold"], evaluation["threshold"]
    frame = spark.read.parquet(source["score_path"] + "/scores")
    validate_scores(frame, labeled=False)
    if row_fingerprint(ordered_rows(frame), scored=True, threshold=old_threshold) != source["report"]["fingerprint"]:
        raise ValueError("Policy source score fingerprint changed")
    score_only = row_fingerprint(ordered_rows(frame), scored=True)
    identity = {"kind": "purchase-policy-score", "score_version": SCORE_CONTRACT,
        "model_version": policy["model_version"], "policy_version": policy_version,
        "policy_evaluation_manifest_sha256": policy["report"]["evaluation_manifest_sha256"],
        "source_score_run_id": source_run_id, "source_manifest_sha256": storage.sha256(source["score_path"] + "/manifest.json"),
        "silver_path": source_identity["silver_path"], "as_of": source_identity["as_of"],
        "model_manifest_sha256": source_identity["model_manifest_sha256"], "threshold": threshold,
        "policy_code_sha256": code_sha256}
    key = digest(identity)
    # Original score roots are <base>/gold/purchase-scores/<id>.
    suffix = "/gold/purchase-scores/" + source_run_id
    if not source["score_path"].endswith(suffix):
        raise ValueError("Original purchase score path differs from its ID")
    path = source["score_path"][:-len(suffix)] + "/gold/purchase-policy-scores/" + key
    store.prepare_policy_scores(identity, path, storage.exists(path))
    def build(staging):
        changed = frame.withColumn("predicted_label", (F.col("purchase_score") >= F.lit(threshold)).cast("int"))
        fingerprint = row_fingerprint(ordered_rows(changed), scored=True, threshold=threshold)
        if row_fingerprint(ordered_rows(changed), scored=True) != score_only:
            raise ValueError("Policy application changed raw features or purchase scores")
        report = {"passed": True, "policy_version": policy_version, "source_score_run_id": source_run_id,
            "as_of": identity["as_of"], "threshold": threshold, "old_threshold": old_threshold,
            "fingerprint": fingerprint, "score_only_fingerprint": score_only,
            "source_predicted_positive": source["report"]["fingerprint"]["predicted_positive"],
            "feature_order": list(FEATURES), "scores_unchanged": True}
        changed.write.mode("errorifexists").parquet(staging + "/scores")
        write_json(storage, staging + "/quality.json", report)
        return report
    manifest, reused = materialize(storage, path, identity, build,
        before_publish=lambda: fire(fault, "before_policy_score_gold"))
    fire(fault, "after_policy_score_gold")
    report = manifest["metadata"]
    if storage.read_json(path + "/quality.json") != report or report["score_only_fingerprint"] != score_only:
        raise ValueError("Policy score quality/source changed")
    saved = spark.read.parquet(path + "/scores")
    if (row_fingerprint(ordered_rows(saved), scored=True, threshold=threshold) != report["fingerprint"] or
            row_fingerprint(ordered_rows(saved), scored=True) != score_only):
        raise ValueError("Saved policy scores differ from source/decision snapshot")
    store.publish_policy_scores(identity, path, report, ordered_rows(saved), fault=fault)
    return {"passed": True, "policy_version": policy_version, "model_version": policy["model_version"],
        "source_score_run_id": source_run_id, "score_run_id": key, "score_path": path,
        "manifest": manifest, "quality": report, "reused": reused, "minimum_block_replicas": 2}
