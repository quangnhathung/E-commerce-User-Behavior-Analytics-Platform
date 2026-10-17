"""Fixed precision/recall constraints and validation-only threshold selection."""
import math

from ecommerce_prediction.contracts import THRESHOLDS, confusion, digest

POLICY_CONTRACT = "purchase-policy-v1"
SCORE_CONTRACT = "purchase-policy-score-v1"
TARGETS = {"minimum_precision": 0.70, "minimum_recall": 0.10}
RULE = "validation_constraints_then_recall_then_precision_then_higher_threshold"
EVALUATION_KIND = "posthoc_threshold_reassessment"


def meets(metrics):
    return (metrics["tp"] + metrics["fp"] > 0 and
            metrics["precision"] >= TARGETS["minimum_precision"] and
            metrics["recall"] >= TARGETS["minimum_recall"])


def validate_grid(grid):
    if [value["threshold"] for value in grid] != THRESHOLDS:
        raise ValueError("Policy requires the complete fixed 101-point threshold grid")
    totals, previous = None, None
    for row in grid:
        expected = confusion(*(row[key] for key in ("tp", "fp", "fn", "tn")))
        if any(not isinstance(row[key], (int, float)) or isinstance(row[key], bool) or
               not math.isfinite(row[key]) or not math.isclose(row[key], value, rel_tol=1e-12, abs_tol=1e-15)
               for key, value in expected.items()):
            raise ValueError("Policy grid metrics disagree with confusion counts")
        current = (row["tp"] + row["fn"], row["fp"] + row["tn"])
        if not all(current) or (totals is not None and totals != current):
            raise ValueError("Policy grid requires both classes and stable cohort counts")
        if previous is not None and (row["tp"] > previous["tp"] or row["fp"] > previous["fp"]):
            raise ValueError("Policy threshold counts are not monotone")
        totals, previous = current, row
    return grid


def select(grid):
    validate_grid(grid)
    feasible = [row for row in grid if meets(row)]
    winner = max(feasible, key=lambda row: (row["recall"], row["precision"], row["threshold"])) if feasible else None
    return {"threshold": winner["threshold"] if winner is not None else None,
            "validation_targets_met": winner is not None, "validation": winner,
            "reason": "validation_targets_met" if winner is not None else "no_validation_threshold_meets_targets"}


def policy_identity(record, code_sha256):
    identity = record["identity"]
    return {"kind": "purchase-policy", "policy_contract": POLICY_CONTRACT, "targets": TARGETS, "rule": RULE,
            "evaluation_kind": EVALUATION_KIND, "model_version": record["model_version"],
            "model_manifest_sha256": record["report"]["model_manifest_sha256"],
            "data_manifest_sha256": identity["data_manifest_sha256"],
            "candidate_manifest_sha256": record["report"]["selection"]["winner"]["manifest_sha256"],
            "original_evaluation_manifest_sha256": record["report"]["evaluation_manifest_sha256"],
            "original_prediction_code_sha256": identity["code_sha256"],
            "spark_version": identity["spark_version"], "policy_code_sha256": code_sha256}


def selection_identity(identity):
    return {"kind": "purchase-policy-selection", "policy_version": digest(identity), "policy": identity}


def evaluation_identity(identity, selection_sha256):
    return {"kind": "purchase-policy-evaluation", "policy_version": digest(identity),
            "selection_manifest_sha256": selection_sha256, "original_evaluation_manifest_sha256":
            identity["original_evaluation_manifest_sha256"], "evaluation_kind": EVALUATION_KIND}
