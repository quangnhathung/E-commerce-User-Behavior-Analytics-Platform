"""Purchase semantics and immutable artifacts without Docker or Python ML extras."""
import copy
from datetime import timedelta
import importlib.util
from pathlib import Path
import tempfile
import types
import unittest
from unittest import mock
import zipfile

from helpers import APP, LocalStorage
from ecommerce_analytics.snapshots import materialize
from ecommerce_prediction.contracts import (AUDIT, BASELINE_COUNTS, CANDIDATES, FEATURES, LABEL,
    RF_PARAMS, THRESHOLDS, choose_candidate, choose_threshold, chunks, composite, confusion,
    default_schedule, digest, instant, iso, labeled_reference, row_fingerprint, sample_key,
    sample_reference, score_value, validate_schedule)
from ecommerce_prediction.metrics import reference_metrics
from test_ml import event

AS_OF = "2015-07-15T00:00:00.000Z"


def fixture_rows(size=121):
    now = instant(AS_OF)
    return labeled_reference([event(now - timedelta(hours=1), visitor_id=f"{i:04d}") for i in range(size)] +
        [event(now, "purchase", visitor_id="0000", transaction="01"),
         event(now, "purchase", visitor_id="0001", transaction="02")], AS_OF)


class PurchaseContractTests(unittest.TestCase):
    def test_schedule_counts_cutoffs_chunks_and_purge(self):
        schedule = default_schedule()
        self.assertEqual(128, len(schedule))
        self.assertEqual({"train": 66, "validation": 30, "test": 32},
                         {name: sum(value["split"] == name for value in schedule) for name in BASELINE_COUNTS})
        self.assertEqual(20, len(chunks(schedule)))
        self.assertTrue(all(1 <= len(chunk) <= 7 and len({value["split"] for value in chunk}) == 1 for chunk in chunks(schedule)))
        self.assertNotIn("2015-07-16T00:00:00.000Z", [value["as_of"] for value in schedule])
        self.assertNotIn("2015-08-16T00:00:00.000Z", [value["as_of"] for value in schedule])
        self.assertEqual(10136719, sum(value["rows"] for value in BASELINE_COUNTS.values()))

    def test_invalid_temporal_schedules_are_refused(self):
        schedule = default_schedule()
        for bad in ([], list(reversed(schedule)), schedule + [schedule[0]], [value for value in schedule if value["split"] != "test"],
                    [{**value, "as_of": "2015-07-16T00:00:00.000Z"} if value["split"] == "validation" and value["as_of"].startswith("2015-07-17") else value for value in schedule],
                    [{**schedule[0], "as_of": "2015-05-11T12:00:00.000Z"}, *schedule[1:]]):
            with self.assertRaises(ValueError):
                validate_schedule(bad)

    def test_half_open_label_window_and_multiline_transaction(self):
        now = instant(AS_OF)
        events = [event(now - timedelta(milliseconds=1), "purchase", transaction="old"),
            event(now, "purchase", transaction="01"), event(now, "purchase", item="02", transaction="01"),
            event(now + timedelta(days=1), "purchase", visitor_id="2", transaction="02"),
            event(now - timedelta(hours=2), visitor_id="2"),
            event(now - timedelta(hours=1), visitor_id="3"),
            event(now + timedelta(days=1, milliseconds=-1), "purchase", visitor_id="3", transaction="03")]
        values = labeled_reference(events, AS_OF)
        self.assertEqual([1, 0, 1], [value[LABEL] for value in values])
        self.assertEqual(1, values[0]["transaction_count_7d"])
        self.assertEqual(1, values[0]["unique_items_7d"])

    def test_future_does_not_change_features_or_add_visitors_without_history(self):
        now = instant(AS_OF)
        events = [event(now - timedelta(hours=2))]
        before = labeled_reference(events, AS_OF)
        after = labeled_reference(events + [event(now, "purchase", transaction="1"),
            event(now, "purchase", visitor_id="2", transaction="2")], AS_OF)
        self.assertEqual([{key: value for key, value in row.items() if key != LABEL} for row in before],
                         [{key: value for key, value in row.items() if key != LABEL} for row in after])
        self.assertEqual(["001"], [row["visitor_id"] for row in after])
        self.assertEqual(0, before[0][LABEL])
        self.assertEqual(1, after[0][LABEL])

    def test_hash_sample_is_exact_order_independent_and_keeps_every_positive(self):
        rows = fixture_rows()
        original = copy.deepcopy(rows)
        selected = sample_reference(rows, "source")
        self.assertEqual(42, len(selected))
        self.assertEqual(2, sum(row[LABEL] for row in selected))
        self.assertEqual(selected, sample_reference(list(reversed(rows)), "source"))
        self.assertEqual(original, rows)
        expected = sorted((row for row in rows if not row[LABEL]), key=lambda row:
                          (sample_key("source", row["as_of"], row["visitor_id"]), row["as_of"], row["visitor_id"]))[:40]
        self.assertEqual({row["visitor_id"] for row in expected}, {row["visitor_id"] for row in selected if not row[LABEL]})

    def test_small_negative_cohort_is_retained_and_single_class_refused(self):
        rows = fixture_rows(4)
        self.assertEqual(rows, sample_reference(rows, "source"))
        for values in ([row for row in rows if row[LABEL]], [row for row in rows if not row[LABEL]]):
            with self.assertRaisesRegex(ValueError, "both"):
                sample_reference(values, "source")

    def test_oracles_bound_generator_consumption(self):
        count = 0
        def events():
            nonlocal count
            while True:
                count += 1
                yield event(instant(AS_OF) - timedelta(hours=1))
        with self.assertRaisesRegex(ValueError, "limit"):
            labeled_reference(events(), AS_OF)
        self.assertEqual(100001, count)

    def test_row_fingerprint_labels_scores_and_unique_composite_keys(self):
        rows = fixture_rows(4)
        expected = row_fingerprint(rows, labeled=True)
        self.assertEqual(2, expected["positives"])
        with self.assertRaisesRegex(ValueError, "unique"):
            row_fingerprint([rows[0], rows[0]], labeled=True)
        with self.assertRaisesRegex(ValueError, "labels"):
            row_fingerprint([{**rows[0], LABEL: True}], labeled=True)
        score = {**rows[0], "purchase_score": .5, "predicted_label": 1}
        first = row_fingerprint([score], scored=True, threshold=.5)
        self.assertEqual(1, first["predicted_positive"])
        with self.assertRaisesRegex(ValueError, "threshold"):
            row_fingerprint([{**score, "predicted_label": 0}], scored=True, threshold=.5)
        self.assertNotEqual(first["sha256"], row_fingerprint([{**score, "purchase_score": .6}], scored=True, threshold=.5)["sha256"])

    def test_score_validation_and_composite_reconciliation(self):
        for bad in (float("nan"), float("inf"), -1, 1.1, None, True):
            with self.assertRaises(ValueError):
                score_value(bad)
        value = row_fingerprint(fixture_rows(4), labeled=True)
        days = [{"as_of": AS_OF, "fingerprint": value}]
        self.assertEqual(4, composite(days)["rows"])
        with self.assertRaisesRegex(ValueError, "Duplicate"):
            composite(days * 2)


class PurchaseMetricTests(unittest.TestCase):
    def test_known_confusion_and_zero_denominators(self):
        value = confusion(2, 1, 2, 5)
        self.assertAlmostEqual(2/3, value["precision"])
        self.assertEqual(.5, value["recall"])
        self.assertAlmostEqual(4/7, value["f1"])
        self.assertEqual(0, confusion(0, 0, 0, 5)["f1"])
        with self.assertRaises(ValueError):
            confusion(-1, 0, 1, 1)

    def test_constant_baseline_auc_and_daily_top_ties(self):
        rows = [{**value, "purchase_score": .1} for value in fixture_rows()]
        metrics = reference_metrics(rows, .5)
        self.assertAlmostEqual(2/121, metrics["pr_auc"])
        self.assertEqual(.5, metrics["roc_auc"])
        self.assertEqual(0, metrics["f1"])
        self.assertEqual(2, metrics["top_1pct"]["selected"])
        self.assertEqual(1, metrics["precision_at_1pct"])

    def test_perfect_ranking_and_threshold_equality(self):
        rows = [{**value, "purchase_score": .5 if value[LABEL] else .1} for value in fixture_rows(4)]
        metrics = reference_metrics(rows, .5)
        self.assertEqual(1, metrics["pr_auc"])
        self.assertEqual(1, metrics["roc_auc"])
        self.assertEqual(1, metrics["f1"])

    def test_threshold_ties_favor_precision_then_higher_threshold(self):
        grid = [{"threshold": value, "f1": .1, "precision": .2} for value in THRESHOLDS]
        self.assertEqual(1.0, choose_threshold(grid)["threshold"])
        grid[10]["precision"] = .8
        self.assertEqual(.1, choose_threshold(grid)["threshold"])
        grid[20]["f1"] = .2
        self.assertEqual(.2, choose_threshold(grid)["threshold"])
        with self.assertRaisesRegex(ValueError, "101"):
            choose_threshold(grid[:2])

    def test_candidate_ties_and_missing_or_nonfinite_metrics(self):
        values = [{"parameters": parameters, "metrics": {"pr_auc": .8}} for parameters in CANDIDATES]
        self.assertEqual({"numTrees": 50, "maxDepth": 5}, choose_candidate(list(reversed(values)))["parameters"])
        values[-1]["metrics"]["pr_auc"] += 5e-13
        self.assertEqual(50, choose_candidate(values)["parameters"]["numTrees"])
        values[-1]["metrics"]["pr_auc"] = .9
        self.assertEqual(100, choose_candidate(values)["parameters"]["numTrees"])
        with self.assertRaises(ValueError):
            choose_candidate(values[:3])
        values[0]["metrics"]["pr_auc"] = float("nan")
        with self.assertRaises(ValueError):
            choose_candidate(values)


class PurchasePackagingTests(unittest.TestCase):
    def test_prediction_profile_isolated_reproducible_and_all_legacy_hashes_frozen(self):
        spec = importlib.util.spec_from_file_location("purchase_packaging", APP / "package_code.py")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        with tempfile.TemporaryDirectory() as directory:
            for name, expected in {"ingest": "ae42a5981bc20e1295df1dabbfab7e553af0f133a39c97fb52fa795ceafcb554",
                "streaming": "009c7d7d1d411e3f1810096f9c2954235ee6b92c7a3789514fe026b9af3516ac",
                "ml": "b3991c1379b84c0a440ce9ad023c411221e357dca6750cbd86bf917c6af831a9"}.items():
                self.assertEqual(expected, module.package_code(Path(directory)/(name+'.zip'), profile=name)["sha256"])
            first = module.package_code(Path(directory)/'prediction.zip', profile='prediction')
            self.assertEqual(first['sha256'], module.package_code(Path(directory)/'again.zip', profile='prediction')['sha256'])
            with zipfile.ZipFile(first['archive']) as archive:
                self.assertIn('ecommerce_prediction/integration.py', archive.namelist())
                self.assertFalse(any(name.startswith(('ecommerce_streaming/', 'ecommerce_behavior/')) for name in archive.namelist()))
                for name in archive.namelist():
                    compile(archive.read(name), name, 'exec')

    def test_candidate_jvm_pipeline_has_only_raw_features_and_pinned_parameters(self):
        from ecommerce_prediction.training import fit_pipeline
        class Stage:
            def __init__(self):
                self.values = {}
            def __getattr__(self, name):
                if not name.startswith('set'):
                    raise AttributeError(name)
                def setter(value):
                    self.values[name[3:]] = value
                    return self
                return setter
        assembler, forest = Stage(), Stage()
        native = mock.Mock()
        native.ml.feature.VectorAssembler.return_value = assembler
        native.ml.classification.RandomForestClassifier.return_value = forest
        native.strings.side_effect = lambda values: values
        frame = mock.Mock()
        with mock.patch('ecommerce_prediction.training.NativeML', return_value=native), \
                mock.patch.dict('sys.modules', {'numpy': None, 'pandas': None, 'pyspark.ml': None}):
            fit_pipeline(mock.Mock(), frame, CANDIDATES[0])
        self.assertEqual(list(FEATURES), assembler.values['InputCols'])
        self.assertEqual(LABEL, forest.values['LabelCol'])
        self.assertEqual(64, forest.values['MaxMemoryInMB'])
        self.assertEqual(50, forest.values['NumTrees'])
        self.assertEqual(5, forest.values['MaxDepth'])
        native.pipeline.assert_called_once_with([assembler, forest])
        native.pipeline.return_value.fit.assert_called_once_with(frame._jdf)

    def test_snapshot_failure_and_corruption_cannot_change_published_data(self):
        from ecommerce_ml.features import checked
        with tempfile.TemporaryDirectory() as directory:
            storage = LocalStorage(directory)
            def build(path):
                storage.write_bytes(path+'/rows', b'immutable labeled samples')
                return {'passed': True}
            identity = {'kind':'purchase-day'}
            with self.assertRaisesRegex(RuntimeError, 'injected'):
                materialize(storage, '/day', identity, build, before_publish=lambda: (_ for _ in ()).throw(RuntimeError('injected')))
            self.assertFalse(storage.exists('/day'))
            first,_=materialize(storage,'/day',identity,build)
            second,reused=materialize(storage,'/day',identity,lambda path: self.fail('rebuilt'))
            self.assertTrue(reused)
            self.assertEqual(first,second)
            storage.write_bytes('/day/unexpected',b'bad')
            with self.assertRaisesRegex(ValueError,'inventory'):
                checked(storage,'/day',identity)
