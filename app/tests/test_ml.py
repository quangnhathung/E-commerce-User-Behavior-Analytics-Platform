"""Feature semantics, immutable storage and archive isolation without Spark/Docker."""
import copy
from datetime import timedelta
import importlib.util
import json
from pathlib import Path
import tempfile
import types
import unittest
from unittest import mock
import zipfile

from helpers import APP, LocalStorage
from ecommerce_analytics.snapshots import materialize
from ecommerce_ml.contracts import (COUNTS, DEFAULT_AS_OF, FEATURES, PARAMS, canonical, choose, coverage,
                                    digest, fingerprint, identifier, instant, iso, normalize, reference, visitor, window)


def event(time, kind="view", visitor_id="001", item="01", transaction=None):
    return {"event_time": iso(time), "event_type": kind, "visitor_id": visitor_id,
            "item_id": item, "transaction_id": transaction}


class FeatureTests(unittest.TestCase):
    def setUp(self):
        self.start, self.end = window(DEFAULT_AS_OF)

    def test_half_open_window_milliseconds_and_source_not_mutated(self):
        events = [event(self.start - timedelta(milliseconds=1), visitor_id="excluded"),
            event(self.start), event(self.end - timedelta(milliseconds=1)),
            event(self.end, visitor_id="future"), event(self.end + timedelta(days=1), visitor_id="future")]
        original = copy.deepcopy(events)
        value = reference(events)[0]
        self.assertEqual(2, value[COUNTS[0]])
        self.assertEqual("001", value["visitor_id"])
        self.assertEqual(2, value["active_days_7d"])
        self.assertAlmostEqual(1 / 3_600_000, value["recency_hours"])
        self.assertEqual(original, events)
        self.assertEqual(reference(events), reference(list(reversed(events))))

    def test_distincts_across_days_and_multiline_purchase(self):
        events = [event(self.start, transaction="ignored-on-view"),
            event(self.start + timedelta(days=1), item="01"),
            event(self.start, "purchase", transaction="0001"),
            event(self.start, "purchase", item="02", transaction="0001"),
            event(self.start + timedelta(days=1), "purchase", transaction="0001"),
            event(self.start, "purchase", transaction="0002")]
        value = reference(events)[0]
        self.assertEqual(2, value["transaction_count_7d"])
        self.assertEqual(2, value["unique_items_7d"])
        self.assertEqual(2, value["active_days_7d"])

    def test_zero_views_and_ratios_above_one_are_valid(self):
        events = [event(self.start, "add_to_cart"), event(self.start, "purchase", transaction="1"),
                  event(self.start, visitor_id="1"), *[event(self.start, "add_to_cart", visitor_id="1") for _ in range(3)]]
        first, second = reference(events)
        self.assertEqual(0.0, first["cart_view_ratio_7d"])
        self.assertEqual(0.0, first["transaction_view_ratio_7d"])
        self.assertEqual(3.0, second["cart_view_ratio_7d"])

    def test_empty_window_and_oracle_limit(self):
        self.assertEqual([], reference([event(self.end)]))
        with self.assertRaisesRegex(ValueError, "small-fixture"):
            reference([event(self.start)] * 2, limit=1)

    def test_non_midnight_seven_days_can_cover_eight_utc_dates(self):
        cutoff = "2015-07-15T12:00:00.000Z"
        start, end = window(cutoff)
        events = [event(start + timedelta(days=index)) for index in range(7)]
        events.append(event(end - timedelta(milliseconds=1)))
        value = reference(events, cutoff)[0]
        self.assertEqual(8, value["active_days_7d"])
        self.assertEqual(8, value["view_count_7d"])

    def test_strict_utc_range_and_identifiers(self):
        for bad in ("2015-07-15", "2015-07-15T00:00:00Z", "2015-07-15T07:00:00.000+07:00", "bad"):
            with self.assertRaises(ValueError):
                instant(bad)
        coverage(DEFAULT_AS_OF, iso(self.start), iso(self.end - timedelta(milliseconds=1)))
        for minimum, maximum in ((iso(self.start + timedelta(milliseconds=1)), iso(self.end)),
                                 (iso(self.start), iso(self.end - timedelta(milliseconds=2)))):
            with self.assertRaisesRegex(ValueError, "outside"):
                coverage(DEFAULT_AS_OF, minimum, maximum)
        self.assertEqual("001", visitor("001"))
        self.assertEqual("a" * 64, identifier("a" * 64))
        for bad in ("../outside", "A" * 64, "abc"):
            with self.assertRaises(ValueError):
                identifier(bad)

    def test_invalid_and_inconsistent_features_cannot_be_fingerprinted(self):
        value = reference([event(self.start)])[0]
        for key, bad in (("view_count_7d", -1), ("view_count_7d", 1.0), ("active_days_7d", 8),
                ("unique_items_7d", 0), ("recency_hours", float("nan")), ("cart_view_ratio_7d", float("inf")),
                ("cart_view_ratio_7d", 1.0), ("last_event_time", DEFAULT_AS_OF)):
            with self.subTest(key=key, bad=bad), self.assertRaises(ValueError):
                fingerprint([{**value, key: bad}])

    def test_fingerprint_is_ordered_unique_and_commits_all_exact_inputs(self):
        values = reference([event(self.start), event(self.start, visitor_id="1")])
        with self.assertRaisesRegex(ValueError, "lexical"):
            fingerprint(list(reversed(values)))
        with self.assertRaisesRegex(ValueError, "unique"):
            fingerprint([values[0], values[0]])
        changed = copy.deepcopy(values)
        changed[0]["unique_items_7d"] += 1
        self.assertNotEqual(fingerprint(values), fingerprint(changed))
        self.assertNotEqual(fingerprint([{**values[0], "cluster_id": 0}], assignments=True),
                            fingerprint([{**values[0], "cluster_id": 1}], assignments=True))
        self.assertEqual(8, len(canonical(values[0])))


class SelectionTests(unittest.TestCase):
    def test_max_silhouette_and_smallest_k_within_tolerance(self):
        candidates = [{"k": 5, "valid": True, "silhouette": .8}, {"k": 2, "valid": True, "silhouette": .8 - 5e-13},
                      {"k": 3, "valid": True, "silhouette": .8 - 2e-12}, {"k": 4, "valid": False}]
        self.assertEqual(2, choose(candidates)["k"])
        candidates[1]["silhouette"] = .7
        self.assertEqual(5, choose(candidates)["k"])

    def test_all_invalid_or_nonfinite_has_no_winner(self):
        for values in ([], [{"k": 2, "valid": False}], [{"k": 2, "valid": True, "silhouette": float("nan")}],
                       [{"k": 2, "valid": True, "silhouette": 1.1}]):
            with self.assertRaisesRegex(ValueError, "No valid"):
                choose(values)

    def test_pipeline_order_and_only_counts_logged_with_centered_scaling(self):
        from ecommerce_ml.training import preprocessor
        constructed = {}
        class Stage:
            def __init__(self, name):
                self.name, self.params = name, {}
                constructed[name] = self
            def __getattr__(self, method):
                if not method.startswith("set"):
                    raise AttributeError(method)
                def setter(value):
                    self.params[method[3:]] = value
                    return self
                return setter
        spark = mock.Mock()
        spark._jvm.org.apache.spark.ml = types.SimpleNamespace(Pipeline=lambda: Stage("Pipeline"), PipelineStage=object(),
            feature=types.SimpleNamespace(**{key: (lambda name=key: Stage(name)) for key in
                                            ("SQLTransformer", "VectorAssembler", "StandardScaler")}))
        spark.sparkContext._gateway.new_array.side_effect = lambda kind, length: [None] * length
        with mock.patch.dict("sys.modules", {"numpy": None, "pandas": None, "pyspark.ml": None}):
            preprocessor(spark)
        statement = constructed["SQLTransformer"].params["Statement"]
        self.assertEqual(5, statement.count("LOG1P("))
        self.assertNotIn("LOG1P(`recency_hours`)", statement)
        columns = constructed["VectorAssembler"].params["InputCols"]
        self.assertEqual(["ml_" + key for key in COUNTS] + list(FEATURES[5:]), columns)
        self.assertTrue(constructed["StandardScaler"].params["WithMean"])
        self.assertTrue(constructed["StandardScaler"].params["WithStd"])
        self.assertEqual(["SQLTransformer", "VectorAssembler", "StandardScaler"],
                         [stage.name for stage in constructed["Pipeline"].params["Stages"]])
        self.assertEqual([2, 3, 4, 5], PARAMS["candidates"])

    def test_native_bridge_keeps_vectors_in_jvm_and_wraps_sql_without_python_ml(self):
        from ecommerce_ml.native import NativeML
        spark, model, frame = mock.Mock(), mock.Mock(), mock.Mock()
        dataframe, column = mock.Mock(), mock.Mock()
        modules = {"numpy": None, "pandas": None, "pyspark.ml": None,
                   "pyspark.sql": types.SimpleNamespace(DataFrame=dataframe),
                   "pyspark.sql.column": types.SimpleNamespace(Column=column)}
        with mock.patch.dict("sys.modules", modules):
            native = NativeML(spark)
            native.transform(model, frame)
            native.vector_array(frame)
            native.load("/checked/pipeline")
        model.transform.assert_called_once_with(frame._jdf)
        dataframe.assert_called_once_with(model.transform.return_value, spark)
        native.ml.functions.vector_to_array.assert_called_once_with(frame._jdf.col.return_value, "float64")
        frame._jdf.col.assert_called_once_with("scaled_features")
        native.ml.PipelineModel.load.assert_called_once_with("/checked/pipeline")


class MLSnapshotTests(unittest.TestCase):
    def test_reuse_skips_build_and_corrupt_marker_identity_inventory_are_refused(self):
        from ecommerce_ml.features import checked
        with tempfile.TemporaryDirectory() as directory:
            storage = LocalStorage(directory)
            identity = {"kind": "features", "as_of": DEFAULT_AS_OF}
            def build(root):
                storage.write_bytes(root + "/data", b"feature payload")
                return {"passed": True}
            first, _ = materialize(storage, "/features", identity, build)
            second, reused = materialize(storage, "/features", identity, lambda root: self.fail("rebuilt"))
            self.assertTrue(reused)
            self.assertEqual(first, second)
            with self.assertRaisesRegex(ValueError, "identity"):
                checked(storage, "/features", {"kind": "wrong"})
            storage.write_bytes("/features/extra", b"corrupt")
            with self.assertRaisesRegex(ValueError, "inventory"):
                checked(storage, "/features")
            storage.remove("/features/extra")
            storage.remove("/features/_SUCCESS")
            with self.assertRaisesRegex(ValueError, "completion"):
                checked(storage, "/features")

    def test_ml_archive_is_reproducible_compilable_and_legacy_hashes_unchanged(self):
        spec = importlib.util.spec_from_file_location("ml_packaging_test", APP / "package_code.py")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        with tempfile.TemporaryDirectory() as directory:
            destination = Path(directory)
            self.assertEqual("ae42a5981bc20e1295df1dabbfab7e553af0f133a39c97fb52fa795ceafcb554",
                             module.package_code(destination / "ingest.zip")["sha256"])
            self.assertEqual("009c7d7d1d411e3f1810096f9c2954235ee6b92c7a3789514fe026b9af3516ac",
                             module.package_code(destination / "stream.zip", profile="streaming")["sha256"])
            first = module.package_code(destination / "ml.zip", profile="ml")
            self.assertEqual(first["sha256"], module.package_code(destination / "again.zip", profile="ml")["sha256"])
            with zipfile.ZipFile(first["archive"]) as archive:
                self.assertIn("ecommerce_ml/integration.py", archive.namelist())
                self.assertTrue(all(name.startswith(("ecommerce_analytics/", "ecommerce_ml/")) for name in archive.namelist()))
                for name in archive.namelist():
                    compile(archive.read(name), name, "exec")
