"""Reject changed provenance and future policy observations before loading a model."""
import copy
import types
import unittest
from unittest import mock

from helpers import APP
from test_streaming_ml import request
from ecommerce_ml.contracts import PARAMS, FEATURES, digest
from ecommerce_streaming_ml.models import resolve


class ModelBindingTests(unittest.TestCase):
    def setUp(self):
        self.hashes={key:str(index)*64 for index,key in enumerate(("ingest","ml","prediction","purchase-policy"),1)}
        self.request=request(demo_source="replay")
        self.feature={"kind":"features","feature_version":"features-v1","lookback_days":7,"as_of":"2015-07-15T00:00:00.000Z",
            "code_sha256":self.hashes["ml"],"silver_path":"/silver","dataset_version":self.request["dataset_version"],"silver_manifest_sha256":"a"*64}
        self.segment_identity={"kind":"segments","model_version":"segments-v1","parameters":PARAMS,"spark_version":"3.5.7",
            "code_sha256":self.hashes["ml"],"feature_snapshot_id":digest(self.feature),"feature_manifest_sha256":"b"*64,
            "feature_path":"/features","as_of":self.feature["as_of"]}
        self.segment={"identity":self.segment_identity,"model_version":digest(self.segment_identity),"model_path":"/segment",
            "report":{"selected_k":2,"feature_order":list(FEATURES)}}
        self.request["segment_model_version"]=self.segment["model_version"]
        self.purchase={"model_version":self.request["purchase_model_version"],"model_path":"/purchase",
            "identity":{"model_contract":"purchase-rf-v1","code_sha256":self.hashes["prediction"],"data_manifest_sha256":"f"*64},
            "report":{"model_manifest_sha256":"c"*64,"evaluation_manifest_sha256":"7"*64,
                "selection":{"threshold":.93,"winner":{"manifest_sha256":"8"*64}}}}
        self.policy={"policy_version":self.request["policy_version"],"model_version":self.purchase["model_version"],
            "identity":{"model_manifest_sha256":"c"*64,"data_manifest_sha256":"f"*64,
                "candidate_manifest_sha256":"8"*64,"original_evaluation_manifest_sha256":"7"*64,
                "original_prediction_code_sha256":self.hashes["prediction"]},
            "report":{"selection_manifest_sha256":"d"*64,"evaluation_manifest_sha256":"e"*64}}
        self.evaluation={"old_threshold":.93,"threshold":.96,"evaluation_kind":"posthoc_threshold_reassessment"}
        self.data={"identity":{"dataset_version":self.request["dataset_version"],"silver_path":"/silver","silver_manifest_sha256":"a"*64}}
        self.index={"days":[{"split":"test","as_of":"2015-09-17T00:00:00.000Z"}]}
        self.silver={"identity":{"kind":"silver","source":"retailrocket","schema_version":1,"transform_version":"events-v1",
            "dataset_version":self.request["dataset_version"],"code_sha256":self.hashes["ingest"]},
            "metadata":{"quality":{"quality_status":"passed","min_event_time":"2015-05-03T00:00:00.000Z","max_event_time":"2015-09-18T02:59:47.788Z"}}}
        self.storage=mock.Mock()
        checksums={"/features/manifest.json":"b"*64,"/silver/manifest.json":"a"*64,"/segment/manifest.json":"0"*64}
        self.storage.sha256.side_effect=checksums.__getitem__
        self.storage.read_json.return_value=self.segment["report"]

    def call(self,request_override=None,hashes=None):
        def checked(storage,path,*args):
            return {"/segment":{"metadata":self.segment["report"]},"/features":{"identity":self.feature},"/silver":self.silver}[path]
        with mock.patch("ecommerce_streaming_ml.models.SegmentStore") as segment, \
                mock.patch("ecommerce_streaming_ml.models.checked",side_effect=checked), \
                mock.patch("ecommerce_streaming_ml.models.model_source",return_value=(self.purchase,self.data,self.index)), \
                mock.patch("ecommerce_streaming_ml.models.load_policy",return_value=(self.policy,self.evaluation)):
            segment.return_value.status.return_value=self.segment
            return resolve(types.SimpleNamespace(version="3.5.7"),self.storage,object(),request_override or self.request,hashes or self.hashes)

    def test_valid_binding_keeps_threshold_ids_source_and_original_archive_hashes(self):
        result=self.call()
        self.assertEqual(.96,result["threshold"]);self.assertEqual(.93,result["original_threshold"])
        self.assertEqual(self.hashes,result["frozen_hashes"])
        self.assertEqual("2015-09-18T00:00:00.000Z",result["not_before"])
        self.assertFalse(result["fresh_holdout"])

    def test_changed_rf_code_is_not_compared_to_online_archive_or_silently_accepted(self):
        changed=dict(self.hashes);changed["prediction"]="9"*64
        with self.assertRaisesRegex(ValueError,"provenance"):self.call(hashes=changed)

    def test_policy_must_belong_to_rf_and_original_threshold(self):
        self.policy["model_version"]="0"*64
        with self.assertRaisesRegex(ValueError,"provenance"):self.call()
        self.policy["model_version"]=self.purchase["model_version"];self.evaluation["old_threshold"]=.1
        with self.assertRaisesRegex(ValueError,"threshold"):self.call()

    def test_replay_cannot_precede_last_observed_policy_label_window(self):
        with self.assertRaisesRegex(ValueError,"observation cutoff"):
            self.call({**self.request,"start":"2015-09-10T10:00:00.000Z"})

    def test_feature_source_and_silver_hash_must_match_both_models(self):
        self.data["identity"]["silver_manifest_sha256"]="9"*64
        with self.assertRaisesRegex(ValueError,"share"):self.call()

    def test_missing_or_ineligible_policy_failure_propagates(self):
        # The frozen policy loader performs the full validation/test eligibility checks.
        with mock.patch("ecommerce_streaming_ml.models.SegmentStore") as segment, \
                mock.patch("ecommerce_streaming_ml.models.checked",return_value={"metadata":self.segment["report"],"identity":self.feature}), \
                mock.patch("ecommerce_streaming_ml.models.model_source",return_value=(self.purchase,self.data,self.index)), \
                mock.patch("ecommerce_streaming_ml.models.load_policy",side_effect=ValueError("not eligible")):
            segment.return_value.status.return_value=self.segment
            with self.assertRaisesRegex(ValueError,"not eligible"):
                resolve(types.SimpleNamespace(version="3.5.7"),self.storage,object(),self.request,self.hashes)
