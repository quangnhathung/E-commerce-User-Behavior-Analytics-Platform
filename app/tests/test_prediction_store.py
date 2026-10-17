"""Stateful publication boundary tests, including partial batches and corruption."""
import copy
import types
import unittest
from unittest import mock

from helpers import APP
from ecommerce_prediction.contracts import digest, row_fingerprint
from ecommerce_prediction.store import Store
from test_prediction import fixture_rows
from test_streaming_store import Database, Replace


class PurchaseStoreTests(unittest.TestCase):
    def setUp(self):
        patch = mock.patch.dict('sys.modules', {'pymongo': types.SimpleNamespace(ReplaceOne=Replace)})
        patch.start(); self.addCleanup(patch.stop)
        self.db=Database();self.store=Store(self.db)
        self.model_identity={'kind':'purchase-model','code_sha256':'a'*64}
        self.version=digest(self.model_identity)
        self.training={'identity':self.model_identity,'model_version':self.version,'model_path':'/model',
                       'data':{'data_path':'/data/index'}}
        self.identity={'kind':'purchase-score','model_version':self.version,'silver_path':'/fixture/silver'}
        self.run_id=digest(self.identity)
        self.rows=[{**row,'purchase_score':.75 if index%2 else .25,'predicted_label':index%2}
                   for index,row in enumerate(fixture_rows())]
        self.report={'threshold':.5,'fingerprint':row_fingerprint(self.rows,scored=True,threshold=.5)}

    def publish(self, fault=None):
        self.store.publish_scores(self.identity,'/scores',self.report,iter(self.rows),fault=fault)

    def test_partial_batch_hidden_retry_complete_without_duplicate(self):
        def fault(stage):
            if stage=='mongo_mid_purchase_scores':raise RuntimeError('partial')
        with self.assertRaisesRegex(RuntimeError,'partial'):self.publish(fault)
        self.assertEqual(100,len(self.db.purchase_scores.documents))
        with self.assertRaisesRegex(ValueError,'not published'):self.store.score_status(self.run_id)
        self.publish()
        self.assertEqual(121,len(self.db.purchase_scores.documents))
        self.store.verify_scores(self.run_id,self.report,.5)

    def test_score_pointer_faults_preserve_visibility_and_retry_is_immutable(self):
        for stage in ('before_purchase_score_pointer','after_purchase_score_pointer'):
            with self.subTest(stage=stage):
                self.db.purchase_score_runs.documents.clear();self.db.purchase_scores.documents.clear()
                def fault(actual):
                    if actual==stage:raise RuntimeError('injected')
                with self.assertRaisesRegex(RuntimeError,'injected'):self.publish(fault)
                if stage.startswith('before'):
                    with self.assertRaises(ValueError):self.store.score_status(self.run_id)
                else:self.assertEqual('published',self.store.score_status(self.run_id)['status'])
                previous=copy.deepcopy(self.db.purchase_scores.documents)
                self.publish();self.publish()
                self.assertEqual(previous,self.db.purchase_scores.documents)

    def test_missing_or_changed_scores_never_publish(self):
        self.rows.pop()
        with self.assertRaisesRegex(ValueError,'fingerprint'):self.publish()
        with self.assertRaises(ValueError):self.store.score_status(self.run_id)

    def test_corrupt_published_score_or_metadata_is_detected_without_healing(self):
        self.publish()
        key=self.run_id+':0000'
        for field,value in [('purchase_score',.8),('threshold',.7),('feature_version','wrong'),('model_version','b'*64)]:
            with self.subTest(field=field):
                original=copy.deepcopy(self.db.purchase_scores.documents[key])
                self.db.purchase_scores.documents[key][field]=value
                with self.assertRaises(ValueError):self.publish()
                self.assertEqual(value,self.db.purchase_scores.documents[key][field])
                self.db.purchase_scores.documents[key]=original

    def test_exact_lookup_preserves_ids_and_default_status_does_not_list_visitors(self):
        self.publish()
        self.assertNotIn('visitor',self.store.score_status(self.run_id))
        self.assertEqual('0001',self.store.score_status(self.run_id,'0001')['visitor']['visitor_id'])
        self.assertIsNone(self.store.score_status(self.run_id,'1')['visitor'])
        with self.assertRaises(ValueError):self.store.score_status(self.run_id,'../outside')

    def test_prepared_model_hidden_and_model_pointer_faults_retry(self):
        self.store.prepare_model(self.training,False)
        with self.assertRaises(ValueError):self.store.model_status(self.version)
        def fault(stage):
            if stage=='before_purchase_model_pointer':raise RuntimeError('injected')
        with self.assertRaises(RuntimeError):self.store.publish_model(self.training,{'test':'metrics'},fault=fault)
        with self.assertRaises(ValueError):self.store.model_status(self.version)
        self.store.publish_model(self.training,{'test':'metrics'})
        self.assertEqual('published',self.store.model_status(self.version)['status'])
        self.store.publish_model(self.training,{'test':'metrics'})
        with self.assertRaisesRegex(ValueError,'report'):
            self.store.publish_model(self.training,{'test':'changed'})

    def test_published_missing_gold_and_changed_identity_paths_are_refused(self):
        self.publish()
        with self.assertRaisesRegex(ValueError,'missing'):
            self.store.prepare_scores(self.identity,'/scores',False)
        with self.assertRaisesRegex(ValueError,'mismatch'):
            self.store.prepare_scores(self.identity,'/other',True)
        self.store.publish_model(self.training,{'test':'metrics'})
        with self.assertRaisesRegex(ValueError,'missing'):
            self.store.prepare_model(self.training,False)
