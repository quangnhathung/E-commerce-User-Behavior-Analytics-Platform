"""Real stateful registry/upsert boundaries with fault injection."""
import copy
import types
import unittest
from unittest import mock

from helpers import APP
from ecommerce_prediction.contracts import digest,row_fingerprint
from ecommerce_prediction.store import Store as OriginalStore
from ecommerce_purchase_policy.contracts import SCORE_CONTRACT
from ecommerce_purchase_policy.store import Store
from test_prediction import fixture_rows
from test_streaming_store import Database,Replace


class PolicyStoreTests(unittest.TestCase):
    def setUp(self):
        patch=mock.patch.dict('sys.modules',{'pymongo':types.SimpleNamespace(ReplaceOne=Replace)})
        patch.start();self.addCleanup(patch.stop)
        self.db=Database();self.store=Store(self.db)
        self.policy={'kind':'purchase-policy','model_version':'a'*64}
        self.policy_id=digest(self.policy)
        self.identity={'kind':'purchase-policy-score','score_version':SCORE_CONTRACT,'model_version':'a'*64,
            'policy_version':self.policy_id,'source_score_run_id':'b'*64,'threshold':.96}
        self.key=digest(self.identity)
        self.rows=[{**r,'purchase_score':.96 if i%2 else .95,'predicted_label':i%2} for i,r in enumerate(fixture_rows())]
        self.report={'threshold':.96,'fingerprint':row_fingerprint(self.rows,scored=True,threshold=.96)}

    def publish(self,fault=None):
        self.store.publish_policy_scores(self.identity,'/new-scores',self.report,iter(self.rows),fault=fault)

    def test_policy_prepared_hidden_and_pointer_retry_immutable(self):
        self.store.prepare_policy(self.policy,'/selection','/evaluation',False)
        with self.assertRaisesRegex(ValueError,'not published'):self.store.policy_status(self.policy_id)
        for expected in ('before_policy_pointer','after_policy_pointer'):
            def fault(stage):
                if stage==expected:raise RuntimeError('injected')
            with self.assertRaisesRegex(RuntimeError,'injected'):
                self.store.publish_policy(self.policy,'/selection','/evaluation',{'eligible':True},fault=fault)
            self.store.publish_policy(self.policy,'/selection','/evaluation',{'eligible':True})
            self.assertEqual('published',self.store.policy_status(self.policy_id)['status'])
        with self.assertRaisesRegex(ValueError,'report'):
            self.store.publish_policy(self.policy,'/selection','/evaluation',{'eligible':False})

    def test_missing_published_policy_gold_and_changed_path_refused(self):
        self.store.publish_policy(self.policy,'/selection','/evaluation',{})
        with self.assertRaisesRegex(ValueError,'missing'):
            self.store.prepare_policy(self.policy,'/selection','/evaluation',False)
        with self.assertRaisesRegex(ValueError,'mismatch'):
            self.store.prepare_policy(self.policy,'/other','/evaluation',True)

    def test_partial_scores_hidden_and_retry_has_no_duplicates(self):
        def fault(stage):
            if stage=='mongo_mid_policy_scores':raise RuntimeError('partial')
        with self.assertRaises(RuntimeError):self.publish(fault)
        self.assertEqual(100,len(self.db.purchase_scores.documents))
        with self.assertRaises(ValueError):self.store.score_status(self.key)
        self.publish();self.publish()
        self.assertEqual(121,len(self.db.purchase_scores.documents))

    def test_score_pointer_before_after_faults_and_original_status_compatibility(self):
        for expected in ('before_policy_score_pointer','after_policy_score_pointer'):
            self.db.purchase_scores.documents.clear();self.db.purchase_score_runs.documents.clear()
            def fault(stage):
                if stage==expected:raise RuntimeError('injected')
            with self.assertRaises(RuntimeError):self.publish(fault)
            if expected.startswith('before'):
                with self.assertRaises(ValueError):self.store.score_status(self.key)
            self.publish()
            value=OriginalStore(self.db).score_status(self.key,'0001')
            self.assertEqual(self.policy_id,value['visitor']['policy_version'])
            self.assertEqual('b'*64,value['visitor']['source_score_run_id'])
            self.assertIsNone(OriginalStore(self.db).score_status(self.key,'1')['visitor'])

    def test_corrupt_published_scores_or_policy_metadata_are_not_healed(self):
        self.publish();key=self.key+':0001'
        for field,value in [('purchase_score',.97),('policy_version','c'*64),('source_score_run_id','d'*64),('predicted_label',0)]:
            old=copy.deepcopy(self.db.purchase_scores.documents[key])
            self.db.purchase_scores.documents[key][field]=value
            with self.assertRaises(ValueError):self.publish()
            self.assertEqual(value,self.db.purchase_scores.documents[key][field])
            self.db.purchase_scores.documents[key]=old

    def test_missing_coverage_never_publishes_and_original_documents_untouched(self):
        self.db.purchase_scores.documents['original']={'source':'untouched'}
        self.rows.pop()
        with self.assertRaisesRegex(ValueError,'fingerprint'):self.publish()
        with self.assertRaises(ValueError):self.store.score_status(self.key)
        self.assertEqual({'source':'untouched'},self.db.purchase_scores.documents['original'])

    def test_missing_published_score_gold_is_refused(self):
        self.publish()
        with self.assertRaisesRegex(ValueError,'missing'):
            self.store.prepare_policy_scores(self.identity,'/new-scores',False)
