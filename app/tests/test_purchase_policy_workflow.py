"""Snapshot/selection sequencing with real files and bounded scalar adapters."""
import json
import tempfile
import types
import unittest
from unittest import mock

from helpers import APP,LocalStorage
from ecommerce_analytics.snapshots import materialize,write_json
from ecommerce_prediction.contracts import METRIC_VERSION,composite,iso,row_fingerprint
from ecommerce_prediction.metrics import reference_metrics
from ecommerce_purchase_policy.integration import rows_for
from ecommerce_purchase_policy.store import Store
from ecommerce_purchase_policy import workflow
from test_purchase_policy import grid_for
from test_streaming_store import Database


class Frame:
    def __init__(self,rows):self.rows=rows
    def persist(self,level):return self
    def unpersist(self):pass


class PolicyWorkflowTests(unittest.TestCase):
    def setUp(self):
        directory=tempfile.TemporaryDirectory();self.addCleanup(directory.cleanup)
        self.storage=LocalStorage(directory.name);self.store=Store(Database())
        self.code='c'*64;self.version='a'*64
        self.schedule=[{'split':'validation','as_of':'2015-05-13T00:00:00.000Z'},
                       {'split':'test','as_of':'2015-05-15T00:00:00.000Z'}]
        self.val=Frame(rows_for(self.schedule[0]['as_of']))
        self.test=Frame(rows_for(self.schedule[1]['as_of']))
        self.frames={'/candidate/validation_scores':self.val,'/old-test/test_scores':self.test}
        self.spark=types.SimpleNamespace(version='3.5.7',read=types.SimpleNamespace(parquet=self.read))
        self.record={'model_version':self.version,'identity':{'code_sha256':'f'*64,'spark_version':'3.5.7','data_manifest_sha256':'b'*64},
            'report':{'model_manifest_sha256':'d'*64,'evaluation_path':'/old-test'}}
        self.index={'days':self.schedule}
        for patch in (mock.patch.object(workflow,'Hdfs',return_value=self.storage),
                      mock.patch.object(workflow,'model_source',side_effect=lambda *args:(self.record,{'base':'/base'},self.index)),
                      mock.patch.object(workflow,'fingerprints',side_effect=self.fingerprints),
                      mock.patch.object(workflow,'validate_scores'),
                      mock.patch.object(workflow,'threshold_metrics',side_effect=self.thresholds),
                      mock.patch.dict('sys.modules',{'pyspark':types.SimpleNamespace(StorageLevel=types.SimpleNamespace(DISK_ONLY='disk')),
                                                    'pyspark.sql':types.SimpleNamespace(functions=types.SimpleNamespace())})):
            patch.start();self.addCleanup(patch.stop)
        self.refresh_sources()

    def read(self,path):
        if path=='/old-test/test_scores':
            policies=list(self.store.db.purchase_policies.documents.values())
            self.assertTrue(policies and self.storage.exists(policies[-1]['selection_path']+'/_SUCCESS'))
        return self.frames[path]

    def thresholds(self,frame,thresholds=None):
        grid=grid_for([(row['purchase_score'],row['purchase_in_next_24h']) for row in frame.rows])
        return grid if thresholds is None else [next(row for row in grid if row['threshold']==value) for value in thresholds]

    def fingerprints(self,frame,schedule,*,threshold=None):
        days=[{'as_of':entry['as_of'],'fingerprint':row_fingerprint(
            (row for row in frame.rows if iso(row['as_of'])==entry['as_of']),labeled=True,scored=True,threshold=threshold)} for entry in schedule]
        return {'days':days,'summary':composite(days)}

    def refresh_sources(self):
        selection={'threshold':.93,'threshold_grid':self.thresholds(self.val),
            'validation_metrics':reference_metrics(self.val.rows,.93),
            'winner':{'candidate_path':'/candidate','manifest_sha256':'e'*64,
                      'validation_fingerprint':self.fingerprints(self.val,[self.schedule[0]])}}
        evaluation={'passed':True,'test':reference_metrics(self.test.rows,.93),
                    'test_fingerprint':self.fingerprints(self.test,[self.schedule[1]],threshold=.93)}
        identity={'kind':'purchase-evaluation','model_version':self.version,'model_manifest_sha256':'d'*64,
                  'data_manifest_sha256':'b'*64,'metric_version':METRIC_VERSION}
        self.storage.remove('/old-test')
        def build(staging):
            write_json(self.storage,staging+'/evaluation.json',evaluation)
            self.storage.write_bytes(staging+'/saved-scalar-fixture',b'fixture')
            return evaluation
        materialize(self.storage,'/old-test',identity,build)
        self.record['report'].update({'selection':selection,'evaluation':evaluation,
            'evaluation_manifest_sha256':self.storage.sha256('/old-test/manifest.json')})

    def tune(self,fault=None):return workflow.tune(self.spark,self.store,self.version,self.code,fault=fault)

    def test_selection_is_frozen_before_test_and_reassessment_preserves_ranking(self):
        result=self.tune();report=result['report']['evaluation']
        self.assertTrue(result['eligible']);self.assertEqual(.96,result['threshold'])
        self.assertFalse(report['fresh_holdout'])
        self.assertEqual(report['test_original']['pr_auc'],report['test']['pr_auc'])
        self.assertEqual(report['test_original']['top_1pct'],report['test']['top_1pct'])
        self.assertGreater(report['test']['precision'],report['test_original']['precision'])

    def test_failed_validation_does_not_open_test_and_application_blocked(self):
        self.val.rows=rows_for(self.schedule[0]['as_of'],mode='infeasible');self.refresh_sources()
        self.storage.remove('/old-test')
        result=self.tune();self.assertFalse(result['eligible']);self.assertIsNone(result['threshold'])
        self.assertTrue(result['report']['evaluation']['test_skipped'])
        with self.assertRaisesRegex(ValueError,'not eligible'):
            workflow.apply(self.spark,self.store,result['policy_version'],'b'*64,self.code)

    def test_failed_test_is_recorded_and_never_reselects_threshold(self):
        self.test.rows=rows_for(self.schedule[1]['as_of'],mode='test_fail');self.refresh_sources()
        result=self.tune();self.assertEqual(.96,result['threshold']);self.assertFalse(result['eligible'])
        self.assertEqual('test_targets_not_met',result['report']['evaluation']['reason'])
        with self.assertRaisesRegex(ValueError,'not eligible'):
            workflow.apply(self.spark,self.store,result['policy_version'],'b'*64,self.code)

    def test_interruption_after_selection_has_no_evaluation_and_retry_reuses_it(self):
        def fault(stage):
            if stage=='after_policy_selection':raise RuntimeError('injected')
        with self.assertRaises(RuntimeError):self.tune(fault)
        value=next(iter(self.store.db.purchase_policies.documents.values()))
        self.assertTrue(self.storage.exists(value['selection_path']+'/_SUCCESS'))
        self.assertFalse(self.storage.exists(value['evaluation_path']))
        with self.assertRaises(ValueError):self.store.policy_status(value['policy_version'])
        with mock.patch.object(workflow,'threshold_metrics',side_effect=self.thresholds) as metric:
            self.tune()
        self.assertEqual(1,metric.call_count)  # Only the two-point test reassessment remains.

    def test_reuse_does_not_recompute_metrics_and_corrupt_snapshot_is_not_healed(self):
        first=self.tune()
        with mock.patch.object(workflow,'threshold_metrics',side_effect=AssertionError('recomputed')):
            self.assertTrue(self.tune()['reused'])
        self.storage.write_bytes(first['evaluation_path']+'/extra',b'corrupt')
        with self.assertRaisesRegex(ValueError,'inventory mismatch'):self.tune()
        self.assertTrue(self.storage.exists(first['evaluation_path']+'/extra'))

    def test_missing_test_marker_prevents_registry_publish(self):
        self.storage.remove('/old-test/_SUCCESS')
        with self.assertRaisesRegex(ValueError,'Incomplete snapshot'):self.tune()
        value=next(iter(self.store.db.purchase_policies.documents.values()))
        self.assertEqual('prepared',value['status'])
