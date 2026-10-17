"""Host pointers, stage failures and resource gates at real local file boundaries."""
import json
from pathlib import Path
import tempfile
import types
import unittest
from unittest import mock

from helpers import APP
from test_streaming_host import load


class PurchaseHostTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.host = load('purchase_host_tests', APP/'prediction_manage.py')

    def args(self, action):
        return types.SimpleNamespace(action=action, purchase_dataset_id=None, model_version=None,
            score_run_id=None, visitor_id=None, as_of=self.host.DEFAULT_SCORE_AS_OF)

    def test_launcher_prediction_profile_status_and_resource_rejection(self):
        with mock.patch.object(self.host, 'launch', return_value={'passed':True,'resources':{
                'no_oom_or_restart':True,'memory_observations_complete':False}}) as launch:
            with self.assertRaisesRegex(self.host.infra.InfraError,'resource'):
                self.host.run('train-purchase-candidate', {}, Path('/unused'))
            self.assertEqual({'profile':'prediction','entry':'prediction.py','result_prefix':'PREDICTION_RESULT=',
                             'spark_job':True,'monitor':True},launch.call_args.kwargs)
            self.host.run('purchase-status', {}, Path('/unused'))
            self.assertFalse(launch.call_args.kwargs['spark_job'])
            self.assertFalse(launch.call_args.kwargs['monitor'])

    def test_failed_chunk_keeps_latest_pointer_and_records_only_completed_progress(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory); artifacts=root/'artifacts/prediction'
            source=root/'artifacts/data/latest-ingest.json'; source.parent.mkdir(parents=True)
            source.write_text(json.dumps({'quality_status':'passed','dataset_version':'source','silver_path':'/silver'}))
            artifacts.mkdir(parents=True)
            latest=artifacts/'latest-purchase-data.json';latest.write_text('{"purchase_dataset_id":"previous"}')
            chunk1=[{'as_of':'2015-05-11T00:00:00.000Z','split':'train'}]
            chunk2=[{'as_of':'2015-05-13T00:00:00.000Z','split':'validation'}]
            captured=[]
            def run(action,spec,destination):
                captured.append((action,spec('code-sha')))
                if action=='prepare-purchase-data':return {'config':{'purchase_dataset_id':'a'*64},'chunks':[chunk1,chunk2]}
                if spec('code-sha')['entries']==chunk2:raise self.host.infra.InfraError('chunk failed')
                return {'reused':False,'days':chunk1,'log':'first-log','resources':{'accepted':True},'duration_seconds':1}
            with mock.patch.object(self.host,'ROOT',root),mock.patch.object(self.host,'ARTIFACTS',artifacts), \
                    mock.patch.object(self.host,'run',side_effect=run):
                with self.assertRaisesRegex(self.host.infra.InfraError,'chunk failed'):self.host.build_data()
            self.assertEqual('previous',json.loads(latest.read_text())['purchase_dataset_id'])
            progress=json.loads((artifacts/('a'*64)/'progress.json').read_text())
            self.assertEqual(1,progress['completed_chunks'])
            self.assertEqual(chunk1,progress['days'])
            self.assertEqual('code-sha',captured[0][1]['request']['code_sha256'])
            self.assertFalse(any(action=='finalize-purchase-data' for action,_ in captured))

    def test_complete_reused_data_advances_pointer_after_finalization(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory); source=root/'artifacts/data/latest-ingest.json';source.parent.mkdir(parents=True)
            source.write_text('{"quality_status":"passed","dataset_version":"source","silver_path":"/silver"}')
            artifacts=root/'artifacts/prediction';chunk=[{'as_of':'2015-05-11T00:00:00.000Z','split':'train'}]
            def run(action,spec,destination):
                if action=='prepare-purchase-data':return {'config':{'purchase_dataset_id':'a'*64},'chunks':[chunk]}
                if action=='build-purchase-chunk':return {'reused':True,'days':chunk,'log':'chunk','resources':{},'duration_seconds':1}
                return {'passed':True,'purchase_dataset_id':'a'*64,'reused':True}
            with mock.patch.object(self.host,'ROOT',root),mock.patch.object(self.host,'ARTIFACTS',artifacts), \
                    mock.patch.object(self.host,'run',side_effect=run):self.host.build_data()
            self.assertEqual('a'*64,json.loads((artifacts/'latest-purchase-data.json').read_text())['purchase_dataset_id'])
            self.assertTrue(json.loads((artifacts/('a'*64)/'report.json').read_text())['reused'])

    def test_train_never_builds_missing_data_and_candidate_failure_keeps_old_model(self):
        with tempfile.TemporaryDirectory() as directory,mock.patch.object(self.host,'ARTIFACTS',Path(directory)):
            root=Path(directory)
            with mock.patch.object(self.host,'run') as run:
                with self.assertRaises(FileNotFoundError):self.host.train(self.args('train-purchase'))
                run.assert_not_called()
            (root/'latest-purchase-data.json').write_text(json.dumps({'purchase_dataset_id':'a'*64}))
            data_path=root/('a'*64)/'report.json';data_path.parent.mkdir()
            data_path.write_text(json.dumps({'passed':True,'purchase_dataset_id':'a'*64,'config':{},'data_path':'/index','manifest_sha256':'sha'}))
            pointer=root/'latest-purchase-model.json';pointer.write_text('{"model_version":"previous"}')
            calls=[]
            def run(action,spec,destination):
                calls.append((action,spec('code')))
                if action=='prepare-purchase-training':return {'training':{'model_version':'b'*64},'sample_reused':True}
                if len(calls)==3:raise self.host.infra.InfraError('candidate failed')
                return {'reused':True}
            with mock.patch.object(self.host,'run',side_effect=run),self.assertRaisesRegex(self.host.infra.InfraError,'candidate failed'):
                self.host.train(self.args('train-purchase'))
            self.assertEqual('previous',json.loads(pointer.read_text())['model_version'])
            self.assertEqual(1,len(json.loads((root/('b'*64)/'progress.json').read_text())['candidates']))
            self.assertNotIn('as_of',calls[0][1])

    def test_status_scopes_and_leading_zero_lookup(self):
        with tempfile.TemporaryDirectory() as directory,mock.patch.object(self.host,'ARTIFACTS',Path(directory)), \
                mock.patch.object(self.host,'run',return_value={'status':'published'}) as run:
            args=self.args('purchase-score-status');args.score_run_id='a'*64;args.visitor_id='001'
            self.host.dispatch(args)
            self.assertEqual({'code_sha256':'sha','score_run_id':'a'*64,'visitor_id':'001'},run.call_args.args[1]('sha'))
            args=self.args('purchase-status');args.model_version='b'*64
            self.host.dispatch(args)
            self.assertEqual({'code_sha256':'sha','model_version':'b'*64},run.call_args.args[1]('sha'))

    def test_score_request_pins_model_cutoff_and_saves_score_scope(self):
        with tempfile.TemporaryDirectory() as directory,mock.patch.object(self.host,'ARTIFACTS',Path(directory)), \
                mock.patch.object(self.host,'run',return_value={'passed':True,'score_run_id':'c'*64,'reused':True}) as run:
            args=self.args('score-purchases');args.model_version='b'*64
            self.host.dispatch(args)
            self.assertEqual({'model_version':'b'*64,'as_of':self.host.DEFAULT_SCORE_AS_OF},run.call_args.args[1]('sha')['request'])
            self.assertEqual('c'*64,json.loads((Path(directory)/'latest-purchase-score.json').read_text())['score_run_id'])

    def test_auc_native_intermediate_unpersisted_on_failure(self):
        from ecommerce_prediction.metrics import auc
        native=mock.Mock();frame=mock.Mock()
        frame.agg.return_value.first.return_value=[2,1]
        metrics=native.ml.evaluation.BinaryClassificationEvaluator.return_value.setLabelCol.return_value.setRawPredictionCol.return_value.setNumBins.return_value.getMetrics.return_value
        metrics.areaUnderPR.side_effect=RuntimeError('evaluation failed')
        functions=types.SimpleNamespace(count=mock.Mock(),sum=mock.Mock())
        with mock.patch.dict('sys.modules',{'pyspark.sql':types.SimpleNamespace(functions=functions)}), \
                mock.patch('ecommerce_prediction.metrics.NativeML',return_value=native):
            with self.assertRaisesRegex(RuntimeError,'evaluation failed'):auc(mock.Mock(),frame)
        metrics.unpersist.assert_called_once_with()
