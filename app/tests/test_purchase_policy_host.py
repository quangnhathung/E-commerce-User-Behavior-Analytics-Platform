"""Host selectors, isolated latest pointers and resource acceptance."""
import json
from pathlib import Path
import tempfile
import types
import unittest
from unittest import mock

from helpers import APP
from test_streaming_host import load


class PolicyHostTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):cls.host=load('policy_host_tests',APP/'purchase_policy_manage.py')

    def args(self,action):
        return types.SimpleNamespace(action=action,model_version=None,policy_version=None,score_run_id=None)

    def test_resource_gate_and_plain_python_status(self):
        with mock.patch.object(self.host,'launch',return_value={'passed':True,'resources':{'no_oom_or_restart':True,'memory_observations_complete':False}}) as launch:
            with self.assertRaises(self.host.infra.InfraError):self.host.run('tune-purchase-policy',{},Path('/unused'))
            self.assertEqual('purchase-policy',launch.call_args.kwargs['profile'])
            self.host.run('purchase-policy-status',{},Path('/unused'))
            self.assertFalse(launch.call_args.kwargs['spark_job']);self.assertFalse(launch.call_args.kwargs['monitor'])

    def test_ineligible_evaluation_is_saved_without_changing_original_pointers(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);policies=root/'policies'
            original=root/'latest-purchase-model.json';original.write_text(json.dumps({'model_version':'a'*64}))
            with mock.patch.object(self.host,'ARTIFACTS',root),mock.patch.object(self.host,'POLICIES',policies), \
                    mock.patch.object(self.host,'run',return_value={'policy_version':'b'*64,'eligible':False,'threshold':None,'reused':False}) as run:
                self.host.dispatch(self.args('tune-purchase-policy'))
            self.assertEqual({'model_version':'a'*64},json.loads(original.read_text()))
            self.assertFalse(json.loads((policies/('b'*64)/'report.json').read_text())['eligible'])
            self.assertEqual({'model_version':'a'*64},run.call_args.args[1])

    def test_apply_uses_original_source_pointer_and_separate_policy_score_pointer(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);policies=root/'policies';policies.mkdir()
            (policies/'latest-policy.json').write_text(json.dumps({'policy_version':'b'*64}))
            original=root/'latest-purchase-score.json';original.write_text(json.dumps({'score_run_id':'a'*64}))
            with mock.patch.object(self.host,'ARTIFACTS',root),mock.patch.object(self.host,'POLICIES',policies), \
                    mock.patch.object(self.host,'run',return_value={'score_run_id':'c'*64,'reused':True}) as run:
                self.host.dispatch(self.args('apply-purchase-policy'))
            self.assertEqual({'score_run_id':'a'*64},json.loads(original.read_text()))
            self.assertEqual({'policy_version':'b'*64,'source_score_run_id':'a'*64},run.call_args.args[1])
            self.assertEqual('c'*64,json.loads((policies/'latest-policy-score.json').read_text())['score_run_id'])

    def test_failed_apply_or_resource_gate_does_not_advance_latest(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);pointer=root/'latest-policy-score.json';pointer.write_text('previous')
            args=self.args('apply-purchase-policy');args.policy_version='b'*64;args.score_run_id='a'*64
            with mock.patch.object(self.host,'ARTIFACTS',root),mock.patch.object(self.host,'POLICIES',root), \
                    mock.patch.object(self.host,'run',side_effect=self.host.infra.InfraError('not eligible')):
                with self.assertRaisesRegex(self.host.infra.InfraError,'eligible'):self.host.dispatch(args)
            self.assertEqual('previous',pointer.read_text())

    def test_explicit_policy_status_is_scoped(self):
        with tempfile.TemporaryDirectory() as directory,mock.patch.object(self.host,'POLICIES',Path(directory)), \
                mock.patch.object(self.host,'run',return_value={'status':'published'}) as run:
            args=self.args('purchase-policy-status');args.policy_version='b'*64
            self.host.dispatch(args)
            self.assertEqual({'policy_version':'b'*64},run.call_args.args[1])
