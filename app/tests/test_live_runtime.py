"""Lifecycle boundary tests: one model load, rotation, pause and checkpoint guards."""
import importlib.util
import io
import json
import os
from pathlib import Path
import signal
import sys
import time
import tempfile
import types
import unittest
import uuid
from unittest import mock

from helpers import APP
from test_live import Database, request
from ecommerce_live.contracts import child_request
from ecommerce_live.store import LiveStore


def runtime_module():
    spec=importlib.util.spec_from_file_location('ecommerce_live.runtime_boundary',APP/'ecommerce_live/runtime.py')
    module=importlib.util.module_from_spec(spec)
    with mock.patch.dict(sys.modules, {
        'ecommerce_streaming_ml.batch':types.SimpleNamespace(Processor=mock.Mock()),
        'ecommerce_streaming_ml.stream':types.SimpleNamespace(final_report=mock.Mock(),verify_committed=mock.Mock())}):
        spec.loader.exec_module(module)
    return module


class LiveRuntimeTests(unittest.TestCase):
    def test_rotation_and_resume_do_not_reload_models_per_segment_or_recount(self):
        runtime=runtime_module();store=LiveStore(Database());req=request()
        storage=mock.Mock();storage.fs.getStatus.return_value.getRemaining.return_value=10**12
        visited=[]
        def prepare(storage,store,req,index,bindings,seed):
            return child_request(req,index),[]
        def process(spark,storage,store,config,source,models,owner,stopping):
            visited.append(config['segment_index'])
            if config['segment_index'] in (2,3):store.request_stop(req['live_id'],owner)
            return {'overview':{'processed_events':1000}}
        with mock.patch.object(runtime,'Storage',return_value=storage), \
                mock.patch.object(runtime,'resolve',return_value={}),mock.patch.object(runtime,'prepare_seed',return_value={}), \
                mock.patch.object(runtime,'prepare_segment',side_effect=prepare),mock.patch.object(runtime,'run_segment',side_effect=process), \
                mock.patch.object(runtime,'Models') as models, mock.patch.object(runtime.shutil,'disk_usage',return_value=types.SimpleNamespace(free=10**12)):
            first=runtime.run(mock.Mock(),store,req,{},'owner')
            self.assertEqual([1,2],visited);self.assertEqual(2000,first['completed_events'])
            models.assert_called_once()
            second=runtime.run(mock.Mock(),store,req,{},'next-owner')
            self.assertEqual([1,2,3],visited);self.assertEqual(3000,second['completed_events'])
            self.assertEqual('stopped',second['status']);self.assertEqual(2,models.call_count)

    def test_disk_reserve_stops_before_preparing_another_source(self):
        runtime=runtime_module();store=LiveStore(Database());req=request()
        storage=mock.Mock();storage.fs.getStatus.return_value.getRemaining.return_value=1
        with mock.patch.object(runtime,'Storage',return_value=storage),mock.patch.object(runtime,'resolve',return_value={}), \
                mock.patch.object(runtime,'prepare_seed',return_value={}),mock.patch.object(runtime,'Models'), \
                mock.patch.object(runtime.shutil,'disk_usage',return_value=types.SimpleNamespace(free=10**12)), \
                mock.patch.object(runtime,'prepare_segment') as prepare:
            with self.assertRaisesRegex(RuntimeError,'disk reserve'):
                runtime.run(mock.Mock(),store,req,{},'owner')
            prepare.assert_not_called();self.assertEqual('failed',store.session(req['live_id'])['status'])

    def segment(self,runtime,*,batches=False,metadata=False,completed=False):
        state={'consumer':{'status':'completed' if completed else 'running'},'producer':{'status':'running','end_offsets':{'0':0}},
            'batch':None,'committed_batch_id':-1}
        store=mock.Mock();store.status.side_effect=lambda config:state
        storage=mock.Mock();storage.batch_paths.return_value=['/batch'] if batches else []
        storage.exists.return_value=metadata;storage.checkpoint_commits.return_value=set()
        config={**child_request(request(),1),'paths':{'prepared':'/prepared','checkpoint':'/checkpoint',
            'bronze':'/bronze','silver':'/silver','gold':'/gold'},'initial_offsets':{'0':0}}
        return config,store,storage,state

    def test_pause_with_no_events_is_not_a_completed_reconciliation(self):
        runtime=runtime_module();config,store,storage,state=self.segment(runtime)
        class Query:
            isActive=True
            def awaitTermination(self,seconds):time.sleep(.001)
            def stop(self):self.isActive=False
        query=Query()
        class Builder:
            @property
            def writeStream(self):return self
            def __getattr__(self,key):return lambda *args,**kwargs:query if key=='start' else self
        spark=types.SimpleNamespace(readStream=Builder())
        def emit(*args,**kwargs):state['producer']['status']='paused'
        with mock.patch.object(runtime,'read_config'),mock.patch.object(runtime,'emit',side_effect=emit), \
                mock.patch.dict(os.environ,{'KAFKA_BOOTSTRAP_SERVERS':'kafka:9092'}):
            result=runtime.run_segment(spark,storage,store,config,[],mock.Mock(),'owner',lambda:True)
        self.assertIsNone(result);store.finish.assert_not_called()
        self.assertIn(mock.call(config,'paused'),store.consumer_status.call_args_list)
        runtime.final_report.assert_not_called()

    def test_missing_checkpoint_and_completed_commit_are_refused(self):
        for completed in (False,True):
            with self.subTest(completed=completed):
                runtime=runtime_module();config,store,storage,state=self.segment(runtime,batches=not completed,completed=completed)
                with mock.patch.object(runtime,'read_config'),self.assertRaises(ValueError):
                    runtime.run_segment(mock.Mock(),storage,store,config,[],mock.Mock(),'owner',lambda:False)


class LiveHostTests(unittest.TestCase):
    def test_observation_tail_is_bounded_and_remembers_error_count(self):
        from live_monitor import Tail
        samples=Tail(720)
        for index in range(5000):samples.append(index)
        self.assertEqual(720,len(samples));self.assertEqual(5000,samples.total)
        self.assertEqual(4280,samples[0])
        errors=Tail(100)
        for _ in range(200):errors.append('InfraError')
        self.assertTrue(errors);self.assertEqual(200,errors.total)

    def load_host(self):
        spec=importlib.util.spec_from_file_location('live_host_boundary',APP/'live_manage.py')
        module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module);return module

    def test_only_live_driver_uses_unbounded_wrapper(self):
        host=self.load_host()
        with mock.patch.object(host,'launch',return_value={}) as launch:
            host.invoke('live-demo',{},Path('/unused'))
            self.assertEqual('run-live',launch.call_args.kwargs['wrapper_action'])
            self.assertTrue(launch.call_args.kwargs['monitor'])
            self.assertTrue(launch.call_args.kwargs['producer_owned'])
            host.invoke('stop-live',{},Path('/unused'))
            self.assertEqual('run',launch.call_args.kwargs['wrapper_action'])
            self.assertFalse(launch.call_args.kwargs['spark_job'])

    def test_unconfirmed_live_cancellation_guards_driver_and_producer(self):
        import streaming_manage as host
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder);directory=root/'live/test'
            process=mock.Mock(stdout=io.StringIO(''))
            process.wait.return_value=1;process.poll.return_value=1
            def execute(args,**kwargs):
                if 'cancel' in args:raise host.infra.InfraError('unconfirmed')
                return json.dumps({'sha256':'a'*64}) if '--profile' in args else ''
            with mock.patch.object(host,'ROOT',root),mock.patch.object(host,'ARTIFACTS',root/'streaming'), \
                    mock.patch.object(host.infra,'ARTIFACTS',root/'infra'),mock.patch.object(host,'upload_spec'), \
                    mock.patch.object(host.infra,'execute',side_effect=execute), \
                    mock.patch.object(host.subprocess,'Popen',return_value=process):
                with self.assertRaises(host.infra.InfraError):
                    host.launch('live-demo',{},directory,profile='streaming-live',spark_job=True,monitor=False,producer_owned=True)
            driver=json.loads((root/'infra/driver-unconfirmed.json').read_text())
            producer=json.loads((root/'streaming/producer-unconfirmed.json').read_text())
            self.assertEqual(driver,producer);self.assertEqual('live-demo',driver['action'])

    def test_unbounded_wrapper_keeps_cancellation_without_changing_old_timeout(self):
        spec=importlib.util.spec_from_file_location('live_wrapper_boundary',APP/'run_job.py')
        wrapper=importlib.util.module_from_spec(spec);spec.loader.exec_module(wrapper)
        for bounded in (False,True):
            with self.subTest(bounded=bounded),mock.patch.object(wrapper.signal,'signal'), \
                    mock.patch.object(wrapper.subprocess,'Popen') as popen:
                popen.return_value.wait.return_value=0;popen.return_value.poll.return_value=0
                self.assertEqual(0,wrapper.run(uuid.uuid4().hex,['spark-submit','live.py'],bounded=bounded))
                command=popen.call_args.args[0]
                self.assertEqual(['timeout','--kill-after=15s','1800s'] if bounded else [],command[:-2])
                self.assertTrue(popen.call_args.kwargs['start_new_session'])

    def test_ctrl_c_requests_control_plane_stop_before_cancelling_driver(self):
        host=self.load_host();installed=[]
        with mock.patch.object(host,'specification',return_value={}), \
                mock.patch.object(host.signal,'signal',side_effect=lambda sig,handler:installed.append(handler)), \
                mock.patch.object(host,'atomic_json'):
            calls=[]
            def invoke(action,spec,directory,**kw):
                calls.append(action)
                if action=='live-demo':
                    installed[0](signal.SIGINT,None)
                    for _ in range(100):
                        if 'stop-live' in calls:break
                        time.sleep(.01)
                    return {'passed':True,'status':'stopped','resources':{'no_oom_or_restart':True,'memory_observations_complete':True}}
                return {'passed':True}
            with mock.patch.object(host,'invoke',side_effect=invoke):
                host.run_demo(types.SimpleNamespace(live_id='live-test'),{},Path('/unused'))
            self.assertEqual(['live-demo','stop-live'],calls)

    def test_failed_attempt_does_not_leave_a_previous_passed_report(self):
        host=self.load_host()
        with tempfile.TemporaryDirectory() as folder:
            directory=Path(folder)
            (directory/'report.json').write_text(json.dumps({'passed':True,'status':'completed'}))
            with mock.patch.object(host,'specification',return_value={}), \
                    mock.patch.object(host,'invoke',side_effect=host.infra.InfraError('failed')):
                with self.assertRaises(host.infra.InfraError):
                    host.run_demo(types.SimpleNamespace(live_id='live-test'),{},directory)
            report=json.loads((directory/'report.json').read_text())
            self.assertFalse(report['passed']);self.assertEqual('failed',report['status'])
