"""Simulator phases wait on committed ML, and resume retains ACK boundaries."""
import io
import json
import unittest
from unittest import mock

from helpers import APP
from test_streaming_ml import request
from ecommerce_streaming_ml.contracts import simulator_events
from ecommerce_streaming_ml.producer import replay, wait_phase


class SimulatorProducerTests(unittest.TestCase):
    def state(self, ready=False, failed=False):
        return {"consumer":{"status":"failed" if failed else "running"},
            "batch":{"processed_offsets":{"0":3}} if ready else None,
            "committed_batch_id":0,"scoring":{"status":"ready" if ready else "waiting_for_events"}}

    def test_phase_does_not_advance_until_offsets_and_ml_are_committed(self):
        store=mock.Mock(); store.status.side_effect=[self.state(),self.state(True)]
        sleep=mock.Mock()
        wait_phase(request(),store,{"last_seq":2,"end_offsets":{"0":3}},clock=lambda:0,sleep=sleep)
        self.assertEqual(1,sleep.call_count)

    def test_stopped_consumer_and_timeout_fail_without_pretending_completion(self):
        store=mock.Mock(); store.status.return_value=self.state(failed=True)
        with self.assertRaisesRegex(RuntimeError,"consumer stopped"):
            wait_phase(request(),store,{"last_seq":2,"end_offsets":{"0":3}})
        store.status.return_value=self.state()
        with self.assertRaises(TimeoutError):
            wait_phase(request(),store,{"last_seq":2,"end_offsets":{"0":3}},clock=mock.Mock(side_effect=[0,2]),timeout=1)

    def test_resume_at_phase_boundary_waits_before_sending_next_phase(self):
        config={**request(),"event_count":9}
        store=mock.Mock(); store.load.return_value={"producer":{"status":"running","last_seq":2,"end_offsets":{"0":3}}}
        source=io.StringIO(''.join(json.dumps(e)+'\n' for e in simulator_events(config)[3:]))
        trace=[]
        def wait(*args,**kwargs): trace.append('wait')
        def send(*args,**kwargs):trace.append('send');return ('0',4+trace.count('send'))
        with mock.patch('ecommerce_streaming_ml.producer.producer'), \
                mock.patch('ecommerce_streaming_ml.producer.send_confirmed',side_effect=send), \
                mock.patch('ecommerce_streaming_ml.producer.wait_phase',side_effect=wait):
            result=replay(source,config,store)
        self.assertEqual('wait',trace[0]);self.assertEqual(3,trace.count('wait'));self.assertEqual(6,trace.count('send'))
        self.assertEqual('completed',result['producer']['status'])
        self.assertEqual('completed',store.producer_progress.call_args.args[2])

    def test_completed_producer_never_resends(self):
        store=mock.Mock();store.load.return_value={'producer':{'status':'completed','last_seq':8}}
        with mock.patch('ecommerce_streaming_ml.producer.producer') as producer:
            self.assertTrue(replay(io.StringIO(),request(),store)['reused'])
            producer.assert_not_called()
