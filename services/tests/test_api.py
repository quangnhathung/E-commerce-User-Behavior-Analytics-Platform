import copy
import os
import unittest
import uuid
from unittest import mock

from fastapi.testclient import TestClient
from serving.api import create_app
from fixtures import Database, seed


class APITests(unittest.TestCase):
    def database(self): return Database()

    def setUp(self):
        self.db = self.database()
        self.config = seed(self.db)
        self.client = self.enterContext(TestClient(create_app(self.db), raise_server_exceptions=False))

    def get(self, endpoint, **query):
        return self.client.get('/api/v1/'+endpoint, params={"run_id": "web-fixture", **query})

    def test_health_and_read_only_methods(self):
        self.assertEqual(200, self.client.get('/api/v1/health').status_code)
        self.assertEqual(405, self.client.post('/api/v1/overview').status_code)
        self.assertEqual(200, self.client.get('/openapi.json').status_code)

    def test_live_server_cors_and_remote_origins(self):
        for origin in ['http://127.0.0.1:5500','http://localhost:5501']:
            response=self.client.get('/api/v1/overview?run_id=web-fixture',headers={'Origin':origin})
            self.assertEqual(origin,response.headers.get('access-control-allow-origin'))
        response=self.client.get('/api/v1/overview?run_id=web-fixture',headers={'Origin':'https://example.com'})
        self.assertNotIn('access-control-allow-origin',response.headers)

    def test_run_list_hides_test_namespaces_and_old_pipelines(self):
        for run_id, key, value in [('test-hidden','base','/ecommerce/_tests/streaming-ml/abc'),('old','pipeline_version','stream-v2')]:
            row = self.db['stream_runs'].find_one({'_id':'web-fixture'})
            row['_id']=row['run_id']=run_id;row['config'][key]=value
            self.db['stream_runs'].insert_one(row)
        self.assertEqual(['web-fixture'],[r['run_id'] for r in self.client.get('/api/v1/runs').json()['data']])
        self.assertEqual(404,self.get('overview',run_id='test-hidden').status_code)

    def test_unknown_run_and_invalid_arguments(self):
        self.assertEqual(404,self.get('overview',run_id='missing').status_code)
        for endpoint, params in [('overview',{'batch_id':-1}),('overview',{'run_id':'bad/run'}),
                ('predictions',{'limit':201}),('predictions',{'predicted_label':2}),('predictions',{'cluster_id':3})]:
            with self.subTest(endpoint=endpoint,params=params):self.assertEqual(422,self.get(endpoint,**params).status_code)
        self.assertEqual(422,self.client.get('/api/v1/overview').status_code)

    def test_snapshot_and_overview_match(self):
        response=self.get('overview')
        self.assertEqual(200,response.status_code)
        self.assertEqual(1,response.json()['batch_id'])
        self.assertEqual(self.config['expected']['overview'],response.json()['data'])
        self.assertEqual('no-store',response.headers['cache-control'])

    def test_future_batch_rows_are_invisible(self):
        self.assertEqual(409,self.get('overview',batch_id=2).status_code)
        self.assertEqual(9,self.get('pipeline').json()['data']['counts']['accepted'])
        self.assertEqual(3,len(self.get('predictions').json()['data']))

    def test_pointer_advance_does_not_change_a_pinned_page(self):
        first=self.get('predictions',batch_id=1,limit=1).json()
        self.db['stream_runs'].update_one({'_id':'web-fixture'},{'$set':{'committed_batch_id':2}})
        second=self.get('predictions',batch_id=1,limit=1,after=first['next_cursor']).json()
        self.assertEqual(1,second['batch_id'])
        self.assertNotEqual(first['data'][0]['visitor_id'],second['data'][0]['visitor_id'])
        self.assertEqual(422,self.get('predictions',batch_id=2,after=first['next_cursor']).status_code)

    def test_stable_tie_pagination_and_filter_binding(self):
        page=self.get('predictions',limit=1).json(); seen=[page['data'][0]['visitor_id']]
        while page['next_cursor']:
            page=self.get('predictions',limit=1,after=page['next_cursor']).json()
            seen.extend(r['visitor_id'] for r in page['data'])
        self.assertEqual(['000099000000003','990000000001','990000000002'],seen)
        cursor=self.get('predictions',limit=1).json()['next_cursor']
        self.assertEqual(422,self.get('predictions',after=cursor,predicted_label=1).status_code)
        self.assertEqual(2,len(self.get('predictions',predicted_label=1).json()['data']))
        self.assertEqual(422,self.get('predictions',after='bad!').status_code)

    def test_visitor_leading_zero_and_not_found(self):
        row=self.get('visitors/000099000000003').json()['data']
        self.assertEqual('000099000000003',row['visitor_id'])
        self.assertEqual(1,row['transaction_count_7d'])
        self.assertEqual(404,self.get('visitors/99000000003').status_code)
        self.assertEqual(422,self.get('visitors/abc').status_code)

    def test_missing_prediction_is_not_reported_as_zero(self):
        self.db['stream_ml_predictions'].delete_one({'_id':'web-fixture:1:000099000000003'})
        self.assertEqual(503,self.get('overview').status_code)

    def test_wrong_model_binding_in_prediction_is_rejected(self):
        self.db['stream_ml_predictions'].update_one({'_id':'web-fixture:1:000099000000003'},
                                                   {'$set':{'policy_version':'f'*64}})
        self.assertEqual(503,self.get('visitors/000099000000003').status_code)

    def test_missing_funnel_product_is_not_published_as_empty(self):
        self.db['funnel_products'].delete_one({'_id':'web-fixture:1:10'})
        self.assertEqual(503,self.get('funnel').status_code)

    def test_consumer_failure_text_is_redacted(self):
        self.db['stream_runs'].update_one({'_id':'web-fixture'},{'$set':{'consumer.reason':'secret-password'}})
        self.assertNotIn('secret-password',self.get('pipeline').text)
        self.assertNotIn('secret-password',self.client.get('/api/v1/runs').text)

    def test_missing_registry_or_mismatched_quality_is_rejected(self):
        self.db['stream_ml_snapshots'].update_one({'_id':'web-fixture:1'},{'$set':{'status':'prepared'}})
        self.assertEqual(503,self.get('overview').status_code)

    def test_waiting_and_failed_without_snapshot(self):
        self.db['stream_runs'].update_one({'_id':'web-fixture'},{'$set':{'committed_batch_id':-1}})
        result=self.get('overview').json()
        self.assertIsNone(result['data']);self.assertEqual('waiting_for_events',result['status'])
        self.db['stream_runs'].update_one({'_id':'web-fixture'},{'$set':{'consumer.status':'failed'}})
        self.assertEqual('not_ready',self.get('overview').json()['status'])

    def test_timeseries_timezone_and_half_open_filter(self):
        result=self.get('timeseries',start='2015-09-18T07:01:00+07:00',end='2015-09-18T00:02:00Z').json()['data']
        self.assertEqual(1,len(result));self.assertEqual('2015-09-18T00:01:00.000Z',result[0]['window_start'])
        for params in [{'start':'2015-09-18T00:00:00'},{'start':'2015-09-19T00:00:00Z','end':'2015-09-18T00:00:00Z'}]:
            self.assertEqual(422,self.get('timeseries',**params).status_code)

    def test_empty_cluster_and_snapshot_funnel(self):
        result=self.get('segments').json()['data']
        self.assertEqual([2,1,0],[r['count'] for r in result])
        self.assertIsNone(result[2]['raw_feature_means'])
        self.assertTrue(self.get('funnel').json()['data']['is_final'])

    def test_models_are_bound_to_the_run(self):
        models=self.get('models').json()['data']
        self.assertEqual(self.config['bindings']['purchase_model_version'],models['purchase']['version'])
        self.db['purchase_models'].update_one({'_id':models['purchase']['version']},{'$set':{'status':'prepared'}})
        self.assertEqual(503,self.get('models').status_code)

    def live_fixture(self, **changes):
        from serving.contracts import now
        from ecommerce_streaming.contracts import digest
        request={**self.config,'rate':5,'events_per_segment':9}
        row={'_id':'live-fixture','live_id':'live-fixture','base':'/ecommerce','request':request,
            'request_sha256':digest(request),'owner':'private-owner','heartbeat_at':now(),'created_at':now(),
            'status':'running','active_run_id':'web-fixture','last_snapshot_run_id':None,'segment_index':1,
            'completed_segments':0,'completed_events':0,'acknowledged_events':9,**changes}
        self.db['live_sessions'].insert_one(row)
        return row

    def test_live_sessions_are_read_only_local_and_hide_ownership(self):
        self.live_fixture()
        self.live_fixture(_id='hidden',live_id='hidden',base='/ecommerce/_tests/live')
        response=self.client.get('/api/v1/live/sessions')
        self.assertEqual(200,response.status_code)
        self.assertEqual(['live-fixture'],[r['live_id'] for r in response.json()['data']])
        self.assertNotIn('private-owner',response.text)
        self.assertEqual(404,self.client.get('/api/v1/live/sessions/hidden').status_code)
        self.assertEqual(405,self.client.post('/api/v1/live/sessions/live-fixture').status_code)
        self.assertEqual(422,self.client.get('/api/v1/live/sessions/bad%20id').status_code)

    def test_live_heartbeat_is_stale_but_stopped_is_not_running(self):
        self.live_fixture(heartbeat_at='2026-10-20T00:00:00.000Z')
        with mock.patch('serving.live.now',return_value='2026-10-20T00:00:21.000Z'):
            data=self.client.get('/api/v1/live/sessions/live-fixture').json()['data']
            self.assertEqual('stale',data['status'])
            self.db['live_sessions'].update_one({'_id':'live-fixture'},{'$set':{'status':'stopped'}})
            self.assertEqual('stopped',self.client.get('/api/v1/live/sessions/live-fixture').json()['data']['status'])

    def test_live_rotation_retains_previous_snapshot_and_counts_once(self):
        from ecommerce_streaming.contracts import digest
        self.live_fixture(last_snapshot_run_id='web-fixture',completed_segments=1,completed_events=9,segment_index=2,active_run_id='next')
        row=self.db['stream_runs'].find_one({'_id':'web-fixture'})
        row['_id']=row['run_id']='next';row['config']['run_id']='next';row['config_sha256']=digest(row['config'])
        row['committed_batch_id']=-1;row['producer']['last_seq']=-1
        self.db['stream_runs'].insert_one(row)
        data=self.client.get('/api/v1/live/sessions/live-fixture').json()['data']
        self.assertEqual('web-fixture',data['snapshot_run_id']);self.assertEqual(1,data['snapshot_batch_id'])
        self.assertEqual(9,data['processed_events']);self.assertEqual(9,data['acknowledged_events'])
        self.db['live_sessions'].update_one({'_id':'live-fixture'},{'$set':{'active_run_id':'web-fixture','segment_index':1}})
        self.assertEqual(9,self.client.get('/api/v1/live/sessions/live-fixture').json()['data']['processed_events'])

    def test_live_events_cursor_gap_and_ack_is_not_processed(self):
        self.live_fixture()
        for sequence in range(100,105):
            self.db['live_events'].insert_one({'_id':str(sequence),'live_id':'live-fixture','sequence':sequence,
                'run_id':'web-fixture','segment_index':1,'event_id':str(sequence),'visitor_id':'0001','item_id':'10',
                'event_type':'view','event_time':'2015-09-18T00:00:00.000Z','emitted_at':'2026-10-20T00:00:00.000Z',
                'acked_at':'2026-10-20T00:00:00.001Z','partition':'0','next_offset':sequence-99})
        base='/api/v1/live/sessions/live-fixture/events'
        tail=self.client.get(base+'?limit=2').json()
        self.assertEqual([103,104],[r['sequence'] for r in tail['data']]);self.assertEqual(104,tail['next_after'])
        self.assertEqual('acked',tail['data'][0]['processing_status'])
        page=self.client.get(base+'?after=99&limit=2').json()
        self.assertEqual([100,101],[r['sequence'] for r in page['data']])
        self.assertEqual('processed',page['data'][0]['processing_status']);self.assertEqual('0001',page['data'][0]['visitor_id'])
        self.assertEqual([],self.client.get(base+'?after=104').json()['data'])
        self.assertTrue(self.client.get(base+'?after=0&limit=2').json()['gap'])
        self.assertEqual(422,self.client.get(base+'?limit=201').status_code)
        self.assertEqual(422,self.client.get(base+'?after=-2').status_code)

    def test_backend_failure_is_redacted(self):
        with mock.patch.object(self.client.app.state.repository,'context',side_effect=RuntimeError('secret-password')):
            response=self.get('overview')
            self.assertEqual(503,response.status_code)
            self.assertNotIn('secret-password',response.text)


@unittest.skipUnless(os.getenv('SERVING_LIVE_MONGO')=='1','requires make test-web and the real MongoDB')
class LiveMongoAPITests(APITests):
    def database(self):
        from pymongo import MongoClient
        self.mongo=MongoClient(os.getenv('MONGO_HOST','127.0.0.1'),port=int(os.getenv('MONGO_PORT','27018')),
            username=os.environ['MONGO_USERNAME'],password=os.environ['MONGO_PASSWORD'],
            authSource=os.environ['MONGO_DATABASE'],serverSelectionTimeoutMS=5000)
        raw=self.mongo[os.environ['MONGO_DATABASE']]
        prefix='_webtest_'+uuid.uuid4().hex+'_'
        owned=set()

        class Namespace:
            def __getitem__(self,key):
                owned.add(prefix+key)
                return raw[prefix+key]
            def command(self,name):return raw.command(name)

        def cleanup():
            try:
                for name in owned:raw.drop_collection(name)
                remaining=set(raw.list_collection_names()) & owned
                if remaining:raise AssertionError('Serving fixture cleanup incomplete')
            finally:self.mongo.close()
        self.addCleanup(cleanup)
        return Namespace()


if __name__=='__main__':unittest.main()
