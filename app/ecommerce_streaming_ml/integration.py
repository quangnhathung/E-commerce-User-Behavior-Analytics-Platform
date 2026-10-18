"""Real Spark/Kafka/HDFS/Mongo integration, using UUID-owned models and namespaces."""
from datetime import datetime, timedelta, timezone
import io
import json
from pathlib import Path
import tempfile
import threading
import time
import unittest
from unittest import mock
import uuid

from pyspark.sql import functions as F, types as T
from ecommerce_analytics.contracts import canonical_json
from ecommerce_ml.contracts import FEATURES, fingerprint, reference
from ecommerce_ml.features import rows
from ecommerce_streaming.contracts import instant, iso, utc_now
from ecommerce_streaming.kafka_io import create_test_topics, delete_test_topics
from ecommerce_streaming.storage import Storage
from .batch import Processor
from .contracts import PIPELINE_VERSION, simulator_events
from .fixtures import START, END, build_fixture
from .inference import verify
from .models import Models, resolve
from .prepare import prepare
from .producer import replay
from .stream import run_stream


def run_tests(spark, store, code_sha256, hashes):
    token = uuid.uuid4().hex
    base = "/ecommerce/_tests/streaming-ml/" + token
    raw, dlq = "app-stream-ml-" + token, "app-stream-ml-dlq-" + token
    storage, run_ids, topics, bundle, outcome = Storage(spark), [], [], None, None
    print("STREAM_ML_TEST_ROOT=" + base, flush=True)

    def request(source="replay"):
        identity="test-ml-"+uuid.uuid4().hex
        run_ids.append(identity)
        return {"run_id":identity,"start":START,"end":END,"rate":10,"demo_source":source,
            "pipeline_version":PIPELINE_VERSION,"behavior_version":"behavior-v1","code_sha256":code_sha256,
            "dataset_version":bundle["dataset_version"],"silver_path":bundle["silver_path"],"base":base,
            "segment_model_version":bundle["segment_model_version"],"purchase_model_version":bundle["purchase_model_version"],
            "policy_version":bundle["policy_version"],"topic":raw,"dlq_topic":dlq}

    def config_for(source="replay", end=END):
        spec=request(source)
        spec["end"]=end
        with tempfile.TemporaryDirectory(prefix="stream-ml-fixture-") as directory:
            result=prepare(spark,spec,str(Path(directory)/"source.jsonl"),store,hashes)
        return result["config"]

    def frame(config, payloads, offsets=None):
        current=dict(offsets or config["initial_offsets"]); values=[]
        for index,payload in enumerate(payloads):
            partition=str(index%3)
            value=payload if isinstance(payload,bytes) else canonical_json({**payload,
                "run_id":payload.get("run_id",config["run_id"]),"emitted_at":utc_now()}).encode()
            values.append((b"fixture",value,raw,int(partition),current[partition],datetime.now(timezone.utc)))
            current[partition]+=1
        schema=T.StructType([T.StructField("key",T.BinaryType()),T.StructField("value",T.BinaryType()),
            T.StructField("topic",T.StringType()),T.StructField("partition",T.IntegerType()),
            T.StructField("offset",T.LongType()),T.StructField("timestamp",T.TimestampType())])
        return spark.createDataFrame(values,schema).repartition(6),current

    def gold(config,batch_id):return config["paths"]["gold"]+f"/{batch_id:020d}"

    def fault_at(stage):
        def fault(actual):
            if actual==stage:raise RuntimeError("injected "+stage)
        return fault

    try:
        topics=[raw,dlq]
        create_test_topics(raw,dlq)
        bundle=build_fixture(spark,storage,store.db,base+"/fixture",hashes)
        common=resolve(spark,storage,store.db,request(),hashes)
        models=Models(spark,common)

        class StreamingMLTests(unittest.TestCase):
            def test_a_prepare_reuse_and_pinned_models(self):
                spec=request()
                with tempfile.TemporaryDirectory() as directory:
                    first=prepare(spark,spec,str(Path(directory)/"a.jsonl"),store,hashes)
                    second=prepare(spark,spec,str(Path(directory)/"b.jsonl"),store,hashes)
                self.assertFalse(first["reused"]);self.assertTrue(second["reused"])
                self.assertEqual(first["config"],second["config"])
                config=first["config"]
                seed=spark.read.parquet(config["seed"]["path"])
                self.assertFalse(seed.where(F.col("event_time")>=F.lit(instant(START))).limit(1).count())
                self.assertEqual(len(bundle["replay_events"]),config["event_count"])
                bad=dict(hashes);bad["prediction"]="0"*64
                with self.assertRaisesRegex(ValueError,"provenance"):resolve(spark,storage,store.db,spec,bad)
                with self.assertRaisesRegex(ValueError,"observation cutoff"):
                    resolve(spark,storage,store.db,{**spec,"start":"2015-05-15T00:00:00.000Z"},hashes)
                with self.assertRaises(ValueError):
                    prepare(spark,{**spec,"rate":5},"/tmp/unused-stream-ml-source",store,hashes)

            def test_b_simulator_feature_oracle_and_batch_parity_without_fit(self):
                config=config_for("simulator");events=simulator_events(config);offsets=None
                with mock.patch("ecommerce_ml.training.fit",side_effect=AssertionError("must not fit KMeans")), \
                        mock.patch("ecommerce_prediction.training.fit_pipeline",side_effect=AssertionError("must not fit RF")), \
                        mock.patch("ecommerce_ml.native.NativeML.load",side_effect=AssertionError("must not reload models per batch")):
                    for phase in range(3):
                        incoming,offsets=frame(config,events[phase*3:phase*3+3],offsets)
                        Processor(spark,storage,store,config,models)(incoming,phase)
                        quality,scored=verify(spark,storage,config,gold(config,phase))
                        expected=reference(events[:phase*3+3],quality["as_of"])
                        actual=list(rows(scored.select("visitor_id","as_of","last_event_time",*FEATURES)))
                        self.assertEqual(expected,[{**v,"as_of":iso(v["as_of"]),"last_event_time":iso(v["last_event_time"])} for v in actual])
                        self.assertEqual(3,quality["fingerprint"]["rows"])
                        self.assertIsNotNone(store.status(config,"000099000000003")["visitor"])

            def test_c_fault_retries_keep_gold_fingerprints_and_counts(self):
                for stage in ("after_bronze","after_silver","after_gold","mongo_mid_ml","mongo_before_pointer","after_mongo_publish"):
                    with self.subTest(stage=stage):
                        config=config_for();incoming,_=frame(config,bundle["replay_events"])
                        with self.assertRaisesRegex(RuntimeError,"injected"):
                            Processor(spark,storage,store,config,models,fault_at(stage))(incoming,0)
                        path=gold(config,0)
                        before=storage.sha256(path+"/manifest.json") if storage.exists(path) else None
                        Processor(spark,storage,store,config,models)(incoming,0)
                        if before:self.assertEqual(before,storage.sha256(path+"/manifest.json"))
                        state=store.status(config)
                        store.verify_ml(config,0,state["batch"]["ml"])
                        self.assertEqual(state["overview"]["visitors"],state["scoring"]["fingerprint"]["rows"])

            def test_d_validation_duplicate_and_no_acceptance_batch(self):
                config=config_for();source=bundle["replay_events"]
                incoming,offsets=frame(config,source+[source[0],b"{",{**source[0],"run_id":"foreign"}])
                Processor(spark,storage,store,config,models)(incoming,0)
                state=store.status(config);counts=state["batch"]["counts"]
                self.assertEqual({"accepted":len(source),"duplicate":1,"invalid":1,"foreign":1,"late":0},counts)
                fingerprint_before=state["scoring"]["fingerprint"]
                empty,_=frame(config,[source[0]],offsets)
                Processor(spark,storage,store,config,models)(empty,1)
                self.assertEqual(fingerprint_before,store.status(config)["scoring"]["fingerprint"])
                waiting=config_for();bad,_=frame(waiting,[b"{"])
                Processor(spark,storage,store,waiting,models)(bad,0)
                self.assertEqual("waiting_for_events",store.status(waiting)["scoring"]["status"])
                delayed=config_for(end="2015-05-16T03:00:00.000Z")
                advanced={**source[0],"event_time":"2015-05-16T00:20:00.000Z"}
                first,offsets=frame(delayed,[advanced])
                Processor(spark,storage,store,delayed,models)(first,0)
                boundary={**source[2],"event_time":"2015-05-16T00:10:00.000Z"}
                second,_=frame(delayed,[source[1],boundary],offsets)
                Processor(spark,storage,store,delayed,models)(second,1)
                self.assertEqual(1,store.status(delayed)["batch"]["counts"]["late"])
                self.assertEqual(1,store.status(delayed)["batch"]["counts"]["accepted"])

            def test_e_corruption_and_missing_checkpoint_are_refused(self):
                config=config_for();incoming,_=frame(config,bundle["replay_events"])
                Processor(spark,storage,store,config,models)(incoming,0)
                with self.assertRaisesRegex(ValueError,"Checkpoint missing"):run_stream(spark,config,store,hashes)
                storage.remove(gold(config,0)+"/_SUCCESS")
                with self.assertRaisesRegex(ValueError,"Incomplete snapshot"):
                    Processor(spark,storage,store,config,models)(incoming,0)
                # A published Mongo corruption is detected, never healed from the next retry.
                other=config_for();incoming,_=frame(other,bundle["replay_events"])
                Processor(spark,storage,store,other,models)(incoming,0)
                row=store.db.stream_ml_predictions.find_one({"run_id":other["run_id"]})
                store.db.stream_ml_predictions.update_one({"_id":row["_id"]},{"$set":{"purchase_score":-1.0}})
                with self.assertRaises(ValueError):Processor(spark,storage,store,other,models)(incoming,0)
                self.assertEqual(-1.0,store.db.stream_ml_predictions.find_one({"_id":row["_id"]})["purchase_score"])

            def test_y_real_kafka_checkpoint_retry_and_completed_reuse(self):
                config=config_for()
                lines=storage.read_json(config["paths"]["prepared"]+"/config.json")
                self.assertEqual(config,lines)
                replay(io.StringIO("".join(canonical_json(e)+"\n" for e in bundle["replay_events"])),config,store)
                with self.assertRaisesRegex(Exception,"injected"):
                    run_stream(spark,config,store,hashes,fault=fault_at("after_mongo_publish"))
                report=run_stream(spark,config,store,hashes)
                self.assertTrue(report["batch_reconciled"]);self.assertEqual(0,report["backlog"])
                self.assertTrue(run_stream(spark,config,store,hashes)["reused"])

            def test_z_real_simulator_waits_for_three_committed_phases(self):
                config=config_for("simulator");events=simulator_events(config);failures=[]
                def produce():
                    try:replay(io.StringIO("".join(canonical_json(e)+"\n" for e in events)),config,store)
                    except BaseException as error:failures.append(error)
                thread=threading.Thread(target=produce,daemon=True);thread.start()
                try:report=run_stream(spark,config,store,hashes)
                finally:
                    store.consumer_status(config,"failed" if thread.is_alive() else store.load(config)["consumer"]["status"])
                    thread.join(10)
                self.assertFalse(thread.is_alive());self.assertFalse(failures,repr(failures))
                self.assertEqual(9,report["overview"]["processed_events"])
                self.assertEqual(3,report["scoring"]["fingerprint"]["rows"])
                self.assertGreaterEqual(store.db.stream_ml_snapshots.count_documents({"run_id":config["run_id"]}),3)
                self.assertTrue(report["batch_reconciled"])

        outcome=unittest.TextTestRunner(verbosity=2).run(unittest.defaultTestLoader.loadTestsFromTestCase(StreamingMLTests))
        if not outcome.wasSuccessful():raise RuntimeError(f"Streaming ML integration failed: {len(outcome.failures)} failures, {len(outcome.errors)} errors")
        result={"passed":True,"tests_run":outcome.testsRun,"fault_scenarios":7,"minimum_block_replicas":2,
                "test_root":base,"run_ids":run_ids}
    finally:
        cleanup=[]
        try:
            store.cleanup(run_ids)
            for name,field in (("segment_models","model_path"),("purchase_models","model_path"),("purchase_policies","selection_path")):
                for record in store.db[name].find({}):
                    if record[field].startswith(base+"/"):
                        if name=="segment_models":
                            for child in ("visitor_segments","segment_profiles"):
                                store.db[child].delete_many({"model_version":record["model_version"]})
                        store.db[name].delete_one({"_id":record["_id"]})
            storage.remove(base)
            if storage.exists(base):raise ValueError("Owned streaming ML HDFS root remains after cleanup")
            for name in ("stream_runs","stream_batches","kpi_overview","kpi_minute","funnel_overview","funnel_products",
                         "stream_ml_snapshots","stream_ml_predictions"):
                if store.db[name].count_documents({"run_id":{"$in":run_ids}}):raise ValueError("Owned Mongo data remains: "+name)
            for name,field in (("segment_models","model_path"),("purchase_models","model_path"),("purchase_policies","selection_path")):
                if any(record[field].startswith(base+"/") for record in store.db[name].find({})):
                    raise ValueError("Owned fixture model/policy remains after cleanup")
        except Exception as error:cleanup.append(error)
        if topics:
            try:delete_test_topics(topics)
            except Exception as error:cleanup.append(error)
        if cleanup:raise RuntimeError("Streaming ML cleanup failed: "+"; ".join(map(str,cleanup)))
    result["cleaned"]=True
    return result
