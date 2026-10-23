"""Disposable real Spark models with genuine saved scalar predictions for live tests."""
from datetime import timedelta

from ecommerce_analytics.snapshots import materialize, write_json
from ecommerce_analytics.ingestion import silver_schema
from ecommerce_ml.contracts import FEATURES, FEATURE_VERSION, digest, instant, iso
from ecommerce_ml.features import build_features
from ecommerce_ml.store import Store as SegmentStore
from ecommerce_ml.training import train_segments
from ecommerce_prediction.contracts import DATA_VERSION, LABEL, MODEL_VERSION, METRIC_VERSION, row_fingerprint, composite
from ecommerce_prediction.training import fit_pipeline, score_frame, fingerprints
from ecommerce_prediction.metrics import evaluate, threshold_metrics
from ecommerce_purchase_policy.store import Store as PolicyStore
from ecommerce_purchase_policy.workflow import tune

START = "2015-05-16T00:00:00.000Z"
END = "2015-05-16T00:10:00.000Z"


def build_fixture(spark, storage, database, base, hashes):
    """No production pointer or model is read or overwritten by this fixture."""
    dataset = digest(["stream-ml-fixture", base])
    events = []
    def event(who, item, kind, when, transaction=None):
        payload = {"schema_version": 1, "dataset_version": dataset, "source": "retailrocket", "visitor_id": who,
            "item_id": item, "event_type": kind, "transaction_id": transaction, "event_time": when}
        payload["event_id"] = digest([base, len(events), payload]); events.append(payload)
    event("9999998", "1", "view", "2015-05-03T00:00:00.000Z")
    for index in range(121):
        who = "001" if index == 120 else f"{index:04d}"
        for number in range(1 + index % 5):
            event(who, str(10 + number), "view", iso(instant(START) - timedelta(hours=12, seconds=index * 10 + number)))
        if index % 3 == 0:
            event(who, "10", "add_to_cart", iso(instant(START) - timedelta(hours=11, seconds=index)))
    replay_events = []
    for minute, who, item, kind, transaction in [(0,"0001","10","view",None), (1,"001","20","view",None),
            (2,"0001","10","add_to_cart",None), (6,"0001","10","purchase","900"),
            (6,"0001","11","purchase","900"), (7,"3","30","view",None)]:
        event(who,item,kind,iso(instant(START)+timedelta(minutes=minute)),transaction)
        replay_events.append(events[-1])
    for index in range(121):
        who = "001" if index == 120 else f"{index:04d}"
        event(who,"40","view",iso(instant(START)+timedelta(minutes=4,seconds=index//3)))
        replay_events.append(events[-1])
    replay_events.sort(key=lambda value:(value["event_time"],value["event_id"]))
    event("0001","999","view","2015-05-16T00:11:00.000Z")  # Must never leak into the prepared run/seed.
    schema = silver_schema()
    typed = []
    for value in events:
        record = dict.fromkeys(schema.fieldNames())
        record.update(value, event_time=instant(value["event_time"]), event_date=instant(value["event_time"]).date())
        typed.append(record)
    frame = spark.createDataFrame(typed, schema)
    source_identity = {"kind":"silver","source":"retailrocket","dataset_version":dataset,
        "schema_version":1,"transform_version":"events-v1","code_sha256":hashes["ingest"]}
    silver_path = base + "/source"
    def silver(staging):
        frame.write.partitionBy("event_date").parquet(staging+"/events")
        return {"quality":{"quality_status":"passed","min_event_time":"2015-05-03T00:00:00.000Z",
                           "max_event_time":"2015-05-16T00:11:00.000Z"}}
    materialize(storage,silver_path,source_identity,silver)
    feature = build_features(spark,{"feature_version":FEATURE_VERSION,"as_of":START,"dataset_version":dataset,
        "silver_path":silver_path,"code_sha256":hashes["ml"],"base":base})
    segment = train_segments(spark,SegmentStore(database),{"feature_snapshot_id":feature["feature_snapshot_id"],
        "feature_path":feature["feature_path"],"feature_manifest_sha256":feature["manifest_sha256"],"code_sha256":hashes["ml"],"base":base})

    from pyspark.sql import types as T
    typed_schema = T.StructType([T.StructField("visitor_id",T.StringType()),T.StructField("as_of",T.TimestampType()),
        T.StructField("last_event_time",T.TimestampType()), *[T.StructField(k,T.LongType()) for k in FEATURES[:5]],
        *[T.StructField(k,T.DoubleType()) for k in FEATURES[5:]], T.StructField(LABEL,T.IntegerType())])
    schedule = [{"split":name,"as_of":day+"T00:00:00.000Z"} for name,day in
                (("train","2015-05-11"),("validation","2015-05-13"),("test","2015-05-15"))]
    frames, days = {}, []
    for entry in schedule:
        values=[]
        for index in range(121):
            positive = index < 20
            values.append({"visitor_id":f"{index:04d}","as_of":instant(entry["as_of"]),
                "last_event_time":instant(entry["as_of"])-timedelta(hours=2),"view_count_7d":20 if positive else 1,
                "cart_count_7d":10 if positive else 0,"transaction_count_7d":0,"unique_items_7d":1,"active_days_7d":1,
                "recency_hours":2.0,"cart_view_ratio_7d":.5 if positive else 0.0,"transaction_view_ratio_7d":0.0,LABEL:int(positive)})
        frames[entry["split"]]=spark.createDataFrame(values,typed_schema)
        days.append({**entry,"fingerprint":row_fingerprint(values,labeled=True),"path":base+"/fixture-days/"+entry["split"]})
    def snapshot(path,identity,filename,metadata,extra=None):
        def build(staging):
            if extra: extra(staging)
            write_json(storage,staging+"/"+filename,metadata)
            return metadata
        materialize(storage,path,identity,build)
        return storage.sha256(path+"/manifest.json")
    data_identity={"kind":"purchase-data","data_version":DATA_VERSION,"feature_version":FEATURE_VERSION,
        "schedule":schedule,"code_sha256":hashes["prediction"],"silver_path":silver_path,"dataset_version":dataset,
        "silver_manifest_sha256":storage.sha256(silver_path+"/manifest.json"),"fixture":True}
    data_id=digest(data_identity)
    config={"identity":data_identity,"purchase_dataset_id":data_id,"base":base,"root":base+"/gold/purchase-data/"+data_id}
    index={"passed":True,"days":days,"feature_order":list(FEATURES),
        "splits":{name:composite([day for day in days if day["split"]==name]) for name in frames}}
    data_path=config["root"]+"/index"
    data_hash=snapshot(data_path,{"kind":"purchase-index","config":config},"index.json",index)
    parameters={"numTrees":5,"maxDepth":2,"featureSubsetStrategy":"all"}
    pipeline=fit_pipeline(spark,frames["train"],parameters)
    scored={name:score_frame(spark,pipeline,frame,labeled=True) for name,frame in frames.items() if name!="train"}
    identity={"kind":"purchase-model","model_contract":MODEL_VERSION,"feature_order":list(FEATURES),
        "spark_version":spark.version,"code_sha256":hashes["prediction"],"purchase_dataset_id":data_id,
        "data_manifest_sha256":data_hash,"fixture":True}
    version=digest(identity); candidate_path=base+"/models/candidate/"+version
    validation=evaluate(spark,scored["validation"],.5)
    candidate={"passed":True,"parameters":parameters,"metrics":{k:validation[k] for k in ("pr_auc","roc_auc")},
        "validation_fingerprint":fingerprints(scored["validation"],[schedule[1]])}
    candidate_hash=snapshot(candidate_path,{"kind":"purchase-candidate","model_version":version,"parameters":parameters},
        "quality.json",candidate,lambda path:scored["validation"].write.parquet(path+"/validation_scores"))
    selection={"threshold":.5,"winner":{**candidate,"candidate_path":candidate_path,"manifest_sha256":candidate_hash},
        "threshold_grid":threshold_metrics(scored["validation"]),"validation_metrics":validation}
    model_path=base+"/models/purchases/"+version
    model_hash=snapshot(model_path,identity,"selection.json",selection,lambda path:pipeline.write().save(path+"/pipeline"))
    labeled=score_frame(spark,pipeline,frames["test"],labeled=True,threshold=.5)
    evaluation={"passed":True,"test":evaluate(spark,labeled,.5),"test_fingerprint":fingerprints(labeled,[schedule[2]],threshold=.5)}
    evaluation_path=base+"/evaluations/"+version
    evaluation_hash=snapshot(evaluation_path,{"kind":"purchase-evaluation","model_version":version,
        "model_manifest_sha256":model_hash,"data_manifest_sha256":data_hash,"metric_version":METRIC_VERSION},
        "evaluation.json",evaluation,lambda path:labeled.write.parquet(path+"/test_scores"))
    policy_store=PolicyStore(database)
    policy_store.publish_model({"identity":identity,"model_version":version,"model_path":model_path,"data":{"data_path":data_path}},
        {"selection":selection,"evaluation":evaluation,"model_manifest_sha256":model_hash,
         "evaluation_manifest_sha256":evaluation_hash,"evaluation_path":evaluation_path})
    policy=tune(spark,policy_store,version,hashes["purchase-policy"])
    if not policy["eligible"]: raise ValueError("Controlled fixture policy unexpectedly failed qualification")
    return {"dataset_version":dataset,"silver_path":silver_path,"segment_model_version":segment["model_version"],
        "purchase_model_version":version,"policy_version":policy["policy_version"],"events":events,"replay_events":replay_events,
        "segment":segment,"purchase_model_path":model_path}
