# Bản đồ trực quan của hệ thống

[← Mục lục tài liệu](../README.md) · [Chạy từ đầu](../GETTING_STARTED.md) · [ML](../ml/README.md) · [API và dashboard](../serving/README.md)

Tài liệu này gom các sơ đồ Mermaid của **E-commerce User Behavior Analytics Platform**. Sơ đồ mô tả cùng một hệ thống ở ba mức: kiến trúc, xử lý model và kết quả trên dashboard. Các mũi tên thể hiện dữ liệu; manifest, model version, policy version và snapshot là metadata dùng để chứng minh nguồn kết quả.

### Quy ước màu

| Nhóm màu | Ý nghĩa |
| --- | --- |
| Xanh xám dịu | Nguồn dữ liệu, event, input hoặc giao diện đọc |
| Xanh olive dịu | Xử lý, validation, training, model hoặc control flow |
| Be dịu | Kết quả, snapshot, registry và output |
| Đỏ đất nhạt | Error, quarantine hoặc DLQ |

Màu nền đã giảm độ bão hòa và dùng chữ tối để giữ độ tương phản khi xem trên nền sáng hoặc dark-mode preview có theme base.

## 1. Kiến trúc từ nguồn đến dashboard

```mermaid
%%{init: {"theme":"base","themeVariables":{"fontFamily":"Inter,Arial,sans-serif","primaryColor":"#e7efee","primaryTextColor":"#173638","primaryBorderColor":"#6f8583","lineColor":"#788987","secondaryColor":"#e9eee5","tertiaryColor":"#f0ebe3"}}}%%
flowchart LR
    subgraph HOST["Máy host"]
        CSV["Retailrocket CSV"]
        MAKE["Make / Python CLI"]
        API["FastAPI :8000"]
        WEB["HTML/CSS/JS<br/>VS Code Live Server"]
    end

    subgraph COMPOSE["Docker Compose"]
        INGEST["Spark ingest"]
        KAFKA["Kafka raw + DLQ"]
        SPARK["Spark master + workers"]
        HDFS["HDFS<br/>Bronze / Silver / Gold / models"]
        MONGO["MongoDB<br/>registry + snapshots"]
    end

    CSV --> INGEST
    MAKE --> INGEST
    INGEST --> HDFS
    HDFS --> SPARK
    KAFKA --> SPARK
    SPARK --> HDFS
    SPARK --> MONGO
    MONGO --> API
    API --> WEB
    MAKE --> KAFKA

    classDef host fill:#e9eeee,stroke:#5d7976,color:#19383b;
    classDef compute fill:#e9eee5,stroke:#77866a,color:#293729;
    classDef storage fill:#eee9e0,stroke:#8b7b63,color:#40372b;
    class CSV,MAKE,API,WEB host;
    class INGEST,SPARK,KAFKA compute;
    class HDFS,MONGO storage;
```

**Cách đọc:** Spark thực hiện batch và streaming trong Compose; API và web chỉ đọc snapshot đã công bố. Model không được fit lại khi streaming inference chạy.

## 2. Vòng đời dữ liệu và kiểm tra chất lượng

```mermaid
%%{init: {"theme":"base","themeVariables":{"fontFamily":"Inter,Arial,sans-serif","primaryColor":"#e7efee","primaryTextColor":"#173638","primaryBorderColor":"#6f8583","lineColor":"#788987","secondaryColor":"#e9eee5","tertiaryColor":"#f0ebe3"}}}%%
flowchart TD
    SOURCE["CSV nguồn"] --> PARSE["Đọc và parse event"]
    PARSE --> BRONZE["Bronze<br/>giữ payload nguồn"]
    BRONZE --> NORMALIZE["Chuẩn hóa schema<br/>ID, event type, timestamp"]
    NORMALIZE --> VALIDATE{"Hợp đồng hợp lệ?"}
    VALIDATE -- "Không" --> QUARANTINE["Quarantine / DLQ<br/>lưu lỗi và source identity"]
    VALIDATE -- "Có" --> DEDUP["Deduplication<br/>business identity"]
    DEDUP --> SILVER["Silver accepted<br/>Parquet + partition"]
    SILVER --> QUALITY["Quality report<br/>counts, min/max time, SHA-256"]
    QUALITY --> MANIFEST["Manifest + _SUCCESS<br/>replication 2"]
    MANIFEST --> READY["Snapshot được phép dùng"]

    classDef source fill:#ebe8e2,stroke:#847a68,color:#3c352b;
    classDef check fill:#e9eeee,stroke:#6f8381,color:#203c3d;
    classDef good fill:#e8eee5,stroke:#77866a,color:#293729;
    classDef bad fill:#f1e5e1,stroke:#a66d62,color:#4a2925;
    class SOURCE,BRONZE source;
    class NORMALIZE,VALIDATE,DEDUP,QUALITY check;
    class SILVER,MANIFEST,READY good;
    class QUARANTINE bad;
```

Kết quả Silver chỉ được sử dụng khi quality report, manifest, checksum và completion marker khớp nhau. Event bị loại vẫn giữ source topic, partition, offset và error code để điều tra.

## 3. Session, funnel và analytics

```mermaid
%%{init: {"theme":"base","themeVariables":{"fontFamily":"Inter,Arial,sans-serif","primaryColor":"#e7efee","primaryTextColor":"#173638","primaryBorderColor":"#6f8583","lineColor":"#788987","secondaryColor":"#e9eee5","tertiaryColor":"#f0ebe3"}}}%%
flowchart LR
    SILVER["Silver events"] --> ORDER["Sắp theo visitor + event_time"]
    ORDER --> SESSION["Sessionization<br/>gap 30 phút"]
    SESSION --> SI["Session-item table"]
    SI --> FUNNEL["Funnel theo thứ tự<br/>view → cart → purchase"]
    SESSION --> KPI["Overview KPI<br/>events, visitors, buyers"]
    SI --> PRODUCTS["Funnel products<br/>counts + ratios"]
    KPI --> GOLD["Gold behavior snapshot"]
    PRODUCTS --> GOLD
    GOLD --> MONGO["MongoDB snapshot"]
    GOLD --> REPORT["Fingerprint + quality report"]
    classDef data fill:#e9eeee,stroke:#6f8381,color:#203c3d;
    classDef process fill:#e9eee5,stroke:#77866a,color:#293729;
    classDef result fill:#eee9e0,stroke:#8b7b63,color:#40372b;
    class SILVER data;
    class ORDER,SESSION,SI process;
    class FUNNEL,KPI,PRODUCTS,GOLD,MONGO,REPORT result;
```

Overview và product funnel dùng cùng snapshot. Tỷ lệ funnel không được trộn với `cart_view_ratio_7d` hoặc `transaction_view_ratio_7d`, vì hai ratio ML chỉ là feature đếm trong cửa sổ 7 ngày.

## 4. Feature và K-Means training

```mermaid
%%{init: {"theme":"base","themeVariables":{"fontFamily":"Inter,Arial,sans-serif","primaryColor":"#e7efee","primaryTextColor":"#173638","primaryBorderColor":"#6f8583","lineColor":"#788987","secondaryColor":"#e9eee5","tertiaryColor":"#f0ebe3"}}}%%
flowchart TD
    SILVER["Silver lịch sử"] --> WINDOW["Cửa sổ 7 ngày<br/>AS_OF cố định"]
    WINDOW --> FEATURES["8 feature / visitor"]
    FEATURES --> TRANSFORM["log1p count<br/>VectorAssembler<br/>StandardScaler"]
    TRANSFORM --> CANDIDATES["K-Means candidates<br/>K = 2, 3, 4, 5"]
    CANDIDATES --> EVAL["Silhouette toàn cohort"]
    EVAL --> SELECT["Chọn silhouette cao nhất<br/>hòa: K nhỏ hơn"]
    SELECT --> PIPELINE["PipelineModel đã lưu"]
    PIPELINE --> ASSIGN["cluster_id / visitor"]
    ASSIGN --> PROFILE["Cluster profile<br/>count, share, mean feature"]
    PIPELINE --> REGISTRY["Mongo segment_models<br/>status = published"]
    PROFILE --> REGISTRY

    classDef data fill:#e9eeee,stroke:#6f8381,color:#203c3d;
    classDef model fill:#e9eee5,stroke:#77866a,color:#293729;
    classDef result fill:#eee9e0,stroke:#8b7b63,color:#40372b;
    class SILVER,WINDOW,FEATURES data;
    class TRANSFORM,CANDIDATES,EVAL,SELECT,PIPELINE model;
    class ASSIGN,PROFILE,REGISTRY result;
```

Tám feature theo đúng thứ tự hợp đồng là: `view_count_7d`, `cart_count_7d`, `transaction_count_7d`, `unique_items_7d`, `active_days_7d`, `recency_hours`, `cart_view_ratio_7d`, `transaction_view_ratio_7d`. `cluster_id` chỉ có ý nghĩa trong `model_version` tương ứng.

## 5. Nhãn mua hàng, Random Forest và policy

```mermaid
%%{init: {"theme":"base","themeVariables":{"fontFamily":"Inter,Arial,sans-serif","primaryColor":"#e7efee","primaryTextColor":"#173638","primaryBorderColor":"#6f8583","lineColor":"#788987","secondaryColor":"#e9eee5","tertiaryColor":"#f0ebe3"}}}%%
flowchart TD
    EVENTS["Silver events"] --> CUT["AS_OF visitor snapshot"]
    CUT --> LABEL["Label = có purchase<br/>trong 24 giờ sau AS_OF"]
    LABEL --> SPLIT["Temporal split<br/>train / validation / test<br/>có purge gap"]
    SPLIT --> SAMPLE["Negative sampling<br/>chỉ trên train"]
    SAMPLE --> RF["4 RF candidates<br/>trees 50/100 · depth 5/8"]
    RF --> PRAUC["PR-AUC validation"]
    PRAUC --> WINNER["Winner + pipeline + predictions"]
    WINNER --> GRID["Threshold grid<br/>0.00 → 1.00"]
    GRID --> POLICY["Policy threshold<br/>precision / recall / F1"]
    POLICY --> APPLY["Apply lên score gốc<br/>purchase_score ≥ threshold"]
    APPLY --> PUBLISH["Model / policy registry<br/>HDFS + MongoDB"]
    WINNER --> TEST["Test metrics<br/>confusion + PR-AUC + ROC-AUC"]
    TEST --> PUBLISH
    classDef data fill:#e9eeee,stroke:#6f8381,color:#203c3d;
    classDef model fill:#e9eee5,stroke:#77866a,color:#293729;
    classDef result fill:#eee9e0,stroke:#8b7b63,color:#40372b;
    class EVENTS,CUT,LABEL,SPLIT,SAMPLE data;
    class RF,PRAUC,WINNER,GRID model;
    class POLICY,APPLY,PUBLISH,TEST result;
```

`purchase_score` là score dùng để xếp hạng và áp threshold. Nó không được đọc như xác suất mua đã hiệu chuẩn. Policy chỉ đổi `predicted_label`, giữ nguyên score, feature, model version và source fingerprint.

## 6. Streaming ML inference theo batch

```mermaid
%%{init: {"theme":"base","themeVariables":{"fontFamily":"Inter,Arial,sans-serif","primaryColor":"#e7efee","primaryTextColor":"#173638","primaryBorderColor":"#6f8583","lineColor":"#788987","secondaryColor":"#e9eee5","tertiaryColor":"#f0ebe3"}}}%%
flowchart LR
    PRODUCER["Replay / simulator / live producer"] --> KAFKA["Kafka raw topic"]
    KAFKA --> READ["Spark Structured Streaming"]
    READ --> VALIDATE{"Schema, source,<br/>range, duplicate, late?"}
    VALIDATE -- "Lỗi" --> DLQ["Kafka DLQ<br/>source topic/partition/offset"]
    VALIDATE -- "Accepted" --> BATCH["foreachBatch"]
    BATCH --> KPI["KPI + session + funnel"]
    BATCH --> FEAT["Feature 7 ngày"]
    FEAT --> MODELS["Frozen K-Means + RF + policy"]
    MODELS --> PRED["cluster_id<br/>purchase_score<br/>predicted_label"]
    KPI --> COMMIT["Gold batch snapshot"]
    PRED --> COMMIT
    COMMIT --> MONGO["Mongo published snapshot"]
    COMMIT --> CHECKPOINT["Spark checkpoint<br/>processed offsets"]
    MONGO --> POINTER["Committed run_id:batch_id"]
    CHECKPOINT --> POINTER
    classDef source fill:#e9eeee,stroke:#6f8381,color:#203c3d;
    classDef process fill:#e9eee5,stroke:#77866a,color:#293729;
    classDef result fill:#eee9e0,stroke:#8b7b63,color:#40372b;
    classDef error fill:#f1e5e1,stroke:#a66d62,color:#4a2925;
    class PRODUCER,KAFKA source;
    class READ,VALIDATE,BATCH,FEAT,MODELS process;
    class KPI,PRED,COMMIT,MONGO,CHECKPOINT,POINTER result;
    class DLQ error;
```

Một batch chỉ được coi là committed sau khi dữ liệu Gold và Mongo đã được kiểm tra. Checkpoint giúp retry lại batch; source identity và fingerprint giúp tránh ghi trùng khi retry.

## 7. Live demo: ACK, processed và chuyển lượt

```mermaid
%%{init: {"theme":"base","themeVariables":{"fontFamily":"Inter,Arial,sans-serif","primaryColor":"#e7efee","primaryTextColor":"#173638","primaryBorderColor":"#6f8583","lineColor":"#788987","secondaryColor":"#e9eee5","tertiaryColor":"#f0ebe3"}}}%%
sequenceDiagram
    participant C as Live controller
    participant P as Producer
    participant K as Kafka
    participant S as Spark consumer
    participant M as Mongo snapshot
    participant A as FastAPI
    participant W as Web dashboard

    C->>P: Tạo segment và event schedule
    P->>K: Produce event
    K-->>P: ACK topic/partition/offset
    P-->>C: acknowledged_events + last_seq
    K->>S: Consumer đọc theo offset
    S->>S: Validate, dedup, feature và inference
    S->>M: Publish KPI/prediction cho batch
    M-->>S: Xác nhận snapshot
    S->>C: committed_batch_id + processed_offsets
    W->>A: GET status/feed/overview
    A->>M: Đọc pointer và snapshot cùng run/batch
    M-->>A: Kết quả đã commit
    A-->>W: ACK/processed, KPI, cluster, prediction
    C->>P: Chuyển lượt sau khi batch drain
    C->>C: Dừng, resume, stale hoặc completed
```

`ACK` chỉ chứng minh Kafka đã nhận event. `processed` chỉ xuất hiện khi Spark đã xử lý và batch snapshot đã commit. Backlog bằng 0 là điều kiện drain của snapshot, không phải xác nhận Kafka consumer group lag.

## 8. Snapshot consistency từ API đến web

```mermaid
%%{init: {"theme":"base","themeVariables":{"fontFamily":"Inter,Arial,sans-serif","primaryColor":"#e7efee","primaryTextColor":"#173638","primaryBorderColor":"#6f8583","lineColor":"#788987","secondaryColor":"#e9eee5","tertiaryColor":"#f0ebe3"}}}%%
flowchart TD
    SELECT["Người dùng chọn RUN_ID"] --> API["FastAPI"]
    API --> POINTER["Đọc latest/committed pointer"]
    POINTER --> COORD["Ghim run_id + batch_id"]
    COORD --> OVERVIEW["/overview"]
    COORD --> FUNNEL["/funnel"]
    COORD --> SEGMENTS["/segments"]
    COORD --> PREDICT["/predictions"]
    COORD --> MODELS["/models"]
    COORD --> PIPELINE["/pipeline"]
    OVERVIEW --> MONGO["Mongo collections cùng snapshot"]
    FUNNEL --> MONGO
    SEGMENTS --> MONGO
    PREDICT --> MONGO
    MODELS --> MONGO
    PIPELINE --> MONGO
    MONGO --> JSON["JSON có run_id, batch_id,<br/>model/policy bindings"]
    JSON --> WEB["Dashboard render KPI,<br/>funnel, cluster, prediction, status"]
    classDef input fill:#e9eeee,stroke:#6f8381,color:#203c3d;
    classDef control fill:#e9eee5,stroke:#77866a,color:#293729;
    classDef output fill:#eee9e0,stroke:#8b7b63,color:#40372b;
    class SELECT,API input;
    class POINTER,COORD control;
    class OVERVIEW,FUNNEL,SEGMENTS,PREDICT,MODELS,PIPELINE,MONGO,JSON,WEB output;
```

Dashboard không tự tính lại model. Nó đọc dữ liệu đã công bố và dùng cùng tọa độ snapshot để tránh trộn KPI của batch này với prediction của batch khác.

## 9. Bản đồ kết quả trên giao diện

```mermaid
%%{init: {"theme":"base","themeVariables":{"fontFamily":"Inter,Arial,sans-serif","primaryColor":"#e7efee","primaryTextColor":"#173638","primaryBorderColor":"#6f8583","lineColor":"#788987","secondaryColor":"#e9eee5","tertiaryColor":"#f0ebe3"}}}%%
flowchart LR
    SNAPSHOT["Committed snapshot"] --> KPI["KPI tổng quan"]
    SNAPSHOT --> FUNNEL["Funnel"]
    SNAPSHOT --> CLUSTER["Phân cụm"]
    SNAPSHOT --> SCORE["Dự đoán mua"]
    SNAPSHOT --> HEALTH["Trạng thái pipeline"]

    KPI --> K1["processed_events\nvisitors\nviews / cart / purchases"]
    FUNNEL --> F1["visitor → view → cart → purchase"]
    CLUSTER --> C1["cluster_id\ncount / share\nraw feature means"]
    SCORE --> S1["purchase_score\nthreshold\npredicted_label"]
    HEALTH --> H1["producer / consumer\nbacklog / latency\nrun_id : batch_id"]

    C1 --> DECISION["Hiểu nhóm hành vi"]
    S1 --> DECISION2["Xếp hạng và chọn visitor theo policy"]
    H1 --> TRUST["Đánh giá độ mới và tính toàn vẹn"]
    classDef input fill:#e9eeee,stroke:#6f8381,color:#203c3d;
    classDef result fill:#eee9e0,stroke:#8b7b63,color:#40372b;
    classDef interpretation fill:#e9eee5,stroke:#77866a,color:#293729;
    class SNAPSHOT input;
    class KPI,FUNNEL,CLUSTER,SCORE,HEALTH,K1,F1,C1,S1,H1 result;
    class DECISION,DECISION2,TRUST interpretation;
```

| Khu vực | Câu hỏi trả lời | Nguồn chính |
| --- | --- | --- |
| KPI | Có bao nhiêu event, visitor và buyer? | `kpi_overview` |
| Funnel | Visitor rơi ở bước nào? | `funnel_overview`, `funnel_products` |
| Phân cụm | Các nhóm hành vi khác nhau thế nào? | `segment_profiles`, `visitor_segments` |
| Dự đoán | Visitor nào có score vượt policy threshold? | `stream_ml_predictions` hoặc purchase score snapshot |
| Pipeline | Dữ liệu đã ACK, processed và commit đến đâu? | run status, batch metadata, offsets |

## 10. Trạng thái run và khả năng phục hồi

```mermaid
%%{init: {"theme":"base","themeVariables":{"fontFamily":"Inter,Arial,sans-serif","primaryColor":"#e7efee","primaryTextColor":"#173638","primaryBorderColor":"#6f8583","lineColor":"#788987","secondaryColor":"#e9eee5","tertiaryColor":"#f0ebe3"}}}%%
stateDiagram-v2
    [*] --> preparing
    preparing --> running: config + model provenance hợp lệ
    preparing --> failed: thiếu source/model/config
    running --> rotating: hết event segment
    rotating --> running: batch drain + snapshot commit
    running --> stop_requested: người vận hành yêu cầu dừng
    stop_requested --> draining: producer ngừng phát
    draining --> completed: ACK đã xử lý + backlog 0
    running --> stale: mất heartbeat quá thời hạn
    stale --> running: process tiếp tục và heartbeat trở lại
    stale --> failed: không thể resume
    running --> failed: Spark/Kafka/Mongo lỗi không phục hồi
    failed --> preparing: retry cùng identity
    completed --> [*]
```

Retry cùng identity phải kiểm tra snapshot, manifest, checkpoint và source fingerprint. Không dùng một trạng thái `completed` của run khác để thay thế cho run đang kiểm tra.

## Liên kết tới tài liệu chi tiết

- [Data contract và Silver](../data/README.md)
- [Analytics, session và funnel](../analytics/README.md)
- [Feature và K-Means](../ml/02-methodology-and-architecture.md)
- [Random Forest](../ml/06-random-forest.md)
- [Policy precision](../ml/09-purchase-policy.md)
- [Streaming ML](../ml/10-streaming-scoring.md)
- [Kiến trúc API/dashboard](../serving/01-architecture.md)
- [Hướng dẫn đọc kết quả trên web](../serving/06-web-user-guide.md)

## Quy ước cập nhật

Khi thay đổi contract, feature order, model version, policy, snapshot hoặc trạng thái live, cập nhật sơ đồ liên quan cùng pull request. Không ghi cứng `run_id`, số lượng event hoặc metric của một máy cụ thể vào sơ đồ kiến trúc; số liệu thực tế phải đọc từ report của run.
