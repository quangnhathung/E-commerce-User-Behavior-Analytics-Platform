# E-commerce User Behavior Analytics Platform

Nền tảng phân tích hành vi thương mại điện tử trên Kafka, Spark, HDFS và MongoDB. Mã hiện tại gồm hạ tầng Docker, ingestion Retailrocket CSV → Bronze → Silver, replay Kafka, streaming KPI, session/funnel, feature 7 ngày/K-Means, Random Forest batch và ML streaming có policy precision. ML streaming mới chờ nghiệm thu trên cụm; API và dashboard thuộc các giai đoạn tiếp theo.

**Developer mới:** dùng [hướng dẫn chạy từ đầu đến policy precision](docs/GETTING_STARTED.md), gồm Ubuntu/Arch, Windows qua WSL2, dataset, cấu hình, từng lệnh, tiêu chí đạt và phục hồi.

Kế hoạch sản phẩm đầy đủ: [IMPLEMENTATION_PLAN.md](IMPLEMENTATION_PLAN.md).

Mục lục tài liệu: [docs/README.md](docs/README.md). Hướng dẫn hạ tầng: [docs/infra/README.md](docs/infra/README.md). Ingestion: [docs/data/README.md](docs/data/README.md). Replay/KPI: [docs/streaming/README.md](docs/streaming/README.md). Session/funnel: [docs/analytics/README.md](docs/analytics/README.md).

Machine Learning: [docs/ml/README.md](docs/ml/README.md).

## Yêu cầu

- Linux `amd64`, CPU hỗ trợ AVX, tối thiểu bốn CPU logic cho Docker.
- Docker Engine đang chạy, Docker Compose hỗ trợ `up --wait` và plugin Docker Buildx cho BuildKit cache. Trên Arch Linux: `sudo pacman -S docker-buildx`.
- Python 3.10+ và GNU Make trên máy chủ. Job trong container dùng Python 3.11.
- Khoảng **7 GiB RAM khả dụng** trước khi khởi động toàn bộ cụm; chín container có tổng giới hạn 6,25 GiB, cộng tối đa 704 MiB cho ba job init, tổng 6,94 GiB. Docker daemon và hệ điều hành cần RAM riêng. Xem [ngân sách RAM](docs/infra/10-memory-budget.md).
- Ít nhất 30 GiB trống trên phân vùng chứa Docker data; nên dành thêm dung lượng khi nạp dataset.
- Internet trong lần build đầu để tải base image, binary Apache và dependency.

Kiểm tra daemon trong terminal dùng để chạy dự án:

```bash
docker info
docker compose version
docker buildx version
```

Nếu không truy cập được daemon, kiểm tra Docker context, trạng thái dịch vụ và quyền socket của môi trường đó. Script không tự thay quyền socket, thay nhóm người dùng hoặc dừng container dự án khác.

Dataset dự kiến có bốn file tại `Dataset/retailrocket-recommender-system-dataset/`: `events.csv`, `category_tree.csv`, `item_properties_part1.csv`, `item_properties_part2.csv`. Có thể đổi `DATASET_PATH` trong cấu hình local. Dataset chỉ được mount read-only vào container chạy job và không được đóng gói vào image.

## Khởi động

Chạy tại thư mục gốc dự án:

```bash
make setup
make build
make up
make doctor
make smoke
```

- `setup` tạo `.env` với quyền `0600`, sinh hai mật khẩu độc lập và Kafka cluster ID. Chạy lại giữ nguyên file hiện có; file thiếu cấu hình sẽ được báo lỗi để sửa thủ công.
- `build` kiểm tra Buildx, tải image Kafka/MongoDB theo digest và build Hadoop/Spark. Binary Spark khoảng 382 MiB được tải từ mirror Huawei; Hadoop lean khoảng 489 MiB từ CDN Apache, có nguồn dự phòng. BuildKit cache lưu binary đã xác minh và file tải dở để dùng lại. Xem [tối ưu build](docs/infra/09-build-optimization.md).
- `up` kiểm tra môi trường, khởi động các image đã build, đợi healthcheck và xác nhận ba tác vụ init hoàn tất.
- `doctor` kiểm tra daemon, dataset, tài nguyên, cổng và readiness của cụm nếu đã khởi tạo.
- `smoke` chạy kiểm tra dữ liệu thật giữa các dịch vụ, không tác động topic nghiệp vụ.

Cấu hình Compose có thể kiểm tra mà không cần daemon:

```bash
make config
make test
```

`make test` chạy 34 test hạ tầng và 187 test ứng dụng, dùng Python standard library, Compose CLI, Bash, curl và GNU coreutils. Lượt hiện tại có 214 đạt, 7 HTTP skipped vì môi trường cấm tạo socket loopback. Không cần cài PySpark trên host. Các kiểm tra này không thay thế smoke hoặc kiểm thử dữ liệu trên cụm đang chạy.

## Nạp dữ liệu lịch sử

Sau khi image đã có, chạy lần lượt:

```bash
make up
make doctor
make smoke
make test-data
make ingest
```

`make up` áp dụng mount mã ứng dụng vào Spark client; thay đổi ứng dụng này không cần build lại image. `test-data` chạy fixture trên Spark/HDFS thật và dọn namespace kiểm thử riêng. `ingest` giữ nguyên bốn CSV trong Bronze, chuyển `events.csv` thành Parquet Silver phân vùng theo ngày UTC, kèm quarantine, duplicate audit và quality report.

Chạy lại `make ingest` với cùng dữ liệu và mã biến đổi sẽ xác minh rồi dùng lại snapshot. Job từ chối snapshot thiếu marker, sai checksum hoặc khác phiên bản mã; không tự ghi đè dữ liệu đã công bố. Báo cáo host nằm trong `artifacts/data/`. Xem [cách chạy và xử lý lỗi](docs/data/03-ingestion.md).

Artifact hiện có xác nhận smoke, chín test dữ liệu và ingest lần đầu thành công. Lần ingest xác nhận reuse và các lệnh runtime mới cần chạy trên terminal có quyền Docker; phiên công cụ bị từ chối Docker socket. Xem [nghiệm thu dữ liệu](docs/data/05-testing.md).

## Replay và streaming KPI

Sau khi Silver lịch sử đã sẵn sàng:

```bash
make test-stream
make prepare-demo RUN_ID=demo-001
```

Terminal A chạy `make stream RUN_ID=demo-001`. Khi có `STREAM_READY`, terminal B chạy `make replay RUN_ID=demo-001`; dùng `make stream-status RUN_ID=demo-001` để xem KPI và tiến độ. Streaming tự kết thúc sau khi mọi offset đã ACK được xử lý, checkpoint đã commit và số liệu đã đối soát.

Nguồn mặc định là 2.692 event trong `[10:00,16:00) UTC ngày 10/09/2015`, tốc độ 10 event/giây. Mỗi run giới hạn 10.000 event, giữ ngân sách container khoảng 7 GiB; tính năng này không cần image/service mới. Source và checkpoint cũ được giữ để tiếp tục. Chi tiết: [vận hành streaming](docs/streaming/03-operations.md).

Artifact hiện tại xác nhận 10/10 test streaming đạt, cleanup/monitor thành công. Run `behavior-demo-001` đã xử lý 2.692 event, backlog 0, p95 37,287 giây và hai worker/hai replica. Dùng RUN_ID mới khi chuẩn bị demo khác. ML có archive riêng, giữ nguyên fingerprint streaming. Xem [kiểm thử và phục hồi](docs/streaming/04-testing-and-recovery.md).

## Session và funnel

Với Silver đã sẵn sàng và không có Spark driver khác đang chạy:

```bash
make test-behavior
make analyze-behavior
make behavior-status
make behavior-status ITEM_ID=355908
```

Batch phân tích toàn bộ lịch sử. Streaming tính cùng quy tắc trên accepted event của từng run: ngắt phiên khi không hoạt động quá 30 phút, funnel đúng thứ tự và cùng sản phẩm. Gold giữ session/session–item; MongoDB giữ funnel tổng quan và toàn bộ sản phẩm, công bố theo snapshot cùng KPI. `make behavior-status RUN_ID=...` đọc kết quả demo. Chi tiết: [hợp đồng, vận hành và nghiệm thu](docs/analytics/README.md).

Artifact đã xác nhận 6/6 test behavior đạt và phân tích lịch sử có 2.755.641 event, 1.761.675 session; lần chạy lại `reused=true`.

## Feature và K-Means lịch sử

Sau khi Silver đã sẵn sàng, chạy tuần tự:

```bash
make test-ml
make build-features
make train-segments
make segment-status
```

Build mặc định chốt lúc `2015-07-15T00:00:00.000Z`, tạo tám feature trên bảy ngày trước đó. Train thử K=2..5 tuần tự, chọn silhouette tốt nhất, lưu/tải lại pipeline và công bố assignment/profile vào MongoDB. Report ở `artifacts/ml/`, hướng dẫn đầy đủ tại [docs/ml/](docs/ml/README.md). Không cần build lại image hoặc tăng ngân sách RAM.

Artifact người dùng đã chạy xác nhận 6/6 test ML đạt, feature khớp CSV, 99.038 visitor, K=2 và silhouette 0,853847. Full-source build/train lần hai reuse cùng manifest/quality, không fit mới; monitor đủ, không OOM/restart. Xem [bằng chứng K-Means](docs/ml/04-testing.md).

## Dự đoán mua hàng theo batch

Với Silver đã sẵn sàng, chạy tuần tự từ terminal có quyền Docker:

```bash
make test-purchase
make build-purchase-data
make train-purchase
make purchase-status
make score-purchases
make purchase-score-status
```

Dataset có 128 cutoff hằng ngày, feature bảy ngày và nhãn mua trong 24 giờ tiếp theo; chia train/validation/test theo thời gian với khoảng nghỉ. Builder chạy 20 chặng có thể resume. RF dùng 36.750 mẫu train đã sampling, thử bốn cấu hình tuần tự, chọn mô hình/ngưỡng trên validation rồi đánh giá test nguyên bản. Score lịch sử mặc định ngày 18/09/2015; tham chiếu CSV có 69.052 visitor. Có thể tra một visitor bằng `make purchase-score-status VISITOR_ID=001`.

Artifact người dùng đã chạy xác nhận RF gốc thành công lần đầu: 7/7 test, đủ 128 ngày, model 50 cây/depth 5/ngưỡng 0,93, PR-AUC test 0,18172, precision 43,63%, recall 21,21%; score 69.052 visitor. Full-source reuse còn chờ kiểm chứng. Report ở `artifacts/prediction/`; xem [dữ liệu](docs/ml/05-purchase-data.md), [phương pháp](docs/ml/06-random-forest.md), [vận hành](docs/ml/07-purchase-operations.md) và [nghiệm thu](docs/ml/08-purchase-testing.md).

Đã thêm [policy ưu tiên precision ≥70%, recall ≥10%](docs/ml/09-purchase-policy.md), dùng predictions đã lưu và giữ nguyên dataset/model/score gốc:

```bash
make test-purchase-policy
make tune-purchase-policy
make purchase-policy-status
```

Chỉ khi `eligible=true`, chạy `make apply-purchase-policy`; lệnh in ID mới để dùng với `purchase-score-status SCORE_RUN_ID=...`. Không cần build dataset, fit RF hoặc build image lại. Artifact người dùng tạo đã xác nhận 5/5 live test policy, sáu fault scenario, cleanup/monitor đạt; ngưỡng 0,96 có precision test 73,88%, recall 13,64%, `eligible=true`. Apply đủ 69.052 visitor, bốn predicted positive; lần hai `reused=true`, cùng fingerprint, không duplicate. Đây là đánh giá lại ngưỡng trên test đã từng được xem, không phải fresh holdout. Máy mới phải kiểm tra report/eligibility riêng; xem [runbook](docs/GETTING_STARTED.md#policy).

## ML streaming và simulator

Sau khi K-Means, RF và policy eligible đã published, không có driver khác:

```bash
make test-stream-ml
make prepare-ml-demo RUN_ID=ml-replay-001
```

Terminal A chạy `make stream-ml RUN_ID=ml-replay-001`; đợi `STREAM_ML_READY`, terminal B chạy `make replay-ml RUN_ID=ml-replay-001`. Theo dõi bằng `make stream-ml-status RUN_ID=ml-replay-001`. Replay mặc định ngày 18/09/2015 `[00:00,03:00)` UTC có 1.528 event/1.016 visitor; seed chỉ chứa bảy ngày trước START, event mới chỉ tham gia feature sau khi accepted qua Kafka.

Simulator dùng RUN_ID khác: `make prepare-ml-demo RUN_ID=ml-sim-001 DEMO_SOURCE=simulator`, terminal A chạy `stream-ml`, terminal B chạy `make simulate-ml RUN_ID=ml-sim-001` sau READY. Ba visitor mới phát chín event qua ba đợt; mỗi đợt chờ prediction được committed. Score không bắt buộc tăng sau mỗi hành động.

Profile `streaming-ml` khóa model/policy/checksum riêng, giữ nguyên năm archive cũ; không thêm image/service hoặc fit lại model nguồn thật. Đã có 26 test offline mới đạt; live suite/replay/simulator ML nguồn thật còn chờ terminal có quyền Docker. Xem [vận hành, retry và nghiệm thu streaming ML](docs/ml/10-streaming-scoring.md).

## Dịch vụ và phiên bản

| Service | Vai trò | Bộ nhớ tối đa |
|---|---|---:|
| `kafka` | Kafka 3.9.1, KRaft broker/controller | 768 MiB |
| `namenode` | Hadoop 3.4.2 NameNode | 512 MiB |
| `datanode-1`, `datanode-2` | HDFS DataNodes, replication 2 | 384 MiB mỗi node |
| `spark-master` | Spark 3.5.7 Standalone master | 384 MiB |
| `spark-worker-1`, `spark-worker-2` | Hai worker, một core và heap executor 640 MiB mỗi node | 1.280 MiB mỗi worker |
| `spark-client` | Python 3.11 / Java 17, chạy driver | 896 MiB |
| `mongodb` | MongoDB 8.0, authentication bật | 512 MiB |

Ba job chạy một lần là `kafka-init`, `hdfs-init`, `mongo-init`. Init phải thoát với mã 0; `Exited (0)` là trạng thái đúng cho các job này.

Image Hadoop dùng Java 11; image Spark dùng Java 17. Spark master, workers và client dùng cùng image. Các tiến trình custom chạy bằng user `analytics` UID/GID 1000; volume Hadoop được chuẩn bị quyền ghi trong image.

Base image Kafka, MongoDB, Python và Java được khóa bằng SHA-256 digest. Binary Apache và bốn JAR connector được kiểm tra SHA-512. Các thư viện Python được khóa phiên bản. Package hệ điều hành được cài từ repository của base distribution lúc build; đây không phải bản build hermetic từng byte.

Sau `make build`, manifest image ID/digest và phiên bản Docker/Compose nằm ở `artifacts/infra/versions.json`. Có thể cập nhật manifest bằng `make versions`.

## Endpoint

| Thành phần | Từ container | Từ máy chủ |
|---|---|---|
| Kafka | `kafka:9092` | `127.0.0.1:19092` |
| HDFS | `hdfs://namenode:9000` | Chạy qua `spark-client` |
| NameNode UI | `namenode:9870` | <http://127.0.0.1:9870> |
| Spark master | `spark://spark-master:7077` | Chạy qua `spark-client` |
| Spark master UI | `spark-master:18080` | <http://127.0.0.1:18080> |
| Worker 1 UI | `spark-worker-1:18081` | <http://127.0.0.1:18081> |
| Worker 2 UI | `spark-worker-2:18082` | <http://127.0.0.1:18082> |
| Spark application UI | `spark-client:14040` | <http://127.0.0.1:14040>, khi có job |
| MongoDB | `mongodb:27017` | `127.0.0.1:27018` |

Cổng publish chỉ bind `127.0.0.1`. Các số cổng host có thể đổi trong `.env`; dùng trực tiếp URL mới khi đổi cổng. `localhost` trong container không phải máy chủ hoặc container khác.

MongoDB dùng database `ecommerce`, app user `analytics`, `authSource=ecommerce`. Mật khẩu lấy từ `.env`; root account chỉ dùng cho khởi tạo/healthcheck. `spark-client` không nhận root password. Cổng `27018` giúp tránh MongoDB sẵn có trên cổng `27017`.

## Khởi tạo và lưu trữ

Kafka tạo hai topic nghiệp vụ, không tự tạo topic từ request producer:

| Topic | Partitions | Replication | Retention |
|---|---:|---:|---:|
| `ecommerce.events.raw` | 3 | 1 | 48 giờ |
| `ecommerce.events.dlq` | 1 | 1 | 48 giờ |

Listener Kafka riêng cho network Docker và host. Kafka cluster ID được giữ nguyên trong `.env`; startup từ chối khi ID khác với metadata trong volume.

HDFS dùng hai volume DataNode độc lập. NameNode chỉ format khi volume rỗng; nếu metadata không khớp thì startup thất bại thay vì xóa dữ liệu. Init đợi đủ hai DataNode và safe mode OFF rồi tạo:

```text
/ecommerce/bronze
/ecommerce/silver
/ecommerce/gold
/ecommerce/models
/ecommerce/checkpoints
/ecommerce/spark-events
/ecommerce/_smoke
```

Healthcheck NameNode chỉ kiểm tra NameNode, không chờ DataNode, để tránh dependency vòng. Spark client chỉ sẵn sàng sau khi init hoàn tất, hai Spark worker hoạt động, các topic tồn tại và app account đọc được bootstrap metadata.

Log container có rotation 10 MiB × 3 file. Spark event logs được giữ trên HDFS. Thư mục scratch của executor là dữ liệu tạm; không dùng nó làm nơi lưu model hoặc checkpoint lâu dài.

## Chạy job

```bash
docker compose exec spark-client bash
docker compose exec -T spark-client python3 /opt/infra/smoke/readiness.py
```

Driver chạy ở `spark-client`, executor kết nối về các cổng nội bộ cố định. Chỉ chạy một Spark driver tại một thời điểm với cấu hình hiện tại. Smoke, persistence-test, ingest, test-data, prepare-demo, stream và test-stream dùng chung khóa. Producer replay là Python, dùng khóa riêng để chạy cùng streaming.

Ứng dụng được mount read-only từ `app/` vào `/opt/app`; CLI đóng gói dependency Python thành ZIP và phân phối đến executor bằng `spark-submit --deploy-mode client --py-files`. Không dùng `local[*]` để chứng minh xử lý trên hai worker. Kafka connector đã có trong image, không thêm `--packages` tải dependency lúc demo.

## Kiểm thử tích hợp

```bash
make smoke
make persistence-test
```

Smoke test:

1. Tạo topic `infra-smoke-<uuid>` riêng, gửi sáu event và kiểm tra nội dung bằng consumer.
2. Xác nhận task chạy trên cả hai worker và kiểm tra tổng phân tán của dãy 0..999.999.
3. Structured Streaming đọc Kafka, ghi Parquet và checkpoint trên HDFS.
4. Chạy lại cùng checkpoint, xác nhận vẫn có đúng sáu event và các block Parquet có hai replica đang hoạt động.
5. Executor ghi MongoDB hai lần bằng upsert; xác nhận chỉ có sáu document đúng nội dung.
6. Dọn topic, dữ liệu HDFS và document chỉ thuộc lần smoke test đó.

Kết quả và log nằm trong `artifacts/infra/smoke.json` và `smoke.log`. Nếu cần giữ artifact để xem block replica:

```bash
docker compose exec -T spark-client timeout 600s spark-submit /opt/infra/smoke/pipeline.py --keep
docker compose exec -T namenode hdfs fsck /ecommerce/_smoke -files -blocks -locations
```

Chế độ `--keep` chủ động giữ topic/document/thư mục của run được in ra; xóa riêng run đó sau khi xem, không xóa toàn bộ dữ liệu nghiệp vụ.

Persistence test tạo marker riêng trong Kafka, HDFS và MongoDB, thực hiện Compose `down` rồi `up`, xác nhận cả ba marker còn nguyên và dọn marker. Bài kiểm tra này tạm dừng cụm của project; không chạy trong lúc đang xử lý job nghiệp vụ. Nếu thất bại, token và log trong `artifacts/infra/` giúp kiểm tra lại.

Các kiểm tra persistence/replication trên một máy không chứng minh HA khi máy chủ hoặc ổ đĩa vật lý bị hỏng.

## Vận hành

```bash
make ps
make logs
make down
make up
```

`down` giữ named volume và cấu hình local. Khi muốn xóa toàn bộ dữ liệu của riêng Compose project:

```bash
make reset CONFIRM=delete-project-data
```

Lệnh reset không gọi `docker system prune`, không xóa dataset và không xóa container/volume của project khác.

## Xử lý lỗi

- **Daemon không truy cập được:** kiểm tra `docker info` tại terminal thực thi. Một số môi trường agent chỉ cho phép một phần lệnh Docker; kiểm tra offline vẫn chạy được nhưng build/runtime cần quyền daemon thực tế.
- **Thiếu RAM:** mặc định cần 7 GiB khả dụng lúc chưa có container project chạy; tổng limit gồm init là 6,94 GiB. Đóng bớt ứng dụng rồi chạy lại `make up`. Không chỉ hạ ngưỡng môi trường xuống dưới tổng limit. Preflight trừ ngân sách container của project đã chạy để cho phép gọi `up` lại. Không tự giảm worker hoặc replication.
- **Thiếu image:** chạy `make build`; `make up` không tự tải dependency hoặc build lại.
- **Cổng bị chiếm:** đổi biến cổng host tương ứng trong `.env`; không dừng dịch vụ đang giữ cổng.
- **Sai cluster ID:** khôi phục `.env` khớp volume; startup không tự format/xóa dữ liệu.
- **Sai mật khẩu sau khi đổi `.env`:** MongoDB giữ tài khoản trong volume. Khôi phục mật khẩu cũ hoặc đổi mật khẩu trong database một cách tường minh; đổi env không phải thao tác rotate mật khẩu.
- **Init thất bại:** dùng `make logs` hoặc `docker compose logs hdfs-init kafka-init mongo-init`, sửa nguyên nhân rồi chạy lại `make up`.
- **Không đủ worker/DataNode:** kiểm tra UI và log; trạng thái container `running` chưa có nghĩa là cluster đã sẵn sàng.
- **Phân vùng Docker không thấy từ môi trường hiện tại:** preflight thông báo không thể xác minh dung lượng; kiểm tra trực tiếp phân vùng chứa Docker data trên máy chạy daemon.

## Nguồn cấu hình

- [Docker Compose startup order](https://docs.docker.com/compose/how-tos/startup-order/)
- [Kafka 3.9.1 Docker example](https://github.com/apache/kafka/blob/3.9.1/docker/examples/docker-compose-files/single-node/plaintext/docker-compose.yml)
- [Spark 3.5.7 Standalone](https://spark.apache.org/docs/3.5.7/spark-standalone.html)
- [Spark Kafka integration](https://spark.apache.org/docs/3.5.7/structured-streaming-kafka-integration.html)
- [HDFS architecture](https://hadoop.apache.org/docs/r3.4.2/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html)
