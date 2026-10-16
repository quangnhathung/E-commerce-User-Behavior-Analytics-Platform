# E-commerce User Behavior Analytics Platform

Nền tảng phân tích hành vi thương mại điện tử trên Kafka, Spark, HDFS và MongoDB. Phạm vi hiện tại gồm hạ tầng Docker và luồng batch Retailrocket CSV → Bronze → Silver, với chuẩn hóa sự kiện, loại trùng, cách ly dòng lỗi và báo cáo chất lượng. Replay Kafka, KPI/funnel, ML, API và dashboard thuộc các giai đoạn tiếp theo.

Kế hoạch sản phẩm đầy đủ: [IMPLEMENTATION_PLAN.md](IMPLEMENTATION_PLAN.md).

Mục lục tài liệu: [docs/README.md](docs/README.md). Hướng dẫn hạ tầng: [docs/infra/README.md](docs/infra/README.md). Hợp đồng dữ liệu, ingestion và kiểm thử ứng dụng: [docs/data/README.md](docs/data/README.md).

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

`make test` chạy 34 test hạ tầng và 16 test ứng dụng, dùng Python standard library, Compose CLI, Bash, curl và GNU coreutils. Không cần cài PySpark trên host. Test HTTP dùng socket loopback; nếu môi trường cấm tạo socket, bảy test này được ghi rõ là skipped. Các kiểm tra này không thay thế smoke hoặc kiểm thử dữ liệu trên cụm đang chạy.

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

Mã nguồn và kiểm thử offline đã được kiểm tra; phiên công cụ chưa chạy được Spark/HDFS do quyền truy cập Docker socket. Cần hoàn tất [nghiệm thu dữ liệu](docs/data/05-testing.md) trên máy có quyền Docker trước khi xác nhận ingest thực tế thành công.

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

Driver chạy ở `spark-client`, executor kết nối về các cổng nội bộ cố định. Chỉ chạy một Spark driver tại một thời điểm với cấu hình hiện tại. Các lệnh smoke, persistence-test, ingest và test-data dùng chung khóa để không chạy chồng nhau.

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
