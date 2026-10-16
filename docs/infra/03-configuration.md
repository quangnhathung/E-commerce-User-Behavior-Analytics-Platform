# 03 — Giải thích cấu hình

[← Mục lục](README.md)

Nguồn giá trị là [template môi trường](../../.env.example), [Compose](../../compose.yaml), [Hadoop XML](../../infra/config/hadoop/) và [Spark defaults](../../infra/config/spark/spark-defaults.conf).

## 1. Cấu hình trên host và trong container

`.env.example` là template có thể đưa vào Git. `make setup` tạo cấu hình local với ID/mật khẩu thật; file local không được đưa vào Git.

[Script quản lý](../../infra/manage.py) đọc và kiểm tra biến trước khi điều khiển Compose. Khi gọi tiến trình con, nó bỏ các biến project cùng tên khỏi environment kế thừa để cấu hình trong file local không bị shell cha ghi đè ngoài ý muốn. Các thiết lập Docker như `DOCKER_HOST` vẫn được giữ.

Quy tắc này áp dụng cho lệnh chạy qua Makefile/script quản lý. Nếu gọi trực tiếp `docker compose` thì cần lưu ý quy tắc ưu tiên biến của chính Compose.

Ví dụ không chứa credential thật:

```dotenv
COMPOSE_PROJECT_NAME=ecommerce-analytics
DATASET_PATH=./Dataset/retailrocket-recommender-system-dataset
MONGO_DATABASE=ecommerce
MONGO_USERNAME=analytics
```

Giữ nguyên mật khẩu/cluster ID đã sinh khi dùng lại volume hiện có. Không chạy shell `source` file cấu hình để “nạp” credential.

## 2. Danh mục biến môi trường

| Biến                        | Mặc định/template                                   | Công dụng và ảnh hưởng khi đổi                                                                                   |
| --------------------------- | --------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------- |
| `COMPOSE_PROJECT_NAME`      | `ecommerce-analytics`                               | Namespace container, network, volume và tên custom image; đổi tên tạo namespace khác, không tự chuyển dữ liệu cũ |
| `DATASET_PATH`              | `./Dataset/retailrocket-recommender-system-dataset` | Nguồn bind mount dataset; phải tồn tại và chứa đủ bốn CSV                                                        |
| `KAFKA_IMAGE`               | Kafka 3.9.1 kèm digest                              | Chọn đúng image Kafka; validator yêu cầu digest SHA-256                                                          |
| `MONGO_IMAGE`               | MongoDB 8.0 kèm digest                              | Chọn đúng image MongoDB; tag và digest thật nằm trong template                                                   |
| `KAFKA_CLUSTER_ID`          | `__GENERATE__`                                      | Setup sinh UUID dạng Kafka một lần; phải khớp metadata đã lưu                                                    |
| `HDFS_CLUSTER_ID`           | `CID-ecommerce-analytics`                           | Định danh HDFS; startup đối chiếu với VERSION của storage đã khởi tạo                                            |
| `MONGO_ROOT_USERNAME`       | `admin`                                             | Account bootstrap MongoDB                                                                                        |
| `MONGO_ROOT_PASSWORD`       | `__GENERATE__`                                      | Setup sinh password root; đổi file không tự đổi account trong database                                           |
| `MONGO_DATABASE`            | `ecommerce`                                         | Database của ứng dụng và nơi cấp quyền app user                                                                  |
| `MONGO_USERNAME`            | `analytics`                                         | Application account có quyền readWrite trên database ứng dụng                                                    |
| `MONGO_PASSWORD`            | `__GENERATE__`                                      | Password ứng dụng, độc lập với root password                                                                     |
| `KAFKA_HOST_PORT`           | `19092`                                             | Cổng Kafka trên host; được dùng cả khi quảng bá listener HOST                                                    |
| `MONGO_HOST_PORT`           | `27018`                                             | Cổng MongoDB trên host; cổng nội bộ vẫn là 27017                                                                 |
| `HDFS_UI_PORT`              | `9870`                                              | Cổng host cho NameNode UI                                                                                        |
| `SPARK_MASTER_UI_PORT`      | `18080`                                             | Cổng host cho master UI                                                                                          |
| `SPARK_WORKER_1_UI_PORT`    | `18081`                                             | Cổng host cho worker 1 UI                                                                                        |
| `SPARK_WORKER_2_UI_PORT`    | `18082`                                             | Cổng host cho worker 2 UI                                                                                        |
| `SPARK_APPLICATION_UI_PORT` | `14040`                                             | Cổng host cho driver UI, chỉ có khi ứng dụng chạy                                                                |
| `MIN_AVAILABLE_RAM_GIB`     | `7` | Ngưỡng preflight; tổng giới hạn container gồm init không được vượt ngưỡng này; không trực tiếp cấu hình Docker memory limit                                                              |
| `MIN_DOCKER_DISK_GIB`       | `30`                                                | Ngưỡng dung lượng trống tại Docker data partition khi kiểm tra được                                              |

Validator còn kiểm tra biến thiếu, giá trị chưa sinh, format project/cluster ID, cổng trong khoảng 1024–65535, cổng không trùng và image có digest.

Mặc định ngân sách 7 GiB bao phủ 6,25 GiB limit thường trực và 704 MiB init. Giảm biến môi trường không tự giảm các limit trong Compose; preflight sẽ báo lỗi nếu tổng limit lớn hơn ngân sách. Xem [phân bổ RAM và cách áp dụng](10-memory-budget.md).

Setup dùng hai lần `secrets.token_hex(24)` để sinh hai password 48 ký tự hex độc lập. Tài liệu chỉ mô tả cách sinh, không ghi giá trị local.

## 3. Endpoint container và host

| Mục đích                  | Trong network                       | Host mặc định                       |
| ------------------------- | ----------------------------------- | ----------------------------------- |
| Kafka data                | `kafka:9092`                        | `127.0.0.1:19092`                   |
| Kafka controller          | `kafka:9093`                        | Không publish                       |
| HDFS RPC                  | `namenode:9000`                     | Không publish                       |
| NameNode UI/JMX           | `namenode:9870`                     | `127.0.0.1:9870`                    |
| DataNode transfer/IPC/JMX | `9866 / 9867 / 9864` trên từng node | Không publish                       |
| Spark master RPC          | `spark-master:7077`                 | Không publish                       |
| Spark master UI           | `spark-master:18080`                | `127.0.0.1:18080`                   |
| Worker UI                 | `18081` hoặc `18082`                | Cùng số cổng mặc định trên loopback |
| Driver RPC                | `spark-client:39001`                | Không publish                       |
| Driver block manager      | `spark-client:39002`                | Không publish                       |
| Driver UI                 | `spark-client:14040`                | `127.0.0.1:14040`                   |
| MongoDB                   | `mongodb:27017`                     | `127.0.0.1:27018`                   |

`127.0.0.1` trong container là chính container đó. Ví dụ Spark không dùng Kafka host listener để kết nối nội bộ.

Đổi cổng publish không đổi cổng bên trong container. Khi đổi cổng UI host, mở URL mới trực tiếp; các liên kết UI quảng bá theo cổng nội bộ có thể không phản ánh cổng host đã đổi.

## 4. Cấu hình Kafka

- `process.roles=broker,controller`: một node đảm nhiệm cả hai vai trò.
- `node.id=1` và quorum voters `1@kafka:9093`: controller quorum một node.
- `INTERNAL`, `HOST`, `CONTROLLER`: tách địa chỉ cho client container, client host và điều khiển.
- `auto.create.topics.enable=false`: tên topic sai không âm thầm tạo topic mới.
- Topic nội bộ offsets/transaction có replication 1 để phù hợp một broker.
- `min.insync.replicas=1`: yêu cầu phù hợp topology một broker; không tạo thêm bản sao.
- Retention raw/DLQ là `172800000` ms, tương đương 48 giờ.
- Producer smoke bật idempotence và `acks=all`; broker đơn vẫn là điểm lỗi đơn.

Raw có ba partition; DLQ có một partition. Init kiểm tra cả partitions, replication factor và retention. Init báo lỗi khi topic hiện hữu sai cấu hình thay vì tự xóa topic.

## 5. Cấu hình Hadoop

| Thuộc tính                            | Giá trị                       | Công dụng                                                     |
| ------------------------------------- | ----------------------------- | ------------------------------------------------------------- |
| `fs.defaultFS`                        | `hdfs://namenode:9000`        | Filesystem mặc định cho đường dẫn HDFS                        |
| `dfs.replication`                     | `2`                           | Số replica mục tiêu khi tạo file                              |
| `dfs.namenode.name.dir`               | `file:///var/lib/hadoop/name` | Metadata NameNode                                             |
| `dfs.datanode.data.dir`               | `file:///var/lib/hadoop/data` | Block DataNode                                                |
| `dfs.client.use.datanode.hostname`    | `true`                        | Client kết nối DataNode qua hostname                          |
| `dfs.datanode.use.datanode.hostname`  | `true`                        | DataNode trao đổi với nhau qua hostname                       |
| `dfs.namenode.safemode.min.datanodes` | `2`                           | Yêu cầu tối thiểu hai DataNode để đáp ứng điều kiện safe mode |
| `hadoop.http.staticuser.user`         | `analytics`                   | User tĩnh cho truy cập HTTP không xác thực                    |

Đây là môi trường local chưa cấu hình Kerberos. Dịch vụ và Spark chạy bằng user `analytics`; `hdfs-init` tạo gốc `/ecommerce` với mode 750.

## 6. Cấu hình Spark

| Thuộc tính                                            | Giá trị                                       | Ý nghĩa                                              |
| ----------------------------------------------------- | --------------------------------------------- | ---------------------------------------------------- |
| `spark.master`                                        | `spark://spark-master:7077`                   | Nộp task lên cụm Standalone                          |
| `spark.submit.deployMode`                             | `client`                                      | Driver ở container nộp job                           |
| `spark.cores.max`                                     | `2` | Tổng số core tối đa cấp cho ứng dụng                 |
| `spark.executor.cores`                                | `1` | Số task slot CPU trên mỗi executor                   |
| `spark.executor.memory`                               | `640m` | Heap executor, không phải toàn bộ RSS của tiến trình |
| `spark.driver.memory`                                 | `512m` | Heap driver                                          |
| `spark.driver.maxResultSize` | `128m` | Tổng kết quả serialize tối đa trả về driver cho mỗi action |
| `spark.sql.shuffle.partitions`                        | `4` | Số partition mặc định cho nhiều phép shuffle SQL     |
| `spark.default.parallelism`                           | `4` | Parallelism mặc định cho một số thao tác RDD         |
| `spark.dynamicAllocation.enabled`                     | `false`                                       | Không tự thay số executor theo tải                   |
| `spark.driver.host`                                   | `spark-client`                                | Địa chỉ executor gọi về driver                       |
| `spark.driver.bindAddress`                            | `0.0.0.0`                                     | Driver lắng nghe trên các interface container        |
| `spark.driver.port`                                   | `39001`                                       | RPC driver cố định                                   |
| `spark.driver.blockManager.port`                      | `39002`                                       | Cổng block manager của driver                        |
| `spark.ui.port`                                       | `14040`                                       | UI ứng dụng                                          |
| `spark.sql.session.timeZone`                          | `UTC`                                         | Quy ước timezone cho xử lý SQL                       |
| `spark.eventLog.enabled`                              | `true`                                        | Ghi event log ứng dụng                               |
| `spark.eventLog.dir`                                  | `hdfs://namenode:9000/ecommerce/spark-events` | Nơi giữ event log                                    |
| `spark.pyspark.python`, `spark.pyspark.driver.python` | `/usr/local/bin/python3`                      | Đồng nhất Python driver/executor                     |

Mỗi worker quảng bá một core và 640 MiB executor memory, container được cấp 1.280 MiB. Phần chênh lệch còn cần cho worker daemon, Python, native buffer và JVM overhead; không nên tăng heap đến bằng container limit.

CPU quota container không phải thao tác ghim vào một core vật lý. Số core Spark cũng không có nghĩa tạo thêm CPU cho máy chủ.

## 7. Storage và vòng đời dữ liệu

| Vùng                         | Nội dung                               | Sau `down`                 | Sau `reset`                               |
| ---------------------------- | -------------------------------------- | -------------------------- | ----------------------------------------- |
| Năm named volume chính       | Kafka, NameNode, hai DataNode, MongoDB | Giữ                        | Xóa trong phạm vi project                 |
| Dataset bind mount           | CSV do người dùng cung cấp             | Giữ trên host              | Giữ trên host                             |
| Config/script bind mount     | Mã/cấu hình repository                 | Giữ                        | Giữ                                       |
| Scratch container            | Dữ liệu tạm executor                   | Không dùng làm dữ liệu bền | Không bảo đảm giữ                         |
| `artifacts/infra/` trên host | Log và báo cáo script                  | Giữ                        | Lệnh reset hiện tại không xóa thư mục này |

Image gốc có thể khai báo thêm anonymous volume cho đường dẫn phụ. Năm named volume trong Compose là các vùng dữ liệu chính được dự án chủ động định nghĩa.

### Các thư mục HDFS

| Đường dẫn                 | Mục đích đã chuẩn bị                        |
| ------------------------- | ------------------------------------------- |
| `/ecommerce/bronze`       | Snapshot bốn CSV Retailrocket nguyên bản    |
| `/ecommerce/silver`       | Event chuẩn hóa, quarantine và duplicate audit |
| `/ecommerce/gold`         | Aggregate/feature nghiệp vụ sau này         |
| `/ecommerce/models`       | Model artifact sau khi có training          |
| `/ecommerce/checkpoints`  | Checkpoint nghiệp vụ sau này                |
| `/ecommerce/spark-events` | Event log Spark đã được bật trong cấu hình  |
| `/ecommerce/_smoke`       | Dữ liệu kiểm thử hạ tầng                    |

Smoke dùng checkpoint riêng trong thư mục của từng run dưới `_smoke`, không dùng gốc checkpoint nghiệp vụ.

Job ingestion tạo snapshot bên dưới Bronze/Silver khi chạy `make ingest`; xem [layout dữ liệu](../data/02-contract-and-storage.md). `make test-data` dùng namespace UUID riêng dưới `/ecommerce/_tests/ingest`.

## 8. Khóa phiên bản và thay đổi cấu hình

Kafka/MongoDB được khóa image digest trong template; base Python/Java được khóa digest trong Dockerfile. Binary Hadoop/Spark và bốn JAR được kiểm tra SHA-512. Python dependency được khóa trong [requirements](../../infra/spark/requirements.txt).

Package hệ điều hành được lấy từ repository lúc build; không tuyên bố build hermetic từng byte. Custom image local có image ID trong manifest sau khi build, nhưng có thể chưa có RepoDigest nếu chưa được đẩy lên registry.

| Loại thay đổi                                    | Cách áp dụng                                                           |
| ------------------------------------------------ | ---------------------------------------------------------------------- |
| Cổng publish hoặc cấu hình service trong Compose | Sửa cấu hình rồi `make up`; kiểm tra readiness                         |
| XML Hadoop hoặc Spark defaults bind mount        | Dừng/khởi động lại cụm để tiến trình đọc lại cấu hình                  |
| Dockerfile hoặc dependency                       | `make build` rồi `make up`                                             |
| MongoDB password đã tồn tại                      | Đổi account trong database tường minh hoặc khôi phục giá trị khớp      |
| Kafka/HDFS cluster ID đã tồn tại                 | Giữ/khôi phục ID khớp dữ liệu; không dùng thay đổi ID như cách restart |
| Project name                                     | Xem như namespace khác; cần kế hoạch riêng nếu muốn chuyển dữ liệu     |

Đọc tiếp [vòng đời](04-bootstrap-and-lifecycle.md) và [vận hành](05-operations.md).
