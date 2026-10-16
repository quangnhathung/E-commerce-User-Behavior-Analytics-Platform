# 08 — Bản đồ mã nguồn và điểm mở rộng

[← Mục lục](README.md)

Tài liệu này chỉ ra nơi hiện thực từng chức năng, để có thể đọc source theo dòng thực thi thay vì đọc ngẫu nhiên mọi file.

## 1. Bắt đầu từ đâu?

```text
Makefile
  → infra/manage.py
    → compose.yaml
      → image và entrypoint
      → config bind mount
      → init jobs
      → readiness
    → smoke/persistence jobs
    → artifact trên host
```

Bắt đầu từ [Makefile](../Makefile), xem action tương ứng trong [manager](../infra/manage.py), sau đó xem service trong [Compose](../compose.yaml).

## 2. Các file ở gốc

| File                                                | Công dụng                                                                                     |
| --------------------------------------------------- | --------------------------------------------------------------------------------------------- |
| [Makefile](../Makefile)                             | Giao diện lệnh nhất quán; action quản lý gọi Python, test gọi unittest trực tiếp              |
| [compose.yaml](../compose.yaml)                     | Topology, network, image/build, command, environment, port, volume, healthcheck và dependency |
| [.env.example](../.env.example)                     | Danh mục cấu hình công khai, image đã khóa, placeholder credential                            |
| [.gitignore](../.gitignore)                         | Loại cấu hình local, CSV/ZIP, cache và artifact phát sinh khỏi Git                            |
| [.dockerignore](../.dockerignore)                   | Loại dữ liệu/cấu hình không cần khỏi root build context nếu context đó được dùng              |
| [README.md](../README.md)                           | Hướng dẫn chạy nhanh và cửa vào tài liệu                                                      |
| [IMPLEMENTATION_PLAN.md](../IMPLEMENTATION_PLAN.md) | Kế hoạch sản phẩm đầy đủ, bao gồm các phần chưa được triển khai                               |

Build hiện dùng context con `infra/hadoop` và `infra/spark`. Hai context có dockerignore riêng để chỉ gửi những file thực sự cần.

## 3. Host-side manager

File [infra/manage.py](../infra/manage.py) dùng thư viện chuẩn Python để điều khiển Docker CLI; không gọi Docker API trực tiếp.

| Hàm/nhóm            | Làm gì?                                                                           | Side effect hoặc đầu ra                              |
| ------------------- | --------------------------------------------------------------------------------- | ---------------------------------------------------- |
| `InfraError`        | Loại lỗi vận hành được main trình bày gọn                                         | In lỗi và exit khác 0                                |
| `read_env`          | Parse assignment, bỏ comment, từ chối trùng khóa hoặc format sai                  | Dictionary cấu hình                                  |
| `validate_env`      | Kiểm tra biến bắt buộc, ID, cổng, ngưỡng và image digest                          | Báo lỗi trước khi thao tác                           |
| `setup`             | Sinh file local nếu chưa có; giữ file hiện hữu hợp lệ                             | File credential/identity local                       |
| `child_environment` | Loại biến project khỏi environment kế thừa                                        | Giữ file local làm nguồn cấu hình cho tiến trình con |
| `execute`           | Gọi command với cwd cố định, timeout và xử lý exit code                           | Command thực tế và output khi cần                    |
| `compose`           | Xây danh sách argument Compose với project directory, env file và YAML tường minh | Không tự thực thi                                    |
| `parse_ps`          | Đọc output Compose ps dạng JSON array hoặc NDJSON                                 | Danh sách trạng thái                                 |
| `configuration`     | Lấy Compose config dưới dạng JSON, giữ nội bộ                                     | Cấu hình đã expand                                   |
| `statuses`          | Đọc trạng thái tất cả container project                                           | Danh sách service/health/exit code                   |
| `preflight`         | Kiểm tra daemon, môi trường, dataset, disk, RAM và cổng tùy action                | Báo cáo điều kiện khởi động                          |
| `assert_services`   | Xác nhận chín service healthy, ba init exit 0 và readiness                        | Không tự khởi tạo phần bị thiếu                      |
| `start`             | Preflight rồi Compose up có wait, sau đó assert                                   | Khởi động cụm                                        |
| `driver_lock`       | Dùng flock để ngăn job manager chạy chồng                                         | Lock file trên host                                  |
| `spark_job`         | Exec spark-submit có timeout, thu log và tìm INFRA_RESULT                         | Log và JSON khi thành công                           |
| `versions`          | Inspect image ID/digest và phiên bản công cụ                                      | versions.json; không dump runtime environment        |
| `main`              | Phân luồng action CLI                                                             | Xâu chuỗi đúng tác vụ và kiểm soát reset             |

`make build` bỏ kiểm tra RAM/cổng nhưng vẫn chạy các phần preflight khác. `make up` không build. `make persistence-test` gọi ba job và tự down/up giữa create và verify.

## 4. Hadoop image và entrypoint

- [Dockerfile](../infra/hadoop/Dockerfile): Java 11, Hadoop 3.4.2 lean, checksum, user analytics và thư mục có quyền ghi.
- [Dockerignore](../infra/hadoop/.dockerignore): chỉ đưa Dockerfile/entrypoint vào context.
- [Entrypoint](../infra/hadoop/entrypoint.sh): nhận vai trò namenode/datanode, kiểm tra cluster ID, xử lý format NameNode ban đầu rồi exec Hadoop.
- [core-site.xml](../infra/config/hadoop/core-site.xml): filesystem mặc định và static HTTP user.
- [hdfs-site.xml](../infra/config/hadoop/hdfs-site.xml): replication, địa chỉ, đường dẫn dữ liệu, hostname và safe mode.

Entrypoint dùng `exec` để tiến trình Hadoop nhận tín hiệu dừng. Nó không chạy cụm bằng SSH hoặc script daemon background.

## 5. Spark image và dependency

- [Dockerfile](../infra/spark/Dockerfile): Python 3.11, Java 17, Spark 3.5.7, user và thư mục scratch; cài dependency lúc build.
- [Dockerignore](../infra/spark/.dockerignore): chỉ gửi Dockerfile, requirements và script tải JAR.
- [requirements.txt](../infra/spark/requirements.txt): confluent-kafka, pymongo, dnspython đã khóa phiên bản.
- [download_jars.py](../infra/spark/download_jars.py): tải bốn JAR theo SHA-512; kiểm tra dependency được Spark distribution cung cấp sẵn.
- [spark-defaults.conf](../infra/config/spark/spark-defaults.conf): master/driver/executor, network, parallelism, timezone và event log.

Bốn JAR bổ sung là Kafka SQL connector, Kafka token provider, kafka-clients và commons-pool2. Hadoop runtime, spark-tags, jsr305, SLF4J và thư viện nén dùng từ bản Spark đã tải để tránh cài lặp phiên bản khác.

Master, worker và client chạy khác command nhưng dùng cùng image. Mongo init cũng tận dụng Python/pymongo trong image này; nó không khởi động SparkSession.

## 6. Startup và init scripts

| File                                                | Công dụng và điểm quan trọng                                         |
| --------------------------------------------------- | -------------------------------------------------------------------- |
| [kafka-start.sh](../infra/scripts/kafka-start.sh)   | So cluster ID khi metadata tồn tại rồi gọi launcher chính thức Kafka |
| [kafka-init.sh](../infra/scripts/kafka-init.sh)     | Tạo topic nếu thiếu, đọc lại và kiểm tra partitions/RF/retention     |
| [hdfs-init.sh](../infra/scripts/hdfs-init.sh)       | Đợi hai DataNode/safe mode OFF, tạo thư mục HDFS                     |
| [mongo-health.js](../infra/scripts/mongo-health.js) | Xác thực root từ environment và ping trong healthcheck               |
| [mongo_init.py](../infra/scripts/mongo_init.py)     | Tạo app user nếu thiếu, marker bootstrap và xác minh app credential  |

Những script này tạo nền tảng, không xử lý event thương mại điện tử.

## 7. Runtime kiểm thử

### Common helpers

[common.py](../infra/smoke/common.py):

| Hàm              | Vai trò                                                           |
| ---------------- | ----------------------------------------------------------------- |
| `mongo_settings` | Tạo connection settings từ application environment                |
| `mongo`          | Tạo MongoClient app account                                       |
| `kafka`          | Tạo Kafka AdminClient                                             |
| `create_topic`   | Tạo topic kiểm thử, replication 1 và retention một giờ            |
| `delete_topic`   | Yêu cầu xóa topic được truyền vào                                 |
| `publish`        | Gửi JSON, kiểm tra callback/flush để phát hiện delivery lỗi       |
| `consume`        | Đọc số record mong đợi, không auto-commit, giới hạn thời gian chờ |

Caller chịu trách nhiệm truyền topic kiểm thử đúng; helper delete không phải CLI dọn toàn bộ project.

### Readiness

[readiness.py](../infra/smoke/readiness.py):

- `get_json` đọc endpoint JSON với timeout.
- `check` kiểm tra Spark, HDFS, Kafka và MongoDB.
- `main` hỗ trợ một lần thử hoặc retry hữu hạn.

### Smoke pipeline

[pipeline.py](../infra/smoke/pipeline.py):

- `executor_hostname` chạy ở executor để thu hostname thực thi task.
- `main` tạo fixture, kiểm tra Kafka, nộp Spark job, đọc/ghi HDFS, upsert MongoDB, kiểm tra replica và cleanup.
- `--keep` bỏ cleanup tài nguyên run để kiểm tra thủ công.
- Dòng `INFRA_RESULT` là giao diện kết quả giữa job và manager.

### Persistence

[persistence.py](../infra/smoke/persistence.py):

- Action `create` ghi marker vào ba storage.
- Action `verify` đọc và đối chiếu token.
- Action `clean` dọn đúng marker/topic/file.
- Token được kiểm tra theo format 32 ký tự hex.
- Manager thực hiện down/up; script persistence chỉ thực hiện action đã nhận.

## 8. Test source

[test_infrastructure.py](../infra/tests/test_infrastructure.py) có ba class: BootstrapTests, ComposeTests và HadoopConfigurationTests. Các test này không nộp Spark job hoặc gọi MongoDB.

Đọc [tài liệu kiểm thử](06-testing.md) để biết từng nhóm chứng minh điều gì và những phần còn chưa xác minh runtime.

## 9. Tìm chỗ thay đổi

| Muốn thay đổi                         | Nơi bắt đầu                                                     |
| ------------------------------------- | --------------------------------------------------------------- |
| Cổng hoặc đường dẫn dataset host      | Template/cấu hình local và Compose                              |
| Số service/worker hoặc resource limit | Compose, sau đó readiness và test ràng buộc topology            |
| Heap/core/parallelism Spark           | Compose và Spark defaults                                       |
| Replication/safe mode/đường dẫn HDFS  | Hadoop XML, sau đó init/readiness/tests liên quan               |
| Topic nghiệp vụ                       | Kafka init và readiness                                         |
| Runtime version hoặc JAR              | Dockerfile, requirements, script checksum và cấu hình liên quan |
| Quy trình Makefile                    | Makefile và manager                                             |
| Tiêu chí nghiệm thu hạ tầng           | Smoke/persistence/readiness và tài liệu kiểm thử                |

Thay topology cần cập nhật đồng bộ các phép kiểm tra đang cố ý yêu cầu đúng hai worker/hai DataNode. Không chỉ đổi Compose rồi bỏ qua lỗi test.

## 10. Điểm nối với giai đoạn tiếp theo

- Producer nghiệp vụ dùng listener nội bộ Kafka.
- Batch ingestion đọc dataset tại `/opt/dataset` và ghi vào vùng HDFS phù hợp.
- Job feature/training dùng Spark cluster, lưu artifact model trên HDFS.
- Kết quả phục vụ API sau này được ghi bằng application account MongoDB.
- Checkpoint nghiệp vụ có vùng riêng dưới `/ecommerce/checkpoints`.
- Cần thêm source module, schema nghiệp vụ và tests tương ứng; không đổi smoke fixture thành toàn bộ ứng dụng sản phẩm.

Các điểm nối này là hướng tích hợp, không phải chức năng nghiệp vụ đã có trong repository.
