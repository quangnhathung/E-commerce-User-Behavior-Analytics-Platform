# 05 — Hướng dẫn vận hành

[← Mục lục](README.md)

Cấu hình RAM mặc định hiện là 7 GiB. Khi chuyển từ cấu hình cũ, chạy `make config`, `make test`, rồi `make up`, `make doctor`, `make smoke`; không cần build lại image. [Chi tiết ngân sách và theo dõi OOM](10-memory-budget.md).

Tất cả lệnh bên dưới chạy từ thư mục gốc repository. Cấu hình local được tạo bởi `make setup`. Giá trị mong đợi là tiêu chí đối chiếu, không phải log đã đo trên cụm thật.

## 1. Khởi động lần đầu

```bash
docker info
docker compose version
make setup
make build
make up
make doctor
make smoke
```

Đọc lỗi ngay tại bước thất bại. `make up` không tự build; `make smoke` không tự khởi động cụm.

## 2. Toàn bộ lệnh Makefile

| Lệnh                                     | Điều kiện                                   | Các bước chính và tác động                                     | Kết quả/artifact                              |
| ---------------------------------------- | ------------------------------------------- | -------------------------------------------------------------- | --------------------------------------------- |
| `make setup`                             | Python; template có sẵn                     | Tạo hoặc validate file local, không gọi Docker                 | File local được giữ/sinh với ID và credential |
| `make build`                             | File local hợp lệ; daemon và Internet       | Preflight build, pull Kafka/MongoDB, build Hadoop/Spark        | Image local và `versions.json`                |
| `make up`                                | Image đã có; đủ tài nguyên/cổng             | Preflight, up không build, chờ health/init, kiểm tra readiness | Chín service healthy, ba init exit 0          |
| `make doctor`                            | File local, daemon                          | Preflight; nếu có trạng thái container thì xác nhận toàn cụm   | Báo kết quả hoặc lỗi cụ thể                   |
| `make config`                            | File local và Compose CLI                   | Validate rồi gọi Compose config quiet                          | Không tạo container, không in credential      |
| `make test`                              | Python, Docker Compose CLI                  | Chạy unittest offline                                          | Kết quả test; không cần daemon                |
| `make smoke`                             | Cụm healthy, driver không bận               | Gửi dữ liệu nhỏ, chạy Spark/HDFS/MongoDB, dọn run              | `smoke.log` và `smoke.json` khi thành công    |
| `make persistence-test`                  | Cụm healthy, không có job cần tiếp tục chạy | Tạo marker, down/up, verify, clean                             | Token và ba nhóm log/JSON                     |
| `make test-data`                         | Cụm healthy, mã ứng dụng được mount        | Fixture Spark/HDFS, fault/retry, kiểm tra chất lượng và dọn namespace riêng | `artifacts/data/test-data.json` khi thành công |
| `make ingest`                            | Cụm healthy, đủ bốn CSV, driver không bận  | Snapshot Bronze/Silver, kiểm tra checksum, loại trùng và cách ly lỗi | Manifest, quality và run log trong `artifacts/data/` |
| `make prepare-demo` | Có Silver lịch sử, driver rảnh | Xuất replay, manifest, tham chiếu và offset đầu | `artifacts/streaming/<run_id>/` |
| `make stream` | Run đã chuẩn bị, driver rảnh | Streaming KPI, checkpoint và tự kết thúc | Report streaming và tài nguyên |
| `make replay` | Run đã chuẩn bị, producer rảnh | Phát Kafka theo tốc độ, ACK/cursor | Producer report và ACK local |
| `make stream-status` | Có client/MongoDB và config run | Đọc trạng thái, KPI snapshot đã công bố | Status JSON |
| `make test-stream` | Cụm healthy, driver rảnh | Fixture và fault/restart trên namespace riêng | `artifacts/streaming/test-stream.json` |
| `make ps`                                | File local, daemon                          | Compose ps all                                                 | Container đang chạy và init đã thoát          |
| `make logs`                              | File local, daemon                          | Xem tối đa 100 dòng mỗi service                                | Không follow liên tục                         |
| `make versions`                          | Image đã có, daemon                         | Inspect image và phiên bản công cụ                             | `versions.json`                               |
| `make down`                              | File local, daemon                          | Dừng/gỡ container/network; giữ named volume                    | Cụm dừng, dữ liệu chính còn                   |
| `make reset CONFIRM=delete-project-data` | Cố ý xóa dữ liệu project                    | Down kèm xóa volume                                            | Dữ liệu Kafka/HDFS/MongoDB của project bị xóa |

`doctor` khi chưa có container sẽ báo chưa khởi động. Nếu còn container đã dừng, nó có thể báo service chưa healthy; khi đó dùng `ps` để xem và `up` để khởi động.

Mọi action qua manager ngoài `setup` đều yêu cầu cấu hình local hợp lệ. `make test` gọi unittest trực tiếp nên không cần file local; nó dùng template khi kiểm tra Compose.

`ingest` và `test-data` chạy qua [manager ứng dụng](../../app/manage.py), dùng cùng cấu hình và khóa driver với smoke/persistence. Chi tiết đầu vào, tác động khi chạy lại và xử lý lỗi: [vận hành ingestion](../data/03-ingestion.md).

Các lệnh replay/streaming dùng [manager streaming](../../app/streaming_manage.py). Producer có khóa riêng, các Spark job dùng khóa driver chung. Cách chạy hai terminal và tham số RUN_ID/START/END/RATE: [vận hành streaming](../streaming/03-operations.md).

## 3. Trạng thái và log

```bash
make ps
make logs
docker compose logs --tail 100 kafka
docker compose logs --tail 100 namenode datanode-1 datanode-2
docker compose logs --tail 100 spark-master spark-worker-1 spark-worker-2
docker compose logs --tail 100 kafka-init hdfs-init mongo-init
```

Kỳ vọng: chín service có trạng thái chạy và healthy. Init job phải có exit code 0. Thông tin `running` đơn lẻ không thay thế readiness.

Nếu muốn follow một service:

```bash
docker compose logs --follow --tail 100 kafka
```

Thoát follow log bằng Ctrl+C không phải thao tác dừng service.

## 4. Kiểm tra Kafka

```bash
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --describe --topic ecommerce.events.raw
docker compose exec -T kafka /opt/kafka/bin/kafka-configs.sh --bootstrap-server kafka:9092 --entity-type topics --entity-name ecommerce.events.raw --describe
```

Đối chiếu raw có ba partition, replication factor 1, retention `172800000` ms. Với DLQ, dùng cùng lệnh và thay tên topic; DLQ có một partition.

Trên host dùng `127.0.0.1:19092`; trong container dùng `kafka:9092`. Các lệnh ở trên là đọc metadata, không tự gửi event vào topic nghiệp vụ.

## 5. Kiểm tra HDFS

```bash
docker compose exec -T namenode hdfs dfsadmin -report
docker compose exec -T namenode hdfs dfsadmin -safemode get
docker compose exec -T namenode hdfs dfs -ls /ecommerce
docker compose exec -T namenode hdfs dfs -du -h /ecommerce
docker compose exec -T namenode hdfs fsck /ecommerce/_smoke -files -blocks -locations
```

Kỳ vọng: hai live DataNode, safe mode OFF, bảy thư mục nền. Nếu smoke đã dọn dữ liệu thì `_smoke` có thể không còn file kiểm thử. File cần tồn tại thì mới có block để quan sát replica.

Không format NameNode hoặc xóa VERSION để xử lý một lỗi kết nối.

## 6. Kiểm tra Spark và runtime

Mở UI:

- Master: `http://127.0.0.1:18080`.
- Worker 1: `http://127.0.0.1:18081`.
- Worker 2: `http://127.0.0.1:18082`.
- Ứng dụng: `http://127.0.0.1:14040` khi có job.

```bash
docker compose exec -T spark-client python3 /opt/infra/smoke/readiness.py --once
docker compose exec -T spark-client python3 --version
docker compose exec -T spark-client java -version
docker compose exec spark-client bash
```

Client có Python 3.11 và Java 17 theo image. Hadoop image dùng Java 11. UI app không truy cập được khi không có driver chạy là trạng thái bình thường.

Khi thêm source job nghiệp vụ, cần mount/đóng gói module trước rồi nộp bằng `spark-submit`. Không giả định file trên host tự xuất hiện trong client.

## 7. Kiểm tra MongoDB bằng app account

Lệnh dùng credential đã có trong environment của client; không in password:

```bash
docker compose exec -T spark-client python3 -c 'import os,sys; sys.path.insert(0,"/opt/infra/smoke"); from common import mongo; c=mongo(); print(c[os.environ["MONGO_DATABASE"]].infra_metadata.find_one({"_id":"bootstrap-v1"})); c.close()'
```

Kỳ vọng marker có `_id=bootstrap-v1` và `schema_version=1`. Khi dùng công cụ kết nối từ host, chọn host `127.0.0.1`, port `27018`, database/authSource `ecommerce` và application account local.

## 8. Kiểm tra offline trước khi có daemon

```bash
make config
make test
```

`make config` cần chạy setup trước. Cả hai vẫn cần Compose CLI; chỉ không cần quyền daemon.

Không dùng việc hai lệnh này đạt để kết luận image đã build hoặc các node đã kết nối.

## 9. Smoke test và giữ dữ liệu để quan sát

```bash
make smoke
```

Manager lưu log/JSON dưới `artifacts/infra/`. Chế độ mặc định dọn tài nguyên kiểm thử sau khi chạy.

Để chủ động giữ run và xem replica:

```bash
docker compose exec -T spark-client timeout 600s spark-submit /opt/infra/smoke/pipeline.py --keep
docker compose exec -T namenode hdfs fsck /ecommerce/_smoke -files -blocks -locations
```

Lệnh trực tiếp này không đi qua khóa của manager và không tự tạo `artifacts/infra/smoke.json` trên host. Dòng `INFRA_RESULT` xuất ở terminal chứa run ID, đường dẫn HDFS và `kept=true`.

Dọn thủ công phải nhắm đúng topic `infra-smoke-<uuid>`, thư mục của run đó và document `infra_smoke` có đúng `run_id`. Hiện chưa có lệnh Makefile riêng cho việc dọn một run giữ lại. Không xóa cả database hoặc thư mục nghiệp vụ để dọn một lần smoke.

## 10. Dữ liệu qua lần khởi động lại

```bash
make persistence-test
```

Lệnh tạm dừng cụm project. Nó không tiếp tục job nghiệp vụ đang chạy, vì vậy chỉ dùng khi cụm đang rảnh.

Nếu thất bại sau khi tạo marker, đọc token tại `artifacts/infra/persistence-token.txt`. Sau khi phục hồi cụm, có thể chạy lại phép verify bằng token đó:

```bash
INFRA_PERSIST_TOKEN="$(cat artifacts/infra/persistence-token.txt)"
docker compose exec -T spark-client timeout 600s spark-submit /opt/infra/smoke/persistence.py verify "$INFRA_PERSIST_TOKEN"
```

Khi verify xong và muốn dọn đúng marker:

```bash
docker compose exec -T spark-client timeout 600s spark-submit /opt/infra/smoke/persistence.py clean "$INFRA_PERSIST_TOKEN"
```

Token không phải credential, nhưng cần đúng token của lần thử. Không chạy lại `create` với cùng token đã có vì script dùng thao tác tạo marker, không thiết kế create chạy lặp.

## 11. Dừng, cập nhật và reset

Dừng cuối buổi, giữ dữ liệu:

```bash
make down
```

Khởi động lại:

```bash
make up
make doctor
```

Sau thay đổi Dockerfile/dependency:

```bash
make build
make up
make doctor
```

Sau thay đổi file Hadoop/Spark được mount, dùng chu kỳ down/up để tiến trình đọc lại cấu hình.

Reset chỉ khi muốn xóa dữ liệu hạ tầng của project:

```bash
make reset CONFIRM=delete-project-data
```

Reset giữ source, dataset và artifact host. Sau reset vẫn cần `make up` để dựng lại dịch vụ; chưa cần sinh lại credential/cluster ID nếu tiếp tục dùng file local hiện có.

Đọc thêm [ý nghĩa các kiểm thử](06-testing.md) và [xử lý lỗi](07-troubleshooting.md).
