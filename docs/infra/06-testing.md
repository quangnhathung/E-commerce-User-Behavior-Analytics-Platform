# 06 — Kiểm thử và cách hiểu kết quả

[← Mục lục](README.md)

## 1. Ba tầng kiểm tra

| Tầng                 | Lệnh                                  | Cần daemon?                           | Chứng minh gì?                                               |
| -------------------- | ------------------------------------- | ------------------------------------- | ------------------------------------------------------------ |
| Cấu hình/offline     | `make config`, `make test`            | Không                                 | Cấu hình parse được và các ràng buộc bootstrap/topology đúng |
| Readiness            | `make doctor` hoặc readiness script   | Có, cụm phải chạy để kiểm tra toàn bộ | Service và topology hiện tại đáp ứng phép kiểm tra           |
| Tích hợp/persistence | `make smoke`, `make persistence-test` | Có                                    | Dữ liệu thật đi qua các dịch vụ và marker sống qua down/up   |

Không suy diễn kết quả tầng trước thành bằng chứng của tầng sau.

## 2. Kiểm thử offline

Nguồn: [test_infrastructure.py](../../infra/tests/test_infrastructure.py) và [test_download.py](../../infra/tests/test_download.py).

| Nhóm                     | Số test | Nội dung                                                                                                                                                                                  |
| ------------------------ | ------: | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| BootstrapTests           |       9 | Setup giữ credential/ID, quyền file 0600; không ghi đè file lỗi; từ chối cổng trùng/image không khóa; parse environment và ps; reset cần xác nhận; biến project không bị shell cha ghi đè |
| ComposeTests             |       8 | Đủ topology và ngân sách RAM; cổng loopback duy nhất; dependency không vòng; DataNode volume riêng và bind mount read-only; runtime nhất quán và client không nhận root credential        |
| HadoopConfigurationTests |       1 | Replication, hostname networking và số DataNode tối thiểu                                                                                                                                 |

Bổ sung ba test `CachedDownloadTests` kiểm tra cache/hash/atomic publication mà không cần socket; bảy test `DownloadTests` dùng curl thật và HTTP server loopback để kiểm tra cache, mất kết nối, SIGTERM, HTTP Range/416, checksum sai và fallback. Bổ sung hai test `SparkReadinessTests` xác nhận hai worker một core/640 MiB và từ chối worker thiếu tài nguyên. Các test Compose còn kiểm tra preflight với 8,3 GiB/thiếu RAM, credit đúng project và heap khớp tài nguyên quảng bá/container. Suite hạ tầng có 30 test. Nếu môi trường cấm tạo socket, bảy test HTTP được ghi rõ là skipped; các lỗi bind khác vẫn làm test thất bại. Cần Bash, curl và GNU coreutils trên host.

`make test` còn chạy 16 test ứng dụng, tổng cộng 46 test. Hợp đồng sự kiện, snapshot, đóng gói và hủy job được mô tả trong [kiểm thử dữ liệu](../data/05-testing.md). Kiểm thử Spark/HDFS của ingestion chạy riêng bằng `make test-data`.

Các test Compose gọi CLI để expand cấu hình thành JSON từ template. Chúng không pull image, không tạo container hoặc truy cập daemon. Test setup dùng thư mục tạm; password được sinh trong test không được in ra.

`make config` riêng cần file local hợp lệ, nhưng chỉ chạy Compose config quiet nên không cần daemon và không in environment đầy đủ.

## 3. Readiness

Nguồn: [readiness.py](../../infra/smoke/readiness.py).

Phép kiểm tra xác nhận:

1. Spark master JSON có đúng hai worker `ALIVE`.
2. Mỗi worker báo một core và ít nhất 640 MiB memory.
3. JMX HDFS báo đúng hai live DataNode.
4. Raw có ba partition, DLQ có một partition và mỗi partition có một replica.
5. Application account MongoDB đọc được marker bootstrap.

Readiness không tự sửa topology hoặc tạo lại dữ liệu. Init kiểm tra retention topic; readiness tập trung vào metadata/topology và truy cập account.

`--once` thử một lượt. Khi không có `--once`, script có ngân sách retry mặc định 120 giây; mỗi lượt còn có thời gian gọi I/O. Healthcheck client gọi `--once`; manager sau khi up gọi readiness có retry.

## 4. Smoke test xuyên dịch vụ

Nguồn: [pipeline.py](../../infra/smoke/pipeline.py) và [common.py](../../infra/smoke/common.py).

### 4.1. Chuẩn bị

- Readiness phải đạt.
- Sinh run ID `infra-smoke-<uuid>`.
- Tạo topic riêng có ba partition, replication 1 và retention một giờ.
- Sinh sáu event `view` có visitor/event ID riêng, source `infra-smoke`.
- Producer chỉ định partition để phân phối qua ba partition; bật idempotence và acks all.
- Consumer đọc lại sáu record và đối chiếu tập event ID.

Đây là dữ liệu sinh bởi script, không lấy từ CSV Retailrocket. Phép thử không đánh giá tính đúng của label, feature hoặc funnel nghiệp vụ.

### 4.2. Xử lý phân tán

Spark tạo RDD tám partition, lấy hostname nơi task thực thi và yêu cầu thấy cả `spark-worker-1` lẫn `spark-worker-2`.

Sau đó tính tổng dãy số từ 0 đến 999.999 trên tám partition. Kết quả biết trước là `499999500000`. Việc đối chiếu vừa xác nhận job thực thi được, vừa bắt kết quả tính sai.

Có hai worker đăng ký chưa đủ; kiểm tra hostname mới xác nhận task đã chạy trên cả hai trong kịch bản này.

### 4.3. Streaming và checkpoint

- Đọc topic test với `startingOffsets=earliest`.
- Parse JSON bằng schema tường minh; trong fixture này trường thời gian được giữ dạng chuỗi.
- Dùng `availableNow=True` để xử lý lượng dữ liệu hiện có rồi kết thúc.
- Ghi Parquet và checkpoint trong thư mục run trên HDFS.
- Đọc lại Parquet, xác nhận đúng sáu event ID.
- Khởi chạy lần thứ hai với cùng checkpoint, xác nhận vẫn sáu event.
- Kiểm tra thư mục commit log của checkpoint tồn tại.

Test này không mô phỏng mọi thời điểm crash, event đến trễ hoặc payload lỗi. Những trường hợp nghiệp vụ đó cần bổ sung khi triển khai pipeline thật.

### 4.4. MongoDB và replica HDFS

Executor upsert record vào MongoDB bằng khóa `run_id:event_id`. Thực hiện hai lần ghi rồi đọc lại: vẫn phải có sáu document đúng tập event ID.

Script còn hỏi HDFS về vị trí các block Parquet và yêu cầu mỗi block có hai host replica. Nó kiểm tra vị trí thực tế mà HDFS báo, không chỉ đọc giá trị cấu hình replication.

Hai lần upsert chỉ chứng minh tính lặp lại an toàn cho fixture/khóa này. Chưa có transaction toàn cục giữa Kafka, HDFS và MongoDB.

### 4.5. Cleanup

Mặc định script:

- Xóa thư mục HDFS của run.
- Xóa document MongoDB có đúng run ID.
- Dừng SparkSession.
- Yêu cầu xóa topic test.

Nếu các assertion đạt nhưng cleanup thất bại, script vẫn báo lỗi. Khi dùng `--keep`, tài nguyên của run được giữ để xem và cần được dọn riêng sau đó. Spark event logs nằm ngoài thư mục run smoke và vẫn được giữ.

## 5. Báo cáo smoke

Khi chạy qua manager:

| Artifact host                 | Công dụng                                              |
| ----------------------------- | ------------------------------------------------------ |
| `artifacts/infra/smoke.log`   | Output của Spark job, gồm cả lỗi nếu có                |
| `artifacts/infra/smoke.json`  | Báo cáo được ghi khi job exit 0 và có INFRA_RESULT     |
| `artifacts/infra/driver.lock` | Khóa ngăn các job được manager quản lý chạy chồng nhau |

JSON dưới đây **chỉ minh họa schema**, không phải kết quả đo thực tế:

```json
{
  "spark_workers": 2,
  "hdfs_datanodes": 2,
  "kafka_topics": 2,
  "mongo_authenticated": true,
  "run_id": "infra-smoke-00000000000000000000000000000000",
  "records": 6,
  "executor_hosts": ["spark-worker-1", "spark-worker-2"],
  "aggregation_sum": 499999500000,
  "checkpoint_restart": true,
  "mongo_upsert_retry": true,
  "minimum_block_replicas": 2,
  "hdfs_path": "hdfs://namenode:9000/ecommerce/_smoke/infra-smoke-00000000000000000000000000000000",
  "kept": false
}
```

`kafka_topics=2` là hai topic nghiệp vụ được readiness kiểm tra; topic test là tài nguyên bổ sung tạm thời. Khi `kept=false`, đường dẫn trong report là định danh của run đã dọn, không có nghĩa file còn tồn tại.

`INFRA_RESULT=` là tiền tố manager dùng để tìm JSON trong stdout. Job không có dòng đó hoặc exit khác 0 không được coi là thành công.

## 6. Persistence test

Nguồn: [persistence.py](../../infra/smoke/persistence.py) và action tương ứng trong [manager](../../infra/manage.py).

### Create

- Sinh token 32 ký tự hex và lưu trên host.
- Tạo topic `infra-persist-<token>` có một partition, retention một giờ.
- Gửi một record marker.
- Ghi token bằng Hadoop filesystem API vào HDFS.
- Thêm marker vào collection `infra_persistence`.

### Down/up

Manager dừng/gỡ container nhưng giữ named volume, rồi khởi động lại và kiểm tra readiness. Đây là dừng có kiểm soát, không phải mô phỏng mất điện hoặc đĩa hỏng.

### Verify

- Kafka trả lại đúng record marker.
- HDFS trả đúng token.
- MongoDB có đúng document marker.

### Clean

Xóa topic, file marker và document tương ứng. Các action create/clean không được thiết kế để gọi lặp vô hạn trên marker đã có/đã xóa. Nếu một phần cleanup đã hoàn thành, cần xem lỗi và trạng thái từng nơi trước khi chạy lại.

Các artifact:

| File                          | Nội dung                           |
| ----------------------------- | ---------------------------------- |
| `persistence-token.txt`       | Token để tra cứu/phục hồi phép thử |
| `persistence-create.log/json` | Kết quả tạo marker                 |
| `persistence-verify.log/json` | Kết quả sau down/up                |
| `persistence-clean.log/json`  | Kết quả dọn marker                 |

Tất cả nằm dưới `artifacts/infra/`. Mỗi tên `.log` và `.json` là hai file riêng. Job thất bại có thể chỉ có log; JSON chỉ được ghi khi hoàn thành đúng. Artifact cùng tên được ghi đè ở lần chạy sau, nên lưu bản sao nếu cần giữ lịch sử.

## 7. Giới hạn thời gian và phạm vi

- Manager dùng timeout 600 giây cho mỗi lượt Spark job, kill-after 15 giây.
- Mỗi streaming query trong smoke có thời gian chờ 120 giây.
- Pipeline chạy hai query nối tiếp; persistence có ba lượt job và một chu kỳ down/up.
- Các khoảng chờ không phải số đo latency/throughput của sản phẩm.

Bộ kiểm thử hạ tầng không chứng minh HA vật lý, tải cao, khả năng phục hồi mọi lỗi giữa nhiều sink, chất lượng ML hoặc dữ liệu nghiệp vụ đã được ingest. Phần ingestion có [tiêu chí nghiệm thu riêng](../data/05-testing.md).

## 8. Trạng thái thực tế đã biết

Phiên cập nhật ngân sách RAM đạt 23 test, với 7 test HTTP skipped vì sandbox cấm tạo socket. Compose config và cú pháp Python/Bash đã đạt. Các test HTTP cần chạy lại trên terminal host không bị giới hạn này. Build bị chặn ở truy cập Docker socket; khi rà soát tài liệu chưa có artifact runtime để xác nhận smoke/persistence.

Sau khi chạy trên môi trường có daemon, dùng artifact thật để bổ sung bằng chứng. Không thay trạng thái “chưa xác minh” bằng “đạt” chỉ vì script đã được tạo.

Đọc tiếp [xử lý lỗi](07-troubleshooting.md).
