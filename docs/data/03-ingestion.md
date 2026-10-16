# 03 — Chạy ingest và xử lý lỗi

[← Mục lục dữ liệu](README.md)

## 1. Chuẩn bị

Dùng image đã build, cấu hình 7 GiB và bốn CSV có header đúng. Từ thư mục gốc:

```bash
make up
make doctor
make smoke
make test
make test-data
make ingest
```

Compose mount `app/` read-only vào `/opt/app` chỉ trong Spark client. `make up` tạo lại client nếu cần áp dụng mount. Không cần build lại image để thêm mã ứng dụng này. Nếu chưa có image thì vẫn phải hoàn tất bước build hạ tầng trước.

`make ingest` chạy [host CLI](../../app/manage.py). CLI dùng cùng parser `.env`, preflight, readiness và lock driver với hạ tầng. Nó không khởi động/dừng/reset cụm tự động.

## 2. Bên trong một lượt chạy

1. Kiểm tra daemon, budget/cổng/dataset và trạng thái 9 service + 3 init.
2. Lấy lock chung, kiểm tra mount ứng dụng trong container.
3. Tạo ZIP ứng dụng xác định trong `/tmp` của client; chỉ chứa package Python.
4. Chạy `spark-submit --py-files`, kiểm tra checksum archive trước khi xử lý.
5. Fingerprint nguồn, công bố/tái sử dụng Bronze, tạo/tái sử dụng Silver và kiểm tra lại kết quả.
6. Job xuất `INGEST_RESULT=`; host thu JSON và ghi báo cáo local bằng replace atomic.
7. Dọn ZIP riêng của lượt chạy sau khi job đã dừng.

[Remote wrapper](../../app/run_job.py) áp timeout 1.800 giây và kill-after 15 giây cho process group. Khi Ctrl+C ở host, CLI gửi cancel theo UUID tới wrapper trong container; dừng docker exec client đơn thuần chưa đủ bảo đảm remote driver dừng.

Lock chỉ điều phối các lệnh Make/CLI trong cùng workspace. Không chạy thêm spark-submit thủ công hoặc một clone khác đồng thời với driver đang dùng cổng cố định.

## 3. Đầu ra local

```text
artifacts/data/
  runs/<UUID>.log
  latest-ingest.json
  <dataset_version>/events-v1/
    bronze-manifest.json
    manifest.json
    quality.json
    result.json
  test-data.json
```

Log giữ riêng từng lần chạy. `latest-ingest.json` là con trỏ tới lượt ingest thành công gần nhất, gồm version, trạng thái chất lượng, đường dẫn HDFS và log. Khi job thất bại, con trỏ cũ vẫn có thể tồn tại; kiểm tra exit code/log mới, không coi con trỏ cũ là bằng chứng job mới thành công.

Các bản manifest/quality local là bản sao phục vụ đọc và báo cáo; HDFS là nguồn chính thức. Chạy lại có thể khôi phục bản sao local từ snapshot HDFS đã được xác minh.

`source-audit.json`, nếu có từ phiên phát triển, là đối soát CSV offline, không phải kết quả ingest HDFS.

## 4. Đọc dữ liệu và theo dõi

Chạy `docker stats --no-stream` trong terminal khác khi ingest và đối chiếu tên container project. Hướng dẫn kiểm tra OOM/restart nằm ở [ngân sách RAM](../infra/10-memory-budget.md).

Trong Spark shell/ứng dụng, đọc đường dẫn được trả về trong kết quả:

```python
events = spark.read.parquet(silver_path + "/events")
events.groupBy("event_type").count().show()
```

Chỉ collect thống kê nhỏ; không đưa toàn bộ events về driver. Bộ nhớ driver 512 MiB, executor 640 MiB; DataFrame trung gian dùng DISK_ONLY. Bronze nguồn gần 1 GB, replication 2, cộng Silver và staging; giữ kiểm tra disk tối thiểu 30 GiB hiện có.

## 5. Chạy lại và lỗi thường gặp

| Triệu chứng | Xử lý |
| --- | --- |
| `Application source is not mounted` | Chạy `make up`, sau đó thử lại |
| Thiếu file/sai header | Sửa nguồn/đường dẫn; không sửa file trong snapshot Bronze đã công bố |
| `No valid events` | Xem `INGEST_DIAGNOSTIC` trong log; Bronze còn nguyên, Silver không được công bố |
| `Snapshot identity/version mismatch` | Kiểm tra code hash và transform version; tạo transform version mới khi đổi logic |
| Checksum/inventory mismatch | Điều tra hoặc khôi phục snapshot; job không tự overwrite |
| Chưa có hai replica | Kiểm tra DataNode/disk và HDFS report rồi chạy lại |
| `Expected actual parsing on both Spark workers` | Kiểm tra hai executor/worker và log Spark; không bỏ điều kiện để tuyên bố xử lý phân tán |
| Timeout/OOM | Xem log, memory/restart, disk spill; điều chỉnh workload/code sau khi đo, không tự tăng vượt budget |
| Host ghi report local thất bại sau commit | Khắc phục filesystem local rồi chạy lại; snapshot HDFS có thể đã hoàn tất và sẽ được xác minh/tái sử dụng |

Lỗi/kill sau Bronze hoặc trước commit Silver không làm xuất hiện Silver hoàn tất một phần. Lượt sau dùng staging UUID mới. Bình thường staging riêng được dọn trong finally; kill cứng hoặc mất kết nối có thể để lại staging ẩn. Chỉ dọn đúng thư mục staging của lượt đã dừng sau khi xác minh; không xóa namespace dataset/volume để thử lại.

Khi hủy mà CLI không xác nhận remote process đã dừng, giữ archive/log, kiểm tra container trước khi chạy Spark job khác. Không bỏ khóa hoặc kill process của dự án khác.
