# 01 — Kiến trúc và mã nguồn

[← Mục lục streaming](README.md)

## 1. Luồng hoạt động

`prepare-demo` chạy một Spark job đọc snapshot Silver lịch sử đã xác minh, lọc khoảng UTC, sắp `(event_time, event_id)`, xuất nguồn JSONL và KPI tham chiếu. Bản chính của nguồn replay nằm trên HDFS; bản host được copy và kiểm tra SHA-256 để producer đọc.

Producer chạy Python trong `spark-client`, nhận file qua stdin của `docker compose exec -T`; không cần cài Kafka client trên host. Producer gửi từng event, chờ ACK, lưu cursor MongoDB, rồi mới chuyển sang event tiếp theo. Tốc độ dùng đồng hồ monotonic.

Streaming là Spark application riêng. Mỗi 15 giây, `foreachBatch` ghi raw Kafka, phân loại, công bố Silver, tính Gold, gửi DLQ và ghi snapshot KPI MongoDB. Khi producer hoàn tất và offset đã drain, query dừng, đối soát kết quả rồi ghi report.

## 2. Tiến trình và khóa

| Thành phần | Vai trò và khóa |
| --- | --- |
| Host CLI | Validate cấu hình, preflight, đóng gói, chạy/cancel job, lưu log và artifact |
| Prepare | Spark driver, dùng khóa driver hiện có |
| Streaming | Spark driver foreground, giữ khóa tới khi tiến trình dừng |
| Replay producer | Python không tạo SparkContext; khóa producer riêng |
| Status | Python đọc MongoDB; không lấy khóa driver |
| Test streaming | Một Spark session, giữ khóa driver, tạo namespace kiểm thử riêng |

Khóa file chỉ áp dụng trong cùng workspace. Không chạy Spark thủ công hoặc producer từ clone khác cùng lúc với các lệnh được quản lý. Wrapper có timeout 1.800 giây và kill-after 15 giây. Khi cancel không được xác nhận, giữ archive/log và kiểm tra tiến trình trước khi chạy Spark job khác.

## 3. Mã nguồn làm gì

| File/module | Công dụng |
| --- | --- |
| [Host manager](../../app/streaming_manage.py) | Các lệnh Make, khóa, copy nguồn, stdin producer, ACK local và report atomic |
| [Resource monitor](../../app/streaming_monitor.py) | Chỉ đọc container của project; kiểm tra limit, OOM, restart và lấy mẫu RAM |
| [Runtime entry](../../app/jobs/streaming.py) | Xác minh archive, nạp package từ ZIP rồi gọi action tương ứng |
| [Package builder](../../app/package_code.py) | Profile ingest giữ nguyên; profile streaming thêm package mới |
| [contracts](../../app/ecommerce_streaming/contracts.py) | Contract thuần Python, UTC, fingerprint nghiệp vụ, validation, offset và DLQ |
| [prepare](../../app/ecommerce_streaming/prepare.py) | Xác minh Silver, xuất nguồn có giới hạn, tạo tham chiếu và đăng ký run |
| [kafka_io](../../app/ecommerce_streaming/kafka_io.py) | Offset ban đầu, producer, ACK, rate, replay và delivery DLQ |
| [storage](../../app/ecommerce_streaming/storage.py) | Mở rộng Hadoop adapter cho streaming; đọc batch, copy nguồn và kiểm tra snapshot |
| [batch](../../app/ecommerce_streaming/batch.py) | Parse trên worker; phân loại, commit HDFS và điều phối các sink |
| [metrics](../../app/ecommerce_streaming/metrics.py) | Aggregate Spark chính xác cho toàn run và từng phút; fingerprint event đã nhận |
| [store](../../app/ecommerce_streaming/store.py) | Run registry, cursor, snapshot KPI, con trỏ công bố và trạng thái |
| [stream](../../app/ecommerce_streaming/stream.py) | Checkpoint, retry, tự kết thúc, đối soát cuối và report |
| [integration](../../app/ecommerce_streaming/integration.py) | Fixture thật Spark/HDFS/Kafka/Mongo, fault injection và cleanup riêng |

## 4. Lưu trữ

```text
/ecommerce/replay/<run_id>/
  events.jsonl
  config.json
  manifest.json
  _SUCCESS

/ecommerce/bronze/replay/<run_id>/<batch_id-20-digits>/raw/
/ecommerce/silver/replay/<run_id>/<batch_id-20-digits>/events/
/ecommerce/silver/replay/<run_id>/<batch_id-20-digits>/audit/
/ecommerce/gold/replay/<run_id>/<batch_id-20-digits>/metrics.json
/ecommerce/checkpoints/replay/<run_id>/
```

Mỗi root batch có manifest, marker và checksum payload riêng. Staging UUID nằm cùng filesystem; rename không ghi đè. Payload được xác nhận ít nhất hai replica trước công bố và kiểm tra lại khi tái sử dụng. Bronze giữ key/value binary và tọa độ Kafka. Silver `audit` chứa duplicate, invalid, late và foreign cùng raw bytes/mã lỗi. Gold có `metrics.json` KPI và trong `stream-v2` thêm `behavior.json`, `behavior-quality.json`, Parquet `sessions/`, `session_items/`, `funnel_products/`; xem [kiến trúc hành vi](../analytics/02-architecture.md).

Các namespace lịch sử `bronze/retailrocket` và `silver/retailrocket` được giữ nguyên. Package ingest không đổi byte; archive hiện tại vẫn có SHA-256 `ae42a5981bc20e1295df1dabbfab7e553af0f133a39c97fb52fa795ceafcb554`.

MongoDB dùng `stream_runs`, `stream_batches`, `kpi_overview`, `kpi_minute`, cùng `funnel_overview`/`funnel_products` cho hành vi. Document có ID từ run/batch/cửa sổ hoặc item; KPI và funnel dùng chung con trỏ batch công bố. Chỉ dùng application account hiện có, không thêm quyền root.

## 5. RAM và giới hạn

Giữ driver 512 MiB, executor 640 MiB và giới hạn Compose hiện có. DataFrame trung gian dùng `DISK_ONLY`, thống kê có giới hạn được đưa về driver; không collect toàn bộ lịch sử. Nguồn replay tối đa 10.000 event, producer chỉ có một event đang chờ xác nhận.

Monitor kiểm tra tổng limit thực tế của chín service không vượt 7 GiB, so sánh container ID, thời điểm start, restart count và OOM trước/sau job. RAM được lấy mẫu khoảng mỗi năm giây; đây là quan sát theo mẫu, không phải peak tuyệt đối. Xem [ngân sách hạ tầng](../infra/10-memory-budget.md).
