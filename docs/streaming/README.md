# Replay Kafka và streaming KPI

[← Tài liệu dự án](../README.md)

Luồng ứng dụng: **Silver lịch sử → JSON Lines replay → Kafka → Spark Structured Streaming → HDFS → MongoDB KPI**. Producer dùng dữ liệu lịch sử thật; kết quả của mỗi lượt được tách bằng `run_id`.

| Tài liệu | Nội dung |
| --- | --- |
| [01 — Kiến trúc và mã nguồn](01-architecture.md) | Các tiến trình, vai trò module, lưu trữ và giới hạn tài nguyên |
| [02 — Hợp đồng và KPI](02-contract-and-kpi.md) | Payload, kiểm tra dữ liệu, loại trùng, sự kiện trễ, định nghĩa KPI |
| [03 — Vận hành](03-operations.md) | Lệnh chạy, hai terminal, đọc kết quả, tiếp tục và dọn dữ liệu |
| [04 — Kiểm thử và phục hồi](04-testing-and-recovery.md) | Fixture, fault injection, checkpoint, tiêu chí nghiệm thu và trạng thái thật |

## Phạm vi

Mã nguồn đã có prepare/replay, streaming, Bronze/Silver/Gold theo batch, quarantine, DLQ, KPI và trạng thái MongoDB. `stream-v2` bổ sung [session và funnel](../analytics/README.md) cùng snapshot KPI. Lượt demo tự kết thúc khi offset đã ACK được xử lý, checkpoint batch cuối tồn tại và KPI/funnel đã đối soát.

Nguồn mặc định là `[2015-09-10T10:00:00.000Z, 2015-09-10T16:00:00.000Z)`: 2.692 event, gồm 2.596 view, 69 add to cart, 27 purchase event; 19 transaction, 1.860 visitor và 17 buyer. Tốc độ tối đa trong v1 là 10 event/giây, tương ứng khoảng 4,5 phút phát nguồn mặc định, chưa tính khởi động/drain.

Mỗi run có tối đa 10.000 event; KPI và session/funnel được tính lại từ Silver đã nhận của run sau mỗi batch. Simulator, feature, ML, API/dashboard và replay toàn bộ 2,76 triệu event chưa thuộc giai đoạn này. Batch session/funnel toàn bộ lịch sử chạy riêng bằng `make analyze-behavior`.

## Chạy nhanh

Chạy ở gốc repository, với cụm đã healthy và Silver đã được ingest:

```bash
make test
make test-stream
make prepare-demo RUN_ID=demo-001
```

Terminal A:

```bash
make stream RUN_ID=demo-001
```

Terminal B, sau thông báo `STREAM_READY`:

```bash
make replay RUN_ID=demo-001
make stream-status RUN_ID=demo-001
```

Mã ứng dụng đã được mount tại `/opt/app`; không thêm service, volume hoặc dependency, không cần build lại image cho tính năng này. Giữ cấu hình container khoảng 7 GiB và một Spark driver cùng một producer Python.

## Trạng thái kiểm chứng

Artifact hiện tại xác nhận 10/10 test streaming đạt, tám fault scenario, cleanup/monitor thành công. Run `behavior-demo-001` đã đạt với 2.692 event, backlog 0, p95 37,287 giây, hai worker/hai replica và funnel final. ML batch dùng archive riêng, giữ SHA streaming hiện tại. Dùng RUN_ID mới để chuẩn bị demo khác. Xem [tiêu chí nghiệm thu](04-testing-and-recovery.md) và [bằng chứng hành vi](../analytics/04-testing.md).
