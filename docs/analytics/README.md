# Session và funnel chuyển đổi

[← Tài liệu dự án](../README.md)

Phần này biến Silver đã chuẩn hóa thành **phiên tương tác và hành trình xem → thêm giỏ → mua**, trên toàn bộ lịch sử và trên từng run streaming. Có funnel tổng quan và theo sản phẩm để tìm bước mất khách. Kết quả là số phiên có chuỗi hành vi hợp lệ, không phải doanh thu hoặc hiệu quả của một biện pháp kinh doanh.

| Tài liệu | Nội dung |
| --- | --- |
| [01 — Hợp đồng](01-contract.md) | Session 30 phút, thứ tự thời gian, cùng sản phẩm, công thức và giới hạn phạm vi |
| [02 — Kiến trúc và mã nguồn](02-architecture.md) | Spark dùng chung, HDFS, MongoDB, snapshot, retry và vai trò từng module |
| [03 — Vận hành](03-operations.md) | Kiểm thử, phân tích lịch sử, demo hai terminal, tra sản phẩm và phục hồi |
| [04 — Kiểm thử và nghiệm thu](04-testing.md) | Bộ kiểm thử, số tham chiếu thực tế, checklist dữ liệu/tài nguyên và bằng chứng hiện có |

## Bắt đầu

Với cụm healthy và Silver lịch sử đã ingest:

```bash
make test
make test-behavior
make test-stream
make analyze-behavior
make behavior-status
```

Batch lịch sử dùng cùng khóa Spark driver; đợi streaming/ingest/smoke/test khác dừng trước khi chạy. Streaming và batch giữ cấu hình khoảng 7 GiB, không thêm service hoặc dependency. Mã `/opt/app` dùng bind mount, không cần build image cho tính năng này.

Streaming mới dùng `stream-v2`, hợp đồng hành vi `behavior-v1`. Chuẩn bị RUN_ID mới bằng mã mới; giữ nguyên run/checkpoint cũ nếu cần khôi phục đúng phiên bản mã để tiếp tục.

## Trạng thái

Đã có artifact thật xác nhận 6/6 test behavior, 10/10 test streaming, cleanup và monitor đạt. Phân tích lịch sử có 2.755.641 event, 1.761.675 session và 235.061 sản phẩm; lần hai reuse cùng manifest. `behavior-demo-001` có 2.692 event, 1.893 session, funnel 1.882 → 35 → 9, backlog 0, p95 37,287 giây, hai worker và hai replica.

Giai đoạn [feature 7 ngày và K-Means lịch sử](../ml/README.md) đã nghiệm thu cả nguồn đầy đủ và reuse. Phần tiếp theo [Random Forest và batch purchase scoring](../ml/05-purchase-data.md) đã có mã/tài liệu, còn chờ live runtime ở terminal có quyền Docker. Các archive cũ giữ nguyên SHA; offline toàn dự án hiện có 162 test đạt, 7 HTTP skipped. Xem bằng chứng chi tiết trong [kiểm thử hành vi](04-testing.md) và [ML](../ml/04-testing.md).
