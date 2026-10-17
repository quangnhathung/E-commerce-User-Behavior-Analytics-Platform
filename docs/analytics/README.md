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

Đã triển khai bộ tính Spark, bộ tham chiếu Python, snapshot lịch sử, tích hợp streaming, công bố MongoDB và CLI. Kiểm thử offline có 105 test đạt, 7 HTTP skipped; `make config` đạt và SHA archive ingestion giữ nguyên. Phiên công cụ bị từ chối Docker socket khi chạy `make test-behavior`, nên **chưa nghiệm thu Spark/HDFS/MongoDB của phần hành vi trên cụm thật**.

Report `test-stream` tám test đã đạt thuộc phiên bản trước khi thêm hành vi. Phiên bản hiện tại có mười test streaming và sáu test hành vi cần chạy lại; không dùng report cũ làm bằng chứng cho mã mới.
