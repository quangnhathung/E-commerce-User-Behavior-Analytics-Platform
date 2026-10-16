# Tài liệu dự án

Tài liệu cho **E-commerce User Behavior Analytics Platform** được tổ chức theo từng phần của hệ thống.

| Tài liệu | Nội dung |
| --- | --- |
| [Hạ tầng](infra/README.md) | Docker Compose, Kafka, HDFS, Spark, MongoDB; cấu hình, khởi tạo, vận hành, kiểm thử, xử lý lỗi, tối ưu build và ngân sách RAM. |
| [Dữ liệu lịch sử](data/README.md) | Data dictionary, hợp đồng sự kiện, Bronze/Silver, chuẩn hóa, loại trùng, quarantine, chất lượng và kiểm thử ingestion. |
| [Replay và streaming KPI](streaming/README.md) | Producer Kafka, Structured Streaming, KPI MongoDB, checkpoint/retry, DLQ, vận hành demo và nghiệm thu. |
| [Kế hoạch triển khai tổng thể](../IMPLEMENTATION_PLAN.md) | Mục tiêu, luồng dữ liệu, phân tích hành vi, Machine Learning, API/dashboard và các mốc triển khai. |

Bắt đầu từ [README gốc](../README.md) để xem cách chạy nhanh. Khi dựng hoặc vận hành cụm, đọc [mục lục hạ tầng](infra/README.md).

Tài liệu hạ tầng nằm trong `docs/infra/`, ingestion trong `docs/data/`, replay/streaming trong `docs/streaming/`. Lệnh trong tài liệu được chạy từ thư mục gốc repository, trừ khi có hướng dẫn khác.
