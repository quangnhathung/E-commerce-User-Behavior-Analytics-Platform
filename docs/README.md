# Tài liệu dự án

Tài liệu cho **E-commerce User Behavior Analytics Platform** được tổ chức theo từng phần của hệ thống.

| Tài liệu | Nội dung |
| --- | --- |
| [Chạy toàn bộ trên máy mới](GETTING_STARTED.md) | Runbook từ Ubuntu/Arch hoặc Windows/WSL2, clone/dataset, Docker Compose đến ingestion, streaming, K-Means, RF và policy; output, nghiệm thu, retry và cleanup. |
| [Hạ tầng](infra/README.md) | Docker Compose, Kafka, HDFS, Spark, MongoDB; cấu hình, khởi tạo, vận hành, kiểm thử, xử lý lỗi, tối ưu build và ngân sách RAM. |
| [Dữ liệu lịch sử](data/README.md) | Data dictionary, hợp đồng sự kiện, Bronze/Silver, chuẩn hóa, loại trùng, quarantine, chất lượng và kiểm thử ingestion. |
| [Replay và streaming KPI](streaming/README.md) | Producer Kafka, Structured Streaming, KPI MongoDB, checkpoint/retry, DLQ, vận hành demo và nghiệm thu. |
| [Session và funnel](analytics/README.md) | Phân tích lịch sử và streaming, phiên 30 phút, funnel tổng quan/sản phẩm, snapshot HDFS/MongoDB, vận hành và kiểm thử. |
| [Machine Learning](ml/README.md) | Feature 7 ngày, K-Means; nhãn 24 giờ, RF batch/policy, ML streaming với replay/simulator, snapshot và nghiệm thu. |
| [Kế hoạch triển khai tổng thể](../IMPLEMENTATION_PLAN.md) | Mục tiêu, luồng dữ liệu, phân tích hành vi, Machine Learning, API/dashboard và các mốc triển khai. |

Bắt đầu từ [hướng dẫn máy mới](GETTING_STARTED.md) để chạy đầy đủ, hoặc [README gốc](../README.md) để xem nhanh. Khi cần giải thích sâu về dựng/vận hành cụm, đọc [mục lục hạ tầng](infra/README.md).

Tài liệu hạ tầng nằm trong `docs/infra/`, ingestion trong `docs/data/`, replay/streaming trong `docs/streaming/`, phân tích hành vi trong `docs/analytics/`, Machine Learning trong `docs/ml/`. Lệnh trong tài liệu được chạy từ thư mục gốc repository, trừ khi có hướng dẫn khác.
