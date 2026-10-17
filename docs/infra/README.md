# Tài liệu hạ tầng

[← Tài liệu dự án](../README.md)

Bộ tài liệu này giải thích phần hạ tầng Docker đã được xây dựng cho **E-commerce User Behavior Analytics Platform**: từng thành phần làm gì, vì sao cần có, phối hợp như thế nào và cách vận hành.

Đọc [kế hoạch sản phẩm](../../IMPLEMENTATION_PLAN.md) để xem mục tiêu đồ án tổng thể. Các giá trị và hành vi của bản triển khai hiện tại được xác định bởi [Compose](../../compose.yaml), [template môi trường](../../.env.example) và [mã nguồn hạ tầng](../../infra/), thay vì các lựa chọn dự kiến trong kế hoạch.

## Thứ tự đọc

| Tài liệu                                                   | Trả lời câu hỏi                                                                          |
| ---------------------------------------------------------- | ---------------------------------------------------------------------------------------- |
| [01 — Kiến trúc](01-architecture.md)                       | Toàn bộ hệ thống hoạt động thế nào? Docker, Kafka, Spark, HDFS và MongoDB có vai trò gì? |
| [02 — Dịch vụ](02-services.md)                             | Mỗi container chạy gì, cần ai và cung cấp gì?                                            |
| [03 — Cấu hình](03-configuration.md)                       | Mỗi biến, cổng, giới hạn RAM/CPU và vùng lưu trữ có ý nghĩa gì?                          |
| [04 — Khởi tạo và vòng đời](04-bootstrap-and-lifecycle.md) | Từ setup đến sẵn sàng có những bước nào? Chạy lại có giữ dữ liệu không?                  |
| [05 — Vận hành](05-operations.md)                          | Dùng lệnh nào, khi nào dùng, kết quả và tác động ra sao?                                 |
| [06 — Kiểm thử](06-testing.md)                             | Mỗi phép kiểm tra chứng minh điều gì? Đọc kết quả ở đâu?                                 |
| [07 — Xử lý lỗi](07-troubleshooting.md)                    | Khi một bước thất bại thì kiểm tra và khắc phục thế nào?                                 |
| [08 — Bản đồ mã nguồn](08-source-code-guide.md)            | Cần đọc hoặc sửa file/hàm nào để hiểu một chức năng?                                     |
| [09 — Tối ưu build](09-build-optimization.md) | Nguyên nhân tải chậm, Buildx, nguồn tải, cache và cách build tiếp. |
| [10 — Ngân sách RAM](10-memory-budget.md) | Phân bổ 7 GiB, cách áp dụng và theo dõi memory/OOM. |

Người mới nên đọc 01 → 02 → 03 → 04. Khi chạy thực tế, dùng 05; khi đối chiếu kết quả, dùng 06; khi có lỗi, dùng 07. Tài liệu 08 dành cho người phát triển.

## Những gì đã được tạo

- Một Kafka broker kiêm KRaft controller và hai topic nghiệp vụ.
- Một HDFS NameNode, hai DataNode và các thư mục nền cho Data Lake.
- Một Spark master, hai worker và một container chạy driver/job.
- Một MongoDB instance với root account và application account riêng.
- Ba job khởi tạo chạy một lần.
- Dockerfile, cấu hình mạng, volume, giới hạn tài nguyên, healthcheck và script điều phối.
- Bộ kiểm thử offline, smoke test xuyên dịch vụ và kiểm tra dữ liệu qua vòng dừng/khởi động lại.

Tổng cộng **chín service thường trực** và **ba job init**. Trạng thái kỳ vọng sau khởi động là chín service `healthy` và ba init job `Exited (0)`.

## Ứng dụng trên hạ tầng và phần còn lại

- Dataset và mã ứng dụng được mount read-only vào Spark client. Job batch Bronze/Silver, validation, quarantine và quality report đã có trong mã nguồn; xem [tài liệu dữ liệu](../data/README.md). Runtime ingestion vẫn cần nghiệm thu trên cụm thật.
- Các thư mục Bronze/Silver/Gold được init chuẩn bị; việc tạo thư mục không đồng nghĩa đã có dữ liệu nghiệp vụ.
- Topic raw và DLQ được dùng bởi [replay/streaming KPI](../streaming/README.md), có validation, loại trùng và cách ly/DLQ. Mã mới vẫn cần nghiệm thu runtime.
- Smoke test độc lập với ứng dụng; session/funnel có [kiểm thử riêng](../analytics/04-testing.md), feature ML thuộc giai đoạn sau.
- Chưa có huấn luyện K-Means/Random Forest, dự đoán online, FastAPI hoặc dashboard sản phẩm.
- Spark UI và HDFS UI là giao diện quản trị kỹ thuật, không phải dashboard phân tích thương mại điện tử.

## Trạng thái kiểm chứng

Tại thời điểm biên soạn ngày **16/10/2026**:

| Hạng mục                           | Bằng chứng/trạng thái                                             |
| ---------------------------------- | ----------------------------------------------------------------- |
| Kiểm tra cấu hình Compose          | Đã đạt trong phiên triển khai                                     |
| Kiểm thử offline hạ tầng           | 27 kiểm tra đã đạt; 7 test HTTP skipped vì môi trường cấm socket                         |
| Cú pháp Python và Bash             | Đã kiểm tra                                                       |
| Build image                        | Chưa xác minh thành công; bị chặn tại bước truy cập Docker daemon |
| Smoke trên cụm thật | Artifact smoke hiện có xác nhận sáu event, hai worker, checkpoint và Mongo upsert |
| Persistence trên cụm thật | Chưa có kết quả chạy xác nhận |
| Artifact runtime | Có `artifacts/infra/smoke.json`, `artifacts/data/test-data.json` và ingest lần đầu; streaming mới chưa có report |

Mô tả “kết quả mong đợi” trong các tài liệu là tiêu chí để đối chiếu khi thực thi. JSON minh họa không phải bằng chứng hệ thống đã chạy thành công.

## Quy ước đọc

- Lệnh được chạy từ **thư mục gốc repository**.
- `host` là môi trường chạy Docker CLI; container liên lạc bằng service name trong Docker network.
- Địa chỉ và thông số trong tài liệu là giá trị mặc định. Nếu đổi cấu hình local, cần dùng giá trị mới.
- Không đưa mật khẩu thật, nội dung cấu hình local hoặc credential vào tài liệu và ảnh chụp.
- Khi gặp khác biệt giữa tài liệu và bản kế hoạch cũ, đối chiếu mã nguồn và cấu hình hiện tại trước.
