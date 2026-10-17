# Feature hành vi 7 ngày và K-Means

[← Tài liệu dự án](../README.md)

Giai đoạn này chuyển Silver lịch sử thành tám đặc trưng cho từng visitor, tìm các nhóm hành vi bằng Spark ML và lưu kết quả có phiên bản để tra cứu. Đây là phân nhóm mô tả tại một thời điểm chốt. Random Forest, dự đoán mua hàng, scoring streaming và API/dashboard nằm ở giai đoạn sau.

| Tài liệu | Nội dung |
| --- | --- |
| [01 — Hợp đồng feature](01-features.md) | Cửa sổ thời gian, công thức, kiểu dữ liệu, chống dùng dữ liệu tương lai và đối soát |
| [02 — Phương pháp và kiến trúc](02-methodology-and-architecture.md) | Pipeline, chọn K, model version, HDFS, MongoDB và từng module |
| [03 — Vận hành](03-operations.md) | Lệnh chạy, tham số, báo cáo, retry, tài nguyên và xử lý lỗi |
| [04 — Kiểm thử](04-testing.md) | Offline, tích hợp thật, fault injection và tiêu chí nghiệm thu |

Chạy tại thư mục gốc repository, sau khi Silver đã ingest:

```bash
make test
make test-ml
make build-features
make train-segments
make segment-status
```

Mặc định chốt lúc `2015-07-15T00:00:00.000Z`, sử dụng bảy ngày trước đó. Chạy lại build/train cùng identity sẽ kiểm tra và tái sử dụng snapshot. Chỉ cần mã ứng dụng được mount vào Spark client; không cần build image, reset volume hoặc thêm service.

Hiện trạng 17/10/2026: mã, CLI và tài liệu đã triển khai; bộ offline có 131 test đạt, 7 HTTP skipped, bao gồm 26 test ML mới. Lệnh `make test-ml` trong phiên công cụ bị từ chối Docker socket trước khi job chạy. Sáu test tích hợp ML, số cụm, silhouette và thời gian/RAM của huấn luyện thật **chưa được nghiệm thu**. Các số đối soát CSV trong tài liệu là tham chiếu offline, không phải báo cáo Spark thành công.
