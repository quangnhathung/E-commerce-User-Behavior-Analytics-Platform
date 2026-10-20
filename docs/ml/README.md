# Feature hành vi, K-Means và dự đoán mua hàng

[← Tài liệu dự án](../README.md)

Phần ML chuyển Silver lịch sử thành tám đặc trưng hành vi, phân nhóm visitor bằng K-Means và dự đoán việc mua trong 24 giờ bằng Random Forest. Kết quả có model version, snapshot HDFS, metadata/lookup MongoDB và kiểm tra khi chạy lại. RF batch/policy và ML streaming đã có artifact runtime; live suite, replay, simulator và completed reuse ML đã đạt. [API/dashboard local](../serving/README.md) đọc snapshot ML thật; thao tác và cách hiểu cluster/score/nhãn nằm ở [hướng dẫn web](../serving/06-web-user-guide.md).

| Tài liệu | Nội dung |
| --- | --- |
| [01 — Hợp đồng feature](01-features.md) | Cửa sổ thời gian, công thức, kiểu dữ liệu, chống dùng dữ liệu tương lai và đối soát |
| [02 — Phương pháp và kiến trúc](02-methodology-and-architecture.md) | Pipeline, chọn K, model version, HDFS, MongoDB và từng module |
| [03 — Vận hành](03-operations.md) | Lệnh chạy, tham số, báo cáo, retry, tài nguyên và xử lý lỗi |
| [04 — Kiểm thử K-Means](04-testing.md) | Tích hợp thật, fault injection và bằng chứng nguồn đầy đủ/reuse |
| [05 — Dữ liệu dự đoán mua](05-purchase-data.md) | Nhãn 24 giờ, 128 cutoff, temporal split, sampling và daily snapshots |
| [06 — Random Forest](06-random-forest.md) | Bốn candidate, validation/test, threshold, metrics/baselines, HDFS/Mongo và mã nguồn |
| [07 — Vận hành prediction](07-purchase-operations.md) | Sáu lệnh, batch score/lookup, progress, retry, tài nguyên và xử lý lỗi |
| [08 — Kiểm thử prediction](08-purchase-testing.md) | Offline/live suite, full history, reuse và trạng thái nghiệm thu |
| [09 — Chính sách precision](09-purchase-policy.md) | Mục tiêu precision ≥70%/recall ≥10%, ngưỡng riêng, reassessment, apply/reuse và kiểm thử |
| [10 — ML streaming](10-streaming-scoring.md) | Seed trước replay, inference K-Means/RF/policy, simulator ba đợt, CLI/status, checkpoint/retry và nghiệm thu |

Chạy tại thư mục gốc repository, sau khi Silver đã ingest:

```bash
make test
make test-ml
make build-features
make train-segments
make segment-status
```

K-Means mặc định chốt lúc `2015-07-15T00:00:00.000Z`, sử dụng bảy ngày trước đó. Chạy lại build/train cùng identity sẽ kiểm tra và tái sử dụng snapshot.

Khi K-Means/Silver đã sẵn sàng, thực hiện RF độc lập bằng:

```bash
make test-purchase
make build-purchase-data
make train-purchase
make purchase-status
make score-purchases
make purchase-score-status
```

Builder RF chạy 20 chặng để tạo 128 daily snapshots; train giữ 1.750 positive và 35.000 negative, validation/test giữ toàn cohort. Score mặc định ngày 18/09/2015 có 69.052 visitor theo tham chiếu CSV. Không cần build lại image, reset volume hoặc thêm service; giữ ngân sách khoảng 7 GiB và một Spark driver mỗi lúc.

Hiện trạng 17/10/2026: **K-Means đã nghiệm thu** cả full-source reuse. **RF gốc đã chạy thành công lần đầu**: 7/7 live test, đủ 128 ngày, 50 cây/depth 5/ngưỡng 0,93, PR-AUC test 0,18172 và batch score 69.052 visitor; full-source reuse còn chờ bằng chứng. **Policy có artifact xác nhận 5/5 live test, eligible và apply/reuse đạt**: ngưỡng 0,96, precision test 73,88%, recall 13,64%; score mới vẫn đủ 69.052 visitor, bốn predicted positive. Toàn dự án đã có lượt offline 188 test đạt, 7 HTTP skipped do giới hạn socket của phiên kiểm tra. Xem chi tiết trong tài liệu 08 và 09; developer mới dùng [runbook đầy đủ](../GETTING_STARTED.md).

Khi muốn thử mục tiêu precision ≥70%/recall ≥10%, dùng [quy trình policy](09-purchase-policy.md): `make test-purchase-policy`, `make tune-purchase-policy`, `make purchase-policy-status`, rồi `make apply-purchase-policy` chỉ khi `eligible=true`. Quy trình dùng scores có sẵn, không build dataset hoặc fit RF lại; score run mới có ID riêng.

Cập nhật 18/10/2026: [ML streaming](10-streaming-scoring.md) đã có mã và 26 offline test mới đạt, giữ nguyên năm archive cũ. Bằng chứng runtime bổ sung sau đó xác nhận live suite/replay/simulator và completed reuse; đọc tài liệu 10 thay vì dùng trạng thái pending của lượt công cụ ban đầu. Ngày 20/10, live hữu hạn hai lượt đã có report passed, nhưng stop/resume phiên liên tục còn cần kiểm chứng riêng; [trạng thái serving](../serving/README.md) phân biệt các hạng mục đã và chưa nghiệm thu.
