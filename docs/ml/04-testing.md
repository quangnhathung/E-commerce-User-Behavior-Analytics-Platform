# 04 — Kiểm thử và nghiệm thu ML

[← Mục lục ML](README.md)

## Offline

```bash
make test
make config
```

Bộ toàn dự án hiện có 34 test hạ tầng và 135 test ứng dụng; 162 đạt, 7 HTTP skipped vì môi trường không cho tạo socket loopback. Phần K-Means có 26 test offline: 14 contract/selection/snapshot/package, 6 Mongo boundary và 6 host/launcher. Phần RF bổ sung 31 test, được mô tả [riêng](08-purchase-testing.md). Test offline không cài hoặc import PySpark thực tế; các phép toán Spark được xác nhận bởi bộ live riêng.

Các tình huống đã kiểm tra gồm biên cửa sổ tới millisecond, qua ngày UTC, nguồn không bị sửa, multi-item transaction, distinct nhiều ngày, zero view, ratio >1, input đảo thứ tự, leading zero, giới hạn oracle, timestamp/feature không hợp lệ, canonical fingerprint và tie chọn K. Kiểm tra thứ tự pipeline/log năm count/scaler flags, reuse không gọi build, marker/identity/inventory bị hỏng, archive ML tái lập/compile được và SHA hai archive cũ giữ nguyên.

Kiểm thử bridge dựng pipeline khi NumPy/pandas/Python ML bị chặn import, kiểm tra stage JVM đúng thứ tự và việc bọc SQL DataFrame/chuyển vector thành array. Đây là kiểm tra biên bằng adapter; save/load/fit/evaluate thực tế vẫn phải đạt qua suite live.

Mongo adapter có trạng thái thực sự lưu document, gây lỗi sau 100/121 assignment và kiểm tra phần còn thiếu chưa hiển thị, retry không duplicate, sai coverage không publish, corrupt published bị từ chối, lookup exact và missing Gold. Adapter không thay thế kiểm tra server MongoDB.

Host test kiểm tra request gắn code/source/manifest, không tự build khi train thiếu feature, pointer giữ nguyên nếu tài nguyên chưa đạt, status không Spark, launcher mặc định cũ, lựa chọn archive/entry/prefix ML và guard global khi cancel chưa xác nhận. Kiểm thử large spec qua stdin và ownership/cancel của wrapper tiếp tục nằm trong bộ regression hiện có.

Hai SHA được khóa trong test:

```text
ingestion ae42a5981bc20e1295df1dabbfab7e553af0f133a39c97fb52fa795ceafcb554
streaming 009c7d7d1d411e3f1810096f9c2954235ee6b92c7a3789514fe026b9af3516ac
```

## Sáu test tích hợp thật

```bash
make test-ml
```

Fixture tạo typed Silver có 121 active visitor, nhóm hành vi khác nhau, hơn 100 assignment để thử lỗi giữa lô Mongo, leading zero, mua nhiều item, zero view, biên trái/phải và event ngoài cửa sổ để kiểm tra observed range.

| Test | Nội dung |
| --- | --- |
| a | Feature Spark khớp oracle trên input đảo, đúng cohort, reuse không tính lại |
| b | Feature version sai bị từ chối; lỗi trước/sau Gold có thể retry đúng |
| c | Lỗi trước model Gold; fit lại từ staging chưa commit; winner save/load assignment khớp; reuse không fit; Mongo lookup |
| d | Sau model Gold, giữa Mongo, trước/sau registry pointer; trạng thái partial bị ẩn, retry dùng Gold |
| e | StandardScaler cho vector toàn 0 khi feature hằng; cohort toàn giống nhau không có candidate hợp lệ |
| f | Identity/schema/marker sai và model payload hỏng bị từ chối |

Có bảy điểm fault chủ động. Traceback của lỗi injection là một phần kiểm thử; chỉ kết luận đạt khi cuối suite `OK`, `ML_RESULT.passed=true`, `cleaned=true`, cả hai resource flag true và exit code 0.

HDFS test dưới `/ecommerce/_tests/ml/<UUID>`. Mongo cleanup chỉ xóa model ID được tạo từ feature path UUID đó. Cleanup chạy trong `finally`, kiểm tra không còn document/path sở hữu; cleanup lỗi khiến cả lệnh fail. Không xóa dữ liệu nghiệp vụ hoặc topic Kafka.

## Nghiệm thu nguồn thật

Chạy tuần tự khi không có streaming driver:

```bash
make build-features
make train-segments
make segment-status
make build-features
make train-segments
```

Đối chiếu build mặc định với [tham chiếu CSV](01-features.md): 168.073 event, 99.038 dòng feature, views 162.779, carts 4.011, purchase events 1.283, tổng transaction theo visitor 980, zero-view 192 và buyer 687. Dùng report và fingerprint, không chỉ đọc vài dòng feature.

Với đúng source/cutoff này, `quality.fingerprint.sha256` phải là `43dc8b700488e0a2588dcdc9216d39b3c4fdf9d4ab3edc0441edb9049bff605c`, đã tính độc lập từ CSV bằng chuẩn hóa/loại trùng và aggregate theo visitor. Tổng unique items/active days theo visitor lần lượt 136.776 và 104.481. `source-audit.json` có `runtime_verified=false`; chỉ report build thật mới xác nhận Spark đã khớp.

Mỗi visitor có đúng một assignment, tổng kích thước cluster bằng 99.038 và coverage feature/assignment/Mongo trùng nhau. Candidate metrics hữu hạn, K thắng đúng luật silhouette/tie, không yêu cầu đoán trước K. Profiles raw và standardized centroids phải được đọc đúng ý nghĩa.

Pipeline tải lại phải sinh fingerprint assignment bằng bản đã lưu. Payload đạt hai replica. Build/train lần hai `reused=true`, cùng ID/manifest/fingerprint; không fit lại và không tăng document count. Giữ report/log/resource mỗi lượt để đối chiếu.

Mỗi job không quá timeout 1.800 giây; giữ tổng limit dưới 7 GiB, không OOM/restart và không thiếu quan sát tài nguyên. Thời gian build/train và RAM phải lấy từ kết quả thực tế, không suy ra từ tốc độ test nhỏ. Nếu lỗi tài nguyên, điều tra và tối ưu trong cùng ngân sách trước khi nghiệm thu.

## Trạng thái bằng chứng

Ngày 17/10/2026, đã đối chiếu độc lập artifact runtime do người dùng tạo trong `artifacts/ml/`, gồm report/log/resource của test, feature, train, status và lần chạy lại. `verification.json` có `artifact_checks_passed=true`, `full_source_reuse_verified=true`, `pending=[]`. Đây là kiểm tra bằng chứng đã lưu; phiên công cụ vẫn không có quyền Docker để truy vấn trực tiếp cụm.

| Bằng chứng | Kết quả |
| --- | --- |
| Live suite | 6/6 test đạt, 7 fault scenario, cleanup thành công |
| Cohort | 99.038 visitor, 168.073 event; feature fingerprint khớp CSV |
| Feature fingerprint | `43dc8b700488e0a2588dcdc9216d39b3c4fdf9d4ab3edc0441edb9049bff605c` |
| Model | K=2, silhouette `0.8538471325752618` |
| Kích thước hai cụm | 7.388 và 91.650, tổng 99.038 |
| Assignment fingerprint | `7b602438e868242ba83a41ba75e8a34babb5b891dc88bc384ea57ce54c5d7320` |
| Build lần đầu / reuse | 39,339 / 25,361 giây |
| Train lần đầu / reuse | 97,935 / 50,331 giây |
| RAM tổng quan sát lớn nhất khi train đầu / reuse | 2,4817 / 2,2699 GiB, tổng container limit 6,25 GiB |
| Monitor/reload/reuse | Đủ quan sát chín container, không OOM/restart, hai replica; cùng manifest/quality và không fit mới |

Feature ID là `4b3e9d924c88fa6042ae6851871fcd537e5bdda793d3bc2b059df348ecfa89de`; model ID là `8ec975a8e4b74329ff1c68e4c78835a824a3ea3d98990db2e40a5277cc05779d`. Tên “Cụm 0/1” là nhãn mô tả trung tính; silhouette cao không tự chứng minh một chiến lược kinh doanh hiệu quả. `source-audit.json` vẫn giữ `runtime_verified=false` vì nó chỉ là CSV audit; các report Spark và verification mới xác nhận đối soát runtime.

K-Means đã hoàn tất nghiệm thu trong phạm vi này. Random Forest/batch purchase scoring đã triển khai ở giai đoạn tiếp, còn chờ chạy live/full history/reuse; xem [kiểm thử prediction](08-purchase-testing.md).

Phần hành vi trước ML đã có artifact thật: 6/6 behavior tests, 10/10 streaming tests, history/reuse và `behavior-demo-001` thành công. Bằng chứng đó xác nhận nền dữ liệu/streaming; nó không thay thế nghiệm thu pipeline ML mới.
