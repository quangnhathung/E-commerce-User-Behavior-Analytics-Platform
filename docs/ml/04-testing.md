# 04 — Kiểm thử và nghiệm thu ML

[← Mục lục ML](README.md)

## Offline

```bash
make test
make config
```

Lượt triển khai này: 34 test hạ tầng và 104 test ứng dụng; 131 đạt, 7 HTTP skipped vì môi trường không cho tạo socket loopback. Có 26 test ML mới: 14 contract/selection/snapshot/package, 6 Mongo boundary và 6 host/launcher. Test offline không cài hoặc import PySpark thực tế; các phép toán Spark được xác nhận bởi bộ live riêng.

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

Ngày 17/10/2026, offline đã đạt. Đã thử `make test-ml` nhưng preflight báo permission denied với Docker socket. Host không có PySpark; chưa chạy được sáu test tích hợp, train toàn cohort hoặc đo silhouette/RAM/thời gian thật trong phiên này. Không có báo cáo runtime ML thành công được tạo giả. Chạy các lệnh trên từ terminal có quyền Docker để hoàn tất nghiệm thu.

Phần hành vi trước ML đã có artifact thật: 6/6 behavior tests, 10/10 streaming tests, history/reuse và `behavior-demo-001` thành công. Bằng chứng đó xác nhận nền dữ liệu/streaming; nó không thay thế nghiệm thu pipeline ML mới.
