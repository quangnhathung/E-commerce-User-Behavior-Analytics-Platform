# 03 — Vận hành ML batch

[← Mục lục ML](README.md)

## Điều kiện và thứ tự chạy

Docker healthy, Silver lịch sử đã ingest, `artifacts/data/latest-ingest.json` có quality passed và không có managed Spark job khác. Mã ứng dụng dùng bind mount hiện tại, dependency Spark ML đã có trong image. Không cần chạy `make setup`, `make build`, ingest lại hoặc reset volume chỉ để dùng phần ML mới.

```bash
make up
make doctor
make test
make test-ml
make build-features
make train-segments
make segment-status
```

Nếu cụm đã healthy thì bắt đầu từ `make test`. Các lệnh Spark chạy tuần tự. Chạy trên terminal có quyền truy cập Docker daemon; phiên agent hiện tại chưa có quyền đó.

## Từng lệnh làm gì

| Lệnh | Xử lý | Đầu ra |
| --- | --- | --- |
| `make build-features` | Kiểm tra Silver, chốt cửa sổ, aggregate và đối soát | Feature Parquet/quality/manifest HDFS, report và latest feature host |
| `make train-segments` | Chọn feature đã verified, thử K, lưu/tải lại model, công bố Mongo | Pipeline, assignments, profiles/metrics, model registry và latest model host |
| `make segment-status` | Python đọc model published trên Mongo, không tạo Spark driver | Identity, status, report, candidate metrics và profiles |
| `make segment-status VISITOR_ID=001` | Tra chính xác visitor trong model được chọn | Thêm field `visitor`; null nếu không thuộc cohort |
| `make test-ml` | Sáu test fixture thật, thử lỗi/retry/reuse và dọn dữ liệu test | `artifacts/ml/test-ml.json`, log và resources |

Tuỳ chọn:

```bash
make build-features AS_OF=2015-07-15T00:00:00.000Z
make train-segments FEATURE_SNAPSHOT_ID=<64-hex>
make segment-status MODEL_VERSION=<64-hex>
make segment-status MODEL_VERSION=<64-hex> VISITOR_ID=001
```

Thay `<64-hex>` bằng ID thật, không giữ dấu ngoặc. ID snapshot/model là 64 ký tự hex chữ thường. Visitor là digit string, giữ số 0 đầu. `AS_OF` chỉ áp dụng build feature; train đọc AS_OF từ snapshot, không dùng biến môi trường để chốt lại thời gian. Train không tự build nếu thiếu feature; chạy build trước.

ML code SHA của feature phải bằng code hiện tại khi train. Khi thay công thức/mã ML, build lại để tạo snapshot ID mới. Không sửa report hoặc manifest để vượt kiểm tra này.

## File trên host

```text
artifacts/ml/
  source-audit.json                     Đối soát CSV offline; không phải kết quả Spark
  latest-features.json                  Feature snapshot mới nhất đã đạt
  latest-model.json                     Model mới nhất đã công bố và đạt monitor
  <feature_snapshot_id>/report.json     Feature identity, HDFS path, manifest SHA, quality, reuse
  <model_version>/report.json           Model identity, HDFS path, quality, reuse, resources
  <model_version>/status.json           Lần đọc Mongo gần nhất
  attempts/logs/<action>-<UUID>.log      Log Spark của từng lần build/train
  attempts/logs/resources-<UUID>.json   Bằng chứng tài nguyên từng lần
  attempts/resources.json              Bằng chứng lần build/train gần nhất
  tests/logs/                           Log và resource từng lượt test
  tests/resources.json
  test-ml.json                          Kết quả test thành công gần nhất
```

`quality` feature có fingerprint/count/sums và event counts. `quality` model có candidate metrics, K thắng, profiles, standardized centroids, assignment fingerprint và reload flag. `reused=true` chỉ bỏ build/fit; việc kiểm tra HDFS/model/Mongo vẫn thực hiện.

Log kết thúc bằng `ML_RESULT=...`; host bổ sung đường dẫn log và cờ resources. Một file report thành công cũ không chứng minh lần gọi mới thành công: đối chiếu exit code, log/resource UUID và mã hiện tại. Job/test lỗi không ghi đè latest pointer hoặc report thành công trước đó.

`ML_CANDIDATE=...` trong log ghi metric hoặc lý do invalid của từng K, kể cả khi toàn bộ candidate bị loại và không có model report. `ML_TEST_ROOT=...` ghi namespace UUID để có thể điều tra test bị ngắt trước cleanup.

## Đọc profiles

Xem count/share trước để biết quy mô, rồi so sánh tám raw feature means. Ví dụ chỉ có thể nói “cụm này có trung bình nhiều view và cart hơn” nếu số thực tế cho thấy điều đó. Các centroid dùng để tính khoảng cách sau log/scale, không dùng trực tiếp như số lượt tương tác.

Model phân loại toàn bộ active visitor của cửa sổ; visitor mới ở tương lai chưa có assignment. Lệnh status không quét/xuất toàn bộ `visitor_segments`, tránh log lớn và truy vấn không giới hạn. Parquet assignment là đầu ra đầy đủ phục vụ phân tích tiếp theo.

## Tài nguyên, ngắt và retry

Giữ hai worker × 1 core, executor 640m, driver 512m. Tổng limit chín container dài hạn 6,25 GiB, toàn bộ cả init tối đa 6,94 GiB. Mỗi managed job có timeout 1.800 giây và thời gian dừng cưỡng bức; không chạy nhiều driver để tăng tốc. Không tự sample cohort khi thiếu tài nguyên.

Monitor lấy mẫu Docker khoảng 5 giây và so sánh container ID, thời điểm start, restart count, OOM trước/sau job. Dữ liệu lấy mẫu không chứng minh peak tuyệt đối, và RAM daemon/OS nằm ngoài tổng limit container. Cả `no_oom_or_restart` và `memory_observations_complete` phải true để host công nhận kết quả ML.

Có thể Ctrl+C. Wrapper yêu cầu dừng process group remote; snapshot đã commit được giữ. Chạy lại cùng lệnh để kiểm tra rồi tiếp tục. Nếu chưa xác nhận driver remote đã dừng, guard `artifacts/infra/driver-unconfirmed.json` chặn managed driver mới. Kiểm tra driver/container/log và chỉ gỡ marker sau khi xác nhận tiến trình sở hữu token đã dừng; không xóa guard chỉ để ép chạy.

## Xử lý lỗi

| Dấu hiệu | Cách xử lý |
| --- | --- |
| Docker permission denied | Chạy từ terminal có quyền Docker hiện có, kiểm tra `docker info`/context |
| Thiếu latest ingestion/quality chưa passed | Hoàn thành và kiểm tra ingestion theo `docs/data/` |
| Cửa sổ vượt observed range | Chọn AS_OF có đủ bảy ngày trong source; không đổi timezone hoặc nới điều kiện |
| Thiếu latest feature/report | Chạy build; explicit ID cũng cần report host tương ứng để pin manifest |
| Code changed | Build feature bằng mã hiện tại rồi train; giữ artifact phiên bản cũ |
| No valid K-Means candidate | Đọc candidate reasons; cohort đồng nhất/quá nhỏ không có mô hình hợp lệ |
| Snapshot marker/checksum/identity lỗi | Dừng và điều tra/khôi phục artifact; không tự ghi đè snapshot hỏng |
| Published Gold missing | Khôi phục model từ bản sao đúng manifest; không tái fit để thay model cũ |
| Mongo ghi dở | Registry prepared nên status chưa hiển thị; retry cùng lệnh dùng lại Gold |
| Host thiếu pointer sau khi remote publish | Retry build/train để xác minh và ghi lại pointer host |
| Thiếu resource samples/OOM/restart | Xem resources và log, giải phóng tài nguyên, khắc phục rồi retry |
| Hết 1.800 giây | Xem bottleneck Spark/HDFS trước; không tự tăng RAM hoặc đổi dữ liệu nghiệm thu |

Test tự dọn đúng UUID namespace kể cả khi lỗi. Artifact feature/model nghiệp vụ được giữ để tái lập; chưa có lệnh xóa riêng model. Không xóa toàn collection/Gold để dọn một test. `make reset CONFIRM=delete-project-data` là thao tác xóa dữ liệu của toàn Compose project, chỉ dùng khi chủ động muốn reset toàn bộ theo tài liệu hạ tầng.
