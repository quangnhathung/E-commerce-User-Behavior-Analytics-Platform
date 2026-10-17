# 07 — Vận hành purchase prediction

[← Mục lục ML](README.md) · [Kiểm thử →](08-purchase-testing.md)

## Điều kiện và lệnh chạy lần đầu

Chạy tại thư mục gốc repository từ terminal có quyền Docker. Cụm cần healthy, hai worker/hai DataNode sẵn sàng và có `artifacts/data/latest-ingest.json` với `quality_status=passed`. Dừng/đợi streaming driver và các Spark job khác trước khi chạy batch. Prediction dùng cùng driver lock, wrapper cancel và monitor hiện có.

```bash
make doctor
make test
make test-purchase
make build-purchase-data
make train-purchase
make purchase-status
make score-purchases
make purchase-score-status
```

Không cần chạy lại setup/build/ingest khi Silver và cụm hiện tại đã sẵn sàng. Mã `/opt/app` dùng bind mount, không có service/dependency/image mới cho phần này.

| Lệnh | Công dụng | Spark |
| --- | --- | --- |
| `make build-purchase-data` | Chuẩn bị 128 cutoff, 20 chặng daily snapshots rồi công bố index | Có, các child tuần tự |
| `make train-purchase` | Sample, fit/reuse bốn RF, chọn theo validation, khóa model, đánh giá test, công bố Mongo | Có, các child tuần tự |
| `make purchase-status` | Xem model metadata, lựa chọn, ngưỡng, metrics và baselines | Không |
| `make score-purchases` | Dùng model published để score một cohort lịch sử ở cutoff yêu cầu | Có |
| `make purchase-score-status` | Xem score run metadata và tùy chọn một visitor | Không |
| `make test-purchase` | Bảy test tích hợp thật trong namespace riêng, có cleanup | Có |

Train yêu cầu dataset report hoàn chỉnh đã có; không tự build 128 ngày khi thiếu. `purchase-status` mặc định chọn model prediction gần nhất; không dùng pointer K-Means dù cả hai lệnh có tham số `MODEL_VERSION`.

## Tham số chọn phiên bản

| Biến | Lệnh áp dụng | Mặc định |
| --- | --- | --- |
| `PURCHASE_DATASET_ID` | `train-purchase` | `latest-purchase-data.json` |
| `MODEL_VERSION` | `purchase-status`, `score-purchases` | `latest-purchase-model.json` |
| `SCORE_RUN_ID` | `purchase-score-status` | `latest-purchase-score.json` |
| `AS_OF` | `score-purchases` | `2015-09-18T00:00:00.000Z` |
| `VISITOR_ID` | `purchase-score-status` | Không tra visitor khi bỏ trống |

`MODEL_VERSION` không dùng để ép tên/ghi đè model ở bước train; `PURCHASE_DATASET_ID` cũng không đổi ID builder. Các ID là 64 ký tự hex chữ thường. Lịch train/split là hợp đồng cố định, không thay bằng `AS_OF` trên Makefile.

Ví dụ, thay các chuỗi ID bằng ID thực từ report:

```bash
make train-purchase PURCHASE_DATASET_ID=<purchase_dataset_id>
make purchase-status MODEL_VERSION=<model_version>
make score-purchases MODEL_VERSION=<model_version> AS_OF=2015-09-18T00:00:00.000Z
make purchase-score-status SCORE_RUN_ID=<score_run_id> VISITOR_ID=001
```

Giữ visitor dưới dạng chuỗi; `001` khác `1`. Lookup chỉ trả đúng một visitor, không tải toàn collection. Không tìm thấy visitor thì trường `visitor` bằng null; có thể visitor không hoạt động trong cửa sổ bảy ngày được score.

## Batch scoring lịch sử

Mặc định score tại **18/09/2015 00:00 UTC**, dùng feature `[11/09 00:00, 18/09 00:00)`. Cohort tham chiếu từ CSV là **69.052 visitor**; mã kiểm tra con số này khi đúng source/cutoff mặc định. Bước này không cần nhãn tương lai, không đọc event tương lai để tạo feature và không báo cáo accuracy cho cutoff thiếu đủ ngày nhãn.

Cho phép cutoff từ ngày test đầu tiên trở đi, miễn nguồn quan sát đủ toàn cửa sổ feature. Input là timestamp UTC có milliseconds. Score bắt buộc dùng đúng prediction archive SHA và Spark version đã gắn với model, model đã published, manifest/feature order/threshold khớp. Sai code hoặc payload làm lệnh fail trước công bố.

Kết quả visitor gồm audit, tám feature thô, `model_version`, `score_run_id`, `threshold`, `purchase_score` và `predicted_label`. Score chưa hiệu chuẩn, không diễn giải thành phần trăm xác suất mua. Chưa có API, dashboard, streaming scoring hoặc kết hợp cluster K-Means trong vector RF ở giai đoạn này.

## Report, log và progress

```text
artifacts/prediction/
  source-audit.json                 # CSV tham chiếu offline; runtime_verified=false
  latest-purchase-data.json
  latest-purchase-model.json
  latest-purchase-score.json
  test-purchase.json
  attempts/logs/                    # Chuẩn bị dữ liệu/huấn luyện
  tests/logs/                       # Live suite và resource samples
  <purchase_dataset_id>/
    progress.json                  # completed_chunks, stages, daily results
    report.json                    # Chỉ có sau finalize thành công
    logs/
  <model_version>/
    training.json
    progress.json                  # Các candidate hoàn tất
    report.json                    # Selection + evaluation + publication
    status.json
    logs/                          # Candidate, finalization, status và score attempts
  <score_run_id>/
    report.json
    status.json
```

Mỗi child có log và file resource samples tương ứng do launcher tạo. `PREDICTION_RESULT=` là kết quả JSON của child; `PURCHASE_DAY=`, `PURCHASE_CANDIDATE=` và `PURCHASE_BUILD_CHUNK=` cho biết tiến độ. Không kết luận từ một dòng log giữa job: yêu cầu exit 0, `passed=true` và hai cờ resource đều true. Latest host pointer chỉ đổi khi toàn bộ tác vụ public thành công.

## Khi ngắt hoặc chạy lại

Có thể nhấn `Ctrl+C`; wrapper xử lý dừng driver theo cơ chế hiện tại. Giữ nguyên volume, marker, mã phiên bản và artifacts. Khi driver đã dừng/xác nhận cleanup, chạy lại **đúng lệnh bị ngắt**:

```bash
make build-purchase-data
make train-purchase
make score-purchases
```

Không cần chạy lại cả chuỗi nếu chỉ một bước bị lỗi. Builder sẽ duyệt lại lịch để xác minh/reuse các ngày đã commit; `progress.json` được lập lại cho lượt hiện tại nên số chặng hoàn tất có thể bắt đầu lại từ 0. Các ngày còn nguyên trên HDFS vẫn được tái sử dụng. Candidate đã commit cũng được kiểm tra/reuse, không fit lại. Candidate đang ở staging chưa commit phải chạy lại candidate đó. Mongo partial được upsert từ Gold rồi xác minh trước publish.

Nếu global driver guard báo việc cancel chưa xác nhận, kiểm tra driver theo [hướng dẫn streaming](../streaming/04-testing-and-recovery.md) trước khi chạy tiếp. Không xóa lock/guard để chạy chồng job. Nếu published snapshot/checksum bị hỏng, giữ log để điều tra và khôi phục; retry không sửa âm thầm dữ liệu đã công bố.

## Tài nguyên và thời gian

Giữ hai worker, mỗi worker một core/executor heap 640 MiB; driver 512 MiB, `maxResultSize=128m`, shuffle/default parallelism 4, UTC. Tổng limit chín service 6,25 GiB; tối đa cộng ba init là 6,94 GiB. Chỉ một Spark driver; producer replay có khóa riêng nhưng không nên mở thêm tải trong nghiệm thu batch.

Nguồn, feature và prediction lớn dùng `DISK_ONLY` ở các chặng cần tái sử dụng; không `collect` hàng triệu dòng. Fingerprint validation/test được đọc tuần tự theo ngày; Mongo gửi từng 100 document. Các task Spark vẫn cần scratch disk và HDFS cần hai replica: **10,1 triệu sample và prediction của bốn candidate làm tăng dung lượng đáng kể**, dù không thêm image. Kiểm tra dung lượng trước và sau khi build; chưa có số đo dung lượng/RAM/thời gian RF thực tế trong phiên này.

Timeout **1.800 giây cho mỗi child**, không phải cho cả 20 chặng hoặc cả public command. Không chạy candidate song song hoặc tăng RAM để vượt ngân sách đã chốt. Nếu timeout/OOM, xem child log/resources, xác định chặng và tối ưu trong ngân sách rồi chạy lại.

## Chẩn đoán thường gặp

| Hiện tượng | Hành động |
| --- | --- |
| Docker socket permission denied | Chạy lệnh trong terminal có quyền Docker/context đúng; job chưa bắt đầu |
| Không có latest ingestion/dataset/model | Kiểm tra artifacts; chạy đúng bước tạo dữ liệu/model còn thiếu |
| `Prediction code changed` / `code/Spark version changed` | Dùng lại đúng code gốc để resume hoặc build dataset/train phiên bản mới theo code hiện tại |
| Chưa đủ cửa sổ feature/label | Chọn cutoff score có đủ quan sát; train dùng đúng Silver đầy đủ theo hợp đồng |
| Thiếu một lớp, sample/count khác baseline | Kiểm tra dữ liệu/lịch/nguồn; không bỏ assertion hoặc dùng random split |
| Inventory/marker/checksum sai | Ngừng công bố, kiểm tra/khôi phục snapshot; không xóa volume toàn dự án |
| Registry `prepared` / status từ chối | Tác vụ chưa công bố xong; xem log và retry cùng identity |
| Metrics thấp hơn baseline | Đọc confusion/top 1% và giới hạn nhãn; đây là kết quả thực nghiệm, không sửa test để đảm bảo thắng |

Ngừng cụm bằng `make down` vẫn giữ dữ liệu. `make reset CONFIRM=...` thuộc quy trình hạ tầng và xóa dữ liệu persistent theo [lifecycle](../infra/04-bootstrap-and-lifecycle.md); không dùng để xử lý một job RF lỗi. Chưa có lệnh public xóa riêng mọi purchase version: giữ snapshot phục vụ tái lập và chỉ dọn có chọn lọc sau khi xác định các ID còn được tham chiếu.
