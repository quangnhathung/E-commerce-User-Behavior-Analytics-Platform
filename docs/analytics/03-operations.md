# 03 — Vận hành và phục hồi

[← Mục lục](README.md)

## 1. Điều kiện

Cụm Docker healthy, Silver lịch sử đã ingest và `artifacts/data/latest-ingest.json` tồn tại trong cùng workspace. Dùng terminal có quyền Docker. Nếu chưa có Silver, chạy `make ingest`; nếu chưa xác nhận reuse ingestion, chạy lại và kiểm tra cờ reuse theo [hướng dẫn ingestion](../data/03-ingestion.md).

```bash
make doctor
make test
make test-behavior
make test-stream
```

`test-behavior` kiểm tra batch/session/funnel trên fixture; `test-stream` kiểm tra Kafka, checkpoint và funnel qua nhiều batch. Hai lệnh dùng một Spark driver tại mỗi thời điểm, không chạy đồng thời.

## 2. Phân tích lịch sử

```bash
make analyze-behavior
make behavior-status
make behavior-status ITEM_ID=355908
```

Lệnh analyze đọc toàn bộ Silver đã công bố, không dùng giới hạn nguồn replay 10.000 event. Chạy lại cùng source/code tái sử dụng Gold. Nếu code hoặc source đổi, identity và analysis ID mới được sinh tự động, snapshot cũ được giữ.

Status mặc định trả overview và tối đa 20 sản phẩm, theo phiên xem giảm dần rồi ID chuỗi tăng dần. ITEM_ID là chuỗi chữ số, giữ nguyên số 0 đầu. Không tìm thấy sản phẩm trả `products=[]`; thiếu phân tích đã published báo lỗi. MongoDB vẫn lưu toàn bộ sản phẩm, không chỉ top 20.

## 3. Demo streaming mới

Sau khi cập nhật mã, dùng RUN_ID mới:

```bash
make prepare-demo RUN_ID=behavior-demo-001
```

Terminal A:

```bash
make stream RUN_ID=behavior-demo-001
```

Sau `STREAM_READY=behavior-demo-001`, terminal B:

```bash
make replay RUN_ID=behavior-demo-001
```

Terminal khác hoặc sau khi producer kết thúc:

```bash
make stream-status RUN_ID=behavior-demo-001
make behavior-status RUN_ID=behavior-demo-001
make behavior-status RUN_ID=behavior-demo-001 ITEM_ID=355908
```

`stream-status` có `funnel.overview` và `funnel.is_final`. `behavior-status` bổ sung trang sản phẩm. Trước batch đầu, chưa có funnel được công bố; dùng stream-status để xem producer/consumer pending. Sau offset drain, checkpoint và đối soát, streaming tự dừng, `is_final=true`.

Prepare mặc định dùng `[2015-09-10T10:00:00.000Z,2015-09-10T16:00:00.000Z)`, tối đa 10 event/giây. Khoảng khác vẫn bị giới hạn 10.000 event. Tham chiếu hành vi dùng đúng nguồn của run, không ghép lịch sử ngoài khoảng chọn.

## 4. Báo cáo

```text
artifacts/analytics/
  source-audit.json
  latest-analysis.json
  <analysis_id>/report.json
  <analysis_id>/behavior-status.json
  attempts/logs/analyze-behavior-<UUID>.log
  attempts/logs/resources-<UUID>.json
  attempts/resources.json
  tests/logs/...
  test-behavior.json
```

Report lịch sử chứa identity, source/code fingerprint, đường dẫn Gold, manifest, summary, replica, reused và kết quả monitor. Status nằm cạnh report của analysis. Log/mẫu tài nguyên giữ theo attempt; `resources.json` là lần quan sát mới nhất.

`source-audit.json` ghi đối soát CSV offline đã chạy trong phiên phát triển, với `streaming_runtime_verified=false`; đây không phải report Spark/MongoDB. Prepare và final report mới là đối soát của từng run thực tế.

Streaming tiếp tục lưu dưới `artifacts/streaming/<run_id>/`; `report.json` có thêm `behavior`, prepared config có `expected_behavior`, log có UUID. `passed=true` chỉ xác nhận chức năng; hiệu năng cần samples, `performance_target_met=true`, `no_oom_or_restart=true` và quan sát RAM đầy đủ. Xem [báo cáo streaming](../streaming/03-operations.md).

Report cũ vẫn có thể còn sau lượt lỗi. Kiểm tra exit code, run/analysis ID, code fingerprint và log của attempt hiện tại; không suy từ việc file JSON tồn tại.

## 5. Ngắt, retry và xử lý lỗi

Ctrl+C yêu cầu wrapper dừng tiến trình từ xa trước khi giải phóng khóa. Lịch sử tiếp tục bằng `make analyze-behavior` với cùng source/code; streaming tiếp tục bằng hai lệnh stream/replay của cùng RUN_ID, trong hai terminal. Giữ checkpoint và snapshot đã commit.

| Tình huống | Cách xử lý |
| --- | --- |
| Docker daemon/socket bị từ chối | Chạy từ terminal có quyền Docker; offline không cần daemon |
| Thiếu latest-ingest | Ingest thành công trong cùng workspace trước |
| Another managed Spark job | Đợi driver hiện tại dừng; không chạy batch và streaming đồng thời |
| Run code fingerprint changed / unsupported version | Chuẩn bị RUN_ID mới, hoặc khôi phục đúng mã cũ để tiếp tục run cũ |
| Historical analysis not published | Chạy lại analyze với cùng source/code để hoàn tất sink |
| Gold/Silver thiếu marker, sai checksum hoặc bị mất | Giữ dữ liệu/log để điều tra, khôi phục snapshot tương ứng; không overwrite |
| MongoDB ghi dở | Pointer chưa đổi; chạy lại cùng identity để ghi hoàn tất |
| Final session/funnel differs | So nguồn, khoảng replay, accepted/late và fingerprint; không tắt đối soát |
| Resource monitor báo OOM/restart | Kiểm tra log/resources và limit thực tế; không nâng RAM để che lỗi |
| Remote stop unconfirmed | Theo guard/cancel token trong [hướng dẫn streaming](../streaming/03-operations.md), xác nhận dừng trước khi chạy job khác |

Tính năng dùng bind mount `/opt/app`; không cần build image hoặc reset volume khi cập nhật mã. Test tự dọn namespace UUID. Dữ liệu lịch sử/demo được giữ để đối soát; chưa có lệnh xóa riêng analysis/run nghiệp vụ. Không xóa collection chung hoặc topic để dọn một lượt.
