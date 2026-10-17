# 03 — Vận hành replay và streaming

[← Mục lục streaming](README.md)

## 1. Chuẩn bị

Cụm phải healthy, có Silver lịch sử và `artifacts/data/latest-ingest.json`. Dùng cùng terminal/workspace có quyền Docker. Nếu chưa nghiệm thu tái sử dụng Silver, chạy `make ingest` lần nữa và xác nhận hai cờ reuse bằng true.

Mã hiện tại dùng `stream-v2` và có session/funnel. Dùng RUN_ID mới sau khi cập nhật; run/checkpoint được chuẩn bị bằng mã cũ cần đúng mã cũ để tiếp tục. Xem [vận hành hành vi](../analytics/03-operations.md).

```bash
make doctor
make test
make test-stream
make prepare-demo RUN_ID=demo-001
```

Lệnh prepare không huấn luyện hoặc khởi động streaming. Nó giữ snapshot nguồn, xuất file replay có thứ tự, lưu config HDFS/MongoDB và copy file về host bằng SHA-256. Nguồn không được đọc vào pandas hoặc collect toàn bộ lịch sử.

Đổi nguồn demo bằng:

```bash
make prepare-demo RUN_ID=demo-002 \
  START=2015-09-10T10:00:00.000Z \
  END=2015-09-10T16:00:00.000Z RATE=5
```

RATE phải lớn hơn 0 và tối đa 10. Nguồn có 1..10.000 event. Cùng RUN_ID và cấu hình được tái sử dụng; đổi cấu hình hoặc code phải chọn run mới. Nếu bỏ RUN_ID, prepare sinh UUID, ghi `latest-run.json`; các lệnh sau mặc định dùng con trỏ đó.

## 2. Hai terminal

Terminal A:

```bash
make stream RUN_ID=demo-001
```

Sau `STREAM_READY=demo-001`, terminal B:

```bash
make replay RUN_ID=demo-001
```

Terminal khác hoặc sau khi producer kết thúc:

```bash
make stream-status RUN_ID=demo-001
```

Producer dùng Python trong client nên chạy được đồng thời với Spark. Các lệnh ingest/smoke/test-data/prepare/test-stream khác phải đợi driver hiện tại dừng. Nếu producer đã completed, gọi replay cùng RUN_ID trả trạng thái cũ và không phát lại từ đầu.

Mỗi ACK được lưu vào MongoDB trước khi gửi event tiếp theo; `ack.json` host là bản sao. Khi tiếp tục, CLI đọc cursor MongoDB và bỏ qua đúng số dòng nguồn đã xác nhận. Lỗi trước khi lưu cursor có thể phát lại một event; consumer loại trùng bằng ID/nội dung nghiệp vụ.

## 3. Tự kết thúc và báo cáo

Streaming chờ producer completed, mọi partition đạt offset ACK cuối và checkpoint commit batch cuối tồn tại. Sau đó query dừng, kiểm tra toàn bộ snapshot, tập event/nội dung và KPI tham chiếu, kiểm tra MongoDB rồi ghi report. Tốc độ giới hạn 10 event/giây không phải cam kết throughput thực tế khi broker/backpressure chậm.

```text
artifacts/streaming/
  latest-run.json
  producer.lock
  test-stream.json
  source-audit.json
  baseline/<dataset_version>/events-v1/
  <run_id>/
    config.json
    manifest.json
    events.jsonl
    prepare.json
    ack.json
    producer.json
    status.json
    report.json
    resources.json
    logs/<action>-<UUID>.log
```

`report.json` chứa đối soát chức năng, KPI, counts từng loại, worker, offsets, backlog, batch time và p95 latency. `duration_seconds` là thời gian attempt streaming hiện tại; `replay_wall_seconds` và `acknowledged_rate_eps` đo từ lúc producer bắt đầu đến ACK cuối, gồm thời gian ngắt/backpressure. `passed=true` xác nhận tính đúng dữ liệu; nghiệm thu hiệu năng còn cần `performance_target_met=true`, có samples, `no_oom_or_restart=true` và quan sát RAM đầy đủ. Nếu chỉ có một batch, p95 steady-state/target là null vì batch đầu được loại khỏi phép đo; chưa đủ bằng chứng hiệu năng.

Report thêm `behavior` được đối chiếu với oracle Python đã chuẩn bị. `stream-status` có `funnel.overview` và `is_final`; dùng `make behavior-status RUN_ID=... [ITEM_ID=...]` để xem trang sản phẩm của cùng snapshot.

Thời điểm đo latency là sau khi pointer MongoDB đã công bố, so với `emitted_at` của các event accepted trong batch. Khi retry, mẫu phản ánh lần công bố lại. `resources.json` giữ trạng thái container trước/sau và mẫu `docker stats`; mẫu cách khoảng năm giây nên không thể chứng minh peak tuyệt đối.

Log và bản `logs/resources-<UUID>.json` được giữ riêng mỗi attempt; `resources.json` là quan sát mới nhất. Report cũ có thể còn sau attempt thất bại; luôn kiểm tra exit code, đường dẫn log và run ID, không dựa vào việc file JSON tồn tại.

`source-audit.json` là đối soát nguồn/contract offline, có `streaming_runtime_verified=false`; baseline là bản sao report ingestion cũ, không phải một lượt ingest mới. Các file này không chứng minh streaming đã chạy.

## 4. Ngắt và tiếp tục

Ctrl+C tại terminal producer hoặc streaming. CLI yêu cầu cancel wrapper theo UUID, đợi tiến trình dừng rồi giải phóng khóa. Checkpoint và các batch đã commit được giữ lại.

```bash
make stream RUN_ID=demo-001
make replay RUN_ID=demo-001
```

Chạy hai lệnh trong hai terminal như trên. Không xóa checkpoint để thử lại. Snapshot có marker/checksum sai bị từ chối, không tự overwrite. Nếu code fingerprint khác, khôi phục code cũ để tiếp tục hoặc chuẩn bị RUN_ID mới.

Tác vụ có timeout 30 phút. Nếu chưa chạy producer hoặc một sink lỗi, kiểm tra status/log thay vì chạy Spark driver thứ hai. Nếu CLI chưa xác nhận remote dừng, giữ archive/log và kiểm tra container trước khi chạy tiếp.

Khi cancel không được xác nhận, CLI ghi `artifacts/infra/driver-unconfirmed.json` hoặc `artifacts/streaming/producer-unconfirmed.json`. Guard ngăn job mới dù khóa file của tiến trình host đã được giải phóng. Khôi phục truy cập Docker, đọc token trong marker, chạy `docker compose exec -T spark-client python3 /opt/app/run_job.py cancel <token>`, xác nhận lệnh thành công và wrapper đã dừng; sau đó chỉ gỡ đúng marker đó. Không xóa file khóa để vượt qua guard.

Wrapper công bố PID record atomic và giữ dấu cancel cho token chưa kịp khởi động, để Ctrl+C sớm không làm xuất hiện driver muộn. Dấu cancel tạm của attempt đã thất bại có thể còn trong `/tmp` của client; nó chỉ chặn đúng UUID đó và mất khi container được tạo lại.

## 5. Lưu giữ và dọn dữ liệu

Prepare/replay/stream không xóa nguồn Silver, topic nghiệp vụ hoặc volume. Test tự dọn topic UUID, HDFS namespace UUID và document của riêng mình. Thư mục staging có thể còn sau kill cứng; chỉ dọn staging của attempt đã dừng.

Nguồn HDFS và snapshot nghiệp vụ được giữ để đối soát, không tự xóa khi demo kết thúc. Muốn xóa toàn bộ dữ liệu của project, lệnh reset hiện có vẫn áp dụng; xem [vận hành hạ tầng](../infra/05-operations.md). Không xóa cả topic raw/DLQ hoặc collection chung để dọn một run. Bản này chưa có lệnh xóa riêng run nghiệp vụ.
