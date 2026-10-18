# 10 — ML scoring trên streaming

[← Mục lục ML](README.md) · [Runbook máy mới](../GETTING_STARTED.md#streaming-ml) · [Policy precision](09-purchase-policy.md)

## Mục đích và trạng thái

Pipeline mới đưa event Kafka qua validation/dedup, KPI/session/funnel, feature bảy ngày, K-Means, Random Forest và policy precision. Kết quả được lưu thành snapshot HDFS và công bố vào MongoDB để CLI tra cứu. Model không được fit lại khi streaming; không cần thêm service, dependency, image hoặc tăng ngân sách khoảng 7 GiB.

Ngày **18/10/2026**, đã triển khai hợp đồng `stream-ml-v1`, profile ZIP `streaming-ml`, CLI, simulator và kiểm thử. **26 test offline mới đạt**; năm archive cũ giữ nguyên checksum. Bộ hồi quy có 34 test hạ tầng + 187 test ứng dụng, 7 HTTP skipped trong phiên bị giới hạn socket. `make config` đạt. **Live suite và replay/simulator ML nguồn thật chưa có bằng chứng nghiệm thu**: phiên công cụ không truy cập được Docker socket. Các số và điều kiện trong phần nghiệm thu dưới đây là tiêu chí phải kiểm khi chạy, không phải kết quả runtime đã đạt.

Giai đoạn này chưa thêm API/dashboard. Policy hiện có từng đạt precision test 73,88% và recall 13,64%, nhưng đó là reassessment trên test đã xem; không suy ra precision mới cho streaming khi chưa có nhãn tương lai đầy đủ. `purchase_score` chưa hiệu chuẩn, không diễn giải thành phần trăm xác suất mua.

## Điều kiện và phiên bản được khóa

Trước khi chạy cần có: cụm healthy, Silver ingest `quality_status=passed`, K-Means published, RF published và policy **eligible** của đúng RF. Chạy từng Spark job tuần tự; producer là tiến trình Python riêng có thể chạy cùng consumer.

`prepare-ml-demo` mặc định đọc các pointer hiện có một lần, xác minh registry/snapshot, rồi khóa vào config:

| Selector | Mặc định |
| --- | --- |
| `SEGMENT_MODEL_VERSION` | Model trong `artifacts/ml/latest-model.json` |
| `PURCHASE_MODEL_VERSION` | RF trong `artifacts/prediction/latest-purchase-model.json` |
| `POLICY_VERSION` | Policy trong `artifacts/prediction/policies/latest-policy.json` |

Có thể truyền selector ngay trên lệnh prepare bằng ID thực từ máy mình. Những lệnh stream/replay/status sau đó đọc config của RUN_ID; đổi latest pointer hoặc truyền selector khác không thay model của run đã chuẩn bị. Muốn thay model/mã/tham số, tạo RUN_ID mới.

Kiểm tra nguồn gồm: feature order/version, Spark 3.5.7, checksum model/feature/Silver, RF candidate/evaluation và policy selection/evaluation. K-Means và RF phải dùng cùng Silver. Thiếu model, policy không eligible hoặc provenance khác sẽ bị từ chối; không tự fallback về threshold cũ.

Checksum archive chạy streaming khác checksum archive đã tạo model. Job tái tạo checksum năm profile cũ từ ZIP đang chạy và so đúng profile với nguồn; không sửa metadata model để khớp checksum mới.

## Lịch sử trước replay và cutoff

Mặc định mới:

```text
START = 2015-09-18T00:00:00.000Z
END   = 2015-09-18T03:00:00.000Z
RATE  = 10 event/giây
```

Cửa sổ chuẩn có **1.528 event và 1.016 visitor**, gồm 1.479 lượt xem, 34 event thêm giỏ và 15 event mua theo CSV nguyên bản. Đây không phải cohort batch 69.052 visitor; streaming chỉ score visitor đã có accepted event trong run.

Demo ML không dùng mặc định cũ ngày 10/09: policy đã quan sát nhãn của test đến hết 17/09. `START` phải từ thời điểm kết thúc cửa sổ nhãn test trở đi, đồng thời không trước cutoff K-Means. Giới hạn demo: tối đa 10.000 event, cửa sổ tối đa 24 giờ, `0 < RATE <= 10`.

Prepare tạo seed từ **`[START − 7 ngày, START)`**, giữ trong snapshot HDFS có checksum/hai replica. Trong mỗi batch chỉ đọc seed và accepted events đã qua Kafka đến batch đó. Nguồn replay đã chuẩn bị không được dùng để tính trước feature của event chưa nhận.

Với batch có accepted event:

1. `t = max(event_time đã accepted của run) + 1 ms`, không dùng giờ hiện tại làm cutoff.
2. Xác định toàn bộ visitor đã có accepted event trong run.
3. Gọi hàm feature batch hiện có trên `[t − 7 ngày, t)` cho cohort này; distinct được tính trên toàn cửa sổ.
4. Transform bằng hai PipelineModel đã tải một lần lúc khởi động.
5. Giữ cả label ở ngưỡng RF gốc và label ở ngưỡng policy đã khóa.

Mọi visitor trong snapshot có cùng cutoff. Không yêu cầu cohort phải xuất hiện đủ K cluster; fingerprint giữ cả cluster có count 0. Batch chỉ có duplicate/invalid/foreign/late giữ nguyên nội dung prediction trước; trước accepted event đầu tiên, scoring ở `waiting_for_events`.

## Chạy kiểm thử tích hợp trước

Từ root repository, terminal có quyền Docker, không có driver khác:

```bash
make doctor
make test-stream-ml
```

Suite có **bảy test tích hợp** và **bảy fault scenario**. Fixture tạo Silver, K-Means/RF thật và saved validation/test predictions trong namespace `/ecommerce/_tests/streaming-ml/<UUID>`; RF fixture nhỏ có 5 cây/depth 2, không phải huấn luyện lại model nghiệp vụ.

| Test | Kiểm tra |
| --- | --- |
| Prepare | Source/seed, reuse, model binding, sai checksum và START trước policy observation cutoff |
| Simulator theo batch | Ba đợt, visitor mới/leading zero, feature khớp oracle, không fit/load model trong mỗi batch |
| Retry | Lỗi sau Bronze/Silver/Gold, giữa Mongo, trước/sau committed pointer; manifest/count không đổi |
| Validation | Duplicate, malformed, foreign và batch không accepted; snapshot không bị xóa về số 0 |
| Corruption | Thiếu checkpoint/marker, Mongo published bị sửa phải fail thay vì tự chữa |
| Kafka thật | Producer ACK, lỗi sau Mongo trước checkpoint commit, restart và completed reuse |
| Simulator Kafka thật | Producer chờ ba snapshot ML committed, consumer auto-stop và batch reconciliation |

Các lỗi `injected ...` giữa log là chủ động. Chỉ coi suite đạt khi exit 0, cuối log `OK`, `STREAM_ML_RESULT.passed=true`, `cleaned=true`, `minimum_block_replicas=2`, cùng hai resource flag true. Cleanup chỉ xóa run/model/policy/topic thuộc UUID; lỗi cleanup làm suite fail. Topic test và nguồn/model nghiệp vụ được giữ tách biệt.

## Replay dữ liệu thật: hai terminal

Chuẩn bị RUN_ID mới tại root:

```bash
make prepare-ml-demo RUN_ID=ml-replay-001
```

Terminal A, cùng clone và thư mục gốc:

```bash
make stream-ml RUN_ID=ml-replay-001
```

Đợi **`STREAM_ML_READY=ml-replay-001`**, rồi terminal B:

```bash
make replay-ml RUN_ID=ml-replay-001
```

Terminal thứ ba có thể đọc status:

```bash
make stream-ml-status RUN_ID=ml-replay-001
```

Producer xong chưa đủ: đợi consumer drain ACK, commit checkpoint, đối soát batch và trả về prompt. Không chạy Spark job khác trong lúc đó.

Đạt khi report có `passed=true`, `batch_reconciled=true`, backlog 0, 1.528 processed event, 1.016 prediction, hai worker/hai replica, monitor đầy đủ và không OOM/restart. Số predicted positive hoặc cluster distribution lấy từ report, không hardcode theo batch score cũ. Pipeline tái tính feature và transform batch từ đúng seed + accepted source để kiểm fingerprint cuối.

Mục tiêu steady-state latency p95 ≤60 giây, bỏ batch đầu. Report giữ `startup_seconds` từ khởi động/kiểm model đến query ready và `first_batch_seconds` riêng; completed reuse giữ số startup của lượt stream đã chạy. `performance_target_met=false` là chưa đạt mục tiêu hiệu năng dù các kiểm tra đúng dữ liệu có thể đạt. Không tự tăng RAM để làm kết quả đo đẹp hơn; giữ report/log/resources cho điều tra.

## Simulator: ba visitor mới, ba đợt

Sau khi replay trước đã kết thúc, chuẩn bị run riêng:

```bash
make prepare-ml-demo RUN_ID=ml-sim-001 DEMO_SOURCE=simulator
```

Terminal A:

```bash
make stream-ml RUN_ID=ml-sim-001
```

Sau `STREAM_ML_READY=ml-sim-001`, terminal B:

```bash
make simulate-ml RUN_ID=ml-sim-001
```

| Visitor | Đợt 1 | Đợt 2 | Đợt 3 |
| --- | --- | --- | --- |
| `990000000001` | Xem item 10 | Thêm giỏ item 10 | Mua item 10 |
| `990000000002` | Xem item 20 | Xem item 21 | Xem item 22 |
| `000099000000003` | Thêm giỏ item 30 | Xem item 30 | Mua item 30 |

Prepare xác minh ba ID không có trong seed. Wire dùng `source=simulator`; dataset version chỉ tham chiếu seed chung, không giả đây là event Retailrocket thật. Event ID ổn định theo run và nội dung. Thời gian nghiệp vụ vẫn là năm 2015 trong cửa sổ run; `emitted_at` dùng giờ phát thực tế.

Sau mỗi ba event, producer lưu ACK rồi chờ committed ML snapshot đã xử lý đủ offset. Dòng `SIMULATOR_PHASE_COMMITTED` cho biết đợt/batch/fingerprint. Mỗi lần chờ tối đa 180 giây; timeout hoặc consumer failed sẽ dừng producer và giữ cursor để retry. Không phát tất cả event liên tiếp rồi giả đã quan sát ba lần cập nhật.

Tra visitor ở terminal khác:

```bash
make stream-ml-status RUN_ID=ml-sim-001 VISITOR_ID=990000000001
make stream-ml-status RUN_ID=ml-sim-001 VISITOR_ID=000099000000003
```

Đạt khi đủ chín event/ba visitor, feature đổi qua từng đợt, model/policy ID giữ nguyên và consumer kết thúc. Visitor thứ ba có zero view ở đợt đầu; tỷ lệ chia cho 0 là 0. Funnel của visitor này không tự bổ sung lượt xem trước giỏ. Score hoặc label không bắt buộc tăng sau mỗi hành động.

## Đọc status, report và lưu trữ

`stream-ml-status` dùng Python/Mongo, không tạo Spark driver. `backlog` là tổng ACK end offsets trừ processed offsets của snapshot committed, chặn âm về 0; không phải báo cáo Kafka consumer-group lag. Trước khi producer phát, backlog 0 không có nghĩa run đã hoàn thành.

Status gồm consumer/producer, committed batch, KPI, funnel, bindings và `scoring`:

- `status`: `waiting_for_events`, `ready` hoặc `not_ready` khi startup thất bại mà chưa có snapshot.
- `as_of`: cutoff event-time, `scored_at`: thời điểm tạo snapshot inference đầu tiên.
- `fingerprint`: số visitor, SHA nội dung/score, predicted positive gốc/policy và counts theo cluster.
- `feature_fingerprint`, `carried_forward`, snapshot ID.
- `visitor` khi truyền `VISITOR_ID`; null hợp lệ nếu visitor không thuộc run. `001` và `1` là hai chuỗi khác nhau.

Mỗi prediction giữ tám raw feature, cluster, purchase score, `original_predicted_label`, `predicted_label`, hai threshold, ba model/policy ID và feature version. Label chính dùng policy; score chưa hiệu chuẩn. Các failure state ở `consumer` không làm xóa snapshot committed trước đó.

```text
artifacts/streaming-ml/
  latest-run.json
  test-stream-ml.json
  tests/logs/
  <RUN_ID>/
    config.json, manifest.json, prepare.json, events.jsonl
    ack.json, producer.json, report.json, status.json, resources.json
    logs/

HDFS:
  /ecommerce/streaming-ml/<RUN_ID>/       # config, source và seed
  /ecommerce/{bronze,silver,gold}/streaming-ml/<RUN_ID>/<batch_id>/
  /ecommerce/checkpoints/streaming-ml/<RUN_ID>/
```

Gold mỗi batch có `metrics.json`, session/funnel, `features/`, `predictions/`, `ml-quality.json`, manifest và completion marker. Source/seed cũng nằm trong snapshot được kiểm checksum và hai replica. Mongo dùng thêm `stream_ml_snapshots`, `stream_ml_predictions`; run/KPI/funnel dùng registry streaming chung với RUN_ID bất biến. RUN_ID đã thuộc cấu hình khác bị từ chối.

Rows Mongo được ghi 100 dòng/lô, xác minh count/fingerprint rồi mới công bố chung qua committed run pointer. Reader khóa pointer trước khi đọc, không trộn prediction mới với KPI cũ. Snapshot ML đã published mà thiếu/sai row sẽ fail khi verify/retry, không tự sửa. Report host chỉ được chấp nhận sau khi job và monitor đạt.

## Retry, đổi phiên bản và vận hành

- Ctrl+C: chờ manager xác nhận remote job dừng, giữ source/seed/checkpoint. Consumer restart bằng cùng `stream-ml RUN_ID=...`; producer retry bằng lệnh đúng loại nguồn.
- Simulator resume tại biên đợt phải chờ snapshot đã ACK trước khi phát đợt kế tiếp. Producer completed không phát lại.
- Run completed: chạy lại consumer phải có `reused=true`, cùng fingerprint, không thêm Mongo duplicate. Kiểm checkpoint commit cuối và model/seed nguồn trước khi reuse.
- Dùng producer sai loại nguồn sẽ bị từ chối. `replay-ml` chỉ nhận replay; `simulate-ml` chỉ nhận simulator.
- Mã app thay đổi làm checksum archive mới: khôi phục đúng mã để resume hoặc chuẩn bị RUN_ID mới. Không đổi manifest/config/checkpoint bằng tay.
- Lỗi khóa driver/producer và `driver-unconfirmed.json`/`producer-unconfirmed.json` dùng cơ chế chung. Không xóa lock/marker để chạy chồng job; xem [phục hồi streaming](../streaming/04-testing-and-recovery.md).
- Profile mới không thay các pointer batch/policy gốc. `make stream`/`make replay` vẫn dành cho run v2; dùng các target `*-ml` cho namespace mới.
- Không cần `make build`, reset volume, build-purchase-data hoặc train-purchase để sử dụng tính năng. App bind mount phải có sẵn; nếu thiếu mount, dừng job rồi `make up` để áp cấu hình hiện tại.

## Mã nguồn và nghiệm thu bàn giao

| Thành phần | Vai trò |
| --- | --- |
| `app/streaming_ml_manage.py`, `app/jobs/streaming_ml.py` | CLI host, selector, upload spec, dispatcher và resource gate |
| `contracts.py`, `models.py` | Wire/simulator/fingerprint; đối chiếu profile cũ, provenance và tải PipelineModel |
| `prepare.py`, `producer.py` | Seed/source bất biến; producer ACK, phase barrier và retry |
| `batch.py`, `inference.py`, `store.py`, `stream.py` | Bronze/Silver/Gold, feature/scoring, publication, checkpoint và batch reconciliation |
| `fixtures.py`, `integration.py` | Model fixture thật, fault injection, Kafka/checkpoint và cleanup UUID |
| `app/tests/test_streaming_ml*.py` | 26 offline test cho contracts/package, binding, host, producer và Mongo boundary |

Đánh dấu riêng: offline/config đạt; live suite đạt; replay nguồn thật đạt; simulator thật đạt; completed reuse đạt; latency/resource đạt. Chưa có artifact của hạng mục nào thì để pending. Bằng chứng K-Means/RF/policy batch cũ không thay thế nghiệm thu streaming ML mới.
