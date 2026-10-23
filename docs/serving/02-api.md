# 02 — Hợp đồng API

[← Mục lục](README.md)

Base mặc định: `http://127.0.0.1:8000/api/v1`. Chạy `make api` trên host. OpenAPI ở `/docs`, JSON schema ở `/openapi.json`.

## Endpoint

| GET | Tham số | Kết quả |
| --- | --- | --- |
| `/health` | Không | API/MongoDB sẵn sàng; Mongo lỗi trả 503 |
| `/runs` | `limit=50`, `after` | Run ML nghiệp vụ mới nhất trước, cùng timestamp thì theo RUN_ID; max 200 |
| `/pipeline` | `run_id`, `batch_id` tùy chọn | Producer/consumer, backlog ACK, counts, history, duration và latency |
| `/overview` | Như trên | KPI toàn bộ snapshot |
| `/timeseries` | Như trên; `start`, `end` tùy chọn | KPI phút trong `[start,end)`; chuẩn hóa ISO 8601 có timezone về UTC |
| `/funnel` | Như trên; `limit=20`, max 200 | Funnel và top sản phẩm theo view sessions rồi item ID |
| `/segments` | `run_id`, `batch_id` | Cluster count/share và trung bình tám feature; cluster rỗng vẫn giữ count 0 |
| `/predictions` | Như trên; `limit=50`, `after`, `cluster_id`, `predicted_label=0/1` | Visitor xếp theo score giảm dần, ID tăng dần; max 200 |
| `/visitors/{visitor_id}` | `run_id`, `batch_id` | Feature, cluster, score, hai label/threshold và model/policy IDs |
| `/models` | `run_id`, `batch_id` | Bindings và báo cáo của model/policy đã published |

Các endpoint nghiệp vụ bắt buộc `run_id`. `batch_id` bỏ trống nghĩa là đọc pointer hiện tại trong request đó. Dashboard luôn truyền batch từ bước `/pipeline` để nhiều request dùng chung snapshot. Không chuyển visitor ID sang số.

Response nghiệp vụ có:

```json
{
  "run_id": "ml-sim-001",
  "batch_id": 4,
  "as_of_event_time": "2015-09-18T00:02:02.001Z",
  "observed_at": "2026-10-20T00:00:00.000Z",
  "status": "ready",
  "data": {}
}
```

Đây là ví dụ hình dạng response, không phải kết quả một lần gọi mới. Trước accepted event đầu tiên, `data`/cutoff là `null`, status `waiting_for_events`; nếu consumer lỗi trước đó thì `not_ready`. Run có snapshot cũ vẫn giữ dữ liệu đã commit khi consumer gặp lỗi.

`/predictions` và `/runs` thêm `next_cursor`. Dùng cursor với đúng run/batch/bộ lọc đã tạo nó. Muốn đổi bộ lọc hoặc chuyển sang snapshot mới, bắt đầu từ trang đầu.

## Ví dụ

```bash
curl -fsS http://127.0.0.1:8000/api/v1/health
curl -fsS 'http://127.0.0.1:8000/api/v1/runs?limit=20'
curl -fsS 'http://127.0.0.1:8000/api/v1/pipeline?run_id=ml-sim-001'
curl -fsS 'http://127.0.0.1:8000/api/v1/visitors/000099000000003?run_id=ml-sim-001'
```

Đọc `batch_id` thực từ response pipeline trước khi truyền vào request tiếp theo; không hardcode batch ID từ máy người khác. Các run chưa tồn tại trên máy mới trả 404.

## Ý nghĩa số liệu và lỗi

- KPI/funnel là toàn bộ run đến snapshot. Bộ lọc thời gian chỉ tác động timeseries. Không cộng distinct visitor từng phút để tạo visitor tổng.
- Backlog dùng ACK end offsets trừ processed offsets đã commit, chặn âm về 0; không phải Kafka consumer group lag.
- Latency p95 bỏ batch đầu; chưa có sample sẽ là `null`. Producer rate là trung bình ACK của run, không giả là throughput tức thời.
- Prediction score chưa hiệu chuẩn; policy metrics là đánh giá lịch sử, giữ `fresh_holdout=false`.

| HTTP | Mã lỗi thường gặp | Xử lý |
| --- | --- | --- |
| 404 | `RUN_NOT_FOUND`, `VISITOR_NOT_FOUND` | Kiểm tra bộ chọn run/visitor hoặc chờ visitor xuất hiện |
| 409 | `SNAPSHOT_NOT_COMMITTED` | Đọc lại pointer; không lấy batch tương lai |
| 422 | `INVALID_ARGUMENT`, `INVALID_CURSOR`, `INVALID_TIME_RANGE` | Sửa tham số; bỏ cursor khi đổi snapshot/filter |
| 503 | `BACKEND_UNAVAILABLE`, `SNAPSHOT_INCOMPLETE` | Kiểm tra MongoDB/registry; không chuyển thành dashboard toàn số 0 |

Error body: `{"detail":{"code":"...","message":"..."}}`. API không trả backend exception text hoặc credentials. Consumer reason được rút gọn; dùng log job để chẩn đoán chi tiết.

CORS cho phép origin HTTP `localhost` và `127.0.0.1`, gồm cổng Live Server tùy chọn; không bật credentials. API bind `127.0.0.1`, phù hợp scope máy cá nhân. Chỉ có GET nghiệp vụ; không có endpoint huấn luyện, producer hoặc quản lý Docker.
