# 02 — Hợp đồng sự kiện và KPI

[← Mục lục streaming](README.md)

## 1. Payload Kafka

```json
{
  "schema_version": 1,
  "dataset_version": "<SHA-256 nguồn>",
  "run_id": "demo-001",
  "event_id": "<ID gốc trong Silver>",
  "source": "retailrocket",
  "visitor_id": "001",
  "item_id": "10",
  "event_type": "view",
  "transaction_id": null,
  "event_time": "2015-09-10T10:00:00.384Z",
  "emitted_at": "2026-10-16T15:00:00.123Z"
}
```

Producer gửi vào `ecommerce.events.raw`, key là visitor ID. Các giá trị nguồn được giữ từ Silver; chỉ `run_id` và `emitted_at` được gắn cho lượt phát. ID là string, timestamp đúng UTC với ba chữ số milliseconds. `event_time` và `emitted_at` có ý nghĩa riêng.

Nguồn JSONL đã chuẩn bị chỉ chứa các trường nghiệp vụ, không có thời điểm phát. Checksum file và số event nằm trong config bất biến. RUN_ID có 1–64 ký tự chữ/số/gạch dưới/gạch ngang, bắt đầu bằng chữ hoặc số.

## 2. Kiểm tra và phân loại

| Nhóm | Điều kiện |
| --- | --- |
| `accepted` | Hợp lệ, duy nhất và trong chính sách event-time |
| `duplicate` | ID đã nhận với cùng nội dung nghiệp vụ hoặc dòng dư cùng batch |
| `invalid` | JSON/schema/trường sai, ngoài khoảng replay, hoặc ID xung đột nội dung |
| `late` | Event mới có event-time nhỏ hơn mốc trước batch trừ 10 phút |
| `foreign` | JSON object có run ID string thuộc lượt khác; không tính KPI run hiện tại |

Đối soát từng batch: `source_rows = accepted + duplicate + invalid + late + foreign`.

Validator kiểm tra schema version integer bằng 1, dataset đúng run, source Retailrocket, event ID SHA-256 chữ thường, ID visitor/item dạng chữ số, ba loại hành vi, transaction null/string chữ số và bắt buộc khi purchase. Hai timestamp phải parse đúng UTC; event-time nằm trong `[START, END)`.

Mã lỗi gồm `MALFORMED_JSON`, `INVALID_SCHEMA_VERSION`, `INVALID_RUN_ID`, `INVALID_DATASET_VERSION`, `INVALID_SOURCE`, `INVALID_EVENT_ID`, `INVALID_VISITOR_ID`, `INVALID_ITEM_ID`, `UNKNOWN_EVENT_TYPE`, `INVALID_TRANSACTION_ID`, `MISSING_TRANSACTION_ID`, `INVALID_EVENT_TIME`, `INVALID_EMITTED_AT`, `OUTSIDE_REPLAY_RANGE`, `EVENT_ID_CONFLICT`, `LATE_EVENT`.

Fingerprint nội dung nghiệp vụ dùng JSON canonical của schema/dataset/event/source/visitor/item/type/transaction/event-time. Nó không chứa run ID hoặc thời điểm phát, nên gửi lại sau ngắt vẫn nhận diện đúng duplicate.

Trong batch, dòng đầu theo `(topic, partition, offset)` xác định nội dung được giữ khi ID chưa có. Loại trùng với Silver đã commit trước batch; kiểm tra duplicate trước lateness. Ngưỡng trễ dùng max event-time đã chấp nhận trước batch, không thay đổi theo thứ tự task trong batch. Đúng biên 10 phút được nhận; batch đầu chưa có mốc thì nhận mọi event hợp lệ.

Invalid/late được gửi DLQ và giữ audit HDFS. DLQ ID là SHA-256 tọa độ topic/partition/offset, kèm run, batch, mã lỗi và raw key/value base64. Retry có thể tạo nhiều message cùng DLQ ID; khi đối soát phải dùng ID duy nhất.

## 3. Định nghĩa KPI

| KPI | Định nghĩa |
| --- | --- |
| `processed_events` | Số event accepted duy nhất của run |
| `views`, `add_to_cart`, `purchase_events` | Số event của từng loại |
| `transactions` | Transaction ID khác null, khác nhau |
| `visitors` | Visitor khác nhau |
| `buyers` | Visitor có purchase |
| `buyer_rate` | Buyers / visitors, lưu tỷ số 0..1; mẫu số 0 trả 0 |
| KPI phút | Các số trên theo `[đầu phút UTC, đầu phút tiếp theo)` |
| `max_event_time` | Mốc event-time accepted lớn nhất |
| `active_visitors` | Visitor trong `(max_event_time − 5 phút, max_event_time]` |

Distinct toàn run tính lại từ event đã nhận, không cộng distinct từng phút. Một đơn nhiều item có nhiều purchase event nhưng một transaction. Run không nhận thêm số liệu lịch sử ngoài đoạn replay. Những KPI đếm này có định nghĩa riêng với [funnel có session](../analytics/01-contract.md), doanh thu hoặc prediction. `stream-v2` thêm `behavior_version` và `expected_behavior` vào prepared config; không đổi schema payload Kafka.

## 4. Công bố và retry

Bronze → Silver → Gold → DLQ ACK → MongoDB snapshot → con trỏ công bố → Spark checkpoint. HDFS dùng snapshot không ghi đè. Khi retry, batch đã commit được kiểm tra rồi tái sử dụng, giữ nguyên quyết định accepted/late/duplicate.

MongoDB replace/upsert theo ID xác định, không cộng dồn `$inc`. Mọi document KPI và funnel của batch được ghi xong, xác minh sản phẩm rồi mới đổi `committed_batch_id` trong run. Reader lấy pointer trước rồi đọc đúng batch đó, nên không thấy phần snapshot mới ghi dở.

Không có transaction chung giữa Kafka/HDFS/MongoDB. Lỗi một sink làm callback thất bại để Spark chạy lại. Xem [Spark foreachBatch](https://spark.apache.org/docs/3.5.7/structured-streaming-programming-guide.html#foreachbatch) và [MongoDB atomicity](https://www.mongodb.com/docs/manual/core/write-operations-atomicity/).
