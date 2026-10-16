# 04 — Kiểm thử và phục hồi

[← Mục lục streaming](README.md)

## 1. Kiểm thử offline

`make test` giữ nguyên suite hạ tầng/ingestion và bổ sung 27 test streaming. Chỉ cần Python standard library và công cụ kiểm thử hạ tầng đã có; Kafka/Mongo/PySpark chỉ cần trong container khi chạy tích hợp.

| Nhóm | Bằng chứng |
| --- | --- |
| Contract | Đúng UTC milliseconds, ID string/null, JSON/type/version/range lỗi, foreign run, hash nghiệp vụ và DLQ ID |
| Producer | ACK trước cursor, tốc độ/thứ tự, lỗi delivery, cursor lưu thất bại và tiếp tục, nguồn thiếu không đánh dấu completed |
| KPI tham chiếu | Distinct transaction/visitor, active window đúng biên, buyer rate khi rỗng |
| Packaging | Profile streaming tái lập được; SHA ingest trùng bản đã công bố |
| Mongo boundary | Ghi minute dở không đổi snapshot đang đọc; retry sau pointer không thêm document; chặn pointer lùi |
| Host | Fetch lỗi giữ nguồn cũ, config lớn không truyền qua argv, khóa producer liên tiến trình, cancel trước khi wrapper chạy, guard driver chưa dừng, nhận diện restart và limit container cũ |

Test Mongo offline dùng adapter trạng thái trong bộ nhớ, không chứng minh hành vi server thật. Các test Spark/HDFS/Kafka/Mongo nằm ở bước tiếp theo.

## 2. Kiểm thử tích hợp

```bash
make test-stream
```

Suite có tám test, gồm bảy fault scenario trong test retry:

1. KPI chính xác, duplicate giữa batch, malformed JSON, ID conflict và foreign run.
2. Lateness đúng biên 10 phút; quá biên vào audit/DLQ.
3. Retry sau Bronze, Silver, Gold, DLQ; lỗi ghi minute, trước pointer và sau pointer MongoDB. Manifest đã commit giữ nguyên, số liệu không cộng lặp.
4. Có Silver nhưng mất checkpoint bị từ chối.
5. Mất completion marker bị từ chối.
6. DLQ đã ACK rồi lỗi: retry cho hai message cùng source ID, giữ raw và mã lỗi.
7. Kafka thật → query lỗi sau MongoDB trước Spark checkpoint → restart cùng checkpoint → tự dừng và đối soát đúng, có hai worker parse.
8. Prepare đọc Silver đúng schema, sắp nguồn và tạo tham chiếu chính xác; chạy lại giữ nguyên manifest và checksum.

Fixture gồm sáu event: ba view, một add to cart và hai purchase cùng transaction. Có ba visitor, một buyer, một transaction; active visitors cuối là hai. Tham chiếu Python độc lập được so với aggregate Spark.

Mọi topic có prefix `app-stream-test-<UUID>`/`app-stream-dlq-<UUID>`. HDFS chỉ dưới `/ecommerce/_tests/stream/<UUID>`. MongoDB chỉ xóa document có run ID được suite tạo. Cleanup chạy trong finally; lỗi test hoặc cleanup không tạo report thành công mới.

## 3. Nghiệm thu dữ liệu thật

Nguồn mặc định phải có 2.692 event: view 2.596, cart 69, purchase 27, transaction 19, visitor 1.860, buyer 17. Invalid và late phải bằng 0. Đối soát không chỉ số lượng: fingerprint toàn bộ `(event_id, business_sha256)` và KPI từng phút phải trùng nguồn đã chuẩn bị.

Sau một lượt hoàn chỉnh, chạy thêm lượt mới với RUN_ID khác, ngắt streaming trong khi producer còn phát, khởi động lại cùng RUN_ID/checkpoint rồi xác nhận kết quả cuối. Khi ngắt producer, tiếp tục cùng RUN_ID; sự kiện chưa chắc chắn có thể được phát lại nhưng tổng KPI không tăng sai.

Nghiệm thu thêm: backlog về 0 ở cả ba partition, checkpoint batch cuối tồn tại, payload có hai replica, hai worker thực sự parse, container không OOM/restart, p95 sau khởi động không quá 60 giây. Nếu thiếu samples hoặc monitor lỗi, ghi chưa đủ bằng chứng thay vì suy từ giới hạn Compose.

## 4. Xử lý lỗi

| Lỗi | Cách kiểm tra |
| --- | --- |
| Docker socket permission denied | Chạy tại terminal có quyền Docker; kiểm thử offline vẫn thực hiện được |
| Thiếu latest-ingest/Silver | Hoàn thành ingestion, giữ artifact nguồn đúng workspace |
| Source count rỗng/vượt 10.000 | Chọn lại khoảng START/END, dùng RUN_ID mới |
| Code fingerprint changed | Khôi phục đúng code hoặc chuẩn bị run mới; không tắt kiểm tra |
| Checkpoint missing | Khôi phục checkpoint tương ứng hoặc dùng run mới |
| Snapshot checksum/identity mismatch | Giữ dữ liệu để điều tra/khôi phục; không reset volume để thử lại |
| Producer pending, streaming chưa kết thúc | Khởi động hoặc tiếp tục producer của đúng run |
| Kafka delivery not confirmed | Xem broker và producer log; cursor chỉ lưu khi ACK |
| Mongo pointer chưa đổi | Xem lỗi ghi overview/minute/DLQ; batch sẽ cần retry |
| Final KPI/identity differs | So audit/late/invalid và nguồn; giữ checkpoint/snapshot phục vụ đối soát |
| Project container restarted/OOM | Xem resources và memory budget; không nâng ngưỡng để che limit cũ |
| Remote job not confirmed stopped | Xem marker guard, xác nhận/cancel token đã ghi rồi mới gỡ đúng marker; không mở driver thứ hai |

## 5. Trạng thái triển khai

- Test offline streaming đã đạt, SHA archive ingest giữ nguyên.
- Lượt kiểm tra hiện tại: 70 test đạt, 7 test HTTP skipped; Compose và cú pháp đã đạt. Đối soát nguồn offline có 345 phút chứa event, `max_event_time=2015-09-10T15:59:53.734Z` và 96 active visitor ở cửa sổ cuối.
- Có artifact runtime cũ chứng minh smoke, chín test ingestion và ingest lần đầu đạt; chưa có bằng chứng ingest reuse lần hai trong phiên công cụ.
- Lệnh ingest lại và kiểm thử streaming cần Docker thật. Phiên công cụ bị từ chối Docker socket, chưa có report runtime streaming mới.
- Kết quả đối soát CSV offline chỉ xác nhận nguồn và contract; không chứng minh Kafka, HDFS/MongoDB KPI, restart hoặc RAM thực tế của streaming.

Nguồn kỹ thuật: [Spark foreachBatch và checkpoint](https://spark.apache.org/docs/3.5.7/structured-streaming-programming-guide.html), [Kafka offsets khi restart Spark](https://spark.apache.org/docs/3.5.7/structured-streaming-kafka-integration.html), [MongoDB atomicity](https://www.mongodb.com/docs/manual/core/write-operations-atomicity/).
