# 04 — Kiểm thử và phục hồi

[← Mục lục streaming](README.md)

## 1. Kiểm thử offline

`make test` giữ nguyên suite hạ tầng/ingestion và chạy 39 test streaming, gồm 12 test hồi quy Kafka readiness, cùng 23 test mới cho [session/funnel và host](../analytics/04-testing.md). Chỉ cần Python standard library và công cụ kiểm thử hạ tầng đã có; Kafka/Mongo/PySpark chỉ cần trong container khi chạy tích hợp.

| Nhóm | Bằng chứng |
| --- | --- |
| Contract | Đúng UTC milliseconds, ID string/null, JSON/type/version/range lỗi, foreign run, hash nghiệp vụ và DLQ ID |
| Producer | ACK trước cursor, tốc độ/thứ tự, lỗi delivery, cursor lưu thất bại và tiếp tục, nguồn thiếu không đánh dấu completed |
| Kafka readiness | Metadata chưa có topic/leader, lỗi leader khi đọc dở offset, retry lỗi tạm thời, deadline chung, lỗi quyền/cấu hình báo ngay, đóng consumer và đợi cả raw/DLQ |
| KPI tham chiếu | Distinct transaction/visitor, active window đúng biên, buyer rate khi rỗng |
| Packaging | Profile streaming tái lập được; SHA ingest trùng bản đã công bố |
| Mongo boundary | Ghi minute dở không đổi snapshot đang đọc; retry sau pointer không thêm document; chặn pointer lùi |
| Host | Fetch lỗi giữ nguồn cũ, config lớn không truyền qua argv, khóa producer liên tiến trình, cancel trước khi wrapper chạy, guard driver chưa dừng, nhận diện restart và limit container cũ |

Test Mongo offline dùng adapter trạng thái trong bộ nhớ, không chứng minh hành vi server thật. Các test Spark/HDFS/Kafka/Mongo nằm ở bước tiếp theo.

## 2. Kiểm thử tích hợp

```bash
make test-stream
```

Suite `stream-v2` có mười test, gồm tám fault scenario trong test retry:

1. KPI chính xác, duplicate giữa batch, malformed JSON, ID conflict và foreign run.
2. Lateness đúng biên 10 phút; quá biên vào audit/DLQ.
3. Retry sau Bronze, Silver, Gold, DLQ; lỗi ghi minute, sản phẩm funnel, trước pointer và sau pointer MongoDB. Manifest đã commit giữ nguyên, số liệu không cộng lặp.
4. Có Silver nhưng mất checkpoint bị từ chối.
5. Mất completion marker bị từ chối.
6. DLQ đã ACK rồi lỗi: retry cho hai message cùng source ID, giữ raw và mã lỗi.
7. Kafka thật → query lỗi sau MongoDB trước Spark checkpoint → restart cùng checkpoint → tự dừng và đối soát đúng, có hai worker parse.
8. Prepare đọc Silver đúng schema, sắp nguồn và tạo tham chiếu chính xác; chạy lại giữ nguyên manifest và checksum.
9. Event đến muộn được chấp nhận đúng biên 10 phút nối hai session qua batch, funnel được tính lại đúng.
10. Batch chỉ có invalid công bố funnel rỗng; batch hợp lệ sau đó cho KPI/funnel đúng.

Các test đối soát KPI hoàn chỉnh cũng xác minh session/funnel HDFS và MongoDB với tham chiếu Python độc lập. Fixture lịch sử và các biên funnel khác chạy bằng `make test-behavior`.

Fixture gồm sáu event: ba view, một add to cart và hai purchase cùng transaction. Có ba visitor, một buyer, một transaction; active visitors cuối là hai. Tham chiếu Python độc lập được so với aggregate Spark.

Mọi topic có prefix `app-stream-test-<UUID>`/`app-stream-dlq-<UUID>`. HDFS chỉ dưới `/ecommerce/_tests/stream/<UUID>`. MongoDB chỉ xóa document có run ID được suite tạo. Cleanup chạy trong finally; lỗi test hoặc cleanup không tạo report thành công mới.

Sau khi Future tạo topic thành công, suite đợi raw có ba partition và DLQ có một partition, mỗi partition có leader và đọc được watermark. Việc tạo topic thành công chưa đảm bảo metadata đã sẵn sàng ngay cho client khác; Kafka ghi rõ có thể có độ trễ sau thao tác này. [Tài liệu CreateTopics](https://kafka.apache.org/39/javadoc/org/apache/kafka/clients/admin/Admin.html)

Trong test phục hồi checkpoint, traceback `RuntimeError: injected before Spark checkpoint commit` và Spark `ERROR MicroBatchExecution` là lỗi có chủ đích. Test phải khởi động lại cùng checkpoint, kiểm tra kết quả rồi kết thúc bằng `ok`. Dùng tổng kết cuối `Ran 10 tests` và `OK`, cùng kết quả cleanup/resource monitor của lệnh, để đánh giá toàn bộ suite hiện tại. Log cũ có `Ran 8 tests` thuộc phiên bản trước khi thêm hành vi. Cảnh báo idempotence `Coordinator load inprogress: retrying` trong log đã tự phục hồi, không phải lỗi làm lượt chạy được cung cấp thất bại.

## 3. Nghiệm thu dữ liệu thật

Nguồn mặc định phải có 2.692 event: view 2.596, cart 69, purchase 27, transaction 19, visitor 1.860, buyer 17. Invalid và late phải bằng 0. Đối soát không chỉ số lượng: fingerprint toàn bộ `(event_id, business_sha256)` và KPI từng phút phải trùng nguồn đã chuẩn bị.

Session/funnel mặc định phải có 1.893 phiên, với 1.882 phiên xem, 35 phiên đạt giỏ và 9 phiên hoàn thành chuỗi mua; đối soát fingerprint toàn bộ sản phẩm. Xem [hợp đồng và nghiệm thu](../analytics/04-testing.md).

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
| NOT_LEADER_FOR_PARTITION khi lấy initial offsets | Client gặp leader/metadata chưa sẵn sàng; bước đọc offset tự làm mới metadata và retry trong 30 giây |
| Kafka topic ... not ready within 30s | Hết thời gian chờ; thông báo giữ lỗi Kafka cuối cùng. Kiểm tra `make doctor` và `docker compose logs --tail=200 kafka` |
| Mongo pointer chưa đổi | Xem lỗi ghi overview/minute/DLQ; batch sẽ cần retry |
| Final KPI/identity differs | So audit/late/invalid và nguồn; giữ checkpoint/snapshot phục vụ đối soát |
| Project container restarted/OOM | Xem resources và memory budget; không nâng ngưỡng để che limit cũ |
| Remote job not confirmed stopped | Xem marker guard, xác nhận/cancel token đã ghi rồi mới gỡ đúng marker; không mở driver thứ hai |

### Cơ chế chờ Kafka và chạy lại sau sửa

Hàm `end_offsets(topic, *, partitions=3, timeout=30)` trong [kafka_io.py](../../app/ecommerce_streaming/kafka_io.py) được dùng ở cả prepare và fixture tích hợp. Kết quả vẫn là dictionary partition dạng chuỗi → end offset; raw mặc định yêu cầu đúng partition 0, 1, 2, còn kiểm tra DLQ truyền `partitions=1`.

Mỗi lượt làm mới metadata rồi đọc toàn bộ watermark với `cached=False`. Các lỗi tạm thời được retry gồm `UNKNOWN_TOPIC_OR_PART`, `LEADER_NOT_AVAILABLE`, `NOT_LEADER_FOR_PARTITION`, `REQUEST_TIMED_OUT`, `_UNKNOWN_TOPIC`, `_UNKNOWN_PARTITION`, `_TIMED_OUT`, `_TRANSPORT`, `_ALL_BROKERS_DOWN`. Khoảng chờ tăng từ 0,25 lên 0,5 rồi tối đa 1 giây. Mọi lần gọi broker dùng tối đa 10 giây hoặc thời gian còn lại trong deadline chung 30 giây của topic; không cấp lại 30 giây cho từng partition/lượt thử.

Nếu đọc dở gặp lỗi, toàn bộ kết quả tạm bị bỏ và lượt sau đọc lại mọi partition. Consumer được đóng trong `finally`. Hết hạn báo `TimeoutError` kèm tên topic và lỗi cuối; lỗi quyền, xác thực hoặc sai tập partition được báo ngay. Client chỉ kiểm tra metadata/watermark, không subscribe hoặc commit consumer offset. Các hành vi này có kiểm thử bằng client và đồng hồ giả tại [test_streaming_kafka.py](../../app/tests/test_streaming_kafka.py); chúng không thay thế broker thật.

Sau khi cập nhật mã, với cụm đang healthy:

```bash
make test
make test-stream
```

Không cần build lại image, khởi tạo lại hạ tầng hoặc xóa volume vì `/opt/app` dùng bind mount mã nguồn. `test-stream` tự tạo UUID mới mỗi lượt. Nghiệm thu bằng ba lượt liên tiếp: mỗi lượt đủ mười test đạt, cleanup thành công và monitor không phát hiện OOM/restart. Nếu kiểm thử thất bại, giữ log trong `artifacts/streaming/tests/logs/` và kiểm tra nguyên nhân trước khi thử lại.

Bản sửa đổi fingerprint mã streaming: run demo đã chuẩn bị bằng mã cũ cần khôi phục đúng mã để tiếp tục, hoặc chuẩn bị run mới với RUN_ID khác. Archive ingestion và dữ liệu lịch sử giữ nguyên.

## 5. Trạng thái triển khai

- Test offline streaming đã đạt, SHA archive ingest giữ nguyên.
- Lượt kiểm tra sau thêm session/funnel: 105 test đạt (27 hạ tầng và 78 ứng dụng), 7 test HTTP skipped vì môi trường cấm socket; `make config` đạt. Đối soát nguồn offline trước đó có 345 phút chứa event, `max_event_time=2015-09-10T15:59:53.734Z` và 96 active visitor ở cửa sổ cuối.
- Có artifact runtime cũ chứng minh smoke, chín test ingestion và ingest lần đầu đạt; chưa có bằng chứng ingest reuse lần hai trong phiên công cụ.
- Log người dùng ngày 17/10/2026: tám test chạy trong 131,866 giây, bảy test đạt, một test DLQ lỗi `NOT_LEADER_FOR_PARTITION` tại `config_for()` → `end_offsets()` trước khi kiểm tra DLQ. Test phục hồi checkpoint có fault injection đã restart và đạt.
- Artifact sau sửa Kafka xác nhận tám test streaming phiên bản trước đạt, cleanup thành công, quan sát RAM đầy đủ và không OOM/restart; fault checkpoint đã phục hồi. Phiên bản mới có mười test cần chạy lại.
- Đã thử `make test-behavior` sau triển khai; Docker socket bị từ chối trước khi chạy Spark. Chưa nghiệm thu session/funnel batch/streaming mới hoặc ba lượt test liên tiếp trong phiên công cụ.
- Kết quả đối soát CSV offline chỉ xác nhận nguồn và contract; không chứng minh Kafka, HDFS/MongoDB KPI, restart hoặc RAM thực tế của streaming.

Nguồn kỹ thuật: [Spark foreachBatch và checkpoint](https://spark.apache.org/docs/3.5.7/structured-streaming-programming-guide.html), [Kafka offsets khi restart Spark](https://spark.apache.org/docs/3.5.7/structured-streaming-kafka-integration.html), [MongoDB atomicity](https://www.mongodb.com/docs/manual/core/write-operations-atomicity/).
