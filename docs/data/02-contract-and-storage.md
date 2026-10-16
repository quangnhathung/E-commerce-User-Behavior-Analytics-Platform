# 02 — Hợp đồng dữ liệu và lưu trữ

[← Mục lục dữ liệu](README.md)

Nguồn quy tắc: [contracts.py](../../app/ecommerce_analytics/contracts.py), [ingestion.py](../../app/ecommerce_analytics/ingestion.py), [snapshots.py](../../app/ecommerce_analytics/snapshots.py), [HDFS adapter](../../app/ecommerce_analytics/hdfs.py).

## 1. Phiên bản nguồn và ID

Mỗi file có `{name, size, sha256}`. Sắp danh sách theo `name`, serialize JSON với `sort_keys=True`, `separators=(',', ':')`, `ensure_ascii=False`, rồi SHA-256 chuỗi UTF-8 đó để tạo `dataset_version`.

Đường dẫn máy, mtime và thời điểm ingest không tham gia định danh. Cả bốn file đều tham gia; thay đổi file thuộc tính cũng tạo phiên bản nguồn mới.

`event_id` là SHA-256 của JSON array, theo cùng cách serialize:

```text
[1, "retailrocket", dataset_version,
 timestamp, visitorid, event, itemid, transactionid]
```

Năm trường nguồn là string gốc sau parse CSV; trường transaction trống vẫn là `""` khi tính ID. Event type được chuẩn hóa sau khi định nghĩa đầu vào hash. ID không chứa số dòng, hostname worker hoặc thời điểm chạy. CSV quoting khác nhau nhưng tạo cùng năm giá trị đã parse sẽ có cùng ID.

Version hiện tại của nguồn đã khảo sát:

```text
aef4779c2d9008f4a51f3d12f229c9dbb6718288ea4c380dbbebaa6b2eb24d2a
```

## 2. Schema Silver

| Cột | Kiểu Spark | Giá trị/công dụng |
| --- | --- | --- |
| `schema_version` | integer | `1` |
| `dataset_version` | string | Fingerprint bốn file |
| `source` | string | `retailrocket` |
| `run_id` | string | `history-` + dataset version |
| `event_id` | string | SHA-256 xác định |
| `visitor_id`, `item_id` | string | ID giữ nguyên biểu diễn |
| `event_type` | string | `view`, `add_to_cart`, `purchase` |
| `transaction_id` | string, nullable | Rỗng ở nguồn thành null |
| `event_time` | timestamp | Thời gian UTC, giữ milliseconds |
| `event_date` | date | Ngày UTC, cột partition |
| `emitted_at` | timestamp, nullable | Null cho lịch sử; không bịa thời điểm phát event |
| `source_file` | string | `events.csv` |
| `source_row_number` | long | Dòng vật lý, header là dòng 1, event đầu tiên là dòng 2 |

Giữ dòng hợp lệ sớm nhất cho mỗi event ID. Ghi Parquet Snappy, `repartition(4, event_date)` và `partitionBy(event_date)`. Không gom toàn bộ dữ liệu thành một file bằng `coalesce(1)`.

## 3. Layout HDFS

```text
/ecommerce/bronze/retailrocket/<dataset_version>/
  category_tree.csv
  events.csv
  item_properties_part1.csv
  item_properties_part2.csv
  manifest.json
  _SUCCESS

/ecommerce/silver/retailrocket/<dataset_version>/events-v1/
  events/event_date=YYYY-MM-DD/part-*.snappy.parquet
  quarantine/part-*.snappy.parquet
  duplicates/part-*.snappy.parquet
  quality.json
  manifest.json
  _SUCCESS
```

Consumer phải đọc đúng `events/`, không đọc toàn bộ snapshot root như một dataset Parquet. Vị trí này có cả JSON và các dataset kiểm toán khác.

Quarantine chứa `source_file`, `source_row_number`, `raw_line`, `error_codes`. Nội dung nguyên bản theo byte luôn còn ở Bronze. Duplicate audit chứa `event_id`, `occurrences`, `first_source_row_number`.

## 4. Manifest và công bố

Manifest version 1 ghi identity, thời điểm tạo UTC, snapshot path, minimum block replicas, danh sách payload `{name,size,sha256}` và metadata. Silver metadata còn chứa schema và bản sao báo cáo chất lượng. Manifest và các marker `_SUCCESS` không tự đưa vào inventory checksum để tránh vòng tham chiếu; payload CSV, Parquet và `quality.json` được kiểm tra.

Identity Bronze gồm kind/source/dataset version. Identity Silver thêm schema version, transform version `events-v1` và SHA-256 archive mã ứng dụng. Archive ZIP có tên file và metadata cố định để thay đổi mtime không làm đổi code hash.

Quy trình:

1. Tạo staging `.tên-snapshot.tmp-<UUID>` cùng filesystem với đích.
2. Copy/ghi dữ liệu, đối soát và tính SHA-256 bằng stream có buffer giới hạn.
3. Đợi payload đạt ít nhất hai replica, tối đa 90 giây.
4. Ghi manifest và marker hoàn tất, đóng stream.
5. Dùng Hadoop FileContext rename không overwrite để công bố. Đích có sẵn làm rename thất bại, không lồng staging vào đích.

Bronze được công bố trước Silver; không giả định có transaction chung giữa hai snapshot. Chạy lại kiểm tra marker, identity và checksum trước khi tái sử dụng; Silver còn được đọc lại để kiểm tra schema, số event, quarantine và duplicate audit. Kết quả không khớp làm job thất bại, không tự sửa/xóa snapshot đã công bố.

Thay đổi code ứng dụng làm code hash thay đổi. Khi đã có kết quả dưới `events-v1`, thay đổi chuyển đổi phải tăng `TRANSFORM_VERSION` trong contracts để xuất phiên bản mới; không tắt kiểm tra hash để ghi đè. Đổi nguồn tạo dataset version mới tự động.

Nguồn kỹ thuật: [Hadoop FileContext rename](https://hadoop.apache.org/docs/r3.4.2/api/org/apache/hadoop/fs/FileContext.html), [Py4J byte array](https://www.py4j.org/advanced_topics.html#byte-array-byte), [Spark Python dependency distribution](https://spark.apache.org/docs/3.5.7/submitting-applications.html).
