# 04 — Chất lượng dữ liệu và đối soát

[← Mục lục dữ liệu](README.md)

## 1. Cách phân loại

Mỗi dòng sau header thuộc đúng một nhóm khi đối soát số lượng:

```text
source_rows = silver_rows + duplicate_rows + invalid_rows
valid_rows_before_dedup = silver_rows + duplicate_rows
```

`duplicate_rows` là số dòng dư bị loại, không phải số nhóm ID trùng. Một ID có ba dòng tạo một Silver row, một duplicate group và hai duplicate rows.

Header đầu tiên không được tính. Dòng trống giữa file được tính là dòng lỗi. Một dòng có thể mang nhiều mã lỗi nên tổng `error_counts` có thể lớn hơn `invalid_rows`.

## 2. Mã lỗi

| Mã | Ý nghĩa |
| --- | --- |
| `MALFORMED_CSV` | Parser không đọc được một dòng CSV hợp lệ |
| `FIELD_COUNT_MISMATCH` | Không có đúng năm trường, kể cả trường trống cuối dòng |
| `INVALID_TIMESTAMP` | Timestamp sai định dạng, âm hoặc ngoài giới hạn hỗ trợ |
| `MISSING_VISITOR_ID`, `MISSING_ITEM_ID` | Thiếu ID bắt buộc |
| `INVALID_VISITOR_ID`, `INVALID_ITEM_ID` | ID không phải chuỗi chữ số không âm |
| `UNKNOWN_EVENT_TYPE` | Loại hành vi không nằm trong ba giá trị nguồn |
| `MISSING_TRANSACTION_ID` | Event mua không có mã giao dịch |
| `INVALID_TRANSACTION_ID` | Mã giao dịch có giá trị nhưng sai định dạng |

Dòng sai CSV/số cột được cách ly ngay, không đoán lại vị trí trường. Không tự sửa timestamp, bổ sung ID hoặc biến hành vi lạ thành `view`.

## 3. Báo cáo `quality.json`

- Version nguồn, schema/transform version và `validation_scope=events.csv`.
- Số dòng nguồn, hợp lệ trước dedup, Silver, trùng và lỗi.
- `source_event_counts`: nhóm hành vi đã parse trước loại lỗi/trùng; dòng không xác định được nhóm vào `unknown`.
- `silver_event_counts`: số hành vi trên Silver sau làm sạch.
- Số visitor, item, buyer và transaction khác nhau trên Silver; transaction null không được tính.
- Khoảng UTC nhỏ nhất/lớn nhất và số ngày có event.
- `error_counts`, `worker_rows` từ hostname thực sự parse dữ liệu, thời gian transform.

Đường dẫn, file inventory/checksum, thời điểm công bố và minimum block replicas nằm trong manifest đi cùng. `passed` nghĩa không có dòng lỗi theo hợp đồng v1; `warning` nghĩa phần hợp lệ đã được xuất và có dòng cách ly. Duplicate được thống kê riêng và không tự làm trạng thái thành warning.

Đây là thống kê chất lượng dữ liệu, chưa phải KPI dashboard hay funnel có thứ tự. Không diễn giải số transaction như doanh thu.

## 4. Baseline nguồn đã khảo sát

Checksum SHA-256 của `events.csv`:

```text
3745aa83238b1e6d44d8fda209807899f420084398f94ddf745f3cbcfecbf9e7
```

| Chỉ số | Kỳ vọng với nguồn này |
| --- | ---: |
| Dòng dữ liệu nguồn | 2.756.101 |
| View nguồn | 2.664.312 |
| Add to cart nguồn | 69.332 |
| Purchase event nguồn | 22.457 |
| Dòng lỗi theo hợp đồng v1 | 0 |
| Dòng dư trùng bị loại | 460 |
| Silver rows | 2.755.641 |

Khoảng thời gian: `2015-05-03T03:00:04.384Z` đến `2015-09-18T02:59:47.788Z`. Số hành vi nguồn trong bảng là trước dedup; số Silver theo từng hành vi do job tính và báo riêng.

Một lượt đối soát offline bằng normalizer thật và SQLite tạm đã kiểm tra toàn bộ `events.csv`, khớp 2.756.101 nguồn / 460 trùng / 0 lỗi / 2.755.641 unique. Báo cáo local `artifacts/data/source-audit.json` ghi rõ `hdfs_runtime_verified=false`. Nó không chứng minh Parquet hoặc replication đã được tạo.

Nếu nguồn thay đổi, tính lại version và báo số thực tế; không lọc thêm dữ liệu để ép về baseline. Snapshot mỗi version được đọc riêng, không tự cộng nhiều version của cùng dataset.
