# 05 — Dữ liệu dự đoán mua hàng

[← Mục lục ML](README.md) · [Huấn luyện →](06-random-forest.md)

## Mục đích và đơn vị dự đoán

Giai đoạn này học từ lịch sử để xếp hạng visitor có dấu hiệu mua trong **24 giờ sau thời điểm chốt**. Một dòng là `(as_of, visitor_id)`. Visitor có thể xuất hiện ở nhiều ngày, với feature và nhãn khác nhau. Kết quả phục vụ ưu tiên theo dõi hoặc lựa chọn nhóm thử nghiệm chăm sóc; chưa chứng minh tác động lên doanh thu.

Nguồn là Silver `events-v1` đã ingest thành công. Builder kiểm tra schema, identity, manifest, inventory/checksum và hai replica trước khi sử dụng. Dataset gốc và các pipeline ingestion, streaming, K-Means đã nghiệm thu được giữ nguyên. Hợp đồng mới là `purchase-data-v1`; code prediction được đóng gói riêng.

## Cửa sổ feature và nhãn

Với `t = as_of`, mọi cutoff huấn luyện đều ở `00:00:00.000Z`:

| Thành phần | Quy tắc |
| --- | --- |
| Cohort | Visitor có ít nhất một event trong `[t − 7 ngày, t)` |
| Feature | Chỉ dùng event trong đúng cửa sổ bảy ngày đó |
| Nhãn | `purchase_in_next_24h`, kiểu integer `0` hoặc `1` |
| Nhãn 1 | Visitor thuộc cohort có event `purchase`, `transaction_id` khác null, trong `[t, t + 24 giờ)` |
| Nhãn 0 | Visitor thuộc cohort không có event mua thỏa điều kiện trên |
| Người mới trong ngày nhãn | Không thêm vào cohort nếu không có event trong cửa sổ feature |
| Biên | Event đúng `t` chỉ có thể vào nhãn; event đúng `t + 24 giờ` không vào nhãn ngày này |

Builder yêu cầu nguồn quan sát đủ cả cửa sổ feature và ngày nhãn. Khoảng min/max event của Silver là bằng chứng giới hạn thời gian nguồn; điều này không tự chứng minh hệ thống thu thập ngoài đời không bỏ sót sự kiện.

Ví dụ chốt `2015-07-15T00:00:00.000Z`: feature dùng `[08/07 00:00, 15/07 00:00)` UTC; nhãn dùng `[15/07 00:00, 16/07 00:00)` UTC. Mua nhiều item hoặc nhiều transaction trong ngày nhãn vẫn chỉ tạo nhãn `1`.

## Tám feature đầu vào

Tái sử dụng nguyên hợp đồng [features-v1](01-features.md), đúng thứ tự:

| Thứ tự | Trường | Ý nghĩa |
| ---: | --- | --- |
| 1 | `view_count_7d` | Tổng lượt xem trong bảy ngày |
| 2 | `cart_count_7d` | Tổng lượt thêm giỏ |
| 3 | `transaction_count_7d` | Số transaction khác nhau của visitor |
| 4 | `unique_items_7d` | Số sản phẩm khác nhau đã tương tác |
| 5 | `active_days_7d` | Số ngày UTC có hoạt động |
| 6 | `recency_hours` | Số giờ từ event cuối trước cutoff đến cutoff |
| 7 | `cart_view_ratio_7d` | Cart/view; bằng 0 khi view bằng 0 |
| 8 | `transaction_view_ratio_7d` | Transaction/view; bằng 0 khi view bằng 0 |

Năm trường đầu là `bigint`, ba trường cuối là `double`. Tỷ lệ có thể lớn hơn 1. `visitor_id` là chuỗi chữ số và giữ leading zero; `001` khác `1`. `as_of`, `last_event_time` là timestamp audit. Ba trường audit, nhãn và cluster K-Means đều không vào vector feature. Random Forest dùng feature thô, không dùng log/scaler của K-Means.

## Chia dữ liệu theo thời gian

Lịch mặc định có **128 cutoff**, dùng toàn bộ phần lịch sử có thể quan sát đủ cửa sổ:

| Split | Cutoff đầu → cuối, bao gồm hai đầu | Số cutoff | Số mẫu | Mẫu nhãn 1 |
| --- | --- | ---: | ---: | ---: |
| Train | 11/05/2015 → 15/07/2015 | 66 | 5.196.014 | 1.750 |
| Validation | 17/07/2015 → 15/08/2015 | 30 | 2.560.155 | 729 |
| Test | 17/08/2015 → 17/09/2015 | 32 | 2.380.550 | 726 |
| Tổng | | 128 | 10.136.719 | 3.205 |

Cutoff ngày **16/07 và 16/08** bị loại để tạo khoảng nghỉ giữa các split. Event trong các ngày đó vẫn có thể thuộc cửa sổ feature của những cutoff sau. Không random split, không dùng test để chọn mô hình/ngưỡng. Cùng visitor có thể có mặt ở cả ba split; kết quả đo khả năng dự đoán thời gian tiếp theo trên cohort hoạt động, không phải đánh giá visitor hoàn toàn mới. Các cửa sổ feature liên tiếp có phần giao nhau, nên mẫu theo ngày không độc lập hoàn toàn.

Các số trên được đối chiếu độc lập bằng tập visitor theo ngày từ CSV: union bảy ngày trước cho cohort, giao với tập người mua ngày nhãn cho positive. Duplicate event không làm thay đổi các tập này. Artifact tham chiếu local là `artifacts/prediction/source-audit.json`, có `runtime_verified=false`. Chỉ báo cáo Spark build thực tế mới xác nhận pipeline tạo ra đúng dữ liệu. Với đúng dataset version `aef4779c2d9008f4a51f3d12f229c9dbb6718288ea4c380dbbebaa6b2eb24d2a` và lịch mặc định, finalizer từ chối khi các tổng không khớp bảng.

## Sampling chỉ áp dụng train

Giữ **tất cả positive train**, chọn đúng `min(số negative, 20 × số positive)` negative. Với nguồn hiện tại: 1.750 positive + 35.000 negative = **36.750 dòng để fit**. Validation và test giữ nguyên toàn bộ cohort, không downsample, không lấy tỷ lệ positive của train đã sampling làm baseline.

Mỗi negative có khóa SHA-256 của JSON array compact sau, UTF-8:

```json
["purchase-sampling-v1","<dataset_version>","<as_of UTC có milliseconds>","<visitor_id>","42"]
```

Sắp theo SHA tăng dần, rồi `as_of`, rồi visitor theo thứ tự chuỗi; lấy đúng số lượng cần thiết. Đây là sampling xác định, seed 42, không phụ thuộc thứ tự partition. Fingerprint kiểm tra số positive và số dòng sau khi lưu/tải lại sample. Hợp đồng sampling được gắn vào model identity.

## Snapshot và tiếp tục khi bị ngắt

Builder chia lịch thành **20 chặng**, mỗi chặng tối đa bảy cutoff và chỉ thuộc một split: 10 chặng train, 5 validation, 5 test. Các chặng chạy tuần tự. Mỗi ngày là một snapshot độc lập:

```text
/ecommerce/gold/purchase-data/<purchase_dataset_id>/
  days/YYYY-MM-DD/
    samples/          # Parquet: audit + tám feature + nhãn
    quality.json
    manifest.json
    _SUCCESS
  index/
    index.json        # Danh sách đủ 128 ngày + manifest hash + tổng từng split
    manifest.json
    _SUCCESS
```

Mỗi snapshot ghi vào staging riêng, kiểm tra inventory/checksum/hai replica rồi rename không ghi đè. Fingerprint yêu cầu khóa `(as_of, visitor_id)` duy nhất, sắp tăng dần, đúng nhãn, đúng feature và đúng tổng năm count. Index nằm trong thư mục riêng để việc kiểm tra inventory không lẫn các snapshot ngày.

Chỉ công bố index khi đủ toàn bộ ngày và cả ba split có hai lớp. Không có index hoàn chỉnh thì train từ chối. Khi bị ngắt, chạy lại cùng `make build-purchase-data`: ngày đã commit được kiểm tra và tái sử dụng; staging chưa commit không trở thành dữ liệu công bố. `progress.json` ở host ghi những chặng đã hoàn thành, còn HDFS là nguồn xác định snapshot nào thực sự tồn tại.

Đổi prediction code, nguồn hoặc lịch tạo identity mới; không tiếp tục ghép ngày thuộc các identity khác nhau. Snapshot đã công bố bị mất marker hoặc hỏng payload sẽ bị từ chối để điều tra/khôi phục, không tự ghi đè.
