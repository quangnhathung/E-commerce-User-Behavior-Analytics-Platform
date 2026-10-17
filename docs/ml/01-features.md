# 01 — Hợp đồng feature

[← Mục lục ML](README.md)

## Nguồn và thời điểm chốt

Nguồn là Silver `events-v1` đã chuẩn hóa, loại trùng theo event ID và lưu Parquet. Build xác minh identity, inventory/SHA-256, marker `_SUCCESS`, replica payload và schema typed Silver. Không suy luận lại schema từ CSV, không dùng dữ liệu replay trong bước này.

`features-v1` sử dụng khoảng **`[AS_OF − 7 ngày, AS_OF)`**. Biên trái được lấy; event đúng AS_OF và event tương lai bị loại. Mặc định:

```text
AS_OF  = 2015-07-15T00:00:00.000Z
start  = 2015-07-08T00:00:00.000Z
window = [start, AS_OF)
```

Tham số phải có dạng UTC với đúng ba chữ số millisecond. Bảy ngày phải nằm trong phạm vi quan sát nguồn: `start >= min_event_time` và `AS_OF <= max_event_time + 1 ms`. Không âm thầm rút ngắn cửa sổ. Cửa sổ không có visitor khiến build thất bại.

Spark lọc cả `event_date` để cắt phân vùng và `event_time` để kiểm tra chính xác hai biên. Session SQL phải UTC. Mỗi visitor có ít nhất một event trong cửa sổ tạo đúng một dòng; visitor không hoạt động trong cửa sổ không được tự bổ sung dòng toàn số 0.

## Tám cột đầu vào model

Thứ tự sau là hợp đồng cố định; thay đổi công thức/thứ tự cần phiên bản mới.

| Thứ tự | Feature | Kiểu Parquet | Cách tính và công dụng |
| ---: | --- | --- | --- |
| 1 | `view_count_7d` | bigint | Đếm event `view`; đo mức độ xem |
| 2 | `cart_count_7d` | bigint | Đếm event `add_to_cart`; đo hoạt động thêm giỏ |
| 3 | `transaction_count_7d` | bigint | Distinct `transaction_id` khác null trên event `purchase`; đo giao dịch thay vì số dòng mua |
| 4 | `unique_items_7d` | bigint | Distinct `item_id` trên mọi loại event; đo độ rộng tương tác |
| 5 | `active_days_7d` | bigint | Distinct ngày UTC có event; từ 1 đến 7 ở cutoff nửa đêm, có thể 8 khi cửa sổ chạm tám ngày lịch |
| 6 | `recency_hours` | double | `(AS_OF − last_event_time)` theo millisecond chia 3.600.000; đo thời gian từ tương tác cuối |
| 7 | `cart_view_ratio_7d` | double | `cart_count_7d / view_count_7d`; trả 0 khi không có view |
| 8 | `transaction_view_ratio_7d` | double | `transaction_count_7d / view_count_7d`; trả 0 khi không có view |

Distinct tính trên toàn cửa sổ của từng visitor. Không cộng distinct theo ngày. Một transaction mua hai sản phẩm trên nhiều dòng vẫn chỉ tạo một giao dịch. Transaction ID xuất hiện trên event khác `purchase` không được tính.

Cửa sổ luôn dài đúng 168 giờ. Nếu AS_OF là 12:00 UTC, khoảng từ 12:00 bảy ngày trước đến 12:00 hôm nay chạm tám ngày lịch UTC; vì vậy `active_days_7d=8` hợp lệ trong trường hợp này. Cutoff mặc định 00:00 có tối đa bảy ngày lịch.

Hai ratio có thể lớn hơn 1, vì đây là tỷ lệ giữa số đếm tương tác. Chúng khác tỷ lệ chuyển đổi funnel có yêu cầu thứ tự/cùng sản phẩm trong `docs/analytics/`. Giá trị 0 khi thiếu view là quy ước số học, không chứng minh visitor không có ý định mua; các count gốc vẫn nằm trong vector.

Ba cột audit không nằm trong vector: `visitor_id` string, `as_of` timestamp, `last_event_time` timestamp. Giữ nguyên ID dạng chuỗi, kể cả số 0 đầu. Không biến ID, thời gian tuyệt đối hoặc dữ liệu tương lai thành input ML.

## Kiểm tra chất lượng và fingerprint

Build kiểm tra đúng kiểu cột, AS_OF thống nhất, visitor duy nhất, count nguyên không âm, activity hợp lệ, last event nằm trong cửa sổ và derived feature hữu hạn/khớp công thức trong sai số tuyệt đối `1e-12`. Tổng views/carts phải đối soát với event được chọn. Report giữ thêm số purchase event, visitor không view, visitor có transaction và recency lớn nhất.

Fingerprint sắp visitor theo thứ tự chuỗi. Mỗi dòng canonical là JSON array gồm visitor, AS_OF, last event và năm count nguyên, sau đó newline, băm SHA-256 tăng dần. Derived doubles được kiểm tra rồi đại diện bằng các đầu vào chính xác của công thức để tránh khác biệt biểu diễn float giữa Python/Spark/Mongo. Fingerprint assignment nối thêm cluster ID vào cùng array. Không đưa toàn cohort vào `collect()` hoặc pandas; iterator đọc từng phân vùng đã sắp.

Oracle Python trong `contracts.reference` chỉ dành cho fixture nhỏ, giới hạn 100.000 event. Job lịch sử dùng Spark, không dùng oracle để gom lịch sử vào RAM driver.

## Tham chiếu offline cho nguồn hiện tại

Với dataset version `aef4779c2d9008f4a51f3d12f229c9dbb6718288ea4c380dbbebaa6b2eb24d2a` và AS_OF mặc định, đối soát CSV sau loại trùng có:

| Chỉ số | Kỳ vọng offline |
| --- | ---: |
| Event duy nhất trong cửa sổ | 168.073 |
| Visitor / dòng feature | 99.038 |
| View | 162.779 |
| Add to cart | 4.011 |
| Purchase event | 1.283 |
| Tổng distinct transaction theo visitor | 980 |
| Tổng unique item theo visitor | 136.776 |
| Tổng active day theo visitor | 104.481 |
| Visitor không có view | 192 |
| Visitor có transaction | 687 |
| Recency lớn nhất, giờ | 167,9964286111111 |

Các giá trị này chỉ áp dụng cho nguồn và cửa sổ trên. Tổng transaction theo visitor nói chung có thể khác global distinct transaction; trên cửa sổ hiện tại cả hai cùng bằng 980. Chưa có kết quả Spark feature mới để tuyên bố runtime đã khớp.

Đối soát offline trong lượt triển khai đã lưu tại `artifacts/ml/source-audit.json`, với `runtime_verified=false`. Feature fingerprint theo hợp đồng canonical ở trên là `43dc8b700488e0a2588dcdc9216d39b3c4fdf9d4ab3edc0441edb9049bff605c`. CSV nguồn có SHA-256 `3745aa83238b1e6d44d8fda209807899f420084398f94ddf745f3cbcfecbf9e7`.
