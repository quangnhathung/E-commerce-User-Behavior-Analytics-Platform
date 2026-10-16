# 01 — Data dictionary

[← Mục lục dữ liệu](README.md)

Nguồn mặc định là `Dataset/retailrocket-recommender-system-dataset`, bind mount read-only tại `/opt/dataset` trong Spark client. Đổi vị trí host bằng `DATASET_PATH`; không đóng dataset vào image.

## 1. Bốn file nguồn

| File | Kích thước byte đã khảo sát | Mục đích trong giai đoạn này |
| --- | ---: | --- |
| `events.csv` | 94.237.913 | Hành vi xem, thêm giỏ và mua; tạo Silver |
| `category_tree.csv` | 14.454 | Cây danh mục; giữ nguyên trong Bronze |
| `item_properties_part1.csv` | 484.315.749 | Lịch sử thuộc tính sản phẩm; giữ nguyên trong Bronze |
| `item_properties_part2.csv` | 408.929.907 | Phần còn lại của thuộc tính; giữ nguyên trong Bronze |

Fingerprint thực tế được ghi trong manifest, không dựa vào kích thước trong bảng để quyết định tái sử dụng dữ liệu.

## 2. `events.csv`

Header phải đúng cả tên và thứ tự: `timestamp,visitorid,event,itemid,transactionid`.

| Cột nguồn | Ý nghĩa | Quy tắc |
| --- | --- | --- |
| `timestamp` | Thời điểm hành vi, Unix milliseconds | Chuỗi chữ số không âm; đổi sang UTC bằng số nguyên; tối đa `253402300799999` (giới hạn datetime năm 9999) |
| `visitorid` | Visitor ẩn danh | Bắt buộc; chữ số không âm; giữ kiểu string và số 0 đầu |
| `event` | Hành vi nguồn | Chỉ `view`, `addtocart`, `transaction` |
| `itemid` | Sản phẩm | Bắt buộc; chữ số không âm; giữ kiểu string |
| `transactionid` | Mã giao dịch | Purchase bắt buộc có; nếu có thì phải là chữ số; rỗng chuyển null |

Không trim hoặc tự sửa giá trị trường. Một dòng CSV là một sự kiện; v1 không hỗ trợ một sự kiện có field kéo dài qua nhiều dòng vật lý. Parser hỗ trợ quoting CSV trên một dòng và kiểm tra đúng năm trường, gồm trường giao dịch trống cuối dòng.

Ví dụ `1430622004384` tương ứng `2015-05-03T03:00:04.384Z`. Input luôn được hiểu là milliseconds; pipeline không tự đoán rồi đổi giây thành milliseconds.

ID `0` hợp lệ. Hai trường `"001"` và `"1"` được giữ khác nhau. Visitor là định danh ẩn danh trong dataset, không suy ra tên hoặc thông tin cá nhân.

## 3. Cây danh mục và thuộc tính

| File | Header | Ý nghĩa |
| --- | --- | --- |
| `category_tree.csv` | `categoryid,parentid` | Danh mục và danh mục cha; parent rỗng có thể là gốc |
| Hai file thuộc tính | `timestamp,itemid,property,value` | Thuộc tính một item tại thời điểm quan sát |

Các giá trị thuộc tính được ẩn danh nhiều phần. Giai đoạn này không suy luận giá bán/doanh thu, không join thuộc tính để tạo feature và không kiểm tra toàn bộ nội dung hai file thuộc tính.

## 4. Phân biệt khi diễn giải

- Một event `transaction` là một dòng hành vi mua, có thể tương ứng một item trong một đơn nhiều item.
- Số giao dịch được tính bằng transaction ID khác nhau, không bằng số event mua.
- Dữ liệu nguồn chưa được sắp theo thời gian. Silver được phân vùng ngày; thứ tự replay sẽ được thiết lập ở giai đoạn sau.
- Trùng được định nghĩa bằng cùng dataset version và cùng năm trường đã parse từ CSV; khác vị trí dòng không tạo thêm event.
- `events.csv` không cung cấp bước bắt đầu thanh toán, doanh thu đáng tin cậy hoặc nhãn churn.

Đọc tiếp [schema và lưu trữ](02-contract-and-storage.md).
