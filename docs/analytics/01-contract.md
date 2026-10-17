# 01 — Hợp đồng session và funnel

[← Mục lục](README.md)

## 1. Nguồn và phạm vi

`behavior-v1` nhận event Silver đã hợp lệ và loại trùng. ID visitor, item và event giữ nguyên dạng string; thời gian là UTC, chính xác milliseconds. Bộ tính không tự thêm bước thiếu hoặc nạp event từ audit/DLQ.

- Lịch sử: toàn bộ snapshot Silver `events-v1`, bao gồm các ngày liên tiếp.
- Streaming: toàn bộ event accepted của một run, tối đa 10.000 event; không ghép sự kiện lịch sử ngoài `[START, END)`.
- Duplicate, invalid, late bị loại và foreign không tham gia funnel. Event đến muộn nhưng được validator chấp nhận vẫn tham gia.

Phạm vi khác nhau có thể có ranh giới phiên khác nhau. Không so trực tiếp session ID của lịch sử đầy đủ với session ID của đoạn replay. Các phiên ở biên nguồn có thể chỉ là một phần của hành trình thực tế; `is_final=true` nghĩa là hoàn tất cho nguồn đã quan sát, không chứng minh khách đã kết thúc mọi hoạt động.

## 2. Session

Sắp event của cùng visitor theo `(event_time, event_id)`. Bắt đầu phiên mới nếu chưa có event trước hoặc khoảng cách tới event trước **lớn hơn 1.800.000 milliseconds**. Khoảng cách đúng 30 phút vẫn cùng phiên. Session có thể qua nửa đêm; ngày phân vùng là ngày UTC bắt đầu phiên.

`session_id = SHA256(JSON canonical [behavior_version, scope_id, visitor_id, first_event_id])`. Event ID phân xử trường hợp timestamp trùng để kết quả xác định; nó không chứng minh thứ tự chuyển đổi.

Streaming tính lại trên toàn bộ Silver đã nhận sau mỗi batch. Event đến muộn có thể đổi event đầu, nối hai phiên và đổi session ID. Vì vậy không cộng dồn session theo batch và không dùng ID của snapshot tạm tính như khóa bất biến xuyên mọi batch.

## 3. Chuỗi chuyển đổi

Với mỗi session và sản phẩm:

1. `view_at`: thời điểm xem sớm nhất.
2. `cart_at`: thời điểm thêm giỏ sớm nhất thỏa `cart_at > view_at`.
3. `purchase_at`: thời điểm mua sớm nhất thỏa `purchase_at > cart_at`.

Có thể có sự kiện khác xen giữa. Chỉ cần tồn tại chuỗi đúng thứ tự; không yêu cầu các sự kiện kề nhau. Các bước có timestamp bằng nhau không tạo chuỗi hợp lệ.

Mỗi session–sản phẩm được đếm tối đa một lần ở mỗi bước. Funnel tổng quan đếm một session tối đa một lần tại mỗi bước nếu có ít nhất một sản phẩm đạt bước đó. Bước mua phải đến từ chuỗi hoàn chỉnh của cùng sản phẩm, không ghép xem item A với giỏ item B.

Ví dụ một phiên hoàn thành chuỗi cho cả A và B: tổng quan có một phiên mua, tổng funnel hai sản phẩm có hai. Vì thế không cộng funnel sản phẩm để suy ra tổng quan. Phiên chỉ có purchase vẫn tồn tại trong bảng session và KPI mua, nhưng có ba bước funnel bằng 0.

## 4. Trường kết quả

| Trường | Ý nghĩa |
| --- | --- |
| `event_count` | Sự kiện trong phạm vi; theo sản phẩm chỉ đếm sự kiện của sản phẩm đó |
| `total_sessions` | Tổng phiên; theo sản phẩm là số phiên có tương tác với sản phẩm |
| `view_sessions` | Phiên có bước xem |
| `cart_sessions` | Phiên có xem rồi thêm giỏ hợp lệ |
| `purchase_sessions` | Phiên có chuỗi xem → giỏ → mua hợp lệ |
| `view_to_cart_rate` | cart_sessions / view_sessions |
| `cart_to_purchase_rate` | purchase_sessions / cart_sessions |
| `view_to_purchase_rate` | purchase_sessions / view_sessions |
| `view_drop_count`, `cart_drop_count` | view − cart; cart − purchase |
| `view_drop_rate`, `cart_drop_rate` | Số rơi chia cho số phiên tại bước đầu tương ứng |

Tỷ lệ dùng 0..1, mẫu số 0 trả 0. Bất biến: `purchase ≤ cart ≤ view ≤ total_sessions ≤ event_count`.

Overview thêm `behavior_version`, `scope_id`, `product_count`, `products_sha256`. Fingerprint được tính trên từng dòng `[item_id, event_count, total_sessions, view_sessions, cart_sessions, purchase_sessions]` dạng JSON canonical, sắp item ID theo thứ tự chuỗi. Tỷ lệ được kiểm tra lại theo counts với sai số tuyệt đối tối đa `1e-12`, không đưa định dạng float vào fingerprint.

`is_final` nằm trong envelope truy vấn: lịch sử chỉ trả kết quả đã published; streaming lấy trạng thái completed từ registry cùng snapshot. Không sửa document funnel đã công bố để đổi cờ này.
