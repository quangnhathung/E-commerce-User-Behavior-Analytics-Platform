# Kịch bản demo live 3 phút

## 1. Tình trạng đã kiểm tra

Dashboard đang phục vụ tại:

```text
http://127.0.0.1:5500/web/index.html
```

API local đang sẵn sàng:

```json
{ "status": "ready", "mongodb": true }
```

Các phiên live hiện có:

- `demo-live-002`: trạng thái `stale`, không dùng để demo.
- `demo-live-001`: `failed`, không dùng.
- `live-check-002`: `completed`, 240 event đã ACK và 240 event đã xử lý.
- `demo-live-002-000001`: snapshot hoàn tất, phù hợp để trình bày số liệu.

Không chọn run con `demo-live-002-000002` vì hiện tại chưa có batch được commit.

## 2. Phương án demo khuyến nghị

Dùng snapshot:

```text
demo-live-002-000001
```

Batch hiện tại: `15`.

Số liệu thực tế:

- 1.000 event được xử lý.
- 200 visitor.
- 895 lượt xem.
- 82 lượt thêm giỏ.
- 23 transaction.
- 13 buyer.
- Buyer rate: 6,5%.
- Funnel: 200 phiên xem → 57 phiên thêm giỏ → 13 phiên mua.
- View → cart: 28,5%.
- Cart → purchase: 22,8%.
- K-Means: 2 cluster, lần lượt 108 và 92 visitor.
- Backlog: 0.
- Spark worker: 2 worker.
- Latency p95: 42,269 giây.

## 3. Chuẩn bị trước demo

### 3.1. Mở API

Tại thư mục gốc dự án, mở Terminal A:

```bash
make api
```

Kiểm tra API:

```bash
curl -sS http://127.0.0.1:8000/api/v1/health
```

Kết quả cần có:

```json
{ "status": "ready", "mongodb": true }
```

### 3.2. Mở Dashboard

Trong VS Code:

1. Mở thư mục gốc dự án.
2. Mở file `web/index.html`.
3. Chuột phải → **Open with Live Server**.
4. Mở URL:

```text
http://127.0.0.1:5500/web/index.html
```

Trên Dashboard:

- Chọn **LƯỢT DỮ LIỆU**: `demo-live-002-000001`.
- Bật **Tự cập nhật KPI**.
- Nếu Dashboard tự chuyển sang run live khác, tắt **Theo dõi live**.
- Giữ cửa sổ ở màn hình **Tổng quan**.
- Thu gọn bảng event nếu bảng này che mất KPI.

### 3.3. Theo dõi trạng thái live tùy chọn

Mở Terminal B:

```bash
make live-status LIVE_ID=live-check-002
```

Không cần mở Terminal B trong lúc trình bày nếu Dashboard đã hiển thị feed đầy đủ.

## 4. Nếu cần chạy một live session mới

Không khởi động live session mới trong đúng 3 phút trình bày. Chạy trước buổi demo ít nhất 5 phút bằng một `LIVE_ID` mới:

```bash
make live-demo \
  LIVE_ID=onboarding-live-003 \
  RATE=10 \
  LIVE_EVENTS=120 \
  LIVE_VISITORS=24 \
  LIVE_ITEMS=12 \
  LIVE_SEGMENTS=2
```

Sau đó trên Dashboard:

1. Mở **Phiên live**.
2. Chọn `onboarding-live-003`.
3. Bật **Theo dõi live**.
4. Bật **Tự cập nhật KPI**.

Khi cần quay về snapshot ổn định, tắt **Theo dõi live** rồi chọn `demo-live-002-000001`.

## 5. Kịch bản nói và thao tác trong 3 phút

| Thời gian | Thao tác                                                         | Nội dung nói                                                                                                                                                                                                              |
| --------- | ---------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 0:00–0:15 | Mở Dashboard, chỉ vào tên hệ thống                               | “Đây là nền tảng phân tích hành vi người dùng thương mại điện tử. Hệ thống nhận event xem sản phẩm, thêm giỏ và mua hàng; xử lý qua Kafka và Spark; sau đó dùng K-Means và Random Forest để tạo insight.”                 |
| 0:15–0:30 | Chọn `demo-live-002-000001`, chỉ vào Batch 15                    | “Đây là snapshot đã được Spark commit. Dashboard đọc cùng một run và cùng một batch nên KPI, funnel và prediction được đồng bộ.”                                                                                          |
| 0:30–1:05 | Ở **Tổng quan**, chỉ vào KPI                                     | “Run này đã xử lý 1.000 event của 200 visitor, tạo ra 895 lượt xem, 82 lượt thêm giỏ, 23 transaction và 13 buyer. Buyer rate là 6,5%.”                                                                                    |
| 1:05–1:25 | Chỉ vào biểu đồ và funnel                                        | “Funnel cho thấy 200 phiên có xem, 57 phiên đi tới bước thêm giỏ và 13 phiên đi tới bước mua. Tỷ lệ view-to-cart là 28,5%, còn cart-to-purchase là 22,8%.”                                                                |
| 1:25–1:55 | Bấm **Phân khúc & dự đoán**, chỉ vào biểu đồ cluster             | “Từ feature hành vi bảy ngày, K-Means chia visitor thành hai nhóm. Nhóm thứ nhất có 108 visitor, nhóm thứ hai có 92 visitor. Các feature gồm lượt xem, thêm giỏ, giao dịch, số sản phẩm khác nhau và độ gần của hành vi.” |
| 1:55–2:15 | Chỉ vào bảng prediction và threshold                             | “Random Forest tính điểm mua hàng cho từng visitor. Policy đang dùng ngưỡng 0,96 để ưu tiên precision. Đây là điểm xếp hạng phục vụ remarketing, không phải xác suất đã hiệu chuẩn.”                                      |
| 2:15–2:40 | Bấm **Theo dõi pipeline**, chỉ vào Producer, Consumer và Scoring | “Producer đưa event vào Kafka, Spark Consumer xử lý theo micro-batch, sau đó Scoring cập nhật feature, cluster và prediction.”                                                                                            |
| 2:40–2:52 | Chỉ vào backlog, counts và worker                                | “Backlog bằng 0 nghĩa là các event đã được Kafka xác nhận trong run này đã được Spark xử lý và commit. Run không có event trùng hoặc event không hợp lệ; hệ thống sử dụng hai Spark worker.”                              |
| 2:52–3:00 | Quay lại Tổng quan hoặc chỉ vào live feed                        | “Như vậy hệ thống đã hoàn thành chuỗi từ event streaming đến KPI, funnel, phân khúc và dự đoán mua hàng trên một Dashboard thống nhất.”                                                                                   |

## 6. Nhánh minh họa live feed

Nếu phiên `live-check-002` hoặc phiên mới đang hiển thị trên Dashboard:

1. Mở **Phiên live**.
2. Chọn phiên đang chạy hoặc đã hoàn tất.
3. Bật **Theo dõi live**.
4. Chỉ vào hai trạng thái trong feed:
   - `Kafka đã ACK · chờ Spark`.
   - `Spark đã xử lý`.

Nói:

> “Feed live cập nhật event sau khi Kafka ACK. Khi Spark commit batch, event được đánh dấu đã xử lý. Vì vậy hệ thống phân biệt được event đã vào Kafka với event đã thực sự hoàn tất qua Spark.”

Nếu feed không hiển thị, bỏ qua nhánh này và tiếp tục phần Pipeline.

## 7. Tiêu chí demo đạt

- Dashboard mở được qua Live Server.
- API hiển thị `status=ready`.
- Run được chọn là run completed.
- KPI, funnel, prediction và pipeline cùng hiển thị cùng Batch 15.
- Backlog hiển thị 0.
- Không trình bày `demo-live-002-000002`, `demo-live-001` hoặc bất kỳ run nào có trạng thái `stale`/`failed`.
- Không chạy lại ingestion, train model hoặc replay trong lúc chấm.

## 8. Câu trả lời khi bị hỏi

### Kafka có vai trò gì?

“Kafka là event log trung gian, giữ offset và tách producer khỏi consumer.”

### Spark có vai trò gì?

“Spark Structured Streaming xử lý event theo micro-batch, checkpoint offset và công bố snapshot KPI/scoring.”

### Tại sao funnel và buyer rate khác nhau?

“Buyer rate tính theo visitor; funnel tính theo session và sản phẩm nên hai chỉ số có đơn vị khác nhau.”

### Backlog bằng 0 nghĩa là gì?

“Các event đã ACK trong run đã được Spark xử lý và commit; không đồng nghĩa toàn bộ dataset lịch sử đã xử lý.”

### Hạn chế của hệ thống?

“Live event dùng cửa sổ nghiệp vụ mô phỏng từ dataset RetailRocket. Prediction score là điểm xếp hạng; policy metrics là đánh giá trên dữ liệu đã quan sát.”
