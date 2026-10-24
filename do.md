# Kịch bản demo live 3 phút

## 1. Kết quả phiên live đã xác nhận

Phiên sử dụng cho demo:

```text
LIVE_ID: onboarding-live-003
```

Kết quả tổng phiên:

- Trạng thái: `completed`.
- Số lượt hoàn tất: `2`.
- Tổng event đã ACK: `240`.
- Tổng event đã Spark xử lý: `240`.
- Backlog cuối phiên: `0`.
- Không có OOM hoặc container restart.
- Quan sát tài nguyên đầy đủ.

Artifact xác nhận:

```text
artifacts/live/onboarding-live-003/report.json
artifacts/live/onboarding-live-003/resources.json
```

Run dùng để trình bày trên Dashboard:

```text
onboarding-live-003-000002
```

Batch: `2`.

## 2. Số liệu thực tế của snapshot dùng để demo

Snapshot `onboarding-live-003-000002` có:

- 120 event hợp lệ.
- 24 visitor.
- 100 lượt xem.
- 16 lượt thêm giỏ.
- 4 purchase event.
- 4 transaction khác nhau.
- 3 buyer.
- Buyer rate: 12,5%.
- Funnel: 24 phiên xem → 11 phiên thêm giỏ → 3 phiên mua.
- View → cart: 45,83%.
- Cart → purchase: 27,27%.
- K-Means: 2 cluster, lần lượt 15 và 9 visitor.
- Backlog: 0.
- Accepted: 120.
- Duplicate, invalid, late, foreign: đều bằng 0.
- Spark worker: `spark-worker-1`, `spark-worker-2`.
- Latency p95: 27,881 giây.
- Batch cuối xử lý trong 13,045 giây.
- Policy threshold: 0,96.
- Prediction trong snapshot: 24 visitor được scoring; không visitor nào vượt threshold policy.

Lưu ý: 120 event và các KPI trên là của run con `-000002`. Toàn bộ phiên live có 240 event vì gồm hai run con, `-000001` và `-000002`.

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

- Chọn **LƯỢT DỮ LIỆU**: `onboarding-live-003-000002`.
- Bật **Tự cập nhật KPI**.
- Nếu Dashboard tự chuyển sang run live khác, tắt **Theo dõi live**.
- Giữ cửa sổ ở màn hình **Tổng quan**.
- Thu gọn bảng event nếu bảng này che mất KPI.

### 3.3. Theo dõi trạng thái live tùy chọn

Mở Terminal B:

```bash
make live-status LIVE_ID=onboarding-live-003
```

Không cần mở Terminal B trong lúc trình bày nếu Dashboard đã hiển thị feed đầy đủ.

## 4. Cách chạy lại một phiên tương đương

Không chạy lại `onboarding-live-003` vì phiên này đã có archive hoàn tất. Muốn tạo phiên mới, dùng một `LIVE_ID` chưa tồn tại và chạy trước buổi demo ít nhất 5 phút:

```bash
make live-demo \
  LIVE_ID=onboarding-live-005 \
  RATE=5 \
  LIVE_EVENTS=120 \
  LIVE_VISITORS=24 \
  LIVE_ITEMS=12 \
  LIVE_SEGMENTS=2
```

Sau đó trên Dashboard:

1. Mở **Phiên live**.
2. Chọn `onboarding-live-005`.
3. Bật **Theo dõi live**.
4. Bật **Tự cập nhật KPI**.

Khi cần quay về snapshot ổn định, tắt **Theo dõi live** rồi chọn `onboarding-live-003-000002`.

## 5. Kịch bản nói và thao tác trong 3 phút

| Thời gian | Thao tác                                                         | Nội dung nói                                                                                                                                                                                                                 |
| --------- | ---------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 0:00–0:15 | Mở Dashboard, chỉ vào tên hệ thống                               | “Đây là nền tảng phân tích hành vi người dùng thương mại điện tử. Hệ thống nhận event xem sản phẩm, thêm giỏ và mua hàng; xử lý qua Kafka và Spark; sau đó dùng K-Means và Random Forest để tạo insight.”                    |
| 0:15–0:30 | Chọn `onboarding-live-003-000002`, chỉ vào Batch 2               | “Đây là snapshot đã được Spark commit. Dashboard đọc cùng một run và cùng một batch nên KPI, funnel và prediction được đồng bộ.”                                                                                             |
| 0:30–1:05 | Ở **Tổng quan**, chỉ vào KPI                                     | “Run này xử lý 120 event của 24 visitor, gồm 100 lượt xem, 16 lượt thêm giỏ, 4 transaction và 3 buyer. Buyer rate là 12,5%.”                                                                                                 |
| 1:05–1:25 | Chỉ vào biểu đồ và funnel                                        | “Funnel cho thấy 24 phiên có xem, 11 phiên đi tới bước thêm giỏ và 3 phiên đi tới bước mua. View-to-cart là 45,83%, còn cart-to-purchase là 27,27%.”                                                                         |
| 1:25–1:55 | Bấm **Phân khúc & dự đoán**, chỉ vào biểu đồ cluster             | “Từ feature hành vi bảy ngày, K-Means chia 24 visitor thành hai nhóm: nhóm thứ nhất có 15 visitor và nhóm thứ hai có 9 visitor. Các feature gồm lượt xem, thêm giỏ, giao dịch, số sản phẩm khác nhau và độ gần của hành vi.” |
| 1:55–2:15 | Chỉ vào bảng prediction và threshold                             | “Random Forest tính điểm mua hàng cho từng visitor. Policy đang dùng ngưỡng 0,96 để ưu tiên precision. Đây là điểm xếp hạng phục vụ remarketing, không phải xác suất đã hiệu chuẩn.”                                         |
| 2:15–2:40 | Bấm **Theo dõi pipeline**, chỉ vào Producer, Consumer và Scoring | “Producer đưa event vào Kafka, Spark Consumer xử lý theo micro-batch, sau đó Scoring cập nhật feature, cluster và prediction.”                                                                                               |
| 2:40–2:52 | Chỉ vào backlog, counts và worker                                | “Backlog bằng 0, 120 event đều accepted, không có duplicate, invalid, late hoặc foreign event. Hai Spark worker đã tham gia xử lý.”                                                                                          |
| 2:52–3:00 | Quay lại Tổng quan hoặc chỉ vào live feed                        | “Toàn bộ phiên live đã hoàn tất hai lượt, tổng cộng 240 event ACK và 240 event processed. Hệ thống đã đi từ event streaming đến insight trên Dashboard.”                                                                     |

## 6. Nhánh minh họa live feed

Nếu phiên `onboarding-live-003` đang hiển thị trên Dashboard:

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
- KPI, funnel, prediction và pipeline cùng hiển thị cùng Batch 2.
- Backlog hiển thị 0.
- Không dùng `demo-live-002` đang `stale` hoặc `demo-live-001` đang `failed`.
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
