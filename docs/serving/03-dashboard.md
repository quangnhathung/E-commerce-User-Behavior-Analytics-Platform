# 03 — Dashboard và cách chạy

[← Mục lục](README.md) · [Hướng dẫn thao tác và đọc kết quả](06-web-user-guide.md)

Tài liệu này là runbook cài/mở API và Live Server. Hướng dẫn từng nút, từng chỉ số, phân cụm/prediction và bài thực hành nằm trong [06 — Sử dụng web](06-web-user-guide.md); lộ trình từ clone ở [GETTING_STARTED](../GETTING_STARTED.md#serving).

## Chuẩn bị một lần

Cần Python **3.11+** cho API (khuyến nghị 3.11/3.12), module venv/pip và VS Code với extension Live Server. Python host 3.10 vẫn dùng được cho hạ tầng cũ; khi cần chọn interpreter khác, dùng `PYTHON=python3.11 make api-setup`. Trên Ubuntu nếu thiếu venv: cài package venv tương ứng interpreter. Frontend không có bước npm install hoặc build.

Từ root repository, dùng cụm và MongoDB đã dựng ở các giai đoạn trước:

```bash
make api-setup
make api-indexes
```

`api-setup` tạo `services/.venv/` và cài dependency đã khóa; chạy lại giữ cấu trúc venv và xác minh dependency bằng `pip check`. Lần đầu cần internet. `api-indexes` dùng application account để tạo indexes phục vụ run list, minute series, prediction rank và session/feed live; không sửa snapshot hay xóa dữ liệu. Lệnh này chưa khởi động API; cần giữ `make api` chạy ở bước dưới.

## Mở API và Live Server

Terminal A tại root:

```bash
make api
```

Giữ terminal này mở. Kiểm tra <http://127.0.0.1:8000/api/v1/health> có `status=ready`. API lấy database, username/password và Mongo host port từ `.env`; root password không được truyền vào tiến trình API.

Trong VS Code:

1. Mở folder repository.
2. Mở `web/index.html`.
3. Bấm chuột phải → **Open with Live Server**.
4. Chọn RUN_ID, ví dụ `ml-replay-001` hoặc `ml-sim-001`, nếu các run đó đã có trên máy.

Nếu mở root repository, URL thường là `http://127.0.0.1:5500/web/index.html`; nếu mở riêng folder `web`, URL thường là `/index.html`. Dùng URL Live Server in ra. Không double-click mở bằng `file://`, vì trình duyệt cần HTTP để nạp ES modules và gọi API.

Trên WSL, chạy API cùng môi trường chứa repository và mở project bằng VS Code Remote WSL; xác nhận URL health truy cập được từ browser trước khi đọc dashboard.

## Ba màn hình

### Tổng quan

KPI gồm accepted event, visitor duy nhất, giao dịch, buyer rate và các lượt xem/giỏ/mua. Biểu đồ SVG hiển thị theo phút UTC. Funnel giữ cùng phiên/sản phẩm và đúng thứ tự; bảng sản phẩm xếp theo view sessions.

Hai ô thời gian chỉ lọc biểu đồ. Tỷ lệ funnel dùng đơn vị phiên/sản phẩm, không trộn với buyer rate theo visitor. Trước batch đầu tiên hiển thị trạng thái đang chờ, không tạo số liệu mẫu.

### Phân khúc và dự đoán

Phân bố cluster thuộc cohort đã có event trong run. Bảng feature trung bình lấy từ cùng snapshot; cluster rỗng giữ count 0. Lọc cluster, nhãn policy và tra visitor bằng chuỗi ID. Trang prediction có 50 dòng; bấm trang tiếp/trước, tự quay về đầu khi batch mới xuất hiện.

Chi tiết visitor giữ cả tám feature. Phần metrics đọc đúng model/policy của run, hiển thị precision/recall/PR-AUC lịch sử, threshold và version. `000099000000003` được giữ nguyên. Score không phải xác suất đã hiệu chuẩn, không bắt buộc tăng sau mỗi event. Bộ chọn hiển thị tối đa 200 run ML nghiệp vụ mới nhất; API vẫn hỗ trợ cursor khi cần tra danh sách cũ hơn.

### Pipeline

Hiển thị trạng thái producer/consumer/scoring, backlog đã ACK, counts validation, worker tham gia, batch durations và latency p95 nếu đã đủ sample. Backlog 0 trước lúc phát event không có nghĩa đã hoàn tất.

Liên kết Spark/HDFS UI lấy cổng mặc định hoặc cấu hình công khai trong `web/config.js`.

## Xem dữ liệu thay đổi trực tiếp

Để demo nhiều event và chạy đến khi dừng, dùng chế độ mới [live-demo](05-live-demo.md): `make live-demo LIVE_ID=demo-live-001 RATE=5`. Web có feed sau Kafka ACK mỗi giây và theo dõi snapshot của các run con tự động. Phần dưới là kịch bản kiểm thử nhỏ, cố định 9 event/3 visitor.

Run cũ completed chỉ phục vụ dữ liệu đã lưu. Để xem cập nhật, tạo run mới, ví dụ:

```bash
make prepare-ml-demo RUN_ID=web-sim-001 DEMO_SOURCE=simulator
```

Chọn `web-sim-001` trên dashboard. Terminal B:

```bash
make stream-ml RUN_ID=web-sim-001
```

Sau `STREAM_ML_READY=web-sim-001`, terminal C:

```bash
make simulate-ml RUN_ID=web-sim-001
```

Quan sát ba đợt commit, đủ 9 event/3 visitor. Không ép phải có đúng ba batch, vì một đợt có thể được chia qua nhiều micro-batch. Cuối cùng scoring ready, producer/consumer completed và backlog 0. Chạy từng Spark job tuần tự; API/Live Server có thể hoạt động cùng job.

Tự cập nhật mỗi 5 giây, tạm ngừng khi tab bị ẩn. Có thể tắt checkbox và bấm Cập nhật thủ công. Nếu API lỗi, banner giải thích trạng thái và giữ snapshot hoàn chỉnh gần nhất của đúng ngữ cảnh. Đổi run xóa filter/visitor/cursor cũ.

## Đổi cổng

Thêm `API_HOST_PORT=8001` vào `.env` nếu cổng 8000 đang dùng, rồi khởi động lại API. Sửa cấu hình browser tương ứng:

```js
window.ECOMMERCE_CONFIG = {
  apiBaseUrl: "http://127.0.0.1:8001",
  refreshMs: 5000,
  sparkUiPort: 18080,
  hdfsUiPort: 9870
};
```

Refresh browser sau khi sửa. Live Server đổi cổng độc lập; CORS chấp nhận các cổng localhost HTTP. Không đưa MongoDB URI hoặc mật khẩu vào file JS.
