# API local và dashboard HTML/CSS/JS

[← Tài liệu dự án](../README.md) · [Chạy từ đầu](../GETTING_STARTED.md#serving) · [Sử dụng web và đọc kết quả](06-web-user-guide.md)

Phần này đưa snapshot của **replay/simulator ML** lên trình duyệt. Frontend là HTML/CSS/JavaScript thuần, mở bằng **Live Server của VS Code**. FastAPI chạy trực tiếp trên host và đọc MongoDB đang có trong Docker. Không có image web, container web hoặc bước build frontend.

| Tài liệu | Nội dung |
| --- | --- |
| [01 — Kiến trúc](01-architecture.md) | Luồng đọc, snapshot nhất quán, nguồn model, giới hạn và vai trò từng file |
| [02 — API](02-api.md) | Endpoint, tham số, phân trang, HTTP errors và CORS |
| [03 — Dashboard và cách chạy](03-dashboard.md) | Cài API, mở Live Server, ba màn hình, replay/simulator và đổi cổng |
| [04 — Kiểm thử và vận hành](04-testing-and-operations.md) | Test offline/live, smoke, bằng chứng, lỗi thường gặp và dừng/dọn |
| [05 — Demo liên tục](05-live-demo.md) | Nhiều lượt dữ liệu, feed mỗi giây, backpressure, dừng và tiếp tục |
| [06 — Hướng dẫn sử dụng web](06-web-user-guide.md) | Từng thao tác, KPI/funnel, cluster/prediction, pipeline, ví dụ kết quả và bài thực hành |

## Chạy nhanh trên máy đã có dữ liệu

Tại thư mục gốc repository, cụm MongoDB đang chạy và ML streaming đã có run:

```bash
make api-setup
make api-indexes
make api
```

Giữ terminal API hoạt động. Trong VS Code, mở `web/index.html`, bấm **Open with Live Server**. URL thường là `http://127.0.0.1:5500/web/index.html`; đường dẫn thay đổi theo folder bạn mở trong VS Code. Chọn RUN_ID trên dashboard.

API: <http://127.0.0.1:8000/docs>. Health: <http://127.0.0.1:8000/api/v1/health>.

`api-setup` tạo virtualenv riêng tại `services/.venv/`; không cài package vào Python toàn hệ thống. `api-indexes` tạo index đọc có thể chạy lại; không thay model, checkpoint hoặc nội dung snapshot. Frontend không cần Node/npm để chạy. Node 18+ chỉ dùng cho `make test-web`.

## Trạng thái ngày 20/10/2026

- Pipeline streaming ML trước đó đã có artifact xác nhận: 7/7 test tích hợp, replay 1.528 event/1.016 visitor, simulator 9 event/3 visitor và hai lượt replay reuse giữ nguyên fingerprint.
- Serving hiện tại: **23 test HTTP API offline**, **15 test JavaScript**, **24 test live offline** và **4 test host serving** đã đạt. Bốn test pacing bổ sung xác minh lỗi sleep âm, đúng deadline, RATE/late wakeup và pause/resume. `make test` có 38 test hạ tầng + 211 ứng dụng, 7 HTTP skipped trong sandbox; 24 test live nằm trong suite ứng dụng. Số lượng được ghi theo phiên bản đã đối chiếu, không phải yêu cầu giữ nguyên khi thêm test.
- Test API dùng FastAPI/Pydantic/Starlette đã khóa phiên bản, database adapter nhỏ; chưa thay thế truy vấn MongoDB thật. Phiên kiểm thử local dùng Python 3.12 và polling heartbeat trong test runner để khắc phục giới hạn đánh thức event loop của sandbox; app production không có shim này.
- Lượt kiểm tra tiếp theo bằng Chrome headless qua Live Server và API/MongoDB đang chạy đã đạt: ba màn hình với cả `ml-sim-001` và `ml-replay-001`, KPI thật, tra visitor có zero đầu, đổi run/reset filter, Cập nhật và tự cập nhật. Lỗi `Illegal invocation` của native `fetch` đã được sửa. Bằng chứng tại `artifacts/serving/fetch-browser-check.json` trên máy kiểm tra.
- Bộ test HTTP với fixture MongoDB thật, smoke đối chiếu fingerprint/đo latency, nghiệm thu layout desktop/mobile và số đo RAM API vẫn **chưa nghiệm thu** trong lượt sửa lỗi này. Kiểm tra browser trên run đã hoàn tất chưa xác nhận cập nhật khi pipeline tiến batch mới.
- Browser fixture live đã kiểm feed ACK/processed, pointer/chuyển lượt, chọn lịch sử và layout 390px; fixture không thay cho Kafka/Spark thật.
- Artifact người dùng chạy sau bản sửa pacing xác nhận **`live-check-002` completed, 240 event/hai lượt, passed, hai resource flags true**. `demo-live-002-000001` đối soát đủ 1.000 event/200 visitor, backlog 0, p95 42,269 giây; API đọc được KPI, funnel, cluster và prediction batch 15.
- Parent `demo-live-002` bị gián đoạn ở lượt 2: 1.035 ACK/1.000 processed, stop_requested và heartbeat stale; report failed. Monitor không ghi OOM/restart nhưng thiếu một quan sát; log chưa đủ xác định nguyên nhân. Dừng/drain/resume liên tục thật vẫn cần nghiệm thu riêng; không dùng thành công lượt đầu để đánh dấu cả phiên passed.

Scope v1 chỉ đọc run `stream-ml-v1` nghiệp vụ ở base `/ecommerce`. Batch lịch sử và các run KPI v2 chưa nằm trong bộ chọn. Metrics huấn luyện của model gắn với run vẫn được hiển thị riêng.

## Chế độ live liên tục

Sau khi khởi động lại API để nạp route mới, chạy `make live-demo LIVE_ID=demo-live-001 RATE=5`. Producer/consumer được quản lý cùng một lệnh; mặc định 1.000 event/lượt, tối đa 200 visitor/100 sản phẩm, tiếp tục chuyển lượt đến khi Ctrl+C. Chọn phiên và bật **Theo dõi live** trên web. Xem [hướng dẫn đầy đủ và nghiệm thu](05-live-demo.md); simulator 9 event cũ vẫn giữ nguyên.

Nếu ID đã được tạo bằng mã trước bản sửa, dùng ID mới thay vì sửa request. Máy mới có thể theo [lộ trình nghiệm thu hữu hạn rồi chạy liên tục](../GETTING_STARTED.md#live-demo); người đọc kết quả dùng [hướng dẫn web](06-web-user-guide.md#walkthrough).
