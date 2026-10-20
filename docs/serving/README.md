# API local và dashboard HTML/CSS/JS

[← Tài liệu dự án](../README.md) · [Chạy từ đầu](../GETTING_STARTED.md#serving)

Phần này đưa snapshot của **replay/simulator ML** lên trình duyệt. Frontend là HTML/CSS/JavaScript thuần, mở bằng **Live Server của VS Code**. FastAPI chạy trực tiếp trên host và đọc MongoDB đang có trong Docker. Không có image web, container web hoặc bước build frontend.

| Tài liệu | Nội dung |
| --- | --- |
| [01 — Kiến trúc](01-architecture.md) | Luồng đọc, snapshot nhất quán, nguồn model, giới hạn và vai trò từng file |
| [02 — API](02-api.md) | Endpoint, tham số, phân trang, HTTP errors và CORS |
| [03 — Dashboard và cách chạy](03-dashboard.md) | Cài API, mở Live Server, ba màn hình, replay/simulator và đổi cổng |
| [04 — Kiểm thử và vận hành](04-testing-and-operations.md) | Test offline/live, smoke, bằng chứng, lỗi thường gặp và dừng/dọn |

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
- Serving mới: **19 test HTTP API offline**, **12 test JavaScript** và **4 test host mới** đã đạt. `make test` có 38 test hạ tầng + 187 ứng dụng, 7 HTTP skipped trong sandbox.
- Test API dùng FastAPI/Pydantic/Starlette đã khóa phiên bản, database adapter nhỏ; chưa thay thế truy vấn MongoDB thật. Phiên kiểm thử local dùng Python 3.12 và polling heartbeat trong test runner để khắc phục giới hạn đánh thức event loop của sandbox; app production không có shim này.
- DNS, Docker socket và socket Chrome bị giới hạn trong phiên công cụ. Cài venv hoàn chỉnh với PyMongo, test MongoDB thật, browser Live Server và smoke API trên dữ liệu thật **chưa nghiệm thu**. Không có report live serving giả hoặc số đo RAM/latency serving được suy ra từ lượt streaming cũ.

Scope v1 chỉ đọc run `stream-ml-v1` nghiệp vụ ở base `/ecommerce`. Batch lịch sử và các run KPI v2 chưa nằm trong bộ chọn. Metrics huấn luyện của model gắn với run vẫn được hiển thị riêng.
