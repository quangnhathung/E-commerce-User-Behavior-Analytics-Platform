# 04 — Kiểm thử và vận hành serving

[← Mục lục](README.md)

## Kiểm thử offline

```bash
make test
make test-web
make test-api-offline
```

- `make test`: hồi quy hạ tầng/app; không cần dependency serving. Lượt đã kiểm có 38 + 187 test, 7 HTTP skipped do sandbox.
- `test-web`: 12 test Node cho client, snapshot pinning, đổi run/filter, phân trang, visitor leading zero, error recovery, escape và render ba màn hình. DOM adapter nhỏ kiểm tra controller; không thay cho kiểm tra layout trong browser.
- `test-api-offline`: 19 test FastAPI HTTP với database adapter, cần `make api-setup` trước. Không kết nối MongoDB.

Test API gồm: run list ẩn namespace test, config/published snapshot, batch tương lai, pointer tiến, cursor ràng buộc filter/batch, leading zero, CORS Live Server, thời gian có múi giờ, missing prediction/funnel product, pinned model, empty cluster và error redaction.

## HTTP với MongoDB thật

Cụm MongoDB đang chạy; chưa cần giữ `make api` vì test tạo app trong process:

```bash
make test-api
```

Lệnh chạy 19 case offline và 19 case HTTP/Mongo thật. Fixture tạo collection prefix `_webtest_<UUID>_` trong database ứng dụng, chỉ drop các collection thuộc chính UUID và xác nhận cleanup. Không xóa collection nghiệp vụ hoặc reset volume.

Report/log: `artifacts/serving/test-api.json`, `test-api.log`; offline có `test-api-offline.*`. Chỉ nhận `passed=true` khi command exit 0, có kết quả suite và không có skipped case. Test interrupted/failed không được coi là đã nghiệm thu cleanup.

## Smoke dữ liệu thật qua API

Giữ `make api` chạy ở terminal khác, chọn run đã completed:

```bash
make smoke-web RUN_ID=ml-replay-001
make smoke-web RUN_ID=ml-sim-001
```

Smoke đọc pointer Mongo, đối chiếu KPI/minute/funnel với source reference của config, đọc hết prediction qua cursor rồi tính lại fingerprint, kiểm tra cluster/model bindings và visitor. Sau đó gọi overview 100 lần tuần tự, ghi p95 response; mục tiêu ≤1 giây trên máy đang kiểm. Chưa đủ điều kiện hoặc không đạt thì report `passed=false`.

Report/log: `artifacts/serving/smoke-web-<RUN_ID>.json/.log`. Đây là số đo API trên run completed, không phải benchmark toàn bộ pipeline đang nhận event. Latency của streaming lấy riêng từ report/monitor streaming; không cộng số đo từ các lần chạy khác nhau để tuyên bố end-to-end.

## Nghiệm thu browser Live Server

- Ba màn hình desktop/mobile không tràn ngang toàn trang; bảng có vùng cuộn riêng.
- Replay có 1.528 event/1.016 visitor và số liệu khớp report trên chính máy đang chạy.
- Simulator mới có đủ ba đợt cập nhật, 9 event/3 visitor, tra đúng ID có zero đầu.
- Khi pipeline tiến batch, không trộn KPI/funnel/prediction; cursor trở về trang đầu.
- Tắt API: browser báo mất kết nối, giữ dữ liệu lần đọc hoàn chỉnh gần nhất. Bật API lại: Cập nhật khôi phục được.
- Đổi run: không giữ visitor/filter/page từ run trước.
- Không diễn giải precision lịch sử thành precision của run mới hoặc score thành phần trăm xác suất mua.

Phiên công cụ chưa chạy được Chrome do `setsockopt: Operation not permitted`, chưa cài được venv mới do DNS và chưa truy cập Docker socket. Do đó browser, test Mongo thật, smoke API và RAM của API host đang để pending. Các test đã đạt được nêu riêng trong [trạng thái](README.md).

## Lỗi và phục hồi

| Hiện tượng | Kiểm tra / xử lý |
| --- | --- |
| Không kết nối API | Giữ `make api` chạy; mở URL health; đối chiếu cổng trong `.env` và `web/config.js` |
| API 503 | MongoDB đang chạy, cổng host đúng, application credentials còn khớp volume; xem error code. Snapshot incomplete cần điều tra dữ liệu, không tự ghi đè |
| Bộ chọn run rỗng | Chưa có `stream-ml-v1` nghiệp vụ; run v2 và batch lịch sử không nằm trong v1 serving |
| Model 503 | Registry của model/policy đã khóa chưa published hoặc identity sai; không fallback latest model |
| Visitor 404 | Visitor chưa thuộc run/batch; giữ nguyên chuỗi ID có zero đầu |
| Cursor 422 | Bỏ cursor, về trang đầu; không dùng cursor của run/filter/snapshot khác |
| Biểu đồ trống | Khoảng thời gian không có điểm hoặc nhập sai timezone; KPI tổng vẫn theo toàn snapshot |
| Mở file không hoạt động | Dùng Open with Live Server; không dùng file:// |
| Thiếu Node | Node chỉ cần cho test-web; dashboard vẫn chạy bằng Live Server |
| pip không tải được | Kiểm tra DNS/internet/proxy; chạy lại api-setup. Không cần build lại Spark hoặc xóa Docker volume |

API đọc `.env` bằng parser hiện có và truyền mật khẩu qua environment, không qua command-line. Không xoay credential khi khởi động API. `api-indexes` là bước riêng, idempotent.

## Dừng và dọn

- Ctrl+C tại terminal `make api` để dừng API; bấm **Port: ...** / Stop Live Server trong VS Code để dừng frontend.
- `make down` chỉ dừng hạ tầng Docker; API là process host nên dừng riêng bằng Ctrl+C.
- Muốn bỏ dependency local: dừng API rồi xóa riêng `services/.venv/`. Report tại `artifacts/serving/` có thể xóa khi không cần giữ bằng chứng.
- Không có image/container/volume web để prune. Dọn Docker của phần hạ tầng tiếp tục dùng quy trình [lifecycle](../infra/04-bootstrap-and-lifecycle.md).

Container limits giữ nguyên 6,25 GiB thường trực/6,94 GiB gồm init. API và browser dùng RAM host ngoài các cgroup đó; giai đoạn này chưa có số đo để cam kết mức RAM của chúng.
