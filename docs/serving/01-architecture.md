# 01 — Kiến trúc serving

[← Mục lục](README.md)

## Luồng dữ liệu và công dụng

1. Producer replay/simulator gửi Kafka như trước.
2. Spark xử lý event, ghi HDFS, ghi KPI/funnel/ML vào MongoDB và công bố `committed_batch_id` sau cùng.
3. FastAPI trên host kết nối `127.0.0.1:<MONGO_HOST_PORT>`, đọc các snapshot đã công bố.
4. Trang HTML từ Live Server gọi HTTP API; SVG và bảng được dựng trực tiếp bằng JavaScript.

MongoDB credentials chỉ tồn tại ở API trên host. Browser chỉ biết URL API công khai trong `web/config.js`. Các trường dữ liệu đưa vào HTML đều được escape; visitor ID giữ chuỗi, kể cả số 0 đầu.

Serving không chạy Spark, không fit/load model cho inference, không đọc Parquet/HDFS và không thực thi job từ trình duyệt. Kết quả prediction đã được pipeline ML xử lý trước khi API đọc.

## Khóa snapshot khi đọc

Frontend gọi `/pipeline?run_id=...` trước để lấy batch đã commit. Các request overview, timeseries, funnel, segments, predictions, models và visitor sau đó dùng cùng `batch_id`.

API kiểm tra immutable config hash, batch registry, KPI/funnel, trạng thái published của ML snapshot, quality/config binding và số lượng prediction/sản phẩm. Batch tương lai đã có row nhưng chưa qua committed pointer vẫn bị từ chối. Prediction được đọc phải đúng feature version, model/policy, threshold và kiểu dữ liệu.

Một lượt refresh chỉ thay nội dung trang khi các request cần thiết cùng thành công và cùng run/batch. Lỗi giữa chừng giữ lần hiển thị hoàn chỉnh gần nhất. Đổi run/tab/bộ lọc hủy lượt đọc cũ và bỏ cache của ngữ cảnh trước; response đến muộn không được ghi đè trang mới.

Cursor prediction gắn run, batch, cluster và nhãn lọc; thứ tự score giảm dần rồi visitor ID tăng dần. Khi committed batch tiến, frontend quay về trang đầu. Đọc trang tiếp bằng batch đã khóa vẫn hợp lệ khi pointer đã tiến.

## Nguồn dữ liệu

| Collection | Công dụng |
| --- | --- |
| `stream_runs` | Config, model/policy binding, trạng thái producer/consumer và pointer đã commit |
| `stream_batches` | Counts, offsets, batch duration, latency và ML quality của từng batch |
| `kpi_overview`, `kpi_minute` | Tổng KPI của snapshot và chuỗi theo phút |
| `funnel_overview`, `funnel_products` | Funnel có thứ tự và sản phẩm của snapshot |
| `stream_ml_snapshots`, `stream_ml_predictions` | Metadata ML đã published, feature, cluster và score của visitor |
| `segment_models`, `purchase_models`, `purchase_policies` | Báo cáo của đúng ba version đã khóa trong run |

Segments tính phân bố và trung bình feature từ prediction của run đang xem; không lấy distribution huấn luyện để thay thế. Model/policy lấy theo config, không đọc latest pointer của máy chủ.

## Mã nguồn

| Vị trí | Vai trò |
| --- | --- |
| `services/manage.py` | Tạo venv, nạp cấu hình host, chạy API/test/smoke và lưu report |
| `services/serving/api.py` | FastAPI, validation, CORS localhost và xử lý HTTP error |
| `services/serving/repository.py` | MongoDB reads có timeout, khóa snapshot, phân trang và tổng hợp |
| `services/serving/indexes.py` | Bước tạo index rõ ràng, có thể chạy lại, ngoài request HTTP |
| `services/serving/smoke.py`, `checks.py` | Đối chiếu HTTP/Mongo/source, benchmark và runner kiểm thử |
| `web/index.html`, `styles.css` | Cấu trúc ba màn hình và giao diện responsive |
| `web/app.js` | Bộ chọn run, filter, paging, polling, hủy request cũ và trạng thái kết nối |
| `web/core.mjs`, `views.mjs` | HTTP client, contracts frontend, escape, SVG và bảng |
| `web/config.js` | URL API và chu kỳ refresh công khai; không đặt mật khẩu ở đây |

Mã serving nằm ngoài các package đóng vào archive Spark. Cả sáu archive hiện tại được đối chiếu giữ nguyên SHA. Compose, số service và giới hạn RAM container giữ như trước; API và trình duyệt dùng RAM host riêng.

Phạm vi demo vẫn tối đa 10.000 event/cửa sổ 24 giờ. Query Mongo có timeout 2 giây, page tối đa 200. Chỉ một API worker, Mongo pool tối đa 8 connection. Refresh không chạy chồng; tự cập nhật tạm ngừng khi tab browser bị ẩn.
