# 05 — Demo liên tục và feed sự kiện trực tiếp

[← Mục lục](README.md) · [API và Live Server](03-dashboard.md)

## Vì sao hai run cũ giống nhau?

`ml-sim-001` và `web-live-001` dùng cùng simulator kiểm thử: 9 event, 3 visitor, cùng hành trình và model. Đổi RUN_ID hoặc RATE không đổi hành vi của kịch bản đó. Các lệnh `prepare-ml-demo` / `simulate-ml` vẫn giữ nguyên để chạy lại kiểm thử cũ.

Chế độ `live-demo` sinh nguồn đa dạng và chạy nhiều lượt nối tiếp đến khi bạn yêu cầu dừng. Nguồn mỗi phiên có seed riêng; resume giữ seed và nguồn đã chuẩn bị. Thống kê, feature, cluster và nhãn do pipeline/model thực tính; không ép điểm hoặc số người dự đoán mua tăng theo thời gian.

## Chạy trên máy đã có dữ liệu và model

Cần cụm healthy, Silver ingest passed, K-Means/RF published và policy eligible của đúng RF. Dùng cùng workspace chứa các pointer model đã chạy trước đó.

Sau khi cập nhật mã, dừng tiến trình API cũ bằng Ctrl+C rồi chạy trong terminal A:

```bash
make api-indexes
make api
```

Giữ terminal API mở. Mở `web/index.html` bằng Live Server, tải lại trang để nạp JS mới.

Terminal B tại root repository:

```bash
make live-demo LIVE_ID=demo-live-001 RATE=5
```

Lệnh này tự quản lý Spark consumer và producer. Không cần chạy `stream-ml` hoặc `simulate-ml` riêng. Chờ chuẩn bị seed/model và dòng `LIVE_SEGMENT_READY=demo-live-001-000001`.

Trên web, chọn `demo-live-001` trong **Phiên live**, bật **Theo dõi live** và **Tự cập nhật KPI**. Feed mở sẵn, tối đa 50 dòng mới nhất. Chọn run lịch sử ở bộ chọn RUN_ID sẽ tắt theo dõi tự động; bật lại checkbox để trở về snapshot mới của phiên live.

## Những phần cập nhật trên web

| Thành phần | Nguồn và cách cập nhật |
| --- | --- |
| Feed mỗi giây | Mongo telemetry được ghi sau Kafka ACK; có giờ phát, visitor, sản phẩm, hành động |
| Kafka đã ACK · chờ Spark | Broker xác nhận nhận event; chưa có bằng chứng offset đó nằm trong snapshot đã commit |
| Spark đã xử lý | Đối chiếu partition/offset với batch đã commit hoặc lượt đã đối soát hoàn tất |
| Tổng toàn phiên | Event đã ACK, đã xử lý và số lượt hoàn tất; không reset khi chuyển lượt, không cộng lặp khi resume |
| KPI, funnel, phân khúc và dự đoán | Các endpoint cũ, khóa cùng RUN_ID/batch; chỉ thay khi có snapshot đã công bố |
| Chuyển lượt | Giữ snapshot hoàn chỉnh gần nhất đến khi lượt mới có batch đầu tiên |
| Mất heartbeat | Sau 20 giây không có heartbeat mới, báo trạng thái chưa được xác nhận; vẫn giữ snapshot trước |

Feed được đọc mỗi 1 giây, tạm dừng khi tab bị ẩn. KPI có lượt kiểm tra dự phòng mỗi 5 giây và được yêu cầu đọc lại khi feed báo pointer mới. Backend vẫn xử lý micro-batch với trigger 15 giây; thời gian xử lý có thể dài hơn trigger. Mục tiêu feed sau ACK khoảng 2 giây và mục tiêu latency Spark ≤60 giây là hai số đo khác nhau, cần đo trên máy chạy thật.

Giờ phát event là giờ hiện tại, hiển thị UTC. Thời gian nghiệp vụ của nguồn vẫn là cửa sổ mô phỏng năm 2015 để phù hợp provenance/cutoff model. Không gộp trục event-time giữa các lượt thành một chuỗi thời gian hiện tại.

## Dừng và tiếp tục

Tại terminal B, bấm **Ctrl+C một lần**. Controller yêu cầu ngừng phát event mới, lưu ACK/cursor và chờ consumer xử lý phần đã nhận trong tối đa 180 giây. Giữ terminal mở trong thời gian drain.

Có thể yêu cầu dừng từ terminal khác:

```bash
make live-stop LIVE_ID=demo-live-001
```

Tiếp tục sau khi phiên đã dừng:

```bash
make live-demo LIVE_ID=demo-live-001
```

Lệnh tiếp tục run con còn dở từ cursor/checkpoint khi giữ nguyên mã và cấu hình của phiên. LIVE_ID khóa cả checksum archive, RATE và các tham số nguồn/model; cập nhật mã live cũng làm đổi checksum. Muốn dùng mã hoặc cấu hình mới, tạo LIVE_ID mới.

Xem trạng thái mà không mở Spark application mới:

```bash
make live-status LIVE_ID=demo-live-001
```

Một lượt dừng giữa nguồn có trạng thái `paused`, không được coi là đã đối soát toàn bộ 1.000 event. Report phiên có thể `passed=true` cho vòng đời dừng/drain và monitor thành công; `completed_segments` chỉ đếm lượt đã xử lý đủ nguồn và đối soát hoàn tất.

Nếu dừng không được xác nhận, guard driver và producer cùng giữ token của phiên. Xác nhận/dừng đúng process group trước khi gỡ đúng marker theo [quy trình phục hồi](../streaming/04-testing-and-recovery.md). Không dùng Ctrl+C lần hai để thay cho việc đợi drain bình thường; lần hai kích hoạt cancel process group đang được quản lý.

## Khắc phục lỗi `sleep length must be non-negative`

Nếu traceback kết thúc ở `ecommerce_live/producer.py` với `ValueError: sleep length must be non-negative`, nguyên nhân là vòng điều tiết tốc độ đọc đồng hồ hai lần: deadline có thể hết giữa lúc kiểm tra và tính thời gian ngủ. Bản sửa kiểm tra yêu cầu dừng trước, tính thời gian còn lại đúng một lần, rồi chỉ ngủ khi thời gian đó dương. Khi deadline đã qua, producer tiếp tục gửi thay vì truyền số âm vào `sleep()`.

Thông báo Spark `TaskKilled (Stage cancelled...)` có thể xuất hiện khi consumer được dọn sau lỗi producer. Đối chiếu traceback và `resources.json` để xác định nguyên nhân; bản thân thông báo hủy task không chứng minh hết RAM.

Sau khi áp dụng bản sửa, kiểm tra offline:

```bash
make test-live
make test
```

Nếu phiên lỗi là `demo-live-001`, chạy phiên mới để sử dụng checksum mới:

```bash
make live-demo LIVE_ID=demo-live-002 RATE=5
```

Chọn `demo-live-002` trong bộ chọn **Phiên live** trên web. Giữ nguyên request, log, checkpoint và dữ liệu của `demo-live-001` để đối chiếu. Không sửa `code_sha256` trong request cũ để vượt kiểm tra tương thích. Nếu ID mới đã tồn tại với mã khác, chọn một ID chưa dùng.

Không cần build lại image, khởi tạo lại hạ tầng hoặc train lại model cho bản sửa Python này: `app/` được mount vào `spark-client`, và lệnh live đóng gói mã hiện tại khi khởi chạy. Sau khi phiên mới đã dừng/drain bình thường, chạy lại cùng LIVE_ID với cùng mã/cấu hình để resume.

## Quy mô và bộ nhớ

| Biến khi tạo phiên | Mặc định | Giới hạn |
| --- | ---: | --- |
| `RATE` | 5 event/giây | >0 và ≤10 |
| `LIVE_EVENTS` | 1.000 event/lượt | 12..1.000 |
| `LIVE_VISITORS` | 200 | 3..200; không quá nửa số event/lượt |
| `LIVE_ITEMS` | 100 | 3..100 |
| `LIVE_SEGMENTS` | 0 | 0 chạy liên tục; số dương giới hạn số lượt để nghiệm thu |

Mỗi lượt có nguồn hữu hạn, được sắp thời gian và cố định checksum trước khi phát. Visitor có các nhóm xem, bỏ giỏ và mua với hành vi lặp lại khác nhau; namespace visitor khác nhau theo phiên/lượt và được kiểm tra không va vào seed lịch sử. Một Spark application tải cặp model một lần, xử lý từng lượt tuần tự. Không train lại model khi đang live.

Producer chờ khi backlog đạt 200 event đã ACK nhưng chưa được xử lý. Feed Mongo giữ 2.000 event gần nhất/phiên; web giữ 50 dòng; buffer quan sát tài nguyên host giữ 720 mẫu gần nhất và 100 lỗi gần nhất, đồng thời giữ tổng số mẫu/lỗi. Đây là giới hạn buffer, không phải cam kết Spark xử lý nhanh hơn tốc độ phát.

Không thêm service hoặc tăng memory limit Compose. Trong lúc live đang chạy, khóa driver/producer được giữ; chạy job Spark khác sau khi live đã dừng. Profile `streaming-live` mới có cơ chế cancel như cũ và không áp timeout 30 phút; các job cũ vẫn có timeout 1.800 giây.

HDFS snapshot, checkpoint, registry và report từng lượt được giữ. Kiểm tra disk reserve trước mỗi lượt mới, theo `MIN_DOCKER_DISK_GIB` đã khóa khi tạo phiên. Chạm ngưỡng sẽ dừng báo lỗi, không tự xóa lịch sử. Chế độ liên tục vì vậy vẫn phụ thuộc tài nguyên máy và dung lượng còn lại.

## API chỉ đọc và lưu trữ

```text
GET /api/v1/live/sessions
GET /api/v1/live/sessions/{live_id}
GET /api/v1/live/sessions/{live_id}/events?after=123&limit=100
```

Danh sách trả tối đa 20 phiên gần nhất thuộc base `/ecommerce`; không lộ owner token, không trả namespace kiểm thử. Feed có cursor số thứ tự, `next_after`, `gap` và thông tin phiên/pointer. Không truyền `after` thì lấy đuôi mới nhất. Nếu cursor đã nằm ngoài buffer, `gap=true` và trả đuôi mới để đồng bộ lại. `limit` tối đa 200. API không có endpoint bật/tắt job.

| Thành phần | Công dụng |
| --- | --- |
| `app/live_manage.py` | CLI host, pin tham số/model, giữ hai khóa, gửi yêu cầu dừng và giám sát job |
| `app/ecommerce_live/` | Generator, shared seed, nguồn từng lượt, ACK telemetry, pause và vòng xử lý Spark |
| `app/jobs/live_demo.py` | Entry point riêng của archive live; tách action Spark với control/status Python |
| `app/live_monitor.py` | Buffer quan sát tài nguyên có giới hạn cho phiên chạy dài |
| `services/serving/live.py` | Đọc trạng thái/feed, xác định offset đã xử lý và trạng thái heartbeat |
| `web/live.mjs` | Poll không chồng request, cursor, chống trùng, giữ đuôi feed và thông báo snapshot mới |

Artifact host ở `artifacts/live/<LIVE_ID>/`: `request.json`, `control.json`, `report.json`, `resources.json`, log theo UUID và `status.json`. `control.json` chứa owner token để CLI chỉ dừng đúng phiên, không đưa lên frontend.

Mongo thêm `live_sessions`, `live_events`. Run con vẫn lưu trong registry/KPI/funnel/ML hiện có. Shared seed ở `/ecommerce/live/<LIVE_ID>/seed`; nguồn từng run, Bronze/Silver/Gold và checkpoint dùng namespace streaming ML sẵn có. Sáu archive cũ và model/policy đã công bố được giữ nguyên.

## Kiểm thử và nghiệm thu

```bash
make test-live
make test-web
make test-api-offline
make test
```

`test-live` là kiểm thử offline cho generator, wire, fingerprint archive, ACK/retry, buffer, fencing, backpressure, vòng đời qua các boundary giả lập, stop/resume, checkpoint và wrapper timeout. Các test pacing kiểm tra deadline bị vượt trong lúc đọc yêu cầu dừng, đúng deadline, RATE 1/5/10, thức dậy muộn và pause/resume giữ ACK cursor. Nó không thay thế Spark/Kafka/Mongo thật. Test API offline sử dụng adapter; browser fixture nếu có cũng không chứng minh streaming đã chạy.

Nghiệm thu hai lượt thật với model đã có, dùng LIVE_ID riêng:

```bash
make live-demo LIVE_ID=live-check-002 RATE=5 LIVE_EVENTS=120 LIVE_VISITORS=24 LIVE_ITEMS=12 LIVE_SEGMENTS=2
```

Đợi hai dòng `LIVE_SEGMENT_RESULT` có `passed=true`, `batch_reconciled=true`, backlog 0 và cuối phiên `completed_segments=2`, `completed_events=240`. Resource flags cần `no_oom_or_restart=true`, `memory_observations_complete=true`. Trên web thấy feed tăng, trạng thái ACK/processed thay đổi, pointer tự chuyển run con và tổng phiên không giảm.

Sau đó kiểm tra riêng phiên không giới hạn: dừng giữa lượt bằng Ctrl+C, ghi nhận cursor/pointer, chạy lại cùng LIVE_ID và xác nhận không cộng trùng; đợi qua ít nhất hai lượt. Lưu log/report và đo latency thực. Nếu Docker socket hoặc browser bị chặn, ghi hạng mục tương ứng chưa nghiệm thu, không suy kết quả live từ các test offline.
