# 04 — Kiểm thử và nghiệm thu

[← Mục lục](README.md)

## 1. Offline

`make test` có 34 test hạ tầng và 78 ứng dụng: 16 ingestion, 39 streaming và 23 test mới cho contract/publication/host behavior. Lượt hiện tại có 105 đạt, 7 HTTP skipped vì môi trường cấm socket. `make config` và parse Python 3.10 đạt. Package ingestion giữ nguyên SHA; archive streaming cũng được compile từng file mà không import dependency container.

| Nhóm mới | Kiểm tra |
| --- | --- |
| Contract: 12 test | Biên 30 phút tới millisecond, qua ngày, cùng item/visitor/session, thứ tự nghiêm ngặt, ties, nhiều item, mua thiếu bước, late bridge, scope ID, nguồn rỗng/có giới hạn, counts/rates và fingerprint |
| Mongo boundary: 8 test | Lỗi sau 100/101 sản phẩm giữ pointer cũ, retry sau pointer không thêm document, sai nội dung bị chặn, v2 không công bố KPI thiếu funnel, phát hiện dữ liệu hỏng, trang 20, exact item và cờ final |
| Host: 3 test | Analyze lỗi không đổi latest pointer, request gắn source/code/version, status chọn đúng lịch sử/run và giữ string ID |

Test Mongo dùng adapter có trạng thái trong bộ nhớ; chúng không chứng minh hành vi MongoDB server thật. Oracle chỉ nhận nguồn tối đa 10.000 event, không dùng để tính toàn bộ lịch sử.

## 2. Tích hợp thật

```bash
make test-behavior
make test-stream
```

`test-behavior` có sáu test: bảng Spark khớp oracle trên các biên/chuỗi; nguồn rỗng; 300 event xáo trộn với seed cố định; publish/reuse và exact item lookup; bốn điểm fault lịch sử; từ chối Gold mất marker. Fixture lịch sử có hơn 100 sản phẩm để lỗi giữa lô MongoDB thực sự để lại tập sản phẩm chưa đủ.

HDFS nằm dưới `/ecommerce/_tests/behavior/<UUID>`. Mongo cleanup lấy đúng analysis ID có source path UUID này rồi xóa ownership tương ứng. Cleanup chạy cả khi test lỗi; lỗi cleanup cũng khiến lệnh thất bại. Không đụng namespace nghiệp vụ hoặc Kafka topic.

`test-stream` hiện có mười test, gồm tám fault scenario. Tất cả test KPI hoàn chỉnh kiểm tra thêm funnel Gold/MongoDB. Hai test mới kiểm tra late bridge đúng biên 10 phút nối hai session qua batch, và batch chỉ có invalid công bố funnel rỗng trước khi nhận event hợp lệ. Fault `mongo_mid_funnel_products` kiểm tra trước con trỏ chung; fault sau MongoDB/trước checkpoint kiểm tra restart.

Traceback `injected ...` trong fault test là chủ đích; đánh giá tổng kết cuối, cleanup và exit code. Report tám test streaming cũ đã đạt chỉ chứng minh mã trước khi thêm behavior.

## 3. Tham chiếu nguồn demo

Đối soát offline CSV của khoảng mặc định, sau chuẩn hóa/loại trùng và session hóa chỉ trong khoảng replay:

| Chỉ số | Kỳ vọng |
| --- | ---: |
| Event | 2.692 |
| Session | 1.893 |
| Phiên xem | 1.882 |
| Phiên có xem rồi thêm giỏ | 35 |
| Phiên hoàn thành xem → giỏ → mua | 9 |
| Sản phẩm có tương tác | 2.155 |

Tổng các bước theo sản phẩm lần lượt là 2.354, 43, 10; khác tổng quan vì một phiên có thể tương tác nhiều sản phẩm. KPI cũ vẫn phải có 19 transaction, 1.860 visitor, 17 buyer. Không đánh đồng 9 phiên hoàn thành funnel với 27 purchase event hoặc 19 giao dịch.

Các số tham chiếu offline này đã được run `behavior-demo-001` xác nhận bằng Spark runtime. Prepare tạo reference cùng scope ID từ nguồn JSONL; final report đối chiếu cả summary và fingerprint sản phẩm.

## 4. Nghiệm thu lịch sử và phục hồi

Chạy `make analyze-behavior` hai lần. Lần đầu phải đối soát tổng event với manifest Silver hiện tại: 2.755.641. Lần hai phải `reused=true`, manifest và checksum giữ nguyên. Mongo overview/sản phẩm khớp Gold; mọi counts/rates thỏa hợp đồng.

Chạy một demo hoàn chỉnh bằng RUN_ID mới, rồi một run khác để kiểm tra ngắt streaming và tiếp tục khi producer đang phát. Thử ngắt producer và tiếp tục cùng cursor. Kết quả cuối phải khớp reference, backlog 0, checkpoint batch cuối tồn tại và funnel final. Sự kiện phát lại không tăng số liệu.

Giữ bằng chứng hai worker của streaming, hai replica payload, không OOM/restart, tổng limit container dưới 7 GiB, RAM được quan sát đầy đủ. Mục tiêu p95 sau khởi động ≤60 giây; nếu thiếu samples hoặc chỉ có batch đầu thì chưa đủ bằng chứng hiệu năng. Không chạy batch lịch sử cùng streaming.

## 5. Trạng thái thật

- `artifacts/analytics/test-behavior.json`: 6/6 đạt, cleaned, hai replica, cả hai resource flag true.
- `artifacts/streaming/test-stream.json`: 10/10 đạt, tám fault scenario, cleanup và monitor đạt.
- History `f24f8bc4f1363134cb4c8a894e217d20c272a780b00784473fd8742dcf327be7`: 2.755.641 event, 1.761.675 session, 235.061 sản phẩm, funnel 1.755.781 → 34.042 → 10.399. Lần đầu 137,407 giây; lần hai 45,407 giây, reused và manifest/summary giống nhau. Product fingerprint `59ae504d1b3850e1129fc99ae09d5fa8f71f1e1c5cab189920f6f8aa15c2e7ce`.
- `artifacts/streaming/behavior-demo-001/report.json`: passed, 2.692 event, 1.893 session, 2.155 sản phẩm, funnel 1.882 → 35 → 9; backlog 0, checkpoint batch 18, hai worker/hai replica, p95 37,287 giây với 2.627 samples; resources đầy đủ và không OOM/restart.
- Artifact do người dùng chạy trên terminal có Docker. Phiên công cụ vẫn bị từ chối socket, nhưng đọc được artifact chia sẻ để kiểm tra. ML mới giữ nguyên hai archive cũ; nghiệm thu riêng tại `docs/ml/04-testing.md`.
