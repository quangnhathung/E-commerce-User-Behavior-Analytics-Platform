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

Đây là kỳ vọng **offline**, chưa phải kết quả Spark runtime. Prepare tạo reference cùng scope ID từ nguồn JSONL của từng run; final report phải đối chiếu summary và fingerprint sản phẩm, không chỉ các số trong bảng.

## 4. Nghiệm thu lịch sử và phục hồi

Chạy `make analyze-behavior` hai lần. Lần đầu phải đối soát tổng event với manifest Silver hiện tại: 2.755.641. Lần hai phải `reused=true`, manifest và checksum giữ nguyên. Mongo overview/sản phẩm khớp Gold; mọi counts/rates thỏa hợp đồng.

Chạy một demo hoàn chỉnh bằng RUN_ID mới, rồi một run khác để kiểm tra ngắt streaming và tiếp tục khi producer đang phát. Thử ngắt producer và tiếp tục cùng cursor. Kết quả cuối phải khớp reference, backlog 0, checkpoint batch cuối tồn tại và funnel final. Sự kiện phát lại không tăng số liệu.

Giữ bằng chứng hai worker của streaming, hai replica payload, không OOM/restart, tổng limit container dưới 7 GiB, RAM được quan sát đầy đủ. Mục tiêu p95 sau khởi động ≤60 giây; nếu thiếu samples hoặc chỉ có batch đầu thì chưa đủ bằng chứng hiệu năng. Không chạy batch lịch sử cùng streaming.

## 5. Trạng thái thật

- Đã đạt offline, Compose và parse; chưa đổi cấu hình RAM hoặc dependency.
- Artifact hiện có: tám test streaming phiên bản trước đạt, cleanup thành công, quan sát tài nguyên đầy đủ và không OOM/restart.
- Đã thử `make test-behavior` sau triển khai; lệnh bị từ chối Docker socket trước khi chạy job. Host không có PySpark; chưa chạy được sáu test behavior, mười test streaming mới, batch lịch sử thật hay demo mới trong phiên công cụ.
- Không ghi report runtime thành công giả hoặc thay báo cáo cũ. Chạy các lệnh nghiệm thu trên terminal có quyền Docker và kiểm tra fingerprint của mã hiện tại.
