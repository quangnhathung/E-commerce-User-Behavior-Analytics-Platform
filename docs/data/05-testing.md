# 05 — Kiểm thử và nghiệm thu dữ liệu

[← Mục lục dữ liệu](README.md)

## 1. Kiểm thử offline

```bash
make test
```

Lệnh giữ suite hạ tầng rồi chạy [app/tests/](../../app/tests/). Host chỉ cần Python standard library cùng các công cụ vốn cần cho kiểm thử hạ tầng; không cài PySpark trên host.

16 test ingestion kiểm tra (streaming có thêm [suite riêng](../streaming/04-testing-and-recovery.md)):

- Chuẩn hóa ba loại hành vi, ID string/null và chuyển timestamp UTC chính xác.
- CSV/số cột, trường thiếu/sai, repeated header, vị trí nguồn và nhiều lỗi trên một dòng.
- ID canonical ổn định, version không lệ thuộc đường dẫn/mtime nhưng phụ thuộc cả bốn file.
- ZIP dependency tái lập được, chỉ chứa mã package Python.
- Snapshot chạy lại không ghi thêm; không công bố partial; không xóa dữ liệu khác.
- Từ chối checksum sai, copy khác nguồn fingerprint, marker thiếu hoặc identity không khớp.
- Wrapper trả đúng exit code, hủy process group con thật và dọn record UUID.
- Ghi report local thất bại giữ nguyên report trước đó.

Test snapshot offline dùng adapter file local thật; replica count là giá trị mô phỏng và không chứng minh replication HDFS.

## 2. Fixture Spark/HDFS thật

```bash
make test-data
```

[Test driver](../../app/jobs/test_data.py) tạo bốn CSV nhỏ trong thư mục tạm của client, sử dụng chính `run_ingest` của ứng dụng. Mọi dữ liệu HDFS nằm dưới `/ecommerce/_tests/ingest/<UUID>` và được dọn riêng trong finally. Dataset thật, namespace Bronze/Silver sản phẩm và volume không bị xóa.

Fixture cơ sở có:

| Chỉ số | Kết quả biết trước |
| --- | ---: |
| Dòng nguồn | 143 |
| Silver | 132 |
| Dòng trùng | 1 |
| Dòng lỗi | 10 |
| Visitor khác nhau | 129 |
| Item khác nhau | 130 |
| Transaction khác nhau | 1 |
| Buyer khác nhau | 1 |

Hai dòng mua có item khác nhau nhưng cùng transaction `900`, vì vậy transaction count vẫn bằng một. Dòng view trùng được đặt cách xa dòng gốc; kết quả phải giữ source row 2.

Chín case tích hợp kiểm tra chất lượng/schema/UTC và hai worker thực sự parse dữ liệu; chạy lại không đổi manifest; fault trước và sau commit; nguồn thay đổi tạo version mới; checksum/marker lỗi bị từ chối; all-invalid/header sai không xuất Silver; HDFS rename không ghi đè/lồng thư mục; bản copy sai checksum không được công bố Bronze.

Job chỉ xuất `DATA_TEST_RESULT=` và host ghi `artifacts/data/test-data.json` khi suite đạt và đã dọn xong namespace kiểm thử. Test lỗi hoặc cleanup lỗi làm command thất bại, không tạo report thành công mới.

## 3. Nghiệm thu full dataset

Sau doctor/smoke và fixture:

```bash
make ingest
make ingest
```

Lần đầu kiểm tra:

1. Bronze đủ bốn file, SHA-256 trùng nguồn.
2. Với nguồn đã khảo sát: 2.756.101 dòng = 2.755.641 Silver + 460 trùng + 0 lỗi.
3. Silver đọc lại đúng schema/count; quarantine và duplicate audit khớp báo cáo.
4. `worker_rows` có cả hai worker; payload có ít nhất hai replica.
5. Trong lượt chạy không có OOM/restart bất thường; xem [theo dõi RAM](../infra/10-memory-budget.md).

Lần hai phải có `bronze_reused=true`, `silver_reused=true`, không đổi manifest hoặc số dòng. Job vẫn đọc/xác minh checksum và kiểm tra lại Silver, không chỉ thấy thư mục tồn tại rồi bỏ qua.

Sau đó chạy lại smoke nếu cần xác nhận hạ tầng vẫn hoạt động. Chỉ đánh dấu giai đoạn nghiệm thu khi có log/report thật của cả fixture và full ingest.

## 4. Trạng thái trong phiên triển khai

| Hạng mục | Bằng chứng |
| --- | --- |
| Unit ứng dụng | 16 test đã đạt |
| Suite hạ tầng | 27 test đạt, 7 test HTTP skipped vì sandbox cấm socket; suite streaming có thêm kiểm thử riêng |
| Đối soát toàn bộ CSV offline | Khớp baseline; không sử dụng Spark/HDFS |
| Compose/Python/link docs | Được kiểm tra tĩnh trong phiên triển khai |
| Fixture Spark/HDFS | Artifact `test-data.json` xác nhận 9/9 đạt, cleaned=true, hai worker/hai replica |
| Full ingest HDFS | Artifact lần đầu xác nhận 2.755.641 Silver, 460 duplicate, 0 invalid, hai worker/hai replica |
| Full ingest lần hai/OOM | Chưa có bằng chứng reuse lần hai hoặc phép đo OOM của lượt ingest trong phiên này |

Các artifact trên được đọc từ lượt chạy trên host; phiên công cụ vẫn bị từ chối Docker socket khi thử ingest lại. Không thay thế bằng chứng còn thiếu bằng fixture mô phỏng hoặc report nguồn offline. File local cũ có thể tồn tại sau một lượt thất bại; luôn kiểm tra exit code, run log và timestamp/identity của artifact.
