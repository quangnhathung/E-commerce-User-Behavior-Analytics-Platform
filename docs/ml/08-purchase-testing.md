# 08 — Kiểm thử và nghiệm thu purchase prediction

[← Mục lục ML](README.md) · [Lệnh vận hành](07-purchase-operations.md)

## Bằng chứng hiện có

Ngày 17/10/2026: đã triển khai mã, CLI, 31 test offline mới và bảy test tích hợp Spark/HDFS/Mongo. `make test` có 34 test hạ tầng + 135 test ứng dụng: **162 đạt, 7 HTTP skipped** do môi trường cấm tạo socket loopback. `make config` đạt. Ba archive ingestion/streaming/ML cũ giữ nguyên hash.

Đã thử `make test-purchase` trong phiên công cụ; preflight dừng vì không có quyền truy cập `/var/run/docker.sock`, trước khi chạy Spark. Vì vậy **chưa nghiệm thu bảy live test, build 128 ngày, bốn RF, metrics test và batch score nguồn thật**. Không có số accuracy, PR-AUC, winner, threshold, dung lượng hoặc RAM/thời gian RF thực tế để báo cáo. Các số cohort/positive trong [hợp đồng dữ liệu](05-purchase-data.md) là tham chiếu CSV độc lập, không phải artifact Spark thành công.

K-Means đã được nghiệm thu bằng artifact người dùng tạo, gồm full-source reuse; xem [kiểm thử K-Means](04-testing.md). Bằng chứng đó xác nhận pipeline trước, không thay thế kiểm thử RF.

## Kiểm thử offline

```bash
make test
make config
```

| File | Số test | Phạm vi |
| --- | ---: | --- |
| `app/tests/test_prediction.py` | 17 | Lịch/gap/chunk, nhãn/biên/chống leakage, sampling, fingerprint, metrics/tie, package/JVM và snapshot |
| `app/tests/test_prediction_store.py` | 7 | Mongo stateful adapter, ghi dở 100/121 dòng, hidden prepared, retry/idempotence, corruption, leading zero và model pointer |
| `app/tests/test_prediction_host.py` | 7 | Resource gate, failed stage giữ latest, tiến độ/reuse, không autobuild, selectors/status và native AUC unpersist khi lỗi |

Oracle stdlib giới hạn fixture tối đa 100.000 dòng để tránh vô tình đưa full history vào RAM Python. Test host không cần PySpark/NumPy/pandas/Mongo thật; adapter kiểm tra biên và state machine, không thay thế fit/save/load/metric trên Spark.

Regression archive khóa:

```text
ingest    ae42a5981bc20e1295df1dabbfab7e553af0f133a39c97fb52fa795ceafcb554
streaming 009c7d7d1d411e3f1810096f9c2954235ee6b92c7a3789514fe026b9af3516ac
ml        b3991c1379b84c0a440ce9ad023c411221e357dca6750cbd86bf917c6af831a9
```

## Bảy test tích hợp thật

```bash
make test-purchase
```

Fixture có 121 visitor, `001` khác `0001`, hai positive mỗi cutoff, multi-item transaction, ba cutoff theo thứ tự train/validation/test và event ngoài biên bảo đảm coverage. Nguồn typed Silver có Parquet phân vùng ngày, marker/checksum và hai replica. Suite không dùng full CSV để fit trong kiểm thử nhỏ.

| Test | Kiểm tra |
| --- | --- |
| a | Lỗi sau daily commit; index chưa xuất hiện khi thiếu ngày; retry/reuse; feature + nhãn khớp oracle từng ngày; final index reuse |
| b | Spark hash sampling khớp stdlib: giữ hai positive + 40 negative, sample reuse |
| c | Fit đủ bốn RF thật; lỗi trước model commit; selection, locked pipeline, held-out test; reload và reuse không refit |
| d | PR/ROC, confusion, daily top 1% khớp oracle trên điểm phân biệt và điểm hằng; đủ 101 threshold |
| e | Batch score/lookup/leading zero; bốn fault point, registry partial bị ẩn, retry không thêm duplicate |
| f | Feature hằng vẫn fit RF; evaluated split một lớp bị từ chối; sai code và score trước test bị từ chối |
| g | Model inventory bị chèn file hoặc daily marker bị mất phải bị từ chối |

Sáu fault scenario của suite: sau daily Gold, trước model Gold, sau score Gold, giữa Mongo scores, trước và sau score registry pointer. Các fault trước/sau model Mongo pointer còn được kiểm tra trong bộ offline. Không coi số scenario là bao phủ mọi lỗi hệ thống có thể xảy ra.

Lỗi injection cố ý có thể xuất hiện trong log. Chỉ đạt khi cuối suite `OK`, `PREDICTION_RESULT.passed=true`, `cleaned=true`, `minimum_block_replicas=2`, cả hai resource flag true và exit code 0.

Namespace HDFS là `/ecommerce/_tests/purchase/<UUID>`, in ra từ đầu suite. Cleanup trong `finally` chỉ xóa score run/model ID có source/data path của UUID đó và xóa chính HDFS test root. Có kiểm tra không còn score document/registry/model/path; cleanup lỗi làm suite fail. Không xóa topic Kafka hoặc dữ liệu nghiệp vụ.

## Nghiệm thu toàn lịch sử

Sau khi live suite đạt, chạy tuần tự ở terminal có quyền Docker, không có Spark driver khác:

```bash
make build-purchase-data
make train-purchase
make purchase-status
make score-purchases
make purchase-score-status
```

Kiểm tra các điểm sau trong report/log/resource:

1. Dataset đủ 128 ngày/20 chặng; counts từng split đúng 5.196.014/1.750, 2.560.155/729 và 2.380.550/726. Daily/index fingerprints và manifest links đầy đủ; không có index partial.
2. Sample đúng 36.750 dòng, trong đó 1.750 positive; validation/test không downsample. Tám feature đúng thứ tự, không ID/audit/nhãn/cluster trong vector.
3. Đủ bốn candidate. Winner theo PR-AUC validation và tie; threshold đúng lưới/rule. Locked selection được commit trước test, không refit và không lấy test để chọn.
4. Metrics hữu hạn, confusion cộng đúng số dòng; baseline dùng prevalence train nguyên bản; daily top 1% có `ceil(N/100)` mỗi ngày. Đọc thực tế model có hơn baseline hay không.
5. Pipeline reload sinh fingerprint đúng. Validation/test prediction chỉ ở HDFS; Mongo model report khớp selection/evaluation. Status chỉ trả published.
6. Score cutoff mặc định có 69.052 visitor, không có nhãn tương lai, score `[0,1]`, predicted label theo threshold; Mongo count/fingerprint khớp Gold và lookup đúng ID.
7. Mỗi child dưới 1.800 giây, đủ quan sát chín container, không OOM/restart, giữ limit 6,25 GiB và hai worker/hai replica. Đọc số đo thực tế, không suy rộng từ fixture.

## Nghiệm thu reuse và phục hồi

Chạy lại ba lệnh sau với cùng mã/nguồn/tham số:

```bash
make build-purchase-data
make train-purchase
make score-purchases
```

Yêu cầu `reused=true`, cùng dataset/model/score ID, cùng manifest/fingerprint/selection/test report. Không xuất hiện fit mới và không tăng score document count. Giữ cả log/resources của lần đầu và lần hai; một report cuối ghi đè ở host không đủ để so hai lượt, nhưng các attempt log vẫn được giữ riêng.

Nếu bị ngắt giữa build, chạy lại phải reuse ngày đã commit, hoàn tất các ngày thiếu rồi mới finalize. Nếu bị ngắt sau Gold/trước Mongo pointer, retry phải dùng Gold và hoàn tất registry đúng count. Nếu published payload/Mongo bị sửa, lệnh phải fail thay vì tự chữa. Khi monitor hoặc một child fail, latest host pointer trước đó phải còn nguyên.

Chỉ đánh dấu phần RF hoàn tất nghiệm thu khi có đủ artifact live suite, full history, score, reuse và resource. Khi chưa chạy được Docker, giữ trạng thái runtime pending như hiện tại.
