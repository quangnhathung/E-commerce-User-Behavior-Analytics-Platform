# 06 — Huấn luyện Random Forest và kiến trúc công bố

[← Mục lục ML](README.md) · [Dữ liệu](05-purchase-data.md) · [Vận hành →](07-purchase-operations.md)

## Pipeline và lựa chọn mô hình

Pipeline Spark ML gồm `VectorAssembler` với tám feature thô theo thứ tự cố định, rồi `RandomForestClassifier`. Các stage chạy qua JVM bridge đã dùng cho K-Means. Driver không import `pyspark.ml` Python, NumPy hoặc pandas; không cần dependency/image mới. Kết quả vector của Spark được chuyển thành scalar rồi bỏ khỏi DataFrame trước khi Python đọc schema.

Huấn luyện chạy **bốn cấu hình tuần tự**:

| Candidate | `numTrees` | `maxDepth` |
| --- | ---: | ---: |
| 1 | 50 | 5 |
| 2 | 50 | 8 |
| 3 | 100 | 5 |
| 4 | 100 | 8 |

Thông số chung: seed 42, `featureSubsetStrategy=sqrt`, `impurity=gini`, `maxBins=32`, `maxMemoryInMB=64`, `cacheNodeIds=false`, `minInstancesPerNode=1`, `minInfoGain=0`, `subsamplingRate=1`, `bootstrap=true`. `maxMemoryInMB` là bộ nhớ cho thống kê dựng cây, không phải giới hạn tổng RAM của job.

Mỗi candidate fit trên sample train, lưu pipeline và toàn bộ validation predictions vào HDFS, lưu metrics/feature importance/fingerprint. Tải lại candidate phải cho cùng fingerprint trên sample; khi chọn mô hình, pipeline đã tải lại phải cho cùng fingerprint trên toàn validation. Chạy lại candidate đã commit kiểm tra snapshot và predictions, không gọi fit lại.

Chọn **PR-AUC validation cao nhất**. Chênh lệch tối đa `1e-12` được xem là hòa; ưu tiên ít cây hơn, rồi depth nhỏ hơn. Phải có đủ bốn candidate hợp lệ. Lỗi vận hành, lớp dữ liệu thiếu, score không hợp lệ hoặc snapshot hỏng làm lệnh fail; không bỏ qua candidate bị lỗi để chọn từ tập còn lại.

Sau đó chọn threshold trên validation bằng lưới cố định `0.00, 0.01, …, 1.00`: ưu tiên F1 cao nhất, tiếp đến precision, rồi threshold cao hơn. Quy tắc dự đoán là `purchase_score >= threshold`. Lưu model được chọn cùng selection/threshold trước khi đọc test. **Không refit trên train + validation** sau khi chọn.

Đây là quy tắc của RF gốc. Khi cần ưu tiên precision ≥70%, recall ≥10%, sử dụng [policy riêng](09-purchase-policy.md) trên các score đã lưu; không sửa threshold/selection của model gốc đã published.

## Score và các chỉ số

`purchase_score` là phần tử thứ 1 của vector `probability` từ RF, hữu hạn trong `[0,1]`. Negative sampling làm thay đổi phân phối train; điểm này **chưa được hiệu chuẩn theo tỷ lệ mua thực tế**. Dùng để xếp hạng/so ngưỡng, không hiển thị như “xác suất mua X%”. `predicted_label` là integer theo ngưỡng validation, không dùng hard prediction mặc định của RF.

| Chỉ số | Công dụng |
| --- | --- |
| PR-AUC | Chỉ số chính để so candidate trên dữ liệu ít positive |
| ROC-AUC | Bổ sung đánh giá khả năng xếp hạng hai lớp |
| Precision | Trong các mẫu dự đoán mua, tỷ lệ thật sự mua |
| Recall | Trong các mẫu thật sự mua, tỷ lệ được phát hiện |
| F1 | Trung bình điều hòa precision/recall tại ngưỡng đã khóa |
| TP/FP/FN/TN | Đếm đầy đủ confusion matrix, phục vụ đối soát |
| Precision@1% | Mức tập trung positive trong nhóm ưu tiên nhỏ mỗi ngày |
| Feature importance | Tầm quan trọng impurity của tám feature; không suy ra quan hệ nhân quả |

PR/ROC dùng Spark `BinaryClassificationEvaluator`, `numBins=0` để không giảm số điểm trên đường cong. PR-AUC ở đây là **diện tích hình thang theo quy ước Spark**, không phải chỉ số average precision. Bộ oracle nhỏ khớp cùng quy ước. RDD trung gian của native metrics được `unpersist()` trong `finally`.

Precision@1% tính riêng từng cutoff: chọn đúng `ceil(0.01 × cohort_ngày)` dòng, score giảm dần; khi bằng điểm, visitor theo thứ tự chuỗi tăng dần. Báo cáo tổng positive được chọn / tổng dòng được chọn trên mọi ngày của split, kèm đếm từng ngày. Không chọn top 1% gộp cả tháng vì cách đó có thể bỏ qua nhiều ngày.

Validation và test đều được so với hai baseline trên **cohort nguyên bản**:

1. Luôn không mua: score `0`, threshold `1`.
2. Score hằng bằng prevalence của train trước sampling, threshold `0.5`.

Hai baseline đều có AUC, confusion và top 1% theo cùng quy tắc tie. Model report ghi `beats_prevalence_pr_auc` đúng theo kết quả test. Không đặt trước con số accuracy hay đảm bảo RF thắng baseline. Tất cả split được đánh giá AUC phải có cả hai lớp.

## Luồng lưu trữ

```text
Silver đã kiểm tra
  → 128 daily labeled snapshots → purchase index
  → exact training sample
  → 4 candidate pipelines + validation predictions
  → selection + locked pipeline + threshold
  → held-out test evaluation + predictions
  → Mongo purchase_models: published
  → historical batch features → scalar scores
  → HDFS score snapshot → Mongo scores → published score registry
```

| Nơi lưu | Nội dung |
| --- | --- |
| `/ecommerce/gold/purchase-samples/<sample_id>/` | Sample Parquet, quality, manifest/marker |
| `/ecommerce/models/purchase-candidates/<model_version>/<candidate_id>/` | Pipeline, validation scores phân vùng `cutoff_date`, metrics/fingerprint |
| `/ecommerce/models/purchases/<model_version>/` | Pipeline được khóa, `selection.json`, manifest/marker |
| `/ecommerce/gold/purchase-evaluations/<evaluation_id>/` | Test scores theo `cutoff_date`, `evaluation.json`, manifest/marker |
| `/ecommerce/gold/purchase-scores/<score_run_id>/` | Batch score Parquet, quality, manifest/marker |

Mọi payload được kiểm tra SHA và tối thiểu hai block replica trước rename không ghi đè. Model version gắn dataset/index/sample manifest, code SHA, Spark version, thứ tự feature, RF params, candidate grid và quy tắc chọn. Evaluation identity gắn manifest của model đã khóa, nên test không thể âm thầm thay model selection. Candidate/selection/evaluation đã commit được tái sử dụng theo cùng identity.

## MongoDB và tính nhất quán

| Collection | Nội dung và cách đọc |
| --- | --- |
| `purchase_models` | Identity, HDFS paths, selection, validation/test metrics, baselines, trạng thái model |
| `purchase_score_runs` | Identity nguồn/model/cutoff/threshold, path, coverage/fingerprint và trạng thái score batch |
| `purchase_scores` | Một document cho mỗi `(score_run_id, visitor_id)`, tám raw feature, audit, score, label và metadata |

Không đưa hàng triệu validation/test prediction vào MongoDB; các prediction đó nằm trên HDFS. Mongo chỉ nhận model metadata và batch score cohort được yêu cầu. `purchase_scores` có unique index `(score_run_id, visitor_id)` và `_id = score_run_id + ':' + visitor_id`.

Registry bắt đầu ở `prepared`. Ghi score bằng upsert tối đa 100 document mỗi lô, đọc lại và đối chiếu **toàn bộ** count/fingerprint/metadata với Gold, rồi mới cập nhật registry `published`. Lệnh status chỉ trả model/run đã published; partial Mongo chưa trở thành kết quả công bố. Retry sau khi ghi dở dùng lại Gold và deterministic ID, không nhân đôi document.

Nếu registry đã published, retry xác minh dữ liệu hiện có thay vì sửa ngầm document hỏng. Nếu Gold của bản đã published bị mất, hệ thống yêu cầu khôi phục snapshot. Host latest pointer chỉ cập nhật sau khi child trả kết quả thành công và monitor có đủ quan sát, không OOM/restart. Mongo có thể đã commit khi host bị ngắt hoặc monitor fail; chạy lại đúng identity để đối chiếu và hoàn tất pointer host.

Status là thao tác đọc metadata/visitor trong Mongo, không chạy Spark và không tái kiểm toàn bộ HDFS. Để kiểm chứng checksum, reload và coverage toàn bộ, chạy lại build/train/score tương ứng.

## Vai trò mã nguồn

| File/module | Công dụng |
| --- | --- |
| `app/prediction_manage.py` | CLI host, orchestration 20 chặng/bốn candidate, progress/latest và resource gate |
| `app/jobs/prediction.py` | Kiểm archive/spec SHA, dispatch child, quản lý Spark/Store và kết quả JSON |
| `ecommerce_prediction/contracts.py` | Hợp đồng, lịch, hash sampling, fingerprint, oracle nhỏ và tie rules |
| `data.py` | Silver validation, feature/nhãn từng ngày, snapshot và composite index |
| `training.py` | Sample, pipeline JVM, candidate, selection khóa và test evaluation |
| `metrics.py` | Exact PR/ROC, 101 threshold, confusion, baseline và daily top 1% |
| `scoring.py` | Kiểm model/code/Spark/source, batch score, reload fingerprint và công bố |
| `store.py` | Ba collection riêng, partial visibility, upsert và registry cuối cùng |
| `integration.py` | Fixture trên Spark/HDFS/Mongo và fault injection/cleanup theo UUID |

Archive profile `prediction` chứa analytics + ML + prediction; không đưa streaming/behavior vào gói mới. Ba archive ingestion/streaming/ML cũ được khóa hash trong regression để bảo vệ các phiên bản đã nghiệm thu.
