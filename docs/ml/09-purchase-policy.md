# 09 — Chính sách ngưỡng ưu tiên precision

[← Mục lục ML](README.md) · [RF gốc](06-random-forest.md) · [Vận hành RF](07-purchase-operations.md)

## Mục tiêu và kết quả hiện biết

Người dùng chọn mục tiêu **precision ≥70%, recall ≥10%**. Ngưỡng RF gốc 0,93 được chọn để tối đa F1; chính sách mới chọn nhóm dự đoán mua chắc hơn và chấp nhận bỏ sót nhiều lượt mua hơn.

| Ngưỡng | Precision validation | Recall validation | TP / tổng dự đoán mua |
| --- | ---: | ---: | ---: |
| 0,93, bản gốc | 42,90% | 19,48% | 142/331 |
| 0,95 | 59,79% | 15,91% | 116/194 |
| 0,96 | 79,65% | 12,35% | 90/113 |

Đây là số từ **validation đã lưu**, không phải kết quả test của policy mới. Test RF gốc tại 0,93 có precision 43,63%, recall 21,21%, TP=154, FP=199. Mã policy mới chưa chạy được trên cụm trong phiên công cụ vì Docker socket bị từ chối; chưa có số test thật tại 0,96 hoặc số visitor vượt ngưỡng mới để công bố.

`purchase_score` là điểm RF chưa hiệu chuẩn sau negative sampling. Điểm 0,96 không có nghĩa xác suất mua 96%. Thay threshold chỉ đổi `predicted_label`, không làm PR-AUC/ROC-AUC hoặc thứ tự xếp hạng tốt lên.

## Quy tắc chọn, đánh giá và áp dụng

1. Đọc model `published`, kiểm tra identity/Spark version, model/index/winning-candidate manifest, metadata và hai replica HDFS. Đọc đúng validation predictions của candidate thắng; không dựng feature hoặc fit lại.
2. Đối chiếu fingerprint validation với RF gốc, tính lại đủ 101 ngưỡng `0.00..1.00` và so với bảng đã lưu. Mỗi ngưỡng phải có confusion/count hợp lệ, cùng cohort và số dự đoán giảm theo ngưỡng.
3. Chọn trong các ngưỡng có precision ≥0,70, recall ≥0,10 và ít nhất một dự đoán mua. Ưu tiên recall cao nhất; hòa thì precision cao hơn, rồi ngưỡng cao hơn. Với validation hiện có, kết quả dự kiến là **0,96**; không hardcode ngưỡng này.
4. Commit snapshot selection trước khi truy cập test payload. Không có ngưỡng đạt thì lưu lý do, `threshold=null`, `test_skipped=true`, `eligible=false`; không đọc test để tìm ngưỡng thay thế.
5. Nếu validation đạt, kiểm tra original test evaluation/prediction, fingerprint và nhãn gốc; đánh giá đúng một ngưỡng đã khóa. Chỉ `eligible=true` khi **cả validation và test** đạt mục tiêu. Test không đạt thì giữ ngưỡng đã chọn, ghi `test_targets_not_met`, chặn apply; không dò lại test để chọn ngưỡng khác.
6. Apply chỉ nhận original score run của cùng model, policy hợp lệ và đúng policy code/Spark version. Kiểm tra nguồn, đổi label bằng `purchase_score >= threshold`, giữ nguyên audit/tám raw feature/score và đủ mọi visitor. Không lọc bỏ visitor dưới ngưỡng.

Test trước đó đã được xem nên báo cáo policy ghi `evaluation_kind=posthoc_threshold_reassessment`, `fresh_holdout=false`. Đây là đánh giá lại chính sách trên dữ liệu hiện có; để có xác nhận độc lập mạnh hơn cần dữ liệu tương lai chưa sử dụng hoặc một thực nghiệm theo thời gian được thiết kế trước.

PR-AUC, ROC-AUC và daily top 1% được kế thừa từ report gốc sau khi xác minh toàn bộ score/label fingerprint: các chỉ số này không đổi theo threshold. Precision/recall/F1/confusion được tính lại. Report giữ `validation_original`, `validation`, `test_original`, `test`, mục tiêu, lý do và nguồn manifest để so sánh trực tiếp.

## Lệnh vận hành

Chạy từ thư mục gốc, với RF gốc và original batch score đã công bố, không có Spark driver khác:

```bash
make test
make test-purchase-policy
make tune-purchase-policy
make purchase-policy-status
```

Đọc `eligible` và phần `report.evaluation`. `passed=true` nghĩa tác vụ phân tích chạy đúng; **không đồng nghĩa đã đạt precision/recall**. `eligible=false` vẫn có report/status và không cho apply. `latest-policy.json` chọn lần đánh giá mới nhất, kể cả kết quả không đạt; hệ thống không tự quay về một policy cũ để che kết quả.

Khi `eligible=true`, chạy:

```bash
make apply-purchase-policy
```

Lệnh in ra score run ID mới và câu lệnh lookup. Có thể lấy đúng ID này bằng:

```bash
POLICY_SCORE_RUN_ID="$(python3 -c 'import json; print(json.load(open("artifacts/prediction/policies/latest-policy-score.json"))["score_run_id"])')"
make purchase-score-status SCORE_RUN_ID="$POLICY_SCORE_RUN_ID"
make purchase-score-status SCORE_RUN_ID="$POLICY_SCORE_RUN_ID" VISITOR_ID=001
```

Không truyền `SCORE_RUN_ID` cho status thì vẫn xem score **gốc**, vì policy không thay pointer `latest-purchase-score.json`. Visitor là chuỗi: `001` khác `1`.

| Lệnh | Công dụng | Selector mặc định |
| --- | --- | --- |
| `tune-purchase-policy` | Chọn ngưỡng, khóa selection, đánh giá lại test, công bố policy | `MODEL_VERSION` từ latest purchase model |
| `purchase-policy-status` | Đọc policy/report Mongo, không chạy Spark | `POLICY_VERSION` từ latest policy |
| `apply-purchase-policy` | Tạo score run mới từ điểm batch gốc | `POLICY_VERSION` từ latest policy; `SCORE_RUN_ID` từ latest original score |
| `test-purchase-policy` | Năm test Spark/HDFS/Mongo trong namespace UUID | Không dùng dữ liệu nghiệp vụ |

Các selector có thể truyền qua Make như `MODEL_VERSION=...`, `POLICY_VERSION=...`, `SCORE_RUN_ID=...` với ID 64 hex thực từ report. `AS_OF` không áp dụng ở bước policy: cutoff lấy từ original score run. Nếu muốn cutoff khác, tạo original batch bằng `score-purchases AS_OF=...` trước. Policy v1 cố định mục tiêu 0,70/0,10; không nhận biến môi trường để đổi ngưỡng/mục tiêu ngoài identity.

Không áp dụng policy lên output của một policy khác; nguồn phải là `purchase-score-v1` gốc. Sai model/code/Spark, source fingerprint, metadata hoặc payload đều bị từ chối. Ngưỡng mới không sửa model/report/prediction đã công bố trước đó.

## Phiên bản và dữ liệu lưu

`policy_version` là SHA-256 identity có model ID, model/index/candidate/original-evaluation manifest hash, prediction code SHA gốc, policy code SHA riêng, Spark version, mục tiêu và selection rule. Đổi policy code tạo phiên bản policy mới, không đổi identity dataset/model gốc.

```text
HDFS:
  <base>/models/purchase-policies/<policy_version>/selection/
    selection.json, manifest.json, _SUCCESS
  <base>/models/purchase-policies/<policy_version>/evaluation/
    evaluation.json, manifest.json, _SUCCESS
  <base>/gold/purchase-policy-scores/<new_score_run_id>/
    scores/, quality.json, manifest.json, _SUCCESS

Host:
  artifacts/prediction/policies/
    latest-policy.json
    latest-policy-score.json
    test-purchase-policy.json
    attempts/logs/
    tests/logs/
    <policy_version>/report.json, status.json, logs/
  artifacts/prediction/<new_score_run_id>/report.json
```

Registry Mongo `purchase_policies` lưu model, selection/evaluation paths, trạng thái và report. Batch mới dùng lại `purchase_score_runs` và `purchase_scores`, nhưng có ID mới; registry và từng visitor có thêm `policy_version`, `source_score_run_id`. Vì vậy lệnh `purchase-score-status` gốc vẫn đọc được.

Ghi HDFS bằng staging, kiểm SHA/inventory/hai replica rồi rename không ghi đè. Policy registry được published sau khi hai snapshot hoàn tất. Score Mongo upsert 100 dòng/lô, đối chiếu toàn bộ fingerprint/count/metadata rồi cập nhật registry cuối cùng. Khi đã published, retry chỉ xác minh; không tự sửa dữ liệu bị hỏng. Latest host pointer chỉ cập nhật khi child và monitor đều đạt. Nếu Mongo đã commit nhưng host chưa cập nhật, chạy lại cùng lệnh/identity để hoàn tất.

Ctrl+C rồi chạy lại `tune-purchase-policy` hoặc `apply-purchase-policy` sẽ reuse snapshot đã commit. Nếu ngắt sau selection, test vẫn dùng đúng ngưỡng đã khóa. Nếu ngắt sau Gold hoặc giữa Mongo, retry dùng Gold và deterministic ID, không thêm duplicate. Snapshot published bị mất/hỏng phải được điều tra/khôi phục.

## Mã nguồn và tài nguyên

| Thành phần | Vai trò |
| --- | --- |
| `app/purchase_policy_manage.py` | CLI host, selector, pointer riêng, resource gate |
| `app/jobs/purchase_policy.py` | Kiểm archive/spec SHA, dispatch Spark/status và lifecycle |
| `app/ecommerce_purchase_policy/contracts.py` | Targets, validation grid, selection rule và identities |
| `workflow.py` | Kiểm nguồn, freeze selection, reassess test, relabel score |
| `store.py` | Policy registry, immutable publication và score Mongo theo policy |
| `integration.py` | Fixture scalar score trên Spark/HDFS/Mongo và fault/cleanup |

Archive mới `purchase-policy` chứa analytics + ML + prediction + policy. Bốn archive cũ giữ nguyên SHA, đặc biệt prediction `a8b1243293bd1ed091ccb1cbcf1673ca8437d4ef48f0e50b1749d72ec3c7e4e0`. Không sửa module đã đóng gói của RF, không build lại image/dependency/service và không cần chạy lại `build-purchase-data` hay `train-purchase` cho policy này.

Tái sử dụng global driver lock, cancel guard, timeout 1.800 giây/child và monitor. Giữ hai core Spark, driver 512 MiB, executor 640 MiB/worker, tổng limit container 6,25 GiB. Validation/test dùng `DISK_ONLY` và fingerprint theo ngày; không collect hàng triệu dòng. Apply chỉ đọc batch scores hiện có, không tải RF pipeline và không truy cập Silver để tạo feature.

## Kiểm thử và nghiệm thu

26 test offline mới: 8 contract/package, 7 Mongo boundary, 5 host/pointer và 6 workflow/snapshot. Kiểm tra chọn ngưỡng, biên `>=`, no-feasible/zero-prediction, metric/grid hỏng, khóa selection trước test, validation đạt/test fail, report posthoc, reuse không tính lại, giữ nguyên pointer gốc, metadata và partial Mongo.

`make test-purchase-policy` có năm test tích hợp và sáu fault scenario: sau selection, trước policy registry pointer, sau score Gold, giữa Mongo score, trước/sau score registry pointer. Fixture tạo typed scalar predictions có 121 visitor, source model metadata và sentinel pipeline không thể load; không fit RF. Test vá fit/feature-builder/model-loader để fail nếu policy gọi các bước đó. Đây là kiểm thử xử lý policy trên Spark/HDFS/Mongo, không thay thế kiểm thử huấn luyện RF gốc.

Suite kiểm tra qualification, test fail/no feasible, apply/reuse, raw score giữ nguyên, leading zero, zero-positive batch hợp lệ, sai model/code/nested policy, corruption và immutable original run. Namespace `/ecommerce/_tests/purchase-policy/<UUID>`; `finally` xóa/kiểm tra HDFS root và document có model ID thuộc đúng namespace đó. Cleanup lỗi làm lệnh fail.

Nghiệm thu thực tế cần: live suite `passed=true, cleaned=true`, policy test đạt hai mục tiêu hoặc báo rõ không đạt; apply đủ 69.052 visitor cho cutoff mặc định, score-only fingerprint không đổi, label đúng threshold, Mongo published/count khớp; lần hai `reused=true`, không duplicate, resource đủ quan sát/không OOM/restart. Nếu policy test fail thì dừng apply và giữ score gốc.

Hiện tại 26 offline test mới đã đạt; `make test-purchase-policy` đã thử nhưng bị chặn tại preflight Docker socket. **Policy mới chưa có runtime acceptance**, không coi số validation 79,65% là precision test đã đạt.
