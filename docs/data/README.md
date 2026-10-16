# Dữ liệu lịch sử — Bronze, Silver và chất lượng

[← Tài liệu dự án](../README.md)

Giai đoạn này triển khai luồng **Retailrocket CSV → HDFS Bronze → Spark → HDFS Silver**, cùng manifest, cách ly lỗi và báo cáo chất lượng. Nguồn ứng dụng nằm trong [app/](../../app/); hạ tầng được mô tả riêng trong [docs/infra/](../infra/README.md).

| Tài liệu | Nội dung |
| --- | --- |
| [01 — Data dictionary](01-data-dictionary.md) | Bốn file nguồn, ý nghĩa cột, định danh và giới hạn dữ liệu |
| [02 — Hợp đồng và lưu trữ](02-contract-and-storage.md) | Schema Silver, ID, phiên bản, layout HDFS, manifest và commit |
| [03 — Chạy ingest](03-ingestion.md) | Chuẩn bị, lệnh chạy, báo cáo, chạy lại và xử lý lỗi |
| [04 — Chất lượng dữ liệu](04-data-quality.md) | Quy tắc kiểm tra, mã lỗi, đối soát và baseline của dataset |
| [05 — Kiểm thử và trạng thái](05-testing.md) | Unit test, fixture Spark/HDFS, điều kiện nghiệm thu và phần chưa xác minh |

## Phần đã có trong mã nguồn

- Nạp nguyên bản bốn CSV lên Bronze, xác minh SHA-256.
- Chuẩn hóa `events.csv`, lưu Parquet phân vùng ngày UTC.
- Giữ dòng hợp lệ đầu tiên của mỗi event ID; lưu nhóm trùng và dòng lỗi riêng.
- Công bố snapshot không ghi đè, xác minh lại khi chạy lại.
- `make ingest`, `make test-data`, unit test ứng dụng trong `make test`.

Phạm vi kiểm tra chất lượng nội dung hiện là `events.csv`. Ba CSV còn lại được kiểm tra header, fingerprint và bản sao Bronze; chưa được chuyển thành bảng Silver.

Producer replay, streaming nghiệp vụ, KPI/funnel, feature, ML, API và dashboard là các giai đoạn tiếp theo.

## Chạy sau khi đã build image

Chạy từ thư mục gốc repository:

```bash
make up
make doctor
make smoke
make test
make test-data
make ingest
```

`make up` cập nhật mount ứng dụng cho Spark client. Mã ứng dụng được đóng ZIP và chuyển tới executor bằng `--py-files`; thay đổi này không cần build lại image. Giữ ngân sách container 7 GiB và một Spark application tại một thời điểm.

## Trạng thái kiểm chứng

Unit test và đối soát nguồn offline có kết quả riêng; hiện chưa có bằng chứng chạy ingest hoặc fixture trên HDFS thật trong phiên công cụ, vì truy cập Docker socket bị từ chối. Xem [bảng kiểm chứng](05-testing.md). Việc có mã nguồn hoặc kết quả kiểm tra CSV offline không đồng nghĩa Silver đã được tạo trong HDFS.
