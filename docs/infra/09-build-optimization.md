# 09 — Tối ưu build, cache và tải tiếp

[← Mục lục](README.md)

## 1. Nguyên nhân tải chậm

Log `Adding debian:...pem` xuất hiện khi package hệ điều hành cập nhật kho chứng chỉ CA. Bước này đã hoàn tất khi log chuyển sang bảng tiến độ của curl.

Log người dùng cung cấp cho thấy đang tải Spark khoảng 382 MiB với tốc độ khoảng 40–57 KB/s, tương ứng thời gian có thể tới hai giờ. Dockerfile cũ tải trực tiếp từ Apache Archive và gộp cài Java với tải/giải nén vào cùng một RUN. Khi ngắt bước đó, không có cache riêng cho file đang tải để tiếp tục ở lần build sau.

Khi kiểm tra ngày 16/10/2026, máy cũng chưa cài `docker-buildx`. Compose dry-run cảnh báo chuyển sang classic builder, không hỗ trợ các tính năng BuildKit được dùng trong bản tối ưu này.

Kết quả kiểm tra HTTP Range với mẫu nhỏ:

| Nguồn | Kết quả |
| --- | --- |
| Apache Archive, Spark 3.5.7 | Khoảng 0,13–0,21 MiB/s |
| `repo.huaweicloud.com`, Spark 3.5.7 | Khoảng 1,6–5,1 MiB/s |
| `dlcdn.apache.org`, Hadoop 3.4.2 lean | Khoảng 1,5 MiB/s |
| `dlcdn.apache.org`, Spark 3.5.7 | HTTP 404 |

Đây là mẫu 1–2 MiB, không phải benchmark tải toàn bộ distribution hay cam kết thời gian build. Host và build container có thể có đường mạng khác nhau. Apache Archive công bố khả năng throttling khi dùng nhiều; số đo này chưa chứng minh IP của người dùng bị throttling.

## 2. Chuẩn bị Buildx trên host

Trên máy Arch Linux hiện tại, cài plugin trong terminal host:

```bash
sudo pacman -S docker-buildx
docker buildx version
docker info
docker compose version
```

Plugin Buildx cung cấp giao diện build với BuildKit. Dùng builder mặc định của Docker; không cần tạo một builder riêng cho dự án. Nếu đang đặt `DOCKER_BUILDKIT=0`, bỏ giá trị này trước khi build:

```bash
unset DOCKER_BUILDKIT
```

`make build` kiểm tra Buildx trước preflight/pull và báo lỗi rõ nếu thiếu plugin hoặc BuildKit bị tắt. Script không tự cài package host. Các action như `config`, `up`, `doctor` không bị thêm yêu cầu kiểm tra Buildx này. Build trực tiếp bằng Compose cũng cần BuildKit vì Dockerfile có `RUN --mount=type=cache`.

## 3. Nguồn tải và checksum

| Binary | Nguồn ưu tiên | Dự phòng theo thứ tự |
| --- | --- | --- |
| Spark 3.5.7, Hadoop 3 bundle | `repo.huaweicloud.com/apache/spark/` | `archive.apache.org/dist/spark/` |
| Hadoop 3.4.2 lean | `dlcdn.apache.org/hadoop/common/` | `repo.huaweicloud.com/apache/hadoop/common/`, `archive.apache.org/dist/hadoop/common/` |

Dockerfile giữ nguyên phiên bản và SHA-512 đã khóa trước đây. Checksum được lấy từ mã nguồn đã review, không lấy từ mirror tại thời điểm build. Downloader chỉ công bố binary và cho bước giải nén tiếp tục khi SHA-512 khớp. Checksum sai làm xóa phản hồi lỗi và chuyển nguồn; nếu tất cả nguồn thất bại, build dừng.

Base image tiếp tục khóa bằng digest; bốn JAR Kafka connector giữ cơ chế kiểm tra SHA-512 hiện có. Việc tải từ mirror không thay đổi topology, Java/Python runtime, tài khoản hoặc vùng dữ liệu của cụm.

## 4. Cache hoạt động thế nào?

Hai Dockerfile dùng [script tải chung](../../infra/build/download.sh). Cache nằm ở builder, được mount vào `/var/cache/apache` lúc build; không nằm trong named volume của Kafka/HDFS/MongoDB và không được đóng gói vào image.

| Cache ID | Nội dung | Công dụng |
| --- | --- | --- |
| `ecommerce-spark-distributions` | Spark binary đã xác minh và file `.part` | Dùng lại distribution/tải tiếp |
| `ecommerce-hadoop-distributions` | Hadoop binary đã xác minh và file `.part` | Dùng lại distribution/tải tiếp |
| `ecommerce-spark-pip` | Package tải bởi pip | Giảm tải lại khi phải chạy bước pip |

Mỗi binary nằm trong thư mục theo SHA-512. File `.part` có tên theo hash của URL nguồn, tránh ghép dữ liệu của hai mirror vào một file. `sharing=locked` bảo vệ cùng cache khỏi hai build ghi đồng thời; Spark và Hadoop có cache ID khác nhau nên có thể tải song song.

Trình tự downloader:

1. Kiểm tra binary hoàn tất bằng SHA-512. Đúng thì log `Cache hit` và dùng lại, không gọi mạng.
2. Nếu file `.part` đã đủ và checksum đúng, đổi tên atomic thành binary hoàn tất.
3. Với file đang tải dở, yêu cầu tải tiếp từ offset hiện có.
4. Server không hỗ trợ Range hoặc trả 416 cho partial không hợp lệ thì bỏ partial đó và thử tải mới.
5. Mỗi nguồn tối đa hai lượt; connect timeout 15 giây; tốc độ dưới 65.536 B/s liên tục 60 giây làm lượt tải thất bại.
6. Ngân sách tải tổng cộng 30 phút mỗi binary. Hết ngân sách hoặc hết nguồn thì build thất bại và giữ partial còn dùng được.

Log gồm `source=...`, `attempt=...`, `resume_bytes=...`, HTTP status và tốc độ trung bình của lượt tải. Một fallback có thể bắt đầu từ byte zero vì có partial riêng.

Cache có thể bị Docker garbage collection/prune thu hồi. Đổi builder/context hoặc chuyển sang máy khác cũng có thể không thấy cache cũ. Vì vậy, luôn bảo đảm build được khi cache rỗng. `down`/`reset` của dự án không chủ động xóa BuildKit cache.

## 5. Layer và build context

Package hệ điều hành được cài ở layer riêng trước downloader. Đổi downloader không bắt cài lại Java nếu layer cài package còn trong cache. Spark cài dependency Python và tải JAR ở hai layer khác nhau; đổi script JAR không bắt chạy lại pip.

Compose dùng context `infra/` cho cả hai image, chọn `spark/Dockerfile` hoặc `hadoop/Dockerfile`. [infra/.dockerignore](../../infra/.dockerignore) chỉ cho phép các file build cần thiết. Dataset và `.env` nằm ngoài context này; cấu hình runtime và smoke script cũng bị loại khỏi context.

Đường dẫn COPY đã đổi theo context chung. Nếu build thủ công, truyền context đúng:

```bash
docker build -f infra/spark/Dockerfile -t ecommerce-analytics/spark-runtime:3.5.7 infra
docker build -f infra/hadoop/Dockerfile -t ecommerce-analytics/hadoop:3.4.2 infra
```

## 6. Build và xử lý khi ngắt

Luồng chạy khuyến nghị sau khi có Buildx:

```bash
make test
make config
time make build
make up
make smoke
```

Để xem log build rõ hơn khi chẩn đoán riêng hai image:

```bash
docker compose --progress plain build --pull namenode spark-master
```

Có thể nhấn Ctrl+C rồi chạy lại `make build`. Downloader mới có thể tiếp tục partial nếu builder giữ cache. File tải dở trong bước RUN của Dockerfile cũ không tự chuyển sang cache mới; lần đầu sau cập nhật có thể phải tải lại distribution.

Build lại bình thường để tận dụng layer cache. Nếu cần kiểm tra cache download khi layer bị buộc chạy lại, có thể dùng `docker compose --progress plain build --no-cache namenode spark-master`; lệnh này chạy lại cả các bước cài đặt, nhưng cache mount vẫn có thể được dùng. Không cần prune Docker để thử lại một lượt tải lỗi.

## 7. Kiểm chứng và giới hạn hiện tại

- Tại phiên tối ưu build, suite hạ tầng có 30 test: 23 đạt, 7 test HTTP skipped vì môi trường cấm tạo socket loopback. Số lượng hiện tại sau khi bổ sung kiểm thử được ghi trong [tài liệu kiểm thử](06-testing.md).
- Ba test cache dùng hashing thật và chạy script Bash thật, không cần network.
- Bảy test HTTP dùng curl thật với server cục bộ: mất kết nối, ngắt bằng SIGTERM, tải tiếp, không hỗ trợ Range, 416, cache, checksum sai và fallback. Cần chạy lại trên host; kết quả skipped không chứng minh các nhánh này đã chạy thành công.
- Compose config, cú pháp Python/Bash và diff whitespace đã được kiểm tra trong phiên cập nhật.
- Chưa xác nhận build image, cache mount qua lần ngắt thực tế hoặc smoke trên cụm: phiên công cụ chưa có Buildx và không truy cập được Docker daemon. Thời gian build toàn bộ cần đo bằng `time make build` trên host.

Nguồn tham khảo: [Docker build cache](https://docs.docker.com/build/cache/optimize/), [BuildKit](https://docs.docker.com/build/buildkit/), [Arch docker-buildx](https://archlinux.org/packages/extra/x86_64/docker-buildx/), [Apache Archive](https://archive.apache.org/), [curl options](https://curl.se/docs/manpage.html).
