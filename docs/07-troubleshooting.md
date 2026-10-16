# 07 — Chẩn đoán và xử lý lỗi

[← Mục lục](README.md)

## 1. Quy trình bắt đầu

1. Xác định lệnh nào thất bại: setup, build, up, readiness hay job.
2. Đọc dòng lỗi đầu tiên liên quan, không chỉ thông báo cuối của Make.
3. Xem `make ps` và log đúng service/job.
4. Đối chiếu cấu hình với [bảng cấu hình](03-configuration.md).
5. Sửa nguyên nhân, chạy lại bước cần thiết rồi kiểm tra readiness.

Reset là thao tác xóa dữ liệu, không phải bước chẩn đoán mặc định.

## 2. Lỗi host và Docker

| Triệu chứng                             | Nguyên nhân có thể                                          | Kiểm tra                                                  | Khắc phục và dấu hiệu phục hồi                                                            |
| --------------------------------------- | ----------------------------------------------------------- | --------------------------------------------------------- | ----------------------------------------------------------------------------------------- |
| Permission denied với Docker socket     | Phiên không có quyền daemon hoặc socket không được cung cấp | `docker info`, `docker context show`, `docker context ls` | Chạy trong terminal/context được cấp quyền hợp lệ; phục hồi khi info trả thông tin server |
| CLI có nhưng không kết nối server       | Daemon chưa chạy, context/DOCKER_HOST sai                   | Kiểm tra context và Docker daemon tại máy thực thi        | Chọn đúng môi trường hoặc khởi động daemon theo cơ chế của máy; info phải đạt             |
| Chỉ một số lệnh Docker chạy được        | Giới hạn của môi trường thực thi/agent                      | Thử info tại terminal host có quyền                       | Không suy ra build sẽ chạy được từ việc ps thành công; kiểm thử runtime cần quyền thật    |
| Cổng đã bị chiếm                        | Dịch vụ khác đang publish cổng                              | `ss -ltnp`, đối chiếu biến cổng                           | Chọn cổng host khác trong cấu hình; không dừng dịch vụ khác chỉ để giải phóng cổng        |
| Không đủ RAM                            | Host đang dùng nhiều RAM hoặc Docker bị giới hạn            | RAM host và MemTotal trong docker info                    | Giải phóng RAM/cấp đủ cho Docker rồi chạy lại preflight                                   |
| Thiếu disk                              | Docker data partition đầy                                   | Xem DockerRootDir và dung lượng tại máy chạy daemon       | Giải phóng đúng vùng hoặc tăng dung lượng; không tự prune tài nguyên toàn máy             |
| Không thấy Docker data path trong phiên | Daemon ở filesystem/máy khác                                | Xác định context và máy chạy daemon                       | Kiểm tra trực tiếp tại Docker host; thông báo NOTE không phải bằng chứng disk đủ          |
| Sai kiến trúc/AVX                       | Máy không đáp ứng bản runtime này                           | Docker architecture và CPU flags                          | Dùng môi trường Linux amd64 hỗ trợ AVX; không đổi phiên bản MongoDB âm thầm               |
| Dataset missing/unreadable              | Sai đường dẫn, thiếu CSV, thiếu quyền đọc                   | Kiểm tra bốn file và DATASET_PATH                         | Sửa đường dẫn/quyền đúng thư mục, chạy lại preflight                                      |

Cấu hình dataset được preflight kiểm tra ngay cả trước build. Một clone repository không có CSV cần được cung cấp dataset trước; Git không lưu các file CSV/ZIP.

Cấu hình hiện tại cần 7 GiB khả dụng lúc chưa chạy cụm, với tổng memory limit 6,9375 GiB. Nếu vẫn thấy `need 10.0 GiB`, kiểm tra riêng `MIN_AVAILABLE_RAM_GIB` trong `.env` và đổi thành `7`; `make setup` giữ nguyên file đã có. Không hạ thấp hơn tổng limit để bỏ qua kiểm tra. Khi thấy `Configured container memory limits ... exceeding`, cần chỉnh đồng bộ Compose và ngân sách. [Cách áp dụng và theo dõi OOM](10-memory-budget.md).

## 3. Lỗi setup và cấu hình

| Triệu chứng                              | Ý nghĩa                            | Hướng xử lý                                                   |
| ---------------------------------------- | ---------------------------------- | ------------------------------------------------------------- |
| Missing .env                             | Chưa setup                         | Chạy `make setup`                                             |
| Missing/uninitialized environment values | Thiếu biến hoặc còn placeholder    | Bổ sung theo template; setup không ghi đè file đã có          |
| Duplicate environment variable           | Trùng khóa trong file              | Giữ một định nghĩa rõ ràng cho mỗi biến                       |
| Quote values containing spaces           | Giá trị có khoảng trắng chưa quote | Quote đường dẫn/giá trị theo định dạng file môi trường        |
| Invalid project/cluster ID               | Không đúng định dạng validator     | Khôi phục giá trị đã sinh hoặc cấu hình hợp lệ phù hợp volume |
| Published host ports must be distinct    | Hai biến publish dùng cùng cổng    | Chọn các cổng khác nhau                                       |
| Image must be pinned by SHA-256 digest   | Đã bỏ digest khỏi image            | Dùng cấu hình image đã khóa và xác minh digest trước khi đổi  |

Không gửi nội dung file local có password vào issue/log chia sẻ. Chỉ trích tên biến và lỗi cần chẩn đoán.

## 4. Lỗi build và dependency

### Tải rất chậm, bị ngắt hoặc thiếu Buildx

Xem [hướng dẫn tối ưu build](09-build-optimization.md). Các dòng `Adding debian:...pem` là cập nhật CA bình thường; log curl cho biết bước tải binary. `make build` yêu cầu Buildx; trên Arch Linux dùng `sudo pacman -S docker-buildx`, kiểm tra `docker buildx version` rồi chạy lại.

Build mới lưu file tải dở trong cache của builder; Ctrl+C rồi chạy lại `make build` có thể tiếp tục nếu cache còn. File tải dở từ Dockerfile cũ không tự chuyển vào cache mới.

### Tải binary/JAR hoặc checksum thất bại

Kiểm tra network đến `repo.huaweicloud.com`, `dlcdn.apache.org`, Apache Archive, Maven Central, PyPI và registry của base image. Downloader có hai lượt mỗi nguồn, phát hiện tốc độ thấp và ngân sách 30 phút mỗi binary; đọc log source/resume_bytes trước khi thử lại. Nếu là lỗi tạm thời, thử build lại. Nếu checksum không khớp, kiểm tra đúng version/artifact; không bỏ bước checksum để làm build “qua”.

### Image local chưa có

`make up` chạy với `--no-build`. Chạy:

```bash
make build
make up
```

### Không tìm được Kafka data source/JAR

Đối chiếu [script JAR](../infra/spark/download_jars.py), phiên bản Spark/Scala và các image đang dùng:

```bash
docker compose images
docker compose logs --tail 100 spark-client
```

Build lại image Spark và dựng lại các service dùng image đó. Việc cài package trên host không tự cài package vào executor.

## 5. Kafka và init topic

### Broker chưa healthy

```bash
docker compose logs --tail 100 kafka
docker compose logs --tail 100 kafka-init
```

Đối chiếu listeners, metadata storage và cluster ID. Producer container phải dùng `kafka:9092`; client host dùng cổng external.

### Cluster ID mismatch

Khôi phục cluster ID đúng với volume hiện tại hoặc khôi phục bộ cấu hình/volume khớp nhau. Không sửa metadata để ép một storage vào cluster khác.

### Init báo sai partitions/replication/retention

Đọc metadata bằng các lệnh trong [hướng dẫn vận hành](05-operations.md). Init cố ý báo lỗi, không tự xóa topic. Nếu thay đổi cấu hình topic là chủ ý, thực hiện thay đổi có kiểm soát rồi chạy lại init/up; việc thay replication hoặc partition có những ràng buộc riêng, không coi là đổi một biến host.

## 6. HDFS

### Không đủ hai DataNode hoặc safe mode chưa OFF

```bash
docker compose exec -T namenode hdfs dfsadmin -report
docker compose exec -T namenode hdfs dfsadmin -safemode get
docker compose logs --tail 100 namenode datanode-1 datanode-2 hdfs-init
```

Kiểm tra node nào chưa đăng ký, DNS/network, volume và cluster ID. NameNode HTTP healthy nhưng HDFS chưa đủ điều kiện ghi là tình huống init/readiness được thiết kế để phát hiện.

Không dùng lệnh cưỡng bức rời safe mode để che việc node/replica chưa đủ. Phục hồi khi có hai live node và safe mode OFF.

### VERSION hoặc cluster ID lỗi

NameNode từ chối format nếu storage không rỗng nhưng thiếu VERSION. Nếu VERSION tồn tại mà ID khác, kiểm tra có mount nhầm volume hoặc dùng nhầm cấu hình local không.

Bản hiện tại chưa có script tự động backup/restore namespace. Không xóa VERSION rồi format lại để “sửa nhanh” storage có dữ liệu cần giữ.

### Permission denied

Đối chiếu UID/GID 1000 của image custom, owner volume và quyền thư mục HDFS. Init tạo gốc `/ecommerce` cho môi trường user `analytics`. Sửa đúng ownership/mount gây lỗi; không dùng chmod toàn bộ workspace như cách xử lý chung.

## 7. Spark

### Master có UI nhưng không có hai worker

```bash
docker compose logs --tail 100 spark-master spark-worker-1 spark-worker-2
docker compose exec -T spark-client python3 /opt/infra/smoke/readiness.py --once
```

Nếu client chưa khởi động do dependency, mở master UI và xem log worker trực tiếp. Kiểm tra master URL và thông tin worker quảng bá.

### Executor không gọi được driver

Driver phải nằm trong `spark-client`, bind `0.0.0.0` và quảng bá hostname `spark-client`. Kiểm tra cấu hình RPC 39001, driver block manager 39002 và network chung.

Chạy Spark job trên host bằng local mode không phải cách chứng minh worker trong Docker xử lý được job.

### Job khác đang chạy hoặc cổng driver bận

Khóa manager chỉ bảo vệ các job chạy qua manager. Kiểm tra có lệnh `docker compose exec ... spark-submit` trực tiếp đang chạy không. Đợi/dừng đúng job trước khi nộp job tiếp; không chạy nhiều driver với cùng bộ cổng cố định.

### Application UI không mở

Nếu chưa có ứng dụng chạy hoặc ứng dụng đã kết thúc, đây có thể là trạng thái đúng. Master/worker UI vẫn độc lập với application UI.

## 8. MongoDB

### Authentication thất bại sau khi sửa cấu hình

Credential trong volume không tự thay đổi theo environment. Khôi phục credential khớp hoặc thực hiện rotate account trong MongoDB. Root account thuộc authSource `admin`; app account thuộc database cấu hình, mặc định `ecommerce`.

`mongo-init` không tự đổi password account đã tồn tại. Đọc log:

```bash
docker compose logs --tail 100 mongodb mongo-init
```

### Đổi cổng host không sửa lỗi kết nối nội bộ

Client container vẫn dùng `mongodb:27017`. Cổng `27018` chỉ phục vụ truy cập từ host.

### Không có collection kết quả nghiệp vụ

Bản hạ tầng hiện chỉ tạo metadata bootstrap và collection kiểm thử khi chạy test. Chưa có job ML/KPI để tạo kết quả phân tích.

## 9. Container running nhưng make up/doctor thất bại

Xem healthcheck và ba init job. Cụm yêu cầu mọi điều kiện readiness, không chỉ tiến trình tồn tại. Init `Exited (0)` là thành công, còn `Exited (1)` hoặc mã lỗi khác cần sửa.

Khi còn container của lần chạy lỗi, `doctor` có thể thất bại vì cụm chưa đầy đủ. Sau khi sửa nguyên nhân, chạy lại `make up`; không cần reset dữ liệu chỉ vì một init job thất bại.

## 10. Smoke/persistence thất bại

| Triệu chứng                        | Chẩn đoán                                                                             |
| ---------------------------------- | ------------------------------------------------------------------------------------- |
| Không có smoke.json                | Có thể job chưa chạy, exit khác 0 hoặc thiếu dòng INFRA_RESULT; đọc smoke.log         |
| Expected execution on both workers | Task không được quan sát trên đủ hai hostname cấu hình; kiểm tra worker và tài nguyên |
| Parquet mismatch                   | Đọc/ghi hoặc checkpoint không đúng kỳ vọng fixture; xem Spark log và run path         |
| Không đủ hai replica               | Kiểm tra block location, live DataNode và replication                                 |
| Mongo executor write/read mismatch | Kiểm tra app quyền, mạng từ executor và nội dung fixture                              |
| Cleanup failed                     | Assertion có thể đã đạt nhưng dọn lỗi; dùng run ID để kiểm tra tài nguyên còn lại     |
| Persistence verify lỗi sau down/up | Kiểm tra namespace project, volume, identity và marker ở storage báo lỗi              |
| Clean lỗi vì tài nguyên đã mất     | Cleanup có thể đã thành công một phần; xem trạng thái thực trước khi thử lại          |

Sau lỗi persistence, giữ token/log trước khi chạy test mới vì tên artifact sẽ được ghi đè. Cách verify/clean bằng token nằm trong [hướng dẫn vận hành](05-operations.md).

## 11. Xác nhận đã phục hồi

Sau khi sửa:

1. `make up` hoàn tất.
2. `make doctor` xác nhận topology và account.
3. `make smoke` tạo báo cáo thật.
4. Nếu kiểm tra giữ dữ liệu, chạy persistence khi cụm rảnh và lưu report.

Giữ các kết luận trong phạm vi phép kiểm tra thực sự đạt; không dùng smoke test để tuyên bố mô hình ML hoặc khả năng HA đã được kiểm chứng.
