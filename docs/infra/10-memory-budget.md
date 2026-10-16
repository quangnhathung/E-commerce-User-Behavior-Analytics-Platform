# 10 — Ngân sách RAM 7 GiB

[← Mục lục](README.md)

## 1. Mục đích và phạm vi

Ngân sách mặc định giảm từ 10 xuống 7 GiB để có thể khởi động trên host còn khoảng 8,3 GiB RAM khả dụng. Vẫn giữ đủ Kafka, MongoDB, Spark master/hai worker/client, HDFS NameNode/hai DataNode và ba init job.

Đây là cấu hình cho demo và tải nhỏ, với một Spark application tại một thời điểm. Giảm heap và số task đồng thời có thể làm job lớn chạy chậm hơn; smoke đạt không chứng minh toàn bộ dataset hoặc huấn luyện ML lớn sẽ vừa RAM.

## 2. Bảng phân bổ

| Thành phần | Số lượng | Limit mỗi container | Heap/cấu hình bên trong |
| --- | ---: | ---: | --- |
| Kafka | 1 | 768 MiB | `-Xms128m -Xmx384m`; CLI healthcheck giữ heap tối đa 96 MiB |
| NameNode | 1 | 512 MiB | `HADOOP_HEAPSIZE_MAX=256` |
| DataNode | 2 | 384 MiB | `HADOOP_HEAPSIZE_MAX=192` |
| Spark master | 1 | 384 MiB | Daemon heap 128 MiB |
| Spark worker | 2 | 1.280 MiB | Daemon heap 128 MiB; executor heap 640 MiB; một core |
| Spark client | 1 | 896 MiB | Driver heap 512 MiB |
| MongoDB | 1 | 512 MiB | WiredTiger cache 0,25 GiB; mongosh old-space tối đa 128 MiB |
| Kafka init | 1 | 256 MiB | CLI heap tối đa 96 MiB |
| HDFS init | 1 | 256 MiB | CLI heap tối đa 96 MiB |
| Mongo init | 1 | 192 MiB | Python bootstrap |

- Chín service thường trực: **6.400 MiB = 6,25 GiB**.
- Ba init job cộng lại: **704 MiB = 0,6875 GiB**.
- Tổng cộng bảo thủ nếu cộng toàn bộ limit: **7.104 MiB = 6,9375 GiB**, nhỏ hơn ngân sách **7 GiB = 7.168 MiB**.

Đây là tổng các trần bộ nhớ, không phải mức RAM được cấp phát sẵn hay RSS đo được. Init chạy rồi thoát; client còn đợi init hoàn tất nên không phải mọi container đều đạt đỉnh cùng lúc. Docker daemon và hệ điều hành cần RAM riêng ngoài bảng này.

## 3. Cấu hình Spark đi kèm

| Thuộc tính | Giá trị | Công dụng |
| --- | --- | --- |
| Worker `--cores` | `1` | Một task slot mỗi worker |
| Worker `--memory` | `640m` | Bộ nhớ executor được worker quảng bá cho master |
| `spark.executor.cores` | `1` | Một core mỗi executor |
| `spark.cores.max` | `2` | Tổng hai task slot trên hai worker |
| `spark.executor.memory` | `640m` | Heap executor |
| `spark.driver.memory` | `512m` | Heap driver, có hiệu lực khi spark-submit khởi động JVM |
| `spark.driver.maxResultSize` | `128m` | Giới hạn tổng kết quả serialize trả về driver cho mỗi action |
| `spark.sql.shuffle.partitions` | `4` | Giảm số partition mặc định khi shuffle |
| `spark.default.parallelism` | `4` | Giảm parallelism mặc định của một số thao tác RDD |

Limit worker bao gồm daemon, executor, Python worker và JVM/native overhead. Heap 640 MiB không có nghĩa toàn bộ container chỉ sử dụng 640 MiB. Standalone không tự cấp thêm RAM container theo `spark.executor.memoryOverhead`; giới hạn thực tế do Compose đặt.

Readiness yêu cầu đủ hai worker ALIVE, mỗi worker một core và ít nhất 640 MiB memory được quảng bá. Smoke tiếp tục xác nhận task thực thi trên cả hai hostname và block HDFS có hai replica. Các fixture smoke chủ động dùng một số partition riêng; default parallelism không thay toàn bộ fixture này.

## 4. Ngưỡng preflight và file môi trường

`.env.example` mặc định `MIN_AVAILABLE_RAM_GIB=7`. File `.env` local hiện tại cũng đã được cập nhật khóa này; mật khẩu, cluster ID và các cấu hình khác được giữ nguyên.

`make setup` vẫn giữ file `.env` hợp lệ đã có. Với một bản clone/local config khác, kiểm tra riêng khóa `MIN_AVAILABLE_RAM_GIB` và đổi thành `7` nếu nó còn là `10`. Không gửi toàn bộ nội dung file chứa credential để kiểm tra giá trị này.

Khi `make up`/`make doctor` kiểm tra tài nguyên:

1. Tổng memory limit của các service, kể cả init, phải không vượt ngân sách trong biến môi trường.
2. Docker daemon phải có ít nhất lượng RAM tương ứng ngân sách.
3. Host phải còn đủ phần ngân sách chưa tính cho service thường trực của chính project đang chạy.

Ví dụ khi chưa có service project chạy: còn 8,3 GiB thì vượt ngưỡng 7 GiB; còn 6,5 GiB thì bị từ chối. Service dự án khác không được trừ khỏi phần cần bổ sung. Việc trừ ngân sách dựa trên limit cấu hình, không phải đo RSS từng container.

Giảm riêng ngưỡng xuống 6 GiB sẽ không làm container nhỏ lại: tổng limit 6,9375 GiB vượt ngân sách và preflight báo lỗi. Muốn đổi ngân sách khác cần chỉnh đồng bộ Compose, heap, tài nguyên Spark, readiness và kiểm thử.

## 5. Áp dụng sau khi đã build image

Không cần tải lại Spark/Hadoop hoặc chạy `make build` cho thay đổi RAM này. Compose đọc giới hạn/env mới; Spark defaults và readiness được bind mount từ source.

Chạy từ gốc repository:

```bash
make config
make test
make up
make doctor
make smoke
```

`make up` tạo lại container có cấu hình thay đổi và dùng lại named volume hiện có. Đợi job đang chạy hoàn tất trước khi áp dụng để tránh ngắt driver/executor. Không cần reset hoặc xóa volume.

## 6. Theo dõi bộ nhớ và OOM

Khi cụm đã chạy, xem RAM bằng:

```bash
docker compose ps --all
docker stats --no-stream
```

`docker stats` liệt kê container của cả host; đối chiếu tên project khi đọc. Để kiểm tra trạng thái OOM của từng container project bằng Bash:

```bash
while IFS= read -r container_id; do
  docker inspect --format '{{.Name}} limit={{.HostConfig.Memory}} OOMKilled={{.State.OOMKilled}} restarts={{.RestartCount}}' "$container_id"
done < <(docker compose ps --all -q)
```

`OOMKilled=true`, restart tăng hoặc log `OutOfMemoryError` cần được điều tra; giảm kích thước batch/partition dữ liệu hoặc điều chỉnh lại ngân sách theo workload. Không dùng kết quả idle để kết luận đủ RAM cho job nặng. Bằng chứng chấp nhận ban đầu là doctor/smoke đạt, task chạy trên hai worker, không có OOM/restart bất thường trong lượt chạy; OOMKilled chỉ phản ánh trạng thái container hiện tại, không phải lịch sử đầy đủ.

## 7. Trạng thái kiểm chứng

Kiểm tra offline xác nhận tổng limit dưới 7 GiB, heap vừa container, executor vừa tài nguyên worker, preflight nhận 8,3 GiB và từ chối thiếu RAM/ngân sách quá thấp, readiness nhận cấu hình mới và từ chối worker thiếu tài nguyên.

Phiên cập nhật đạt 23 test; 7 test HTTP downloader skipped do sandbox cấm socket. Compose config và cú pháp đã được kiểm tra. Phiên công cụ chưa truy cập được Docker daemon nên chưa xác nhận smoke, OOM hoặc RAM tiêu thụ thực tế dưới tải; cần chạy các lệnh trên tại terminal host có quyền Docker.
