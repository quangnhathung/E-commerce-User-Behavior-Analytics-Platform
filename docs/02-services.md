# 02 — Chức năng từng service

[← Mục lục](README.md)

Tên dưới đây là **service name** trong [Compose](../compose.yaml), không phải tên container cố định. Compose thêm namespace project vào tên tài nguyên.

## 1. Tổng quan tài nguyên

| Service          | Bộ nhớ container | CPU tối đa | Tiến trình/nhiệm vụ                      |
| ---------------- | ---------------: | ---------: | ---------------------------------------- |
| `kafka`          |        1.024 MiB |       0,75 | Kafka broker và KRaft controller         |
| `namenode`       |          768 MiB |        0,5 | HDFS NameNode                            |
| `datanode-1`     |          512 MiB |        0,5 | HDFS DataNode                            |
| `datanode-2`     |          512 MiB |        0,5 | HDFS DataNode                            |
| `spark-master`   |          512 MiB |       0,25 | Spark Standalone master                  |
| `spark-worker-1` |        2.048 MiB |          2 | Worker và executor của nó                |
| `spark-worker-2` |        2.048 MiB |          2 | Worker và executor của nó                |
| `spark-client`   |        1.280 MiB |          1 | Container công cụ và driver khi chạy job |
| `mongodb`        |          512 MiB |        0,5 | MongoDB server                           |
| `kafka-init`     |          256 MiB |       0,25 | Tạo/kiểm tra topic                       |
| `hdfs-init`      |          256 MiB |       0,25 | Đợi HDFS và tạo thư mục                  |
| `mongo-init`     |          192 MiB |       0,25 | Tạo application account và marker        |

Chín service đầu chạy thường trực. Ba init job chạy rồi thoát. Tổng giới hạn thường trực là 9 GiB và tám CPU logic; init bổ sung tối đa 704 MiB và 0,75 CPU khi cùng chạy.

## 2. Kafka

**Công dụng:** giữ event để consumer đọc theo offset; nguồn và bộ xử lý không phải thực thi cùng một tiến trình.

- Dùng image Kafka 3.9.1 khóa digest.
- Một process thực hiện cả vai trò broker và KRaft controller; không cần ZooKeeper.
- Listener `INTERNAL` ở `kafka:9092` cho container.
- Listener `HOST` được quảng bá bằng địa chỉ loopback/cổng host.
- Controller dùng `9093` trong network.
- Log dữ liệu nằm ở `/var/lib/kafka/data` trên volume `kafka-data`.
- Startup guard so cluster ID trong metadata với cấu hình trước khi gọi launcher chính thức.
- Heap server khởi đầu 256 MiB, tối đa 512 MiB; heap khác giới hạn container.

Healthcheck gọi Kafka API versions, không chỉ kiểm tra cổng TCP. Khi broker dừng, producer/consumer và job liên quan không thể trao đổi dữ liệu cho đến khi broker phục hồi. Bản hiện tại không có broker dự phòng.

```bash
docker compose logs --tail 100 kafka
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list
```

Kỳ vọng khi init hoàn tất: có `ecommerce.events.raw` và `ecommerce.events.dlq`.

## 3. HDFS NameNode

**Công dụng:** quản lý namespace, metadata file và thông tin block của DataNode.

- Dùng image Hadoop tự build, Java 11.
- Chạy `hdfs namenode` ở foreground.
- Metadata nằm ở `/var/lib/hadoop/name` trên volume riêng.
- RPC `9000`; UI/JMX `9870`.
- Heap tối đa 384 MiB trong container 768 MiB.
- Startup chỉ format khi thư mục metadata rỗng; metadata không khớp sẽ làm startup thất bại.

Healthcheck gọi JMX của chính NameNode. Nó không chờ đủ hai DataNode, vì DataNode lại phụ thuộc NameNode sẵn sàng.

Khi NameNode dừng, client không thực hiện được các thao tác namespace thông thường, dù block vẫn còn trên DataNode.

```bash
docker compose logs --tail 100 namenode
docker compose exec -T namenode hdfs dfsadmin -report
```

Kỳ vọng sau bootstrap: report hiển thị hai live DataNode.

## 4. Hai HDFS DataNode

**Công dụng:** lưu block và cung cấp nội dung file cho client; thực hiện sao chép block theo chỉ dẫn HDFS.

- Cùng image Hadoop nhưng khác hostname và volume.
- Chạy `hdfs datanode`.
- Dữ liệu ở `/var/lib/hadoop/data`.
- Heap tối đa 256 MiB/container.
- Cổng nội bộ: transfer `9866`, IPC `9867`, HTTP/JMX `9864`.
- Đợi NameNode healthy trước khi khởi chạy.
- Healthcheck kiểm tra JMX DataNode; số node đã đăng ký được kiểm tra thêm ở init/readiness.

Dừng một DataNode làm mất tạm thời một nơi chứa replica; mức đọc/ghi còn thực hiện được phụ thuộc block và trạng thái HDFS. Readiness của bản triển khai yêu cầu đủ hai node. Nếu NameNode khởi động lại khi chỉ còn một node, cấu hình safe mode tối thiểu hai DataNode có thể ngăn cụm ra khỏi safe mode.

```bash
docker compose logs --tail 100 datanode-1 datanode-2
docker compose exec -T namenode hdfs fsck /ecommerce -files -blocks -locations
```

## 5. Spark master

**Công dụng:** quản lý danh sách worker và phân phối tài nguyên cho ứng dụng Spark.

- Dùng image Spark 3.5.7, Scala 2.12, Java 17, Python 3.11.
- Khởi động class `org.apache.spark.deploy.master.Master`.
- RPC `7077`; UI/JSON `18080`.
- Heap daemon 192 MiB; container 512 MiB.
- Không trực tiếp đảm nhiệm các task xử lý dữ liệu của executor.
- Healthcheck đọc JSON UI. Readiness kiểm tra thêm hai worker ở trạng thái `ALIVE`.

Khi master dừng, việc đăng ký và phân phối tài nguyên mới bị ảnh hưởng. Bản hiện tại không cấu hình master dự phòng.

```bash
docker compose logs --tail 100 spark-master
docker compose exec -T spark-client python3 /opt/infra/smoke/readiness.py --once
```

## 6. Hai Spark worker

**Công dụng:** tạo và quản lý executor dùng để thực thi task.

- Mỗi worker quảng bá hai core, 1 GiB executor memory.
- Container giới hạn 2 GiB để chừa phần cho worker daemon, Python và overhead.
- Heap worker daemon 128 MiB.
- UI lần lượt `18081` và `18082`.
- Scratch/work directory ở `/tmp/spark-local` và `/tmp/spark-work`.
- Healthcheck gọi JSON UI; master xác nhận worker đã đăng ký.

Scratch không phải vùng lưu trữ lâu dài. Khi worker mất, task của ứng dụng có thể cần thực thi lại; đây không phải kiểm thử phục hồi ứng dụng nghiệp vụ đã được xác nhận.

```bash
docker compose logs --tail 100 spark-worker-1 spark-worker-2
```

Mở master UI để xem worker đăng ký; dùng smoke test để xác nhận task chạy trên cả hai.

## 7. Spark client

**Công dụng:** cung cấp môi trường chạy Python, Kafka client, MongoDB client và Spark driver.

- Khi chưa chạy job, command là `sleep infinity` để có thể dùng `docker compose exec`.
- Nhận cấu hình Hadoop/Spark, script smoke và dataset mount read-only.
- Nhận application credential MongoDB; không nhận root password.
- Driver bind `0.0.0.0`, quảng bá `spark-client`, dùng cổng RPC `39001` và driver block manager `39002`.
- UI ứng dụng ở `14040` chỉ xuất hiện khi có ứng dụng chạy.
- Heap driver 768 MiB; container 1.280 MiB.
- Chỉ khởi chạy sau ba job init thành công và hai worker healthy.
- Healthcheck xác nhận topology, topic và khả năng đọc bootstrap metadata MongoDB.

Driver điều phối ứng dụng đang chạy; dừng client trong lúc có job có thể làm ứng dụng đó thất bại. Mất client không tự xóa dữ liệu trong HDFS/Kafka/MongoDB.

```bash
docker compose exec spark-client bash
docker compose exec -T spark-client python3 --version
```

## 8. MongoDB

**Công dụng:** lưu document để truy vấn bằng khóa và cập nhật kết quả.

- Image nhánh 8.0 được khóa bằng digest.
- Chạy `mongod` với WiredTiger cache 0,25 GiB.
- Dữ liệu ở `/data/db` trên volume `mongodb-data`.
- Cổng container `27017`, cổng host mặc định `27018`.
- Authentication bật qua root account khi khởi tạo.
- Application account chỉ có `readWrite` trên database `ecommerce`.
- Healthcheck dùng `mongosh` xác thực root rồi ping; password lấy từ environment, không đưa vào command argument.

Bộ kiểm thử dùng các collection `infra_metadata`, `infra_smoke` và `infra_persistence`. Chưa có collection kết quả ML nghiệp vụ.

Khi MongoDB dừng, lưu/truy vấn document thất bại; Kafka và HDFS có vùng dữ liệu riêng, nhưng job nào cần MongoDB sẽ không hoàn tất đúng.

## 9. Ba job init

| Job          | Phụ thuộc                     | Làm gì?                                                            | Hành vi chạy lại                                    |
| ------------ | ----------------------------- | ------------------------------------------------------------------ | --------------------------------------------------- |
| `kafka-init` | Kafka healthy                 | Tạo hai topic nếu thiếu, kiểm tra partitions/RF/retention          | Giữ topic đúng; báo lỗi khi cấu hình quan trọng sai |
| `hdfs-init`  | NameNode và DataNodes healthy | Đợi hai live DataNode, safe mode OFF, tạo thư mục và đặt quyền gốc | Giữ thư mục/dữ liệu hiện có                         |
| `mongo-init` | MongoDB healthy               | Tạo app user nếu thiếu, tạo marker và xác nhận app login           | Giữ account; không tự rotate password               |

`Exited (0)` nghĩa là công việc đã hoàn tất thành công. `Exited` với mã khác 0 cần được chẩn đoán. Các job dùng `restart: "no"` để lỗi không bị che bằng vòng restart vô hạn.

```bash
docker compose logs --tail 100 kafka-init hdfs-init mongo-init
```

Đọc tiếp [cấu hình](03-configuration.md) và [vòng đời khởi tạo](04-bootstrap-and-lifecycle.md).
