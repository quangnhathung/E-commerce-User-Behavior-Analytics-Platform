# Hướng dẫn chạy toàn bộ dự án trên máy mới

[← Mục lục tài liệu](README.md) · [README dự án](../README.md) · [Sử dụng web và đọc kết quả](serving/06-web-user-guide.md)

Tài liệu này dành cho developer nhận repository và tự chạy từ đầu: cài công cụ, chuẩn bị Retailrocket, dựng Docker Compose, ingest, phân tích lịch sử/streaming, K-Means, Random Forest, policy precision, ML streaming có simulator và dashboard HTML/CSS/JS qua Live Server. Không cần `.env`, model, volume hoặc thư mục `artifacts/` của người bàn giao.

Đối chiếu mã nguồn và artifact đến **20/10/2026**. Đã có bằng chứng ingestion/ML, replay/simulator, browser trên run hoàn tất và live hữu hạn hai lượt. Live liên tục đã hoàn tất một lượt 1.000 event nhưng bị gián đoạn ở lượt thứ hai; dừng/drain/resume thật còn cần kiểm chứng. Các lệnh ứng dụng bên dưới chạy trong **Bash trên Linux hoặc Ubuntu WSL2**, tại thư mục gốc repository. Các khối PowerShell chỉ dùng để chuẩn bị Windows. Chạy từng giai đoạn và kiểm tra kết quả trước khi chuyển bước.

Tài liệu này hướng dẫn **tạo ra dữ liệu và chạy ứng dụng**. Sau khi mở được dashboard, dùng [hướng dẫn sử dụng web](serving/06-web-user-guide.md) để đọc từng KPI, phân cụm, prediction và trạng thái pipeline. Ví dụ số liệu của máy bàn giao được ghi riêng; developer mới phải kiểm tra report trên chính máy mình.

## Mục lục

1. [Phạm vi và luồng dữ liệu](#scope)
2. [Yêu cầu máy và công cụ](#requirements)
3. [Cài môi trường Linux hoặc Windows/WSL2](#os)
4. [Clone và chuẩn bị dataset](#source)
5. [Tạo cấu hình local](#configuration)
6. [Kiểm thử offline, build và khởi động](#infrastructure)
7. [Ingest Bronze/Silver](#ingestion)
8. [Phân tích lịch sử và demo streaming](#behavior)
9. [Feature và K-Means](#segments)
10. [Random Forest và batch scoring](#prediction)
11. [Policy precision và score mới](#policy)
12. [ML streaming: replay và simulator](#streaming-ml)
    - [API local và dashboard Live Server](#serving)
    - [Live nhiều lượt và nghiệm thu ứng dụng](#live-demo)
13. [Đọc artifact, selector và theo dõi](#artifacts)
14. [Nghiệm thu chạy lại](#reuse)
15. [Khởi động lại, ngắt job và xử lý lỗi](#recovery)
16. [Dừng và dọn dữ liệu](#cleanup)
17. [Checklist bàn giao](#handoff)

<a id="scope"></a>

## 1. Phạm vi và luồng dữ liệu

Mục tiêu là biến hành vi xem sản phẩm, thêm giỏ và mua thành số liệu giúp hiểu khách hàng: họ dừng ở đâu trong funnel, có những nhóm hành vi nào và ai có dấu hiệu mua trong 24 giờ tiếp theo.

```mermaid
flowchart LR
    CSV["Retailrocket: bốn CSV"] --> INGEST["Spark ingest"]
    INGEST --> BRONZE["HDFS Bronze: nguồn nguyên bản"]
    INGEST --> SILVER["HDFS Silver: event chuẩn hóa"]
    SILVER --> HISTORY["Session, funnel và K-Means"]
    SILVER --> RF["Dataset 128 ngày, train RF, batch score"]
    RF --> POLICY["Chọn policy ngưỡng và đổi nhãn score"]
    SILVER --> REPLAY["Chuẩn bị demo và replay"]
    REPLAY --> KAFKA["Kafka raw"]
    KAFKA --> STREAM["Spark streaming: KPI, session, funnel"]
    STREAM --> LIVE["Pipeline mới: feature, K-Means, RF và policy"]
    RF --> LIVE
    HISTORY --> LIVE
    STREAM --> DLQ["Kafka DLQ: event lỗi"]
    HISTORY --> GOLD["HDFS Gold và models"]
    RF --> GOLD
    POLICY --> GOLD
    STREAM --> GOLD
    LIVE --> GOLD
    HISTORY --> MONGO["MongoDB: kết quả và lookup"]
    RF --> MONGO
    POLICY --> MONGO
    STREAM --> MONGO
    LIVE --> MONGO
    MONGO --> API["FastAPI trên host: đọc snapshot"]
    API --> WEB["HTML/CSS/JS qua VS Code Live Server"]
```

| Thành phần         | Công dụng                                                                               |
| ------------------ | --------------------------------------------------------------------------------------- |
| Docker Compose     | Khởi tạo network, volume, giới hạn tài nguyên, healthcheck và thứ tự init trên một máy. |
| Kafka              | Nhận event replay, giữ offset và tách event lỗi sang DLQ.                               |
| HDFS               | Lưu nguồn, Parquet, snapshot, checkpoint và model; hai DataNode giữ replication 2.      |
| Spark              | Xử lý batch/streaming và ML phân tán qua hai worker.                                    |
| MongoDB            | Giữ kết quả đã công bố, metadata và lookup theo visitor/sản phẩm.                       |
| Make và Python CLI | Kiểm tra điều kiện, điều phối job, khóa driver, ghi log và report.                      |
| FastAPI trên host | Đọc kết quả MongoDB qua HTTP, kiểm tra run/batch/model của snapshot. |
| Web và Live Server | Hiển thị feed, KPI, phân khúc, dự đoán và tiến độ; người vận hành phát stream từ terminal. |

Đã có ingestion, KPI streaming, session/funnel lịch sử và streaming, K-Means lịch sử, RF batch/policy và ML streaming. FastAPI/dashboard đã đọc được dữ liệu thật; xem phạm vi nghiệm thu tại [serving](serving/README.md). Spark/HDFS UI là giao diện vận hành. Retailrocket không có giá/doanh thu hoặc bước bắt đầu thanh toán; funnel hiện tại là **xem → thêm giỏ → mua**. Hai worker/DataNode cùng một máy không bảo vệ khi cả máy hoặc ổ đĩa hỏng.

Các nhánh phụ thuộc như sau; thứ tự trong tài liệu là một lộ trình chạy đầy đủ:

| Giai đoạn                                 | Cần có trước                                                                       |
| ----------------------------------------- | ---------------------------------------------------------------------------------- |
| Hạ tầng                                   | Công cụ, bốn CSV, `.env`, image đã build                                           |
| Ingest                                    | Hạ tầng healthy, smoke đạt                                                         |
| Behavior lịch sử, demo streaming, K-Means | Silver đã ingest đạt chất lượng                                                    |
| RF                                        | Silver; không cần dùng cluster K-Means làm feature                                 |
| Policy                                    | RF published và validation/test predictions; apply cần thêm original batch score |
| ML streaming                              | Silver, K-Means, RF và policy eligible đã published; không cần tạo batch score mới |
| API và dashboard | MongoDB đang chạy, API đã cài; muốn xem kết quả cần có run ML nghiệp vụ |
| Live nhiều lượt | Cùng nguồn/model/policy như ML streaming; một driver/producer được quản lý bởi lệnh live |

### 1.1. Chọn đúng lộ trình

- **Máy mới, nghiệm thu đầy đủ:** thực hiện mục 2–12 theo thứ tự, sau đó 12A–12B. Có cả kiểm thử tích hợp và các pipeline lịch sử.
- **Chỉ cần chuẩn bị đủ điều kiện cho dashboard:** vẫn cần hạ tầng, ingest, K-Means, RF và policy eligible. Phân tích toàn lịch sử và demo KPI v2 ở mục 8 là nhánh phân tích/nghiệm thu bổ sung; dashboard hiện đọc run ML `stream-ml-v1`, không đọc trực tiếp kết quả v2 hoặc batch lịch sử.
- **Máy đã dựng xong:** dùng [khởi động lại](#recovery), rồi [mở ứng dụng](#serving). Không chạy lại cả quá trình huấn luyện mỗi lần muốn xem web.

Huấn luyện lần đầu là phần tốn thời gian nhất: build purchase data tham chiếu 45–55 phút, train RF khoảng 30 phút, chưa kể build image, ingest và các test. Thời gian thực tế phụ thuộc máy/mạng; theo dõi tiến độ và exit code thay vì coi đây là thời hạn bắt buộc.

### 1.2. Quy ước terminal và ID

Mỗi terminal Bash đều mở **cùng clone và cùng thư mục root**. Trong các mục demo v2/ML, terminal consumer và producer được ghi riêng. Khi tới dashboard/live, dùng:

| Terminal | Chạy gì | Khi nào giữ mở |
| --- | --- | --- |
| A — API | `make api` | Suốt thời gian sử dụng web |
| B — job | Một job Spark, hoặc `make live-demo` | Đến khi job hoàn tất hoặc live dừng/drain xong |
| C — điều khiển | Health/status; hoặc producer khi dùng replay/simulator kiểu cũ | Theo từng lệnh |
| VS Code/browser | Live Server và dashboard | Suốt buổi xem kết quả |

Một Spark driver phục vụ hai worker. API và Live Server chạy cùng driver được; không chạy smoke, test tích hợp, train hoặc consumer thứ hai trong lúc live đang giữ driver. `make live-demo` đã có producer bên trong, không chạy thêm `simulate-ml` cho phiên đó.

ID trong các lệnh là tên ví dụ. Lần đầu dùng đúng tên trong tài liệu; khi tạo kịch bản khác hoặc đổi mã/config, chọn tên mới. Chạy tiếp cùng ID chỉ khi giữ nguyên mã, nguồn, model và cấu hình đã khóa. Không lấy ID SHA model của máy tác giả điền vào máy mới.

<a id="requirements"></a>

## 2. Yêu cầu máy và công cụ

| Hạng mục              | Yêu cầu thực tế                                                                                                               |
| --------------------- | ----------------------------------------------------------------------------------------------------------------------------- |
| Hệ điều hành chạy CLI | Linux `amd64`; Windows dùng Ubuntu WSL2, Linux containers                                                                     |
| CPU                   | `x86_64`, hỗ trợ AVX, Docker thấy ít nhất 4 CPU logic                                                                         |
| RAM                   | Khoảng **7 GiB khả dụng** trước khi dựng cả cụm; khuyến nghị máy có ít nhất 16 GB để còn RAM cho OS/Docker                    |
| Dung lượng            | Preflight yêu cầu ít nhất **30 GiB trống ở Docker data**; nên dành 50–60 GiB trở lên và theo dõi khi lưu nhiều snapshot/model |
| Công cụ host          | Git, GNU Make, Python **3.11+** và venv/pip cho toàn bộ lộ trình có API; Bash, curl, unzip, GNU coreutils |
| Giao diện | VS Code, extension Live Server (`ritwickdey.LiveServer`) và trình duyệt hỗ trợ ES modules |
| Test web, tùy chọn | Node 18+ cho `make test-web`; frontend chạy không cần npm/Node |
| Docker                | Daemon truy cập được, Compose hỗ trợ `up --wait`, Buildx plugin hoạt động                                                     |
| Mạng                  | Lần build đầu cần tải image, binary Apache, Python dependencies và dataset                                                    |

Tổng limit chín service thường trực là **6,25 GiB**. Ba init job có thể thêm 704 MiB, tổng tối đa **6,94 GiB**; bộ nhớ hệ điều hành, daemon, API, VS Code và browser nằm ngoài số này. Driver dùng heap 512 MiB, mỗi executor 640 MiB và một core. Chỉ chạy **một Spark driver tại một thời điểm**, kể cả smoke và integration test. Producer replay có thể chạy đồng thời với streaming theo hướng dẫn riêng.

Java, Spark, Hadoop, PySpark và các dependency ứng dụng được cài trong image; không cần cài chúng bằng pip hoặc Java trên host. Không giả định chạy được trên ARM/Apple Silicon hoặc Windows native. Chi tiết: [ngân sách RAM](infra/10-memory-budget.md).

<a id="os"></a>

## 3. Cài môi trường: chọn đúng một nhánh

### 3.1. Ubuntu 24.04 trên Linux

Chạy tại terminal của **máy Linux**, không áp dụng nhánh này trong WSL đang dùng Docker Desktop. Với máy đã có Docker hoạt động, kiểm tra ở mục 3.4 và bỏ qua cài lại. Máy có Docker/containerd cũ cần đối chiếu các package xung đột trước khi đổi bộ cài; tham khảo [hướng dẫn Ubuntu chính thức của Docker](https://docs.docker.com/engine/install/ubuntu/).

Cài công cụ và đăng ký repository Docker:

```bash
sudo apt update
sudo apt install -y ca-certificates curl git make python3 python3-venv python3-pip unzip
sudo install -m 0755 -d /etc/apt/keyrings
sudo curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
sudo chmod a+r /etc/apt/keyrings/docker.asc
sudo tee /etc/apt/sources.list.d/docker.sources >/dev/null <<EOF
Types: deb
URIs: https://download.docker.com/linux/ubuntu
Suites: $(. /etc/os-release && echo "${UBUNTU_CODENAME:-$VERSION_CODENAME}")
Components: stable
Architectures: $(dpkg --print-architecture)
Signed-By: /etc/apt/keyrings/docker.asc
EOF
sudo apt update
sudo apt install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
sudo systemctl enable --now docker
sudo groupadd --force docker
sudo usermod -aG docker "$USER"
```

Đăng xuất và đăng nhập lại để nhận group mới, rồi kiểm tra mục 3.4. Quyền group Docker cho phép điều khiển daemon; chỉ cấp cho người dùng được phép vận hành máy. Không đổi socket sang `chmod 777` hoặc chạy toàn bộ Make bằng `sudo`. Xem [Linux post-install](https://docs.docker.com/engine/install/linux-postinstall/).

### 3.2. Arch Linux

Chạy trong terminal Arch:

```bash
sudo pacman -Syu --needed docker docker-compose docker-buildx git make python python-pip curl unzip
sudo systemctl enable --now docker
sudo groupadd --force docker
sudo usermod -aG docker "$USER"
```

Đăng xuất/đăng nhập lại. Trên Arch, **Buildx là package riêng**; chỉ có `docker` và `docker-compose` chưa đủ cho `make build`. Tên package đối chiếu từ repository Arch: [docker](https://archlinux.org/packages/extra/x86_64/docker/), [docker-compose](https://archlinux.org/packages/extra/x86_64/docker-compose/), [docker-buildx](https://archlinux.org/packages/extra/x86_64/docker-buildx/).

### 3.3. Windows 11 x64, Ubuntu 24.04 WSL2 và Docker Desktop

Nhánh này dùng Docker Desktop làm daemon. Các bước chuẩn bị theo tài liệu chính thức; dự án **chưa có lượt nghiệm thu runtime riêng trên Windows/WSL2**.

1. Bật virtualization trong BIOS/UEFI nếu chưa có. Mở **PowerShell Administrator**, cài WSL và Ubuntu:

```powershell
wsl --install -d Ubuntu-24.04
```

2. Khởi động lại Windows khi được yêu cầu. Mở Ubuntu, tạo Linux username/password. Trong PowerShell, cập nhật và kiểm tra:

```powershell
wsl --update
wsl --version
wsl --list --verbose
```

Ubuntu phải có `VERSION 2`. Nếu bản đã cài còn là WSL1, chuyển trước khi chạy dự án:

```powershell
wsl --set-version Ubuntu-24.04 2
```

Tên distro phải khớp `wsl --list --verbose`. Xem [Microsoft: cài WSL](https://learn.microsoft.com/en-us/windows/wsl/install). Docker yêu cầu WSL tối thiểu 2.1.5 và khuyến nghị cập nhật; kiểm tra theo [Docker Desktop WSL2](https://docs.docker.com/desktop/features/wsl/).

3. Cài [Docker Desktop for Windows](https://docs.docker.com/desktop/setup/install/windows-install/), mở ứng dụng. Chọn **Use the WSL 2 based engine**, bật **Resources → WSL Integration → Ubuntu-24.04**, Apply/Restart. Dùng **Linux containers**. Không cài thêm Docker Engine/CLI bằng apt trong Ubuntu này vì có thể xung đột với Desktop.
4. Trước khi chạy job, cấp đủ tài nguyên cho WSL. Ví dụ máy Windows có ít nhất 16 GB RAM: sửa hoặc tạo `%UserProfile%\.wslconfig` bằng Windows editor, giữ các tùy chỉnh khác nếu file đã tồn tại:

```ini
[wsl2]
memory=10GB
processors=4
swap=4GB
```

Đây là trần VM dùng chung cho WSL2, khác tổng limit container. Swap không thay thế yêu cầu RAM khả dụng. Khi không còn công việc trong các distro, thoát Docker Desktop rồi áp dụng từ PowerShell:

```powershell
wsl --shutdown
```

Mở lại Docker Desktop và Ubuntu. Lệnh trên dừng **mọi distro WSL đang chạy**; thực hiện trước khi có pipeline. Cơ chế cấu hình: [Microsoft `.wslconfig`](https://learn.microsoft.com/en-us/windows/wsl/wsl-config).

5. Từ đây dùng **terminal Ubuntu/Bash**, cài công cụ host:

```bash
sudo apt update
sudo apt install -y git make python3 python3-venv python3-pip curl unzip ca-certificates
```

Clone repository và lưu CSV trong filesystem Linux, ví dụ `~/workspace/`, để bind mount có hiệu năng tốt. Tránh đặt workspace/dataset dưới `/mnt/c`. Có thể mở `explorer.exe .` từ Ubuntu để chép ZIP vào thư mục Linux. Giữ file shell ở newline **LF**. Xem [Docker WSL best practices](https://docs.docker.com/desktop/features/wsl/best-practices/).

### 3.4. Kiểm tra trước khi clone

Trong Bash Linux/Ubuntu WSL của chính người sẽ chạy dự án:

```bash
uname -m
nproc
python3 --version
make --version
docker context show
docker info
docker compose version
docker buildx version
free -h
df -h .
```

Đạt khi kiến trúc `x86_64`, Docker báo Linux/amd64 và ít nhất bốn CPU, Python ≥3.11 cho cả API, không lỗi socket/daemon, Compose và Buildx in phiên bản. Hạ tầng cũ nhận Python 3.10 nhưng API cần 3.11+; nếu dùng interpreter khác, chỉ rõ `PYTHON=python3.11 make api-setup`. Preflight dự án còn kiểm tra AVX và tài nguyên. Trên WSL, cần đủ RAM khả dụng **bên trong Linux**, đồng thời đủ dung lượng ổ Windows chứa virtual disk của Docker Desktop; `df -h .` một mình không xác nhận ổ Docker còn trống.

Nếu Docker chưa truy cập được: Linux kiểm tra service/group và đăng nhập lại; WSL kiểm tra Desktop đang mở, Linux containers và integration đúng distro. Chưa chuyển sang build khi bước này còn lỗi.

<a id="source"></a>

## 4. Clone repository và chuẩn bị Retailrocket

### 4.1. Lấy đúng mã nguồn

Trong Bash, chọn workspace của mình. Ví dụ này dùng `~/workspace/` nhất quán cho mọi terminal:

```bash
mkdir -p ~/workspace
cd ~/workspace
git clone --branch dev https://github.com/quangnhathung/E-commerce-User-Behavior-Analytics-Platform.git
cd E-commerce-User-Behavior-Analytics-Platform
git status --short
git branch --show-current
git rev-parse HEAD
```

Nhánh hiện hành là `dev`. Khi bàn giao một phiên bản cố định, dùng commit/tag đã được nhóm thống nhất thay vì tự lấy mã mới giữa các job. Ghi lại commit vào biên bản; nó giúp giải thích khác biệt identity và model giữa hai máy. Nếu chọn đường dẫn workspace khác, thay `cd` ở các terminal trong tài liệu tương ứng.

Mở folder repository bằng **File → Open Folder** trong VS Code, hoặc `code .` nếu đã có CLI. Cài [Live Server của Ritwick Dey](https://marketplace.visualstudio.com/items?itemName=ritwickdey.LiveServer) qua Extensions (`Ctrl+Shift+X`), tìm đúng extension ID `ritwickdey.LiveServer` rồi Install. Trên WSL mở workspace bằng Remote WSL và cài extension vào môi trường workspace. Phần chạy web ở [mục 12A](#serving).

### 4.2. Tải và giải nén dataset

Tải ZIP từ [Retailrocket recommender system dataset trên Kaggle](https://www.kaggle.com/datasets/retailrocket/ecommerce-dataset), đăng nhập Kaggle nếu trang yêu cầu. Ví dụ lưu ZIP đã tải thành `~/Downloads/retailrocket.zip` **bên trong Linux/WSL**. Không cần đưa Kaggle token vào `.env` hoặc Git.

Từ thư mục gốc repository, kiểm tra ZIP:

```bash
DATASET_ARCHIVE="$HOME/Downloads/retailrocket.zip"
test -f "$DATASET_ARCHIVE"
unzip -l "$DATASET_ARCHIVE"
```

Sửa giá trị biến nếu file tải có tên khác. Sau khi kiểm tra, giải nén vào thư mục mới:

```bash
mkdir -p Dataset/retailrocket-recommender-system-dataset
unzip "$DATASET_ARCHIVE" -d Dataset/retailrocket-recommender-system-dataset
```

Nếu ZIP có thư mục cha, sắp lại để **bốn CSV nằm trực tiếp** như sau, không lồng thêm một tầng:

```text
Dataset/retailrocket-recommender-system-dataset/
  events.csv
  category_tree.csv
  item_properties_part1.csv
  item_properties_part2.csv
```

| File                                                     | Header bắt buộc, đúng thứ tự                     |
| -------------------------------------------------------- | ------------------------------------------------ |
| `events.csv`                                             | `timestamp,visitorid,event,itemid,transactionid` |
| `category_tree.csv`                                      | `categoryid,parentid`                            |
| `item_properties_part1.csv`, `item_properties_part2.csv` | `timestamp,itemid,property,value`                |

Kiểm tra đủ file và header từ host; chưa cần Docker:

```bash
PYTHONPATH=app python3 - <<'PY'
import csv
from pathlib import Path
from ecommerce_analytics.contracts import HEADERS
root = Path("Dataset/retailrocket-recommender-system-dataset")
for name, expected in sorted(HEADERS.items()):
    path = root / name
    with path.open(encoding="utf-8", newline="") as source:
        actual = next(csv.reader(source), None)
    if actual != expected:
        raise SystemExit(f"Sai header: {path}: {actual}")
    print(f"OK {path}: {path.stat().st_size:,} bytes")
PY
```

Đạt khi cả bốn file được đọc và header khớp. File thiếu/sai header phải sửa trước. Dataset không được Git theo dõi hoặc đóng gói vào image; Compose mount nguồn **read-only**. Không sửa CSV trong khi job đang hash/đọc. Có thể dùng vị trí khác qua `DATASET_PATH` ở bước cấu hình, đồng thời đổi đường dẫn trong kiểm tra header.

Host đọc được chưa chắc container UID/GID 1000 đọc được. Với bộ dữ liệu công khai này, có thể cấp quyền đọc CSV và quyền đi qua **các thư mục dataset liên quan** cho runtime; kiểm tra lại trong container ở mục 6. Không đổi toàn bộ home hoặc dùng `chmod 777`.

<a id="configuration"></a>

## 5. Tạo và kiểm tra cấu hình local

**Nơi chạy:** thư mục gốc trong Bash. **Cần có:** công cụ và dataset đã sẵn sàng.

```bash
make setup
```

Lệnh tạo `.env` từ [template](../.env.example), quyền `0600`, sinh hai mật khẩu Mongo độc lập và Kafka cluster ID. Chạy lại giữ file có sẵn. Sửa `.env` bằng editor local nếu cần:

| Biến                                                                       | Mặc định / tác dụng                                                      |
| -------------------------------------------------------------------------- | ------------------------------------------------------------------------ |
| `COMPOSE_PROJECT_NAME`                                                     | `ecommerce-analytics`; namespace container/network/volume của stack      |
| `DATASET_PATH`                                                             | `./Dataset/retailrocket-recommender-system-dataset`; đường dẫn trên host |
| `MIN_AVAILABLE_RAM_GIB`                                                    | `7`; gate bộ nhớ, không phải lệnh cấp RAM cho Docker                     |
| `MIN_DOCKER_DISK_GIB`                                                      | `30`; gate dung lượng Docker data                                        |
| `KAFKA_CLUSTER_ID`, `HDFS_CLUSTER_ID`                                      | Danh tính dữ liệu bền vững trong volume; giữ ổn định khi khởi động lại   |
| `MONGO_*`                                                                  | Database, app/root account; password đã sinh chỉ lưu local               |
| `KAFKA_HOST_PORT`, `MONGO_HOST_PORT`                                       | `19092`, `27018`; endpoint host                                          |
| `HDFS_UI_PORT`                                                             | `9870`                                                                   |
| `SPARK_MASTER_UI_PORT`, `SPARK_WORKER_1_UI_PORT`, `SPARK_WORKER_2_UI_PORT` | `18080`, `18081`, `18082`                                                |
| `SPARK_APPLICATION_UI_PORT`                                                | `14040`, dùng khi driver đang chạy                                       |

Các cổng host phải khác nhau, trong khoảng 1024–65535 và không bị ứng dụng khác chiếm. Đổi trước khi `make up`; dùng cổng mới khi mở UI. Giá trị có khoảng trắng phải được quote đúng theo [cấu hình](infra/03-configuration.md). Cấu hình hạ tầng lấy từ `.env`; đừng chỉ `export DATASET_PATH=...` rồi mong manager thay cấu hình file.

```bash
make config
```

Đạt khi Compose render hợp lệ. Output có thể chứa giá trị nhạy cảm đã resolve; không đưa nguyên output hoặc `.env` vào ticket/commit. Render thành công chưa chứng minh đủ RAM, dataset đọc được hay dịch vụ healthy.

Không tạo lại `.env` khi volume đang giữ dữ liệu. Đổi password trong file không tự đổi account đã tồn tại; đổi cluster ID có thể bị từ chối khi mount volume cũ. Giữ bản sao `.env` an toàn cùng kế hoạch backup volume. Xem [khởi tạo và vòng đời](infra/04-bootstrap-and-lifecycle.md).

<a id="infrastructure"></a>

## 6. Kiểm thử offline, build và khởi động hạ tầng

### 6.1. Kiểm thử local

**Cần có:** cấu hình và các công cụ đã cài; không cần cụm chạy.

```bash
make test
```

Bộ được đối chiếu ngày 20/10 có 38 test hạ tầng và 211 test ứng dụng; 24 test live offline nằm trong suite ứng dụng. Trên máy bình thường kỳ vọng các suite kết thúc `OK`, không failure/error. Phiên sandbox trước có 7 HTTP skipped vì cấm loopback socket; đó **không phải số skip bắt buộc trên máy mới**. Serving có 23 test API offline và 15 test JS qua các lệnh riêng sau khi cài API/Node. Số test có thể tăng theo commit; exit code và tổng kết suite của mã đang dùng mới là bằng chứng. Test offline không chứng minh Spark/HDFS/Mongo/Kafka thực sự hoạt động. Nếu lỗi thiếu Compose/Bash/curl, sửa công cụ host rồi chạy lại.

### 6.2. Build image lần đầu

**Cần có:** Docker/Buildx, bốn CSV và `.env`; preflight build vẫn kiểm tra dataset.

```bash
make build
```

Build kéo Kafka/Mongo theo digest, dựng Hadoop/Spark và connector, ghi `artifacts/infra/versions.json`. Các bản chính: Kafka 3.9.1, Hadoop 3.4.2/Java 11, Spark 3.5.7/Java 17, Python 3.11 và MongoDB 8.0. Xác nhận exit code 0 và manifest được tạo.

Lần đầu tải binary Spark khoảng 382 MiB, Hadoop lean khoảng 489 MiB cộng base image/dependency; có thể lâu tùy mạng. Các dòng `Adding debian:...pem` là cập nhật CA. BuildKit cache giữ download đã kiểm checksum và phần tải dở. Khi mạng lỗi/ngắt, chạy lại `make build`; không prune cache trước khi retry. Lệnh build lại sử dụng cache nhưng cache vẫn chiếm disk. Nếu `docker: unknown command: docker buildx`, cài plugin ở đúng nhánh OS rồi kiểm tra `docker buildx version` trong cùng terminal. Xem [tối ưu build](infra/09-build-optimization.md).

### 6.3. Khởi động và smoke

**Cần có:** build đạt, đủ RAM/disk/cổng; chưa có Spark driver khác.

```bash
make up
make doctor
make smoke
```

Chạy từng lệnh; nếu một lệnh lỗi thì dừng ở đó. `make up` dùng image có sẵn, chờ healthcheck và init; không tự build image thiếu. Đạt khi có:

```text
Infrastructure ready: nine healthy services and three completed init jobs.
```

Readiness phải xác nhận hai Spark worker, hai HDFS DataNode, hai topic nghiệp vụ và app Mongo authentication. `kafka-init`, `hdfs-init`, `mongo-init` ở trạng thái `Exited (0)` là đúng; chín service còn lại phải healthy. Có thể xem lại bằng `make ps`.

Smoke tạo dữ liệu riêng để kiểm Kafka → Spark → HDFS/Mongo, hai worker, hai replica, checkpoint restart và upsert lặp. Đạt khi exit 0 và có `artifacts/infra/smoke.json`; xem `smoke.log` nếu lỗi. Smoke không ingest CSV và không đánh giá chất lượng ML. Cách đọc từng field: [kiểm thử hạ tầng](infra/06-testing.md).

Kiểm tra quyền đọc dataset từ chính runtime sau khi cụm lên:

```bash
docker compose exec -T spark-client bash -lc 'id; for f in /opt/dataset/events.csv /opt/dataset/category_tree.csv /opt/dataset/item_properties_part1.csv /opt/dataset/item_properties_part2.csv; do test -r "$f" || exit 1; done'
```

Nếu lỗi quyền, kiểm tra file và quyền đi qua các thư mục của `DATASET_PATH`, chỉnh đúng phạm vi rồi thử lại; không sửa mount thành writable.

**Tùy chọn trước ingest:** khi không có job, chạy `make persistence-test`. Nó tạo marker riêng, thực hiện `down/up` giữ volume, xác minh Kafka/HDFS/Mongo rồi dọn marker. Vì có restart cả stack, không chạy giữa demo hoặc training. Báo cáo `persistence-create`, `persistence-verify`, `persistence-clean` nằm ở `artifacts/infra/`.

<a id="ingestion"></a>

## 7. Ingest dữ liệu lịch sử vào Bronze/Silver

**Nơi chạy:** root repository, một terminal Bash. **Cần có:** doctor/smoke đạt, bốn CSV ổn định, không có Spark driver khác.

```bash
make test-data
make ingest
```

`test-data` chạy fixture tích hợp thật và dọn namespace riêng. `ingest` fingerprint bốn nguồn, lưu bản nguyên gốc vào Bronze, chuẩn hóa `events.csv`, tách dòng lỗi/quarantine, loại trùng hoàn toàn và ghi Silver Parquet theo ngày UTC. File thuộc tính/cây danh mục được giữ trong Bronze; chưa dùng toàn bộ thuộc tính cho ML.

**Đầu ra:** `artifacts/data/test-data.json`, `artifacts/data/latest-ingest.json`, `<dataset_version>/events-v1/{result,quality,manifest,bronze-manifest}.json` và `runs/<UUID>.log` trong `artifacts/data/`.

**Đạt khi:** test-data đạt cả chín test, cleanup thành công; ingest exit 0, `quality_status=passed`, hai worker/hai replica theo report. Với Retailrocket nguyên bản, có **2.756.101 event đầu vào, 460 dòng trùng và 2.755.641 event Silver**. Thời gian dữ liệu là năm 2015; không đổi cutoff sang ngày hiện tại.

```bash
python3 -m json.tool artifacts/data/latest-ingest.json
```

Nếu thiếu file/sai header, sửa nguồn rồi chạy lại. Nếu snapshot thiếu marker/sai checksum, dừng để kiểm tra log và snapshot; job không tự ghi đè bản published. Chạy lại cùng dữ liệu/mã sẽ xác minh Bronze/Silver trước khi reuse; hai cờ tương ứng là `bronze_reused` và `silver_reused` trong `result.json`. Xem [ingestion](data/03-ingestion.md) và [tiêu chí dữ liệu](data/05-testing.md).

<a id="behavior"></a>

## 8. Phân tích lịch sử và demo streaming

### 8.1. Kiểm thử và phân tích toàn bộ lịch sử

**Cần có:** Silver passed, không có job Spark khác. Chạy lần lượt:

```bash
make test-stream
make test-behavior
make analyze-behavior
make behavior-status
make behavior-status ITEM_ID=355908
```

`test-stream` kiểm replay, validation, duplicate/late event, DLQ, checkpoint và retry. `test-behavior` kiểm quy tắc session/funnel. Batch `analyze-behavior` tính session với khoảng bất hoạt **quá 30 phút**, funnel đúng thứ tự và cùng sản phẩm; Mongo công bố snapshot đã đối soát với HDFS Gold.

**Đầu ra:** `artifacts/streaming/test-stream.json`, `artifacts/analytics/test-behavior.json`, `artifacts/analytics/latest-analysis.json` và report/status theo analysis ID.

**Đạt khi:** 10 test streaming và 6 test behavior đạt, cleanup/resource đạt; phân tích lịch sử published. Cùng nguồn chuẩn có **2.755.641 event, 1.761.675 session và 235.061 sản phẩm**. `behavior-status` không truyền `RUN_ID` đọc phân tích lịch sử; truyền `RUN_ID` sẽ chọn kết quả demo tương ứng.

Test fault injection cố ý làm query lỗi để kiểm tra phục hồi. Ví dụ `RuntimeError: injected before Spark checkpoint commit` có thể xuất hiện giữa log dù suite đạt. Kết luận dựa trên **exit code, `OK`, JSON cuối `passed=true`, cleanup và monitor**, không dựa riêng vào chữ `ERROR`. Nếu cuối cùng `FAILED` hoặc exit khác 0, mở đúng log suite và sửa trước khi chạy dữ liệu chính.

### 8.2. Chuẩn bị demo mới

Chọn RUN_ID chưa dùng. Ví dụ xuyên suốt dưới đây là `onboarding-demo-001`; nếu đã có run này, thay bằng tên mới ở **mọi lệnh/mọi terminal**.

```bash
make prepare-demo RUN_ID=onboarding-demo-001
```

Mặc định lấy event trong `[2015-09-10T10:00:00.000Z, 2015-09-10T16:00:00.000Z)`, sắp theo hợp đồng replay, tốc độ 10 event/giây, giới hạn 10.000 event. Nguồn chuẩn có **2.692 event**; producer mất khoảng 4,5 phút chỉ để phát số event này ở tốc độ mặc định, chưa gồm thời gian drain.

Đầu ra là `artifacts/streaming/onboarding-demo-001/` với config, manifest và source snapshot, cùng pointer `latest-run.json`. Có thể đổi `START`, `END`, `RATE` ngay khi prepare; khi retry phải giữ nguyên config. Không dùng RUN_ID cũ cho một demo có mã/cửa sổ khác.

### 8.3. Terminal A: chạy consumer streaming trước

Mở Bash thứ nhất, vào cùng clone:

```bash
cd ~/workspace/E-commerce-User-Behavior-Analytics-Platform
make stream RUN_ID=onboarding-demo-001
```

Giữ terminal mở. Đợi có dòng **`STREAM_READY=onboarding-demo-001`** rồi mới replay. Consumer đọc Kafka, validate/dedup, cập nhật KPI/session/funnel, lưu checkpoint HDFS và snapshot Mongo.

### 8.4. Terminal B: phát replay

Mở Bash thứ hai, cùng thư mục và RUN_ID:

```bash
cd ~/workspace/E-commerce-User-Behavior-Analytics-Platform
make replay RUN_ID=onboarding-demo-001
```

Producer in `REPLAY_PROGRESS`, ghi ACK/end offsets và kết thúc với `status=completed`. Consumer ở terminal A sẽ drain đến các offset đã ACK, commit checkpoint, đối soát rồi tự kết thúc. **Producer xong chưa đủ để chạy job Spark khác**; đợi cả terminal A trả về prompt.

### 8.5. Theo dõi và nghiệm thu demo

Có thể dùng terminal thứ ba cho các lệnh status:

```bash
cd ~/workspace/E-commerce-User-Behavior-Analytics-Platform
make stream-status RUN_ID=onboarding-demo-001
make behavior-status RUN_ID=onboarding-demo-001
make behavior-status RUN_ID=onboarding-demo-001 ITEM_ID=355908
```

**Đạt khi:** replay completed, streaming exit 0, backlog 0, kết quả cuối được đánh dấu final và report/resource đủ; số accepted/invalid/duplicate/late được giải thích. Artifact tham chiếu demo chuẩn có 2.692 event, 1.893 session, funnel 1.882 → 35 → 9. Latency phụ thuộc máy, không yêu cầu khớp số đo của máy bàn giao.

Nếu ngắt giữa chừng, giữ source/checkpoint/ACK và chạy lại stream/replay **cùng RUN_ID, cùng mã và tham số** theo thứ tự trên. Replay completed không phát lại cả nguồn. Muốn demo lần mới, chuẩn bị RUN_ID mới. Không xóa checkpoint để né lỗi. Xem [vận hành streaming](streaming/03-operations.md), [phục hồi](streaming/04-testing-and-recovery.md) và [session/funnel](analytics/03-operations.md).

<a id="segments"></a>

## 9. Feature bảy ngày và K-Means lịch sử

**Cần có:** Silver passed và mọi stream đã kết thúc. Chạy tuần tự trong root:

```bash
make test-ml
make build-features
make train-segments
make segment-status
```

Builder lấy tám feature trên bảy ngày trước **`2015-07-15T00:00:00.000Z`**, không dùng sự kiện tại/sau cutoff. Pipeline biến đổi feature và StandardScaler, thử K=2..5, chọn silhouette tốt nhất, lưu/tải lại model và công bố assignment/profile theo visitor.

**Đầu ra:** `artifacts/ml/test-ml.json`, `latest-features.json`, `latest-model.json`, report theo snapshot/model và status. **Đạt khi:** 6 test tích hợp/cleanup đạt, feature/model published, fingerprint/replica/resource khớp. Với cửa sổ mặc định có **99.038 visitor**. Lượt tham chiếu chọn K=2, silhouette khoảng 0,853847; đây là kết quả đã đo, không phải con số cần hardcode vào code hay kỳ vọng mọi phiên bản môi trường cho ra tuyệt đối giống nhau.

Có thể tra visitor:

```bash
make segment-status VISITOR_ID=001
```

Visitor ID là chuỗi: `001` khác `1`; kết quả null có thể đúng nếu visitor không nằm trong cohort. Không gán tên “khách giá trị cao” cho cụm khi chưa có dữ liệu doanh thu/cơ sở đánh giá.

Nếu chưa có feature pointer, chạy `build-features` trước; `train-segments` không tự dựng nguồn thiếu. Nếu code hash/snapshot khác, xem report và phiên bản mã trước khi chọn lại ID. Xem [vận hành ML](ml/03-operations.md) và [kiểm thử K-Means](ml/04-testing.md).

<a id="prediction"></a>

## 10. Random Forest và batch scoring mua hàng

### 10.1. Kiểm thử và tạo dữ liệu huấn luyện

**Cần có:** Silver passed, không có driver khác; giữ nguyên mã/CSV trong suốt quá trình.

```bash
make test-purchase
make build-purchase-data
```

`test-purchase` chạy bảy test tích hợp RF với fixture nhỏ, gồm fit/save/load và sáu fault scenario. Builder thật tạo feature bảy ngày và nhãn **visitor có mua trong 24 giờ tiếp theo** ở 128 cutoff hằng ngày. Một visitor có thể có nhiều mẫu ở các cutoff khác nhau; tổng dưới đây không phải số người duy nhất.

| Split      | Cutoff UTC, hai đầu bao gồm | Số ngày |     Số mẫu | Positive |
| ---------- | --------------------------- | ------: | ---------: | -------: |
| Train      | 11/05–15/07/2015            |      66 |  5.196.014 |    1.750 |
| Validation | 17/07–15/08/2015            |      30 |  2.560.155 |      729 |
| Test       | 17/08–17/09/2015            |      32 |  2.380.550 |      726 |
| Tổng       | 128 cutoff                  |     128 | 10.136.719 |    3.205 |

Ngày 16/07 và 16/08 là khoảng nghỉ giữa split. Validation/test giữ nguyên cohort, không downsample. Các mốc đã cố định theo dữ liệu 2015 để kiểm soát dữ liệu tương lai; đừng thay bằng ngày chạy máy.

Builder chạy **20 chặng tuần tự**, thường tối đa bảy ngày/chặng và tạo một Spark application mỗi chặng. Theo dõi `PURCHASE_BUILD_CHUNK=9/20`, `PURCHASE_DAY` và `artifacts/prediction/<purchase_dataset_id>/progress.json`. Mỗi ngày đã commit được reuse khi retry; index cuối chỉ published khi đủ ngày. Chạy lại có thể đếm tiến độ host từ 0 nhưng vẫn dùng các daily snapshot hợp lệ trong HDFS.

Lượt build tham chiếu mất khoảng 45–55 phút; đây không phải cam kết thời gian. Mỗi Spark child có timeout 1.800 giây, **không phải toàn bộ lệnh đa chặng giới hạn 30 phút**. Không chạy thêm job để “tận dụng thời gian”; hai worker đang phục vụ cùng một driver. Nếu log vẫn tiến từng ngày/chặng, đây là công việc bình thường. Xem [dữ liệu prediction](ml/05-purchase-data.md) và [vận hành/progress](ml/07-purchase-operations.md).

**Đạt khi:** test passed/cleaned/resource đạt; đủ 128 ngày/20 chặng, counts khớp; có `latest-purchase-data.json` và report dataset. Nếu ngắt, chạy lại đúng `make build-purchase-data` với cùng mã/nguồn; không reset volume hoặc xóa ngày đã commit.

### 10.2. Train và đánh giá RF gốc

```bash
make train-purchase
make purchase-status
```

Train giữ toàn bộ 1.750 positive và lấy 35.000 negative theo quy tắc sampling, tổng **36.750 mẫu**. Thử bốn cấu hình gồm 50/100 cây × depth 5/8 tuần tự; chọn candidate bằng PR-AUC validation, chọn threshold bằng F1 validation, khóa selection trước khi đánh giá test. Không dùng K-Means cluster hoặc visitor ID làm feature.

**Đầu ra:** `latest-purchase-model.json`, model report/status, `training.json`, `progress.json` và candidate logs dưới `artifacts/prediction/`. **Đạt khi:** model published, đủ bốn candidate, số mẫu đúng, model reload/fingerprint và monitor đạt. Lượt train toàn nguồn tham chiếu mất khoảng 30 phút; đọc tiến độ thay vì suy luận chỉ từ thời gian chờ.

Kết quả tham chiếu RF gốc: 50 cây/depth 5/ngưỡng 0,93; PR-AUC test 0,18172, precision **43,63%**, recall **21,21%**. Phần lớn mẫu là negative nên accuracy đơn lẻ không phản ánh khả năng tìm người mua. Precision thấp dẫn tới bước policy ở mục 11; không chỉnh threshold theo test để làm đẹp kết quả.

Nếu thiếu dataset pointer/report thì quay lại builder. Nếu một candidate lỗi, xem log/resources của đúng child rồi retry cùng phiên bản; candidate published được xác minh trước reuse. Xem [phương pháp RF](ml/06-random-forest.md).

### 10.3. Score một cohort lịch sử

```bash
make score-purchases
make purchase-score-status
make purchase-score-status VISITOR_ID=001
```

Score mặc định chốt **`2015-09-18T00:00:00.000Z`**, tính feature bảy ngày trước đó, dùng RF published và ghi Gold/Mongo. Cohort này không có nhãn mua tương lai đầy đủ để đánh giá lại precision như tập test.

**Đầu ra:** `latest-purchase-score.json` chọn score gốc; report/status theo `score_run_id`. **Đạt khi:** published, đủ **69.052 visitor**, mỗi visitor đúng một record, score/feature/fingerprint/Mongo count khớp, hai replica và resource đạt. Lượt tham chiếu có 10 predicted positive ở ngưỡng 0,93; số người được gắn nhãn này không phải số người chắc chắn sẽ mua.

Không có visitor lookup có thể là kết quả hợp lệ. Nếu score cutoff khác, truyền `AS_OF` có chủ đích và không dùng count 69.052 làm chuẩn cho cửa sổ mới. Không truyền MODEL_VERSION K-Means sang RF; các selector dùng registry/pointer khác nhau. Chi tiết nghiệm thu: [kiểm thử prediction](ml/08-purchase-testing.md).

<a id="policy"></a>

## 11. Policy ưu tiên precision và score run mới

**Cần có:** RF gốc published, saved validation/test predictions, original batch score; không có driver khác. Không cần build lại dataset, fit lại RF hoặc build image.

### 11.1. Kiểm thử và đánh giá policy

```bash
make test-purchase-policy
make tune-purchase-policy
make purchase-policy-status
```

Policy xét lưới ngưỡng 0..1, bước 0,01, chọn bằng validation để đạt **precision ≥70%, recall ≥10%**, ưu tiên recall cao nhất theo tie rule đã định. Selection được lưu trước khi đánh giá test ở đúng ngưỡng đã chọn. Test không đạt thì không tìm ngưỡng thay thế bằng test.

**Đầu ra:** `artifacts/prediction/policies/test-purchase-policy.json`, `latest-policy.json`, `<policy_version>/report.json` và `status.json`.

**Kiểm tra bắt buộc trước apply:**

- Test suite đạt 5 test, cleanup và sáu fault scenario; các resource flag đạt.
- Lệnh tune exit 0 và `passed=true`: tác vụ kỹ thuật thành công.
- Đọc **`eligible`** và phần `report.evaluation`: chỉ `eligible=true` mới được áp dụng. `passed=true` hoặc policy `published` riêng lẻ chưa chứng minh mục tiêu đạt.

Nếu `eligible=false`, lưu report/lý do, giữ original score và **dừng phần apply**. Đây là một kết quả đánh giá hợp lệ; không bỏ gate, chọn lại theo test hoặc coi onboarding bị lỗi kỹ thuật. Điều tra dữ liệu/model và thiết kế thí nghiệm cải tiến riêng.

Artifact tham chiếu đã xác nhận ngưỡng **0,96**, validation precision 79,65%/recall 12,35%, test precision **73,88%**/recall **13,64%** (TP=99, FP=35, FN=627, TN=2.379.789), `eligible=true`. Máy mới phải kiểm tra report của mình. Policy ghi `evaluation_kind=posthoc_threshold_reassessment`, `fresh_holdout=false` vì test đã từng được xem; chưa phải kiểm chứng độc lập bằng dữ liệu tương lai mới.

`purchase_score` chưa hiệu chuẩn sau negative sampling: điểm 0,96 **không có nghĩa xác suất mua 96%**. Đổi ngưỡng giữ nguyên điểm/xếp hạng và PR-AUC/ROC-AUC; nó thay nhóm gắn nhãn, tăng precision và giảm recall trong kết quả tham chiếu.

### 11.2. Apply khi policy đủ điều kiện

Chỉ chạy khối sau sau khi đã đọc `eligible=true`:

```bash
make apply-purchase-policy
```

Lệnh đọc **original score run của cùng RF**, tạo ID/snapshot mới, giữ toàn bộ visitor, raw feature và purchase score, chỉ đổi nhãn theo ngưỡng policy. Không truyền score đã áp policy làm nguồn cho lần apply khác.

Lấy score ID **của máy mình** từ pointer mới, rồi status đúng ID:

```bash
POLICY_SCORE_RUN_ID="$(python3 -c 'import json; print(json.load(open("artifacts/prediction/policies/latest-policy-score.json"))["score_run_id"])')"
make purchase-score-status SCORE_RUN_ID="$POLICY_SCORE_RUN_ID"
make purchase-score-status SCORE_RUN_ID="$POLICY_SCORE_RUN_ID" VISITOR_ID=001
```

Biến shell này chỉ tồn tại ở terminal hiện tại; mở terminal mới thì đọc lại pointer. **Không truyền `SCORE_RUN_ID` thì status vẫn đọc score gốc**, vì apply không sửa `latest-purchase-score.json`. Không hardcode ID dài từ máy bàn giao.

**Đạt khi:** policy score published, count vẫn **69.052**, score-only fingerprint không đổi, nhãn đúng ngưỡng, Mongo/HDFS đối soát và monitor đạt. Lượt tham chiếu có **4 predicted positive** sau policy; đây là nhãn batch, không phải precision test trên bốn người. Apply lần hai đã được xác nhận `reused=true`, cùng ID/fingerprint, không duplicate. Máy mới cần kiểm riêng bước reuse ở mục 14.

Nếu model/source/code/Spark version không khớp, đọc đúng log và selector; không sửa pointer bằng tay để ép apply. Xem [policy: quy tắc, báo cáo và phục hồi](ml/09-purchase-policy.md).

<a id="streaming-ml"></a>

## 12. ML streaming: replay và simulator

**Cần có:** Silver, K-Means, RF và policy eligible đã published, mọi Spark driver trước đã kết thúc. Không build image hoặc fit lại model nguồn thật. Pipeline mới có namespace/checkpoint riêng; các lệnh ở mục 8 vẫn chạy KPI/funnel v2 độc lập.

Kiểm thử và chuẩn bị replay mới, từ root:

```bash
make test-stream-ml
make prepare-ml-demo RUN_ID=ml-replay-001
```

Suite phải đạt bảy test, bảy fault scenario, cleanup và resource acceptance. Artifact bàn giao đã xác nhận các tiêu chí này; máy mới cần chạy lại để nghiệm thu riêng. Prepare khóa model/policy từ latest pointer và seed bảy ngày **trước START**, không dùng phần replay chưa qua Kafka để tính feature.

Replay ML mặc định ngày **18/09/2015 `[00:00,03:00)` UTC**, 10 event/giây, có 1.528 event và 1.016 visitor. Ngày bắt đầu đã nằm sau cửa sổ nhãn test của policy hiện có; không dùng mặc định v2 ngày 10/09 cho policy này. Cửa sổ demo tối đa 24 giờ/10.000 event.

Terminal A, Bash cùng clone:

```bash
cd ~/workspace/E-commerce-User-Behavior-Analytics-Platform
make stream-ml RUN_ID=ml-replay-001
```

Đợi `STREAM_ML_READY=ml-replay-001`, terminal B:

```bash
cd ~/workspace/E-commerce-User-Behavior-Analytics-Platform
make replay-ml RUN_ID=ml-replay-001
```

Terminal khác có thể chạy `make stream-ml-status RUN_ID=ml-replay-001`. Đợi consumer tự kết thúc trước bước tiếp. Mỗi snapshot tính feature tại max accepted event-time +1 ms, transform bằng hai model đã tải và gắn nhãn theo policy. Đạt khi backlog 0, `batch_reconciled=true`, đủ 1.528 event/1.016 prediction, checkpoint/resource/hai worker/hai replica đạt; đọc mục tiêu latency riêng trong report.

Sau đó chuẩn bị simulator bằng RUN_ID khác:

```bash
make prepare-ml-demo RUN_ID=ml-sim-001 DEMO_SOURCE=simulator
```

Terminal A chạy `make stream-ml RUN_ID=ml-sim-001`; đợi READY, terminal B chạy `make simulate-ml RUN_ID=ml-sim-001`. Ba visitor mới phát chín event qua ba đợt; producer đợi snapshot trước committed rồi phát tiếp. Có visitor chỉ xem, visitor xem → giỏ → mua và visitor thêm giỏ trước lượt xem, giữ leading zero.

```bash
make stream-ml-status RUN_ID=ml-sim-001
make stream-ml-status RUN_ID=ml-sim-001 VISITOR_ID=000099000000003
```

Đạt khi đủ ba visitor/chín event, feature cập nhật ba đợt, final snapshot được đối soát và monitor đạt. Không ép score/label phải tăng. Các run mới nằm ở `artifacts/streaming-ml/`; không thay các pointer batch gốc. Retry cùng RUN_ID/mã/config, giữ seed/checkpoint; muốn đổi model hoặc nguồn thì prepare RUN_ID mới. Chi tiết field, selector, timeout, fault và cleanup: [ML streaming](ml/10-streaming-scoring.md).

<a id="serving"></a>

## 12A. API local và dashboard Live Server

### 12A.1. Cần có gì trước khi mở web?

Cần MongoDB đang chạy, `.env` của máy này và Python 3.11+ với venv/pip. Hoàn tất mục 12 để có run ML nghiệp vụ xem được; API vẫn có thể khởi động trước khi có run, nhưng dashboard chưa có số liệu. Không dùng run KPI v2 ở mục 8 để kỳ vọng có bảng prediction.

Frontend dùng HTML/CSS/JS thuần qua Live Server. API là tiến trình host; dữ liệu/model/Kafka/HDFS/MongoDB vẫn ở Docker. Không có bước npm install, build frontend hoặc container web.

### 12A.2. Cài API và indexes một lần

Tại root repository, khi MongoDB đã healthy:

```bash
make api-setup
make api-indexes
```

| Lệnh | Công dụng | Dấu hiệu đạt |
| --- | --- | --- |
| `api-setup` | Tạo `services/.venv/`, cài dependency khóa phiên bản, chạy `pip check` | Exit 0; `No broken requirements found` |
| `api-indexes` | Tạo/kiểm tra indexes cho run, minute, prediction và feed live | `Serving indexes verified; no snapshot content changed.` |

`api-indexes` không mở HTTP server. Dòng pip thông báo có bản mới không phải lỗi cài đặt. Nếu Python mặc định quá cũ, dùng interpreter 3.11+ cho `api-setup`; sau đó manager gọi interpreter trong virtualenv. Không copy `services/.venv/` từ máy khác.

### 12A.3. Terminal A: giữ API hoạt động

```bash
cd ~/workspace/E-commerce-User-Behavior-Analytics-Platform
make api
```

Đợi `Uvicorn running on http://127.0.0.1:8000`. Giữ terminal này mở. Từ terminal C:

```bash
curl -sS http://127.0.0.1:8000/api/v1/health
```

Kỳ vọng `{"status":"ready","mongodb":true}`. Mở [Swagger API](http://127.0.0.1:8000/docs) để kiểm tra các GET endpoint. API lấy application credentials từ `.env`, không cần root Mongo; không đưa mật khẩu/URI Mongo vào JS.

Nếu `make api` đã thoát hoặc bạn bấm Ctrl+C, HTTP server đã dừng dù trước đó có dòng startup thành công. Giữ API chạy độc lập khi mở web hoặc phát live.

### 12A.4. Mở frontend bằng VS Code

1. Mở đúng folder root repository trong VS Code.
2. Mở `web/index.html` trong Explorer.
3. Chuột phải → **Open with Live Server**; có thể dùng lệnh tương ứng trong Command Palette.
4. Mở URL extension cung cấp. Với root repository và cổng mặc định: [http://127.0.0.1:5500/web/](http://127.0.0.1:5500/web/) hoặc `/web/index.html`.
5. Kiểm tra `web/config.js` có `apiBaseUrl: "http://127.0.0.1:8000"` trùng API đang chạy. Nếu Live Server chọn cổng khác, dùng đúng URL in ở VS Code.
6. Chọn `ml-replay-001` hoặc `ml-sim-001` đã chạy ở mục 12. Thu gọn **Xem event mới nhất** nếu khung feed che khu vực bên dưới.

Đạt khi trạng thái báo dữ liệu đã kết nối, RUN_ID hiện đúng, có batch đã commit và các màn hình **Tổng quan**, **Phân khúc & dự đoán**, **Theo dõi pipeline** đọc được dữ liệu. Run đang chuẩn bị có thể chưa có batch; chọn một run completed để kiểm tra ban đầu.

Nếu mở riêng folder `web` trong VS Code, URL có thể là `/index.html`. Không mở bằng `file://`. Trên WSL, mở workspace qua Remote WSL và kiểm tra URL health từ chính browser trước; localhost forwarding phải hoạt động. [Hướng dẫn extension chính thức](https://marketplace.visualstudio.com/items?itemName=ritwickdey.LiveServer) mô tả các cách mở/dừng Live Server.

### 12A.5. Đọc dữ liệu đầu tiên và đổi cổng

Tại **Phân khúc & dự đoán**, đọc biểu đồ cluster và bảng **Visitor ưu tiên theo điểm mô hình**. Giữ nhãn lọc **Tất cả visitor**; lọc **Dự đoán mua** có thể rỗng nếu không ai vượt ngưỡng. Precision/recall ở cuối trang là metrics lịch sử của model/policy, chưa phải chất lượng dự đoán stream đang xem.

Nếu 8000 đã có ứng dụng khác, đặt `API_HOST_PORT=8001` trong `.env`, dừng API cũ rồi chạy `make api`. Sửa `web/config.js` tương ứng:

```js
window.ECOMMERCE_CONFIG = {
  apiBaseUrl: "http://127.0.0.1:8001",
  refreshMs: 5000,
  sparkUiPort: 18080,
  hdfsUiPort: 9870
};
```

Kiểm tra health tại cổng mới, tải lại browser; thay cổng trong các ví dụ curl bên dưới. Live Server có cổng riêng. CORS chấp nhận origin HTTP localhost/127.0.0.1 với các cổng; không cần thay Mongo host port chỉ vì đổi API port.

### 12A.6. Kiểm thử lớp application

Giữ API chạy, dùng terminal C. Chạy từng lệnh và kiểm tra tổng kết:

```bash
make test-api-offline
make test-api
make smoke-web RUN_ID=ml-replay-001
```

- `test-api-offline`: kiểm thử HTTP với adapter, không chứng minh Mongo thật.
- `test-api`: chạy thêm fixture Mongo thật, cần đủ suite và cleanup; report `artifacts/serving/test-api.json`.
- `smoke-web`: cần run `ml-replay-001` đã **completed** và API hoạt động. Đối chiếu KPI/funnel/cluster/model, đọc hết prediction và fingerprint; đo p95 100 request overview, mục tiêu ≤1 giây. Report `artifacts/serving/smoke-web-ml-replay-001.json` phải `passed=true`.

Kịch bản smoke trong tài liệu dùng replay đã hoàn tất hoặc simulator nhỏ chín event `ml-sim-001`. Không thay bằng run con live sinh ngẫu nhiên: smoke hiện có lookup visitor riêng cho simulator nhỏ. Để nghiệm thu live, dùng quy trình mục 12B và report/API của đúng run.

Nếu muốn chạy test frontend, cài Node 18+ từ package manager trước. Ubuntu/WSL: `sudo apt install -y nodejs`; Arch: `sudo pacman -S --needed nodejs`. Sau khi `node --version` đáp ứng yêu cầu:

```bash
make test-web
```

Kỳ vọng 15 test JS theo commit đang đối chiếu; không cần npm install. Dashboard vẫn hoạt động khi chưa cài Node. Test Node dùng adapter/logic, không xác nhận đầy đủ layout hoặc cập nhật trên browser thật.

Hướng dẫn thao tác từng màn hình, định nghĩa mọi chỉ số, cách lọc/tra visitor và tình huống lỗi: [Sử dụng web và đọc kết quả](serving/06-web-user-guide.md).

<a id="live-demo"></a>

## 12B. Live nhiều lượt và nghiệm thu ứng dụng

### 12B.1. Điều kiện và kiểm tra nhanh

Hoàn tất Silver, K-Means, RF và policy eligible, API indexes và Live Server. API có thể đọc dữ liệu đồng thời; cần kết thúc các job Spark trước. Khi cụm đang idle, kiểm tra `make doctor`, rồi:

```bash
make test-live
```

Đây là 24 test offline theo commit được đối chiếu, bao gồm pacing, ACK/cursor, chuyển lượt và pause/resume giả lập. Kết quả offline không thay thế hai lượt Kafka/Spark thật bên dưới.

### 12B.2. Terminal B: nghiệm thu hữu hạn trước

Trong cùng root repository:

```bash
make live-demo LIVE_ID=onboarding-live-check-001 RATE=5 LIVE_EVENTS=120 LIVE_VISITORS=24 LIVE_ITEMS=12 LIVE_SEGMENTS=2
```

Lệnh tự chuẩn bị seed, tải hai model một lần, tạo run con và chạy consumer/producer. Đợi `LIVE_SEGMENT_READY=onboarding-live-check-001-000001`. Trên web chọn phiên `onboarding-live-check-001`, bật **Theo dõi live** và **Tự cập nhật KPI**. Quan sát feed ACK, rồi snapshot Spark và chuyển sang run con `-000002`.

Không mở thêm consumer/producer. API ở terminal A và Live Server vẫn giữ mở. Chờ lệnh terminal B tự kết thúc sau hai lượt; không dùng Ctrl+C để thay cho hoàn tất kịch bản hữu hạn.

Kiểm tra report từ terminal C:

```bash
python3 -m json.tool artifacts/live/onboarding-live-check-001/report.json
python3 -m json.tool artifacts/live/onboarding-live-check-001/resources.json
make live-status LIVE_ID=onboarding-live-check-001
```

**Đạt khi:** `passed=true`, `status=completed`, `completed_segments=2`, `completed_events=240`, hai resource flags true; từng `LIVE_SEGMENT_RESULT` có `passed=true`, `batch_reconciled=true`, backlog 0. API phải trả tổng ACK/processed 240; cluster/prediction của từng run con thuộc đúng batch đã commit. Số giao dịch, cluster distribution và predicted positive phụ thuộc nguồn/model; không bắt buộc có người được dự đoán mua.

### 12B.3. Terminal B: demo liên tục

Sau khi lượt hữu hạn kết thúc, dùng **LIVE_ID mới** vì tham số được khóa:

```bash
make live-demo LIVE_ID=onboarding-live-001 RATE=5
```

Mặc định mỗi lượt 1.000 event, 200 visitor, 100 sản phẩm; tiếp tục nhiều lượt cho đến khi yêu cầu dừng. Chọn `onboarding-live-001` trong **Phiên live**, bật hai checkbox theo dõi/KPI. Run con có dạng `onboarding-live-001-000001`, `-000002`… Mỗi lượt có cohort visitor mới; KPI chính thuộc run con đang được xem, tổng trong panel live thuộc toàn phiên.

| Tham số khi tạo phiên | Mặc định | Ý nghĩa/giới hạn |
| --- | ---: | --- |
| `RATE` | 5 | Event/giây; >0 và ≤10 |
| `LIVE_EVENTS` | 1.000 | Event mỗi lượt, 12..1.000 |
| `LIVE_VISITORS` | 200 | 3..200 và không quá nửa event mỗi lượt |
| `LIVE_ITEMS` | 100 | 3..100 |
| `LIVE_SEGMENTS` | 0 | 0 là liên tục; số dương giới hạn tổng số lượt |

Seed nguồn được giữ theo phiên; đổi LIVE_ID sinh kịch bản khác. Ngày nghiệp vụ vẫn là 2015 theo cutoff model, còn giờ phát là giờ hiện tại UTC. Thêm dữ liệu không buộc score, nhãn hoặc số người mua phải tăng.

Feed kiểm tra khoảng một giây, KPI có nhịp đọc dự phòng năm giây và theo pointer mới. Spark trigger 15 giây nhưng batch có thể xử lý lâu hơn; Kafka ACK trước khi Spark commit là bình thường. Producer chờ khi backlog đạt 200. Khi chuyển lượt, web giữ snapshot hoàn chỉnh trước cho tới khi lượt mới có batch công bố.

### 12B.4. Dừng, kiểm tra và tiếp tục

Nhấn **Ctrl+C một lần** tại terminal B, giữ terminal mở để producer ngừng gửi và Spark drain phần đã ACK, tối đa 180 giây theo runtime. Hoặc từ terminal C:

```bash
make live-stop LIVE_ID=onboarding-live-001
```

`stop_requested=true` chỉ xác nhận yêu cầu dừng đã được ghi, chưa chứng minh drain thành công. Chờ terminal B trả kết quả rồi kiểm tra:

```bash
make live-status LIVE_ID=onboarding-live-001
python3 -m json.tool artifacts/live/onboarding-live-001/report.json
```

Sau khi xác nhận driver đã dừng, cùng mã và cấu hình:

```bash
make live-demo LIVE_ID=onboarding-live-001
```

Tiếp tục từ ACK cursor/checkpoint; không đổi RATE hoặc tham số nguồn để resume. Khi code hash thay đổi, chọn LIVE_ID mới hoặc trở về đúng mã cũ; không sửa request/checkpoint để bỏ kiểm tra. Chạy tiếp tới một lượt hoàn tất và đối chiếu tổng không cộng trùng. Phiên dừng giữa lượt có thể được công nhận vòng đời dừng/drain thành công, nhưng `completed_segments` chỉ đếm lượt đã đối soát đủ nguồn.

Nếu `stale`, `failed`, còn ACK chưa commit hoặc có guard unconfirmed, giữ log/report và kiểm tra [phục hồi](#recovery), [live runbook](serving/05-live-demo.md). Không coi consumer `running` đã lưu trong registry là bằng chứng job còn chạy; kiểm tra heartbeat và Spark UI. Lượt thực tế `demo-live-002` từng dừng với 1.035 ACK/1.000 processed dù lượt đầu thành công, nên quy trình stop/resume phải nghiệm thu riêng trên máy mới.

### 12B.5. Điểm kết thúc của lộ trình

Ứng dụng sẵn sàng để bàn giao khi developer có thể tự làm đủ các việc sau:

- Hạ tầng healthy, ingest/model/policy đã published và có report hợp lệ trên chính máy mình.
- Health API ready; web mở qua Live Server, không có banner kết nối lỗi.
- Chọn run completed và đọc được KPI/funnel, cluster/prediction, pipeline cùng snapshot.
- Live hữu hạn hoàn tất hai lượt và đối soát đúng 240 event, resource đầy đủ, không OOM/restart.
- Trên browser quan sát ACK tăng, processed tăng sau commit và tự chuyển snapshot sang run con tiếp theo.
- Nếu cần vận hành liên tục, đã kiểm tra dừng/drain/resume riêng; ghi pending nếu chưa đạt.

Tài liệu web có [bài thực hành và checklist](serving/06-web-user-guide.md#walkthrough) để người nhận tự xác nhận các thao tác này. Kết thúc demo giữ kết quả xem lại, dừng live trước, dừng API/Live Server riêng; dùng `make down` khi muốn dừng cả hạ tầng nhưng giữ volume.

<a id="artifacts"></a>

## 13. Đọc artifact, selector và theo dõi

### 13.1. File nào dùng để làm gì?

`artifacts/` được tạo khi chạy, không nằm trong Git. HDFS/Mongo giữ dữ liệu và registry; file host là log, bản sao report và pointer cho CLI. Pointer `latest-*` chỉ chọn một phiên bản thành công gần nhất, không phải toàn bộ lịch sử. Khi job mới fail, pointer cũ vẫn có thể tồn tại; đối chiếu log/exit code/thời điểm attempt mới.

| Thư mục/pointer dưới `artifacts/`                        | Nội dung và lúc tạo                                                                            |
| -------------------------------------------------------- | ---------------------------------------------------------------------------------------------- |
| `infra/versions.json`                                    | Image IDs/digests, phiên bản Docker/Compose sau build hoặc `make versions`                     |
| `infra/smoke.log`, `infra/smoke.json`                    | Log và kết quả smoke; có thể chỉ có log khi fail                                               |
| `data/latest-ingest.json`                                | Dataset version, quality status và Bronze/Silver paths sau ingest                              |
| `streaming/latest-run.json`, `streaming/<RUN_ID>/`       | Source/config, ACK, producer, report, status, checkpoint references và logs demo               |
| `analytics/latest-analysis.json`                         | Chọn kết quả session/funnel lịch sử                                                            |
| `ml/latest-features.json`, `ml/latest-model.json`        | Feature snapshot và model K-Means                                                              |
| `prediction/latest-purchase-data.json`                   | Dataset RF đã đủ ngày, không chọn dataset partial                                              |
| `prediction/latest-purchase-model.json`                  | Model RF đã công bố                                                                            |
| `prediction/latest-purchase-score.json`                  | Original batch score; giữ nguyên khi apply policy                                              |
| `prediction/policies/latest-policy.json`                 | Policy đánh giá gần nhất, có thể `eligible=false`                                              |
| `prediction/policies/latest-policy-score.json`           | Score run mới được apply policy                                                                |
| `streaming-ml/latest-run.json`, `streaming-ml/<RUN_ID>/` | Config/model binding, seed/source references, ACK, report/status và resources của ML streaming |
| `live/<LIVE_ID>/` | Request/config đã khóa, report/status, resources, log từng attempt và kết quả các run con |
| `serving/` | Report/log test API và smoke HTTP theo RUN_ID; khác report latency Spark |

Live quản lý log/report ở cấp LIVE_ID; không giả định mỗi run con có thư mục host `streaming-ml/`. Run con và snapshot vẫn ở registry/HDFS. Không đưa `live/<LIVE_ID>/control.json` chứa owner token vào log công khai.

Các lệnh ứng dụng dùng ID để ghi `report.json`, `status.json`, log/resource hoặc attempt log trong namespace tương ứng; đường dẫn log chính xác thường nằm trong field `log` của report. Có thể đọc JSON mà không cần cài `jq`:

```bash
python3 -m json.tool artifacts/prediction/latest-purchase-model.json
python3 -m json.tool artifacts/prediction/policies/latest-policy.json
```

Hai file trên chỉ tồn tại **sau bước tạo tương ứng**; file không có trên fresh clone là bình thường. Muốn đọc policy report mới nhất sau tune:

```bash
python3 - <<'PY'
import json
from pathlib import Path
base = Path("artifacts/prediction/policies")
key = json.loads((base / "latest-policy.json").read_text())["policy_version"]
report = json.loads((base / key / "report.json").read_text())
print(json.dumps(report, indent=2, ensure_ascii=False))
PY
```

### 13.2. Selector và identity

| Tham số Make                                                     | Dùng cho                                                                                     |
| ---------------------------------------------------------------- | -------------------------------------------------------------------------------------------- |
| `RUN_ID`, `START`, `END`, `RATE`                                 | Demo streaming; cấu hình chuẩn bị run, status/replay dùng cùng RUN_ID                        |
| `ITEM_ID`                                                        | Lookup funnel sản phẩm                                                                       |
| `AS_OF`                                                          | Cutoff feature/K-Means hoặc original RF scoring, tùy lệnh; policy lấy cutoff từ source score |
| `FEATURE_SNAPSHOT_ID`                                            | Chọn feature snapshot K-Means khi train                                                      |
| `MODEL_VERSION`                                                  | Model K-Means với lệnh segments, model RF với lệnh purchase/policy; không tráo ID            |
| `PURCHASE_DATASET_ID`                                            | Chọn dataset khi train RF                                                                    |
| `SCORE_RUN_ID`                                                   | Status một score cụ thể; khi apply là **original source score**                              |
| `POLICY_VERSION`                                                 | Chọn policy đã đánh giá                                                                      |
| `VISITOR_ID`                                                     | Lookup ID chuỗi, giữ leading zero                                                            |
| `DEMO_SOURCE`, `SEGMENT_MODEL_VERSION`, `PURCHASE_MODEL_VERSION` | Chọn replay/simulator và model riêng khi prepare ML streaming; policy dùng `POLICY_VERSION`  |
| `LIVE_ID`, `LIVE_EVENTS`, `LIVE_VISITORS`, `LIVE_ITEMS`, `LIVE_SEGMENTS` | Phiên live, quy mô mỗi lượt và giới hạn tổng lượt; cấu hình bị khóa khi tạo phiên |

Dùng assignment trên từng lệnh, ví dụ `make behavior-status RUN_ID=onboarding-demo-001`, thay vì export selector dài hạn làm các giai đoạn sau đọc nhầm registry. ID model/snapshot phụ thuộc dữ liệu, mã, tham số và phiên bản; không lấy ID máy khác để điền trên máy mới. Không tự sửa JSON pointer hoặc snapshot đã published.

Ứng dụng bind mount `app/` vào `/opt/app`; sửa app thường không cần image rebuild. Tuy vậy archive code hash là một phần hợp đồng snapshot/run. Đổi package có thể buộc tạo identity mới hoặc từ chối tiếp tục run cũ. Giữ nguyên commit để resume; thay Dockerfile/dependency thì mới build lại image và áp dụng `make up` khi không có job.

### 13.3. Dịch vụ và quan sát

| Endpoint mặc định từ host                          | Công dụng                                                              |
| -------------------------------------------------- | ---------------------------------------------------------------------- |
| <http://127.0.0.1:9870>                            | NameNode UI, dung lượng/block/DataNodes                                |
| <http://127.0.0.1:18080>                           | Spark master, worker/application                                       |
| <http://127.0.0.1:18081>, <http://127.0.0.1:18082> | Hai Spark worker                                                       |
| <http://127.0.0.1:14040>                           | Spark application UI khi driver còn chạy                               |
| `127.0.0.1:19092`                                  | Kafka host client                                                      |
| `127.0.0.1:27018`                                  | Mongo host client; database/authSource `ecommerce`, app user từ `.env` |
| <http://127.0.0.1:8000/docs> | API host, Swagger; health ở `/api/v1/health` |
| <http://127.0.0.1:5500/web/> | Dashboard nếu Live Server mở root repository ở cổng mặc định |

Publish bind `127.0.0.1`; không tự mở ra Internet. Trên Windows/WSL thử các URL localhost từ browser Windows; nếu forwarding không hoạt động thì kiểm tra Desktop/WSL trước khi đổi network. Trong container dùng `kafka:9092`, `mongodb:27017`, `hdfs://namenode:9000`, `spark://spark-master:7077`; localhost của container không trỏ sang dịch vụ khác.

Terminal quan sát, không tạo thêm Spark driver:

```bash
make ps
docker stats --no-stream
docker compose logs --tail=100 kafka spark-master spark-client
```

`make logs` theo dõi log service liên tục; Ctrl+C chỉ kết thúc việc xem log. Job logs chứa kết quả chi tiết hơn log container. Resource acceptance cần `no_oom_or_restart=true`, `memory_observations_complete=true` với các job có monitor, cùng hai worker/hai replica theo report. Chỉ nhìn một lần `docker stats` không đủ chứng minh toàn job không OOM. Không gửi log/config chứa mật khẩu vào Git hoặc kênh công khai.

<a id="reuse"></a>

## 14. Nghiệm thu chạy lại, không cần dựng lại máy

Sau lượt đầu thành công, lưu report/log/resources và commit dùng để chạy. Lặp từng lệnh dưới đây **tuần tự, cùng nguồn/mã/tham số**, khi không còn driver khác. Các bước reuse vẫn phải đọc và xác minh snapshot, có thể mất vài phút hoặc lâu hơn; đây là phần nghiệm thu bổ sung, không bắt buộc lặp mỗi lần mở máy.

| Lệnh chạy lại                                    | Bằng chứng cần kiểm                                                                                           |
| ------------------------------------------------ | ------------------------------------------------------------------------------------------------------------- |
| `make ingest`                                    | Cùng dataset/manifest, `bronze_reused=true`, `silver_reused=true`                                             |
| `make analyze-behavior`                          | Cùng analysis ID/manifest/counts, `reused=true`                                                               |
| `make build-features`, rồi `make train-segments` | Cùng feature/model ID và fingerprint, `reused=true`, không fit mới                                            |
| `make build-purchase-data`                       | Daily snapshots/index cùng ID/counts, `reused=true` sau đủ chặng                                              |
| `make train-purchase`                            | Sample/candidates/model reuse, cùng selection/metrics, không fit mới                                          |
| `make score-purchases`                           | Cùng score ID/fingerprint, không thêm Mongo duplicate                                                         |
| `make tune-purchase-policy`                      | Cùng policy/selection/evaluation, `reused=true`; không chọn ngưỡng lại                                        |
| `make apply-purchase-policy` nếu eligible        | Cùng policy score ID, `reused=true`, Mongo count/fingerprint không đổi                                        |
| `make stream-ml RUN_ID=...` sau completed        | Cùng config/model, checkpoint cuối và ML fingerprint, `reused=true`, không phát lại event hoặc thêm row trùng |
| `make live-demo LIVE_ID=...` sau dừng/drain | Cùng mã/config/seed, tiếp tục đúng cursor/checkpoint, tổng phiên không cộng trùng; nghiệm thu riêng với run còn dở |

Sau apply lại, đọc lại `latest-policy-score.json` và chạy `purchase-score-status SCORE_RUN_ID=...` bằng cách ở mục 11. Original pointer vẫn phải giữ nguyên. Với cùng identity, retry snapshot published chỉ xác minh; không âm thầm chữa payload hỏng.

Bằng chứng hiện có của máy bàn giao đã xác nhận behavior/K-Means full-source reuse và policy **apply reuse**. RF gốc build/train/score toàn nguồn và full-source tune-policy reuse chưa có đủ bằng chứng chạy lặp; live fixture đã kiểm các nhánh reuse nhưng không thay thế nghiệm thu trên toàn nguồn. Người nhận đánh dấu theo kết quả máy mình, không kế thừa dấu đạt chỉ từ tài liệu.

<a id="recovery"></a>

## 15. Khởi động lại, ngắt job và xử lý lỗi

### 15.1. Sau reboot hoặc ngày làm việc mới

Mở Docker daemon/Desktop; trên WSL mở Ubuntu đã bật integration. Tại **cùng clone**, dùng `.env` và volume cũ:

```bash
cd ~/workspace/E-commerce-User-Behavior-Analytics-Platform
docker info
docker compose version
docker buildx version
make up
make doctor
```

Không cần chạy lại setup/build/ingest/train nếu image, cấu hình và dữ liệu còn nguyên. Có thể chạy smoke trước khi tiếp tục kiểm thử; chỉ khi không có job khác. Dùng status của giai đoạn đã hoàn thành, hoặc retry đúng job bị ngắt. `docker compose up -d` có cú pháp đúng nhưng `make up` là entrypoint khuyến nghị vì có preflight/readiness bổ sung; `docker compose-up -d` là sai cú pháp.

Muốn mở ứng dụng: terminal A chạy `make api`, mở lại Live Server rồi chọn RUN_ID cũ. Muốn dữ liệu mới: terminal B chạy live/replay theo [mục 12B](#live-demo). Chỉ xem lại kết quả không cần phát lại event. Chạy `api-setup` khi venv chưa có hoặc dependency thay đổi, `api-indexes` khi cần áp dụng indexes mới.

### 15.2. Ctrl+C và khóa driver

Các manager batch/replay áp timeout **1.800 giây mỗi child**, gửi cancel theo UUID và xác nhận remote job đã dừng khi ngắt. Riêng `live-demo` dùng wrapper không có timeout 30 phút để chạy liên tục; vẫn có drain/cancel và gate dung lượng trước mỗi lượt. Smoke/persistence giới hạn **600 giây mỗi child**. Chờ lệnh trả về và đọc thông báo trước khi mở job mới; kill Docker exec client không luôn đồng nghĩa driver trong container đã dừng.

Nếu thấy `Another managed Spark job is running`, xác định job/terminal hiện tại và chờ hoặc ngắt đúng terminal. Không xóa `driver.lock` để ép chạy chồng. Lock chỉ điều phối cùng workspace; một clone khác hoặc spark-submit thủ công có thể xung đột cổng driver dù không thấy lock này.

Nếu có `artifacts/infra/driver-unconfirmed.json`, đọc marker, log UUID và kiểm tra Spark UI/process trong client. Manager đang chặn vì chưa xác nhận remote driver dừng. Chỉ gỡ marker sau khi đã xác nhận **đúng job ghi trong marker** dừng; xem [phục hồi streaming](streaming/04-testing-and-recovery.md). Không coi xóa marker là cách dừng job.

Live có cả guard producer `artifacts/streaming/producer-unconfirmed.json` khi cancel chưa xác nhận. Đợi drain bằng Ctrl+C một lần; bấm lần hai có thể kích hoạt force cancel. `stop_requested=true` hoặc `no_oom_or_restart=true` riêng lẻ chưa đủ công nhận stop/resume: cần snapshot/offset, trạng thái cuối và quan sát tài nguyên đầy đủ.

Snapshot đã commit được giữ để retry; phần staging chưa published không được coi là kết quả. Không đổi branch, sửa package hoặc sửa CSV giữa lần bị ngắt và lần resume.

### 15.3. Bảng xử lý nhanh

| Triệu chứng                                      | Kiểm tra và hành động                                                                                                              |
| ------------------------------------------------ | ---------------------------------------------------------------------------------------------------------------------------------- |
| Không kết nối Docker / permission denied         | Đúng context, daemon/Desktop đang mở, Linux group hoặc WSL integration; chạy lại `docker info` trong chính terminal dùng Make.     |
| `unknown command: docker buildx` sau reboot      | Kiểm tra plugin thực sự được cài, PATH/context và cùng môi trường CLI; Arch cài `docker-buildx`, WSL dùng CLI từ Desktop.          |
| Thiếu khoảng 7 GiB khả dụng                      | Đóng ứng dụng/stack khác; WSL cấp đủ VM RAM và restart có kiểm soát; không hạ gate để vượt qua khi máy thiếu RAM thật.             |
| Không đủ 30 GiB Docker disk                      | Kiểm tra Docker data/virtual disk, image/cache/volume đang chiếm; chỉ dọn phần đã xác định không dùng.                             |
| Cổng bị chiếm                                    | Đổi cổng host tương ứng trong `.env` trước start hoặc xử lý đúng tiến trình đang dùng; không dừng stack khác tùy tiện.             |
| CSV thiếu/sai header/permission                  | Kiểm tra đúng `DATASET_PATH`, bốn file trực tiếp và runtime UID 1000 đọc được; không giải nén thêm tầng thư mục.                   |
| `Application source is not mounted`              | Khi không có job, chạy `make up` để áp bind mount ứng dụng.                                                                        |
| Init `Exited (0)`                                | Bình thường; chỉ lỗi khi exit khác 0 hoặc service thường trực không healthy.                                                       |
| Kafka `NOT_LEADER_FOR_PARTITION`                 | Mã hiện tại có refresh/retry; nếu lặp đến fail, xem broker/readiness/log mới và tài nguyên, không dùng script cũ trong clone khác. |
| `Broker handle destroyed`                        | Kiểm tra đang chạy commit có sửa lifecycle AdminClient; xem log Kafka và smoke. Không reset volume để chữa lỗi lifecycle client.   |
| `injected ... checkpoint commit` rồi suite `OK`  | Fault test chủ động; đọc kết quả cuối và resource/cleanup. `FAILED` hoặc exit khác 0 vẫn là lỗi phải xử lý.                        |
| Build purchase lâu ở chặng 9/20                  | Xem `PURCHASE_DAY`, `progress.json`, Spark UI và resources; builder có 20 Spark chặng. Retry reuse ngày đã commit.                 |
| Code SHA/Spark version khác snapshot             | Trở về mã/môi trường gốc để resume hoặc chủ động tạo identity/run mới; không sửa manifest/checkpoint để vượt check.                |
| Thiếu `_SUCCESS`, checksum/count/fingerprint sai | Giữ log, xác định snapshot hỏng và khôi phục đúng bản; không tự xóa cả data lake hoặc overwrite published.                         |
| Status trả score gốc sau apply                   | Đọc `policies/latest-policy-score.json`, truyền chính xác `SCORE_RUN_ID` mới.                                                      |
| Policy passed nhưng eligible false               | Tác vụ chạy đúng nhưng mục tiêu không đạt; giữ score gốc và không apply.                                                           |
| API indexes đã verified nhưng web không kết nối | Chạy và giữ `make api`; kiểm tra URL health/cổng, indexes không mở server. |
| Thiếu venv/pip hoặc Python quá cũ | Dùng Python 3.11+ cho API; Ubuntu cài `python3-venv`/`python3-pip`; xem interpreter của `services/.venv/`. |
| Web không thấy cluster/prediction | Chọn Phân khúc & dự đoán, thu gọn feed, chọn run ML có committed batch; dùng hướng dẫn web. |
| Lọc nhãn Dự đoán mua rỗng | So score với ngưỡng policy, bỏ lọc để đọc toàn bộ visitor; không phải bằng chứng pipeline chưa score. |
| `sleep length must be non-negative` trong live | Bản hiện tại sửa pacing và có test hồi quy. Cập nhật đúng mã; phiên khóa code cũ cần ID mới, không build/train lại. |
| Live stale, registry vẫn running | Đối chiếu heartbeat, ACK/processed, stop request, Spark active application, report/log/resources; trạng thái registry có thể đã cũ. |
| Live dừng ở lượt 2, lượt 1 đã completed | Giữ snapshot lượt 1; đối soát ACK còn dở ở lượt 2 và điều tra job trước khi resume; không đánh dấu toàn phiên passed từ một run con. |

Chi tiết thêm: [hạ tầng](infra/07-troubleshooting.md), [ingestion](data/03-ingestion.md), [prediction](ml/07-purchase-operations.md).

<a id="cleanup"></a>

## 16. Dừng và dọn dữ liệu đúng phạm vi

Trước khi dừng/reset, kết thúc job ở terminal đang chạy và xác nhận remote driver đã dừng. Chọn thao tác theo mục đích:

Dừng live/drain trước; Ctrl+C terminal API và Stop Live Server riêng. `make down` chỉ quản lý Docker, không dừng hai tiến trình host này. Không có image/container/volume web riêng để dọn.

| Thao tác                                 | Container/network project | Named volumes           | Images/cache          | Host `.env`, CSV, `artifacts/` |
| ---------------------------------------- | ------------------------- | ----------------------- | --------------------- | ------------------------------ |
| `make down`                              | Dừng/gỡ                   | Giữ                     | Giữ                   | Giữ                            |
| Reboot rồi `make up`                     | Tạo/chạy lại khi cần      | Dùng lại                | Dùng lại              | Dùng lại                       |
| `make reset CONFIRM=delete-project-data` | Dừng/gỡ                   | **Xóa dữ liệu project** | Giữ                   | Giữ                            |
| Dọn image hoặc BuildKit cache riêng      | Phụ thuộc thao tác        | Không tự xóa            | Xóa đúng phần đã chọn | Giữ                            |

Dừng cuối ngày, giữ kết quả để dùng tiếp:

```bash
make down
```

**Chỉ khi chủ động muốn xóa toàn bộ Kafka/HDFS/Mongo của project**, đã lưu các kết quả/backup cần thiết, dùng riêng lệnh phá hủy sau:

```bash
make reset CONFIRM=delete-project-data
```

Reset không xóa nguồn CSV, `.env`, image hoặc artifact host. Vì volume đã rỗng, những pointer/model ID cũ trong `artifacts/` không còn chứng minh dữ liệu tồn tại. Khi muốn dựng lại sạch từ đầu, sau reset có thể **di chuyển** artifact cũ ra ngoài workspace để lưu bằng chứng trước khi tạo lượt mới:

```bash
ARTIFACT_BACKUP_DIR="../ecommerce-artifacts-backup-$(date +%Y%m%d-%H%M%S)"
if [ -d artifacts ]; then
  mv artifacts "$ARTIFACT_BACKUP_DIR"
fi
```

Sau đó `make up`, doctor, smoke rồi chạy lại ingest và các bước cần thiết. Không cần build lại nếu image còn. Việc tái tạo RF full history tốn thời gian như lượt đầu.

Nếu muốn thu hồi cả image/cache: xem [vòng đời hạ tầng](infra/04-bootstrap-and-lifecycle.md) và [build/cache](infra/09-build-optimization.md), kiểm tra `docker system df` và danh sách image trước. Hadoop/Spark image được nhiều service dùng chung; Kafka/Mongo và BuildKit cache cũng có thể được project khác dùng. Chỉ xóa image ID/cache đã xác định không cần; tránh `docker system prune --all --volumes` như một bước mặc định. Không xóa cache tải dở trong lúc muốn tiếp tục build.

<a id="handoff"></a>

## 17. Checklist bàn giao trên máy người nhận

Điền theo lượt chạy thực tế, giữ commit, phiên bản image, report/log/resources và ghi rõ mục chưa chạy. Không đưa password hoặc toàn bộ `.env` vào biên bản công khai.

- [ ] Linux/Ubuntu WSL Bash, amd64/AVX, ≥4 CPU, đủ RAM và disk; Docker/Compose/Buildx hoạt động.
- [ ] Clone đúng commit; bốn CSV đúng header/layout và runtime đọc được; `.env` tạo riêng cho máy.
- [ ] `make config`, `make test`, build đạt; versions manifest đã lưu.
- [ ] Up/doctor: chín service healthy, ba init exit 0, hai worker/hai DataNode và app Mongo auth.
- [ ] Smoke đạt; persistence-test nếu đã chọn chạy phải có verify/cleanup thành công.
- [ ] Test-data 9 test đạt; Silver 2.755.641 event, quality passed; Bronze/Silver paths và manifest hợp lệ.
- [ ] Test-stream 10 test, test-behavior 6 test và cleanup đạt; history 1.761.675 session/235.061 sản phẩm.
- [ ] Demo mới: 2.692 event ở cửa sổ mặc định, replay completed, consumer kết thúc, backlog 0, snapshot final.
- [ ] Test-ml 6 test đạt; 99.038 visitor feature, model/assignment K-Means published.
- [ ] Test-purchase 7 test đạt; dataset RF 128 ngày/20 chặng, split counts/positive đúng; đủ bốn candidate.
- [ ] RF/model status published, metrics/confusion đã đọc; original score/status đủ 69.052 visitor.
- [ ] Test-purchase-policy 5 test/sáu fault/cleanup đạt; policy report đã đọc cả `passed` và `eligible`.
- [ ] Nếu eligible: apply/status theo ID mới, đủ 69.052 visitor, raw score giữ nguyên, nhãn đúng ngưỡng, original pointer giữ nguyên.
- [ ] Nếu không eligible: ghi lý do/mục tiêu không đạt, giữ original score, đánh dấu apply không thực hiện.
- [ ] Test-stream-ml bảy test/bảy fault/cleanup đạt; replay 1.528 event/1.016 visitor và simulator chín event/ba visitor đạt, batch reconciliation/resource đầy đủ; đánh dấu pending nếu chưa chạy.
- [ ] API venv/indexes sẵn sàng, health ready, `.env`/cổng đúng; API được giữ chạy riêng.
- [ ] Mở Live Server, chọn run completed, xem đủ ba màn hình; hiểu scope live/run/batch, feature/score/ngưỡng và metrics lịch sử.
- [ ] Test API/Mongo và smoke HTTP của replay completed được chạy/ghi nhận riêng; test JS cần Node và không thay browser acceptance.
- [ ] Live hữu hạn hai lượt: 240 ACK/processed, report passed, batch reconciliation/resource đạt và web theo đúng snapshot.
- [ ] Live liên tục: dừng/drain/resume không cộng trùng đã kiểm tra riêng; ghi pending nếu thiếu bằng chứng.
- [ ] Các bước reuse ở mục 14 được đánh dấu riêng theo bằng chứng; policy apply lần hai cùng ID/count/fingerprint và ML streaming completed reuse.
- [ ] Job có monitor đủ quan sát, không OOM/restart, hai worker/hai replica; không còn remote driver bị bỏ lại.
- [ ] Biết cách reboot/start, đọc log, resume, `down` giữ volume và reset phá hủy dữ liệu.

Các counts nêu trên áp dụng **đúng Retailrocket nguyên bản và cutoff mặc định**. Metric, latency, duration và ID trong artifact tham chiếu không phải cam kết cho máy/commit khác. Chất lượng mô hình cần được đánh giá lại từ report của người nhận, đặc biệt nếu đổi dataset, feature, sampling hoặc version runtime.

Tra cứu sâu theo phần: [hạ tầng](infra/README.md), [dữ liệu](data/README.md), [streaming](streaming/README.md), [session/funnel](analytics/README.md), [ML và policy](ml/README.md), [sử dụng web và đọc kết quả](serving/06-web-user-guide.md). [Kế hoạch tổng thể](../IMPLEMENTATION_PLAN.md) còn mô tả các mục tiêu tương lai; dùng runbook này và mã hiện tại để xác định những gì đã chạy được.
