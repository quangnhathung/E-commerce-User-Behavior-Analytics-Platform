# 06 — Hướng dẫn sử dụng web và đọc kết quả

[← Mục lục serving](README.md) · [Chạy từ clone GitHub](../GETTING_STARTED.md) · [Vận hành live](05-live-demo.md)

Tài liệu dành cho developer, người trình bày demo và người phân tích kết quả trên dashboard **E-commerce User Behavior Analytics Platform**. Bạn sẽ biết cách mở ứng dụng, chọn đúng dữ liệu, đọc KPI/funnel, phân cụm, dự đoán mua hàng và xác định pipeline có đang hoạt động.

Đối chiếu giao diện, API, mã mô hình và artifact ngày **20/10/2026**. Các số liệu từ `demo-live-002-000001` là ví dụ đã quan sát trên máy bàn giao; ID này không tự có sau khi clone và nguồn live mới có thể cho kết quả khác. Tài liệu hướng dẫn thao tác theo nhãn đang có trên giao diện, không yêu cầu xem lịch sử chat.

## Mục lục

1. [Ứng dụng trả lời những câu hỏi nào?](#purpose)
2. [Mở ứng dụng và xác nhận kết nối](#open)
3. [Chọn phiên, lượt dữ liệu và snapshot](#selection)
4. [Đọc feed sự kiện trực tiếp](#feed)
5. [Màn hình Tổng quan](#overview)
6. [Màn hình Phân khúc & dự đoán](#predictions)
7. [Hiểu mô hình, feature và kết quả dự báo](#interpretation)
8. [Màn hình Theo dõi pipeline](#pipeline)
9. [Bài thực hành từ xem lịch sử đến demo live](#walkthrough)
10. [Lỗi thường gặp và cách đối chiếu](#troubleshooting)
11. [Dừng, mở lại và lưu bằng chứng](#daily)
12. [Tra cứu nhanh API và thuật ngữ](#reference)

<a id="purpose"></a>

## 1. Ứng dụng trả lời những câu hỏi nào?

| Nhu cầu | Xem ở đâu? | Kết quả nhận được |
| --- | --- | --- |
| Có bao nhiêu người xem, thêm giỏ, mua? | Tổng quan | KPI của event đã được Spark chấp nhận và commit trong run |
| Người dùng dừng ở bước nào? | Tổng quan → Hành trình chuyển đổi | Funnel xem → thêm giỏ → mua, đúng thứ tự và cùng sản phẩm |
| Sản phẩm nào được quan tâm? | Tổng quan → Sản phẩm được quan tâm | Top sản phẩm theo phiên có xem, số phiên đi tiếp qua funnel |
| Có những nhóm hành vi nào? | Phân khúc & dự đoán | Cluster của K-Means, quy mô và feature trung bình |
| Visitor nào có dấu hiệu mua tiếp? | Phân khúc & dự đoán | Điểm Random Forest, nhãn theo policy và danh sách xếp hạng |
| Stream còn chạy hay đang chờ? | Feed live và Theo dõi pipeline | ACK, processed, backlog, heartbeat, producer/consumer/scoring |

Web đọc snapshot đã được công bố. Khởi tạo dữ liệu, train model và bật/tắt producer/consumer được thực hiện từ terminal. API và web có thể mở cùng lúc với một job Spark.

Nếu là máy mới chưa có dữ liệu/model, thực hiện [runbook từ clone đến dashboard](../GETTING_STARTED.md). Bốn CSV hoặc model batch tồn tại chưa đủ để bộ chọn run trên web có dữ liệu: cần run streaming ML nghiệp vụ `stream-ml-v1`. Namespace test, streaming KPI v2 và toàn bộ cohort batch lịch sử không nằm trong bộ chọn v1 này.

<a id="open"></a>

## 2. Mở ứng dụng và xác nhận kết nối

### 2.1. Những phần cần hoạt động

| Phần | Vị trí chạy | Vai trò |
| --- | --- | --- |
| MongoDB và hạ tầng dữ liệu | Docker Compose | Giữ kết quả, model/registry; Kafka/Spark/HDFS xử lý khi chạy job |
| FastAPI | Host, `make api` | Đọc MongoDB và trả snapshot HTTP |
| Frontend | VS Code Live Server | Phục vụ HTML/CSS/JS cho browser |
| Live job, nếu muốn dữ liệu thay đổi | Terminal riêng, `make live-demo` | Sinh event và xử lý nhiều lượt |

Để xem run đã hoàn tất, không cần bật lại producer. Để tạo dữ liệu mới, chạy thêm job theo [bài thực hành](#walkthrough).

### 2.2. Mở API

Tất cả lệnh dưới đây chạy từ root repository trong Bash Linux/Ubuntu WSL2. Dùng `.env` được tạo trên chính máy này. Nếu đã cài API và indexes, bỏ hai lệnh chuẩn bị đầu:

```bash
make api-setup
make api-indexes
make api
```

Giữ terminal `make api` mở. `api-indexes` chỉ chuẩn bị indexes; dòng `Serving indexes verified` không có nghĩa HTTP server đã chạy. Với cấu hình mặc định, từ terminal khác:

```bash
curl -sS http://127.0.0.1:8000/api/v1/health
```

Đạt khi có `{"status":"ready","mongodb":true}`. Swagger ở [http://127.0.0.1:8000/docs](http://127.0.0.1:8000/docs). Nếu health không mở được, xử lý kết nối trước khi điều tra biểu đồ.

### 2.3. Mở web

1. Mở folder root repository trong VS Code.
2. Cài extension [Live Server — Ritwick Dey](https://marketplace.visualstudio.com/items?itemName=ritwickdey.LiveServer), ID `ritwickdey.LiveServer`, nếu chưa có.
3. Mở `web/index.html` → chuột phải → **Open with Live Server**.
4. Dùng URL VS Code cung cấp; thường là [http://127.0.0.1:5500/web/](http://127.0.0.1:5500/web/) hoặc `/web/index.html`.
5. Đợi bộ chọn **LƯỢT DỮ LIỆU** được nạp và trạng thái bên trái báo dữ liệu đã kết nối.

Không mở HTML bằng `file://`. Nếu chỉ mở folder `web` trong VS Code, URL thường là `/index.html`. Khi 5500 bận, Live Server có thể dùng cổng khác. Frontend chạy không cần npm, Node hoặc build image; Node chỉ cần cho test JS.

### 2.4. Địa chỉ nào dùng cho việc gì?

| Địa chỉ mặc định | Chức năng |
| --- | --- |
| `http://127.0.0.1:5500/web/` | Dashboard người dùng |
| `http://127.0.0.1:8000/api/v1/health` | Kiểm tra API/MongoDB |
| `http://127.0.0.1:8000/docs` | Tra cứu và thử GET API |
| `http://127.0.0.1:18080` | Spark master: worker và application thực sự đang chạy |
| `http://127.0.0.1:9870` | HDFS: DataNode, block và dung lượng |
| `127.0.0.1:27018` | MongoDB host port, không phải trang web |

API port nằm ở `API_HOST_PORT` trong `.env`, mặc định 8000. `web/config.js` phải có `apiBaseUrl` tương ứng. Đổi API port cần khởi động lại API và tải lại web; xem [hướng dẫn đổi cổng](03-dashboard.md#đổi-cổng). Live Server có cổng riêng; trên WSL kiểm tra health từ browser Windows và dùng workspace Remote WSL.

<a id="selection"></a>

## 3. Chọn phiên, lượt dữ liệu và snapshot

### 3.1. Phân biệt bốn cấp dữ liệu

| Khái niệm | Ví dụ | Phạm vi |
| --- | --- | --- |
| LIVE_ID | `onboarding-live-001` | Cả phiên demo, có nhiều lượt; tổng ACK/processed cộng qua các lượt |
| RUN_ID | `onboarding-live-001-000001` | Một lượt nguồn hữu hạn; KPI/funnel/prediction phía dưới thuộc lượt này |
| Batch | `15` | Một micro-batch Spark đã được xử lý/công bố trong run; ID có thể có khoảng trống |
| Snapshot | RUN_ID + batch | Phiên bản kết quả được khóa để KPI, funnel và ML đọc cùng ngữ cảnh |

Một run replay như `ml-replay-001` có thể không thuộc phiên live. Số `000001` cuối ID live là thứ tự lượt, không phải cluster, model hoặc batch. Khi lượt mới chuẩn bị, web có thể tiếp tục hiển thị snapshot của lượt trước.

### 3.2. Các điều khiển chung

| Điều khiển | Thao tác | Hành vi cần biết |
| --- | --- | --- |
| LƯỢT DỮ LIỆU | Chọn RUN_ID | Đổi KPI/funnel/ML/pipeline; xóa bộ lọc, visitor và trang cũ; tắt Theo dõi live |
| Nhãn nguồn | Đọc `replay` hoặc `simulator` | Nguồn của run; live sinh ngẫu nhiên vẫn dùng loại simulator |
| Tự cập nhật KPI | Bật/tắt checkbox | Đọc lại nội dung chính tối thiểu theo nhịp 5 giây và khi live có pointer mới |
| Cập nhật | Bấm nút | Đọc lại nội dung chính của run đang chọn; không phát thêm event |
| Dòng khoảng thời gian | Đọc START → END | Cửa sổ nghiệp vụ của nguồn run, UTC |
| Batch / Đọc … UTC | Đọc nhãn snapshot | Batch đang xem và giờ API được đọc; giờ đọc không phải event-time |
| Phiên live | Chọn LIVE_ID | Đổi feed và tổng toàn phiên; bật theo dõi để nội dung chính đi theo snapshot của phiên |
| Theo dõi live | Bật checkbox | Nội dung chính theo run/batch mới đã commit; cần Tự cập nhật KPI đang bật |
| Xem event mới nhất | Bấm để thu gọn/mở | Thu gọn bảng feed để thấy KPI/biểu đồ/bảng phân khúc phía dưới |

Muốn xem lịch sử ổn định: chọn RUN_ID thủ công và có thể tắt Tự cập nhật KPI. Feed vẫn kiểm tra độc lập mỗi giây; việc tắt KPI không dừng producer. Muốn quay lại live: chọn đúng Phiên live, bật Theo dõi live và Tự cập nhật KPI.

Bộ chọn run hiện tối đa 200 run nghiệp vụ gần nhất; danh sách phiên live tối đa 20. Nếu không thấy dữ liệu, kiểm tra run/version/base và phạm vi danh sách trước. Reload trang nạp lại lựa chọn theo logic hiện tại; không coi đó là bookmark cố định RUN_ID.

### 3.3. Nhịp cập nhật

Feed poll khoảng **1 giây**; danh sách phiên được kiểm tra khoảng **5 giây**. KPI có nhịp dự phòng **5 giây**, đồng thời theo pointer snapshot live. Tab bị ẩn tạm dừng polling; trở lại tab sẽ đọc tiếp. Những nhịp này là tần suất đọc, không phải cam kết xử lý xong từng event trong một giây.

Spark trigger **15 giây**, nhưng thời gian xử lý batch có thể dài hơn. Chỉ khi snapshot commit thì số liệu nghiệp vụ thay đổi; event đã ACK trước đó có thể còn chờ. Khi chuyển lượt, tổng live vẫn tích lũy, còn KPI run mới bắt đầu theo dữ liệu riêng của lượt đó.

<a id="feed"></a>

## 4. Đọc feed sự kiện trực tiếp

### 4.1. Mỗi dòng cho biết gì?

| Cột | Ý nghĩa | Cách đọc |
| --- | --- | --- |
| PHÁT LÚC · UTC | Giờ producer phát event, `emitted_at` | Giờ hiện tại của demo; UI rút gọn ngày thành giờ UTC |
| HÀNH ĐỘNG | Xem / Thêm giỏ / Mua | Tương ứng `view`, `add_to_cart`, `purchase` |
| VISITOR | ID chuỗi của visitor | Giữ nguyên mọi chữ số, kể cả số 0 đầu |
| SẢN PHẨM | Item ID của event | Dùng để đọc hành trình tương tác của event đó |
| TRẠNG THÁI | Kafka đã ACK · chờ Spark / Spark đã xử lý | Đối chiếu offset với snapshot đã commit, không suy ra từ giờ phát |

**Kafka đã ACK**: broker đã xác nhận nhận event, telemetry đã được ghi. **Spark đã xử lý**: offset đã nằm trong snapshot đã commit hoặc lượt đã hoàn tất đối soát. Nhận ACK không tự chứng minh có KPI/prediction mới.

Web giữ **50 dòng mới nhất**, Mongo telemetry giữ **2.000 event gần nhất mỗi phiên**. Không thấy event cũ trong feed không có nghĩa pipeline làm mất dữ liệu. Khi cursor nằm ngoài buffer, thông báo đồng bộ lại feed xuất hiện; snapshot/checkpoint và tổng phiên vẫn có phạm vi riêng.

### 4.2. Tổng phiên và cảnh báo

| Dòng thông tin | Đơn vị/phạm vi | Kết luận hợp lệ |
| --- | --- | --- |
| Event đã ACK | Event, toàn LIVE_ID | Đã được broker xác nhận theo cursor của các lượt |
| Đã xử lý | Event, toàn LIVE_ID | Lượt đã hoàn tất cộng accepted event thuộc snapshot của lượt đang chạy |
| Lượt hoàn tất | Số run con | Đã đủ nguồn và đối soát hoàn tất; dừng giữa lượt không tăng chỉ số này |
| Mất heartbeat | Trạng thái kiểm tra sau hơn 20 giây | Chưa xác nhận tiến trình live còn hoạt động; cần đối chiếu job/log |

Ví dụ **1.035 ACK / 1.000 processed / một lượt hoàn tất**: lượt đầu có 1.000 event đã xử lý, lượt thứ hai đã ACK thêm 35 event nhưng chưa có commit tương ứng. KPI phía dưới có thể đang giữ lượt đầu. Chỉ riêng chênh lệch này chưa xác định nguyên nhân gián đoạn.

`stop_requested=true` ở status/API nghĩa yêu cầu dừng đã được ghi. Phải chờ producer ngừng và consumer drain thành công; request stop không phải chứng nhận đã xử lý hết ACK.

### 4.3. Hai loại thời gian

- **Giờ phát/đọc/heartbeat** là wall-clock hiện tại, hiển thị UTC. Nếu so với giờ Việt Nam, UTC+7 tương ứng 13:56 là 06:56 UTC.
- **Event-time** là thời gian nghiệp vụ của dữ liệu; demo hiện dùng năm 2015 để tương thích model/cutoff. Biểu đồ, cửa sổ feature và `as_of` dùng thời gian này.

Không đổi bộ lọc biểu đồ sang ngày hôm nay chỉ vì feed đang phát hôm nay. Các run con có thể dùng lại cùng cửa sổ lịch sử với visitor khác; trục thời gian giữa các lượt không phải một chuỗi thời gian hiện tại liên tục.

<a id="overview"></a>

## 5. Màn hình Tổng quan

Chọn **Tổng quan** ở menu trái, rồi kiểm tra RUN_ID/batch trước khi đọc số. Các bảng/biểu đồ bên dưới feed thuộc snapshot đó.

### 5.1. Các KPI và tổng hành động

| Chỉ số | Cách tính/đơn vị | Lưu ý |
| --- | --- | --- |
| Event đã xử lý | Số accepted event của run đến batch đang xem | Sau validation/dedup; không cộng event lỗi/trùng/khác run |
| Visitor | Visitor ID khác nhau trong accepted event | Một người có nhiều event vẫn tính một |
| Giao dịch | Số `transaction_id` khác nhau | Một giao dịch có thể có nhiều event mua/sản phẩm |
| Event mua hàng | Số event loại purchase | Có thể khác số giao dịch và số người mua |
| Tỷ lệ visitor mua | `buyers / visitors × 100%` | Buyers là visitor có purchase đã quan sát; cohort là toàn run |
| Xem | Tổng accepted event loại view | Cùng số liệu xuất hiện trong legend Nhịp tương tác |
| Thêm giỏ | Tổng accepted event loại add_to_cart | Đếm hành động, không phải số phiên đi đúng funnel |
| Mua | Tổng accepted event loại purchase | Cùng nghĩa event mua; chưa phải forecast |

API/status còn có `active_visitors`: visitor có event trong năm phút event-time cuối tính theo event mới nhất. Đây là chỉ số bổ sung, không phải thẻ Visitor tổng trên màn hình.

Ví dụ run 1.000 event: **895 xem + 82 thêm giỏ + 23 mua = 1.000**; 13/200 visitor đã mua nên buyer rate **6,5%**. Số người mua 13 và giao dịch 23 khác nhau vì một visitor có thể mua nhiều lần.

### 5.2. Nhịp tương tác và bộ lọc thời gian

Biểu đồ theo phút UTC có ba chuỗi Xem, Thêm giỏ, Mua. Mỗi điểm là tổng của phút đó trong accepted event; không cộng distinct visitor/giao dịch theo từng phút để suy ra distinct toàn run.

Hai ô **Biểu đồ từ · ISO 8601** và **Đến · không bao gồm** chỉ lọc dữ liệu biểu đồ. Ví dụ:

```text
Từ:  2015-09-18T00:00:00.000Z
Đến: 2015-09-18T00:10:00.000Z
```

Bấm **Lọc biểu đồ**. API lấy phút có mốc trong khoảng từ đầu tới trước mốc kết thúc. KPI, số tổng trong legend, funnel và bảng sản phẩm vẫn theo toàn snapshot. Biểu đồ không có điểm có thể chỉ do khoảng thời gian không giao với dữ liệu. Để bỏ lọc, chọn lại RUN_ID rồi kiểm tra khoảng nguồn mặc định.

### 5.3. Hành trình chuyển đổi

Phiên mới bắt đầu khi visitor không hoạt động **quá 30 phút**. Chuỗi funnel yêu cầu cùng phiên, cùng sản phẩm, thời gian tăng theo thứ tự **xem → thêm giỏ → mua**. Thêm giỏ trước lượt xem hoặc mua thiếu bước giỏ vẫn được tính ở event/KPI, nhưng không tự được bổ sung vào chuỗi funnel đầy đủ.

Funnel tổng quan đếm phiên đạt từng bước, mỗi phiên tối đa một lần ở mỗi bước nếu có sản phẩm đạt chuỗi tương ứng. Bảng sản phẩm đếm theo cặp phiên–sản phẩm; một phiên tương tác nhiều sản phẩm có thể xuất hiện ở nhiều dòng sản phẩm.

| Chỉ số | Công thức | Ví dụ 200 → 57 → 13 |
| --- | --- | ---: |
| Có lượt xem | Phiên có xem | 200 |
| Xem → thêm giỏ | Phiên có chuỗi xem rồi giỏ cho cùng sản phẩm | 57 |
| Xem → giỏ → mua | Phiên có đủ chuỗi cho cùng sản phẩm | 13 |
| Tỷ lệ xem → giỏ | Cart sessions / view sessions | 28,5% |
| Tỷ lệ xem → mua | Purchase sessions / view sessions | 6,5% |
| Tỷ lệ giỏ → mua | Purchase sessions / cart sessions | 22,81% |
| Rời sau xem | (View sessions − cart sessions) / view sessions | 71,5% |
| Rời sau giỏ | (Cart sessions − purchase sessions) / cart sessions | 77,19% |

Phần trăm bên cạnh ba thanh funnel trên web đều dùng **phiên có xem** làm mẫu số: 100%, 28,5%, 6,5% trong ví dụ. Tỷ lệ giỏ → mua 22,81% là chỉ số điều kiện riêng trong API/report, không phải phần trăm thanh thứ ba.

Badge **TẠM TÍNH** nghĩa run còn cập nhật; **HOÀN TẤT** ứng với funnel của consumer completed. Phiên cha live có thể còn lượt khác dù funnel run đang xem đã hoàn tất.

### 5.4. Sản phẩm được quan tâm

Bảng lấy tối đa 20 sản phẩm, ưu tiên số phiên có xem. Các cột là mã sản phẩm, phiên có xem, phiên xem → giỏ, phiên xem → giỏ → mua, tỷ lệ thêm giỏ và tỷ lệ mua; hai tỷ lệ lấy phiên có xem của chính sản phẩm làm mẫu số. Đồng hạng được sắp theo ID.

Nhiều lượt xem nhưng ít phiên đi tiếp gợi ý sản phẩm cần điều tra thêm về nội dung, giá, trải nghiệm hoặc nhu cầu. Dữ liệu hiện tại không xác định nguyên nhân bỏ giỏ và không có giá để tính doanh thu; tránh biến tương quan trong bảng thành kết luận nguyên nhân.

<a id="predictions"></a>

## 6. Màn hình Phân khúc & dự đoán

### 6.1. Tìm đúng khu vực

1. Bấm **Phân khúc & dự đoán** ở menu trái.
2. Chọn RUN_ID muốn đọc trong **LƯỢT DỮ LIỆU**.
3. Thu gọn **Xem event mới nhất** hoặc cuộn xuống dưới feed.
4. Bắt đầu bằng bộ lọc **Tất cả cluster** và **Tất cả visitor**.

### 6.2. Phân bố khách hàng và đặc trưng phân khúc

Biểu đồ vòng hiển thị visitor đã có accepted event trong run, được gán cụm bởi K-Means tại cutoff của snapshot. Tổng các cụm bằng số prediction/visitor được score trong run. Model thử K=2..5 lúc train; dashboard dùng K của model đã khóa, không tự fit lại khi bạn mở trang.

Bảng **Đặc trưng phân khúc** hiển thị count và trung bình feature gốc của visitor trong cụm. Trung bình là giá trị trên mỗi visitor, không phải tổng event của cụm. Cluster ID chỉ là mã trong model; không có thứ hạng tốt/xấu mặc định.

Ví dụ snapshot `demo-live-002-000001`, batch 15:

| Cụm | Visitor | Tỷ trọng | View trung bình | Cart trung bình | Transaction trung bình |
| --- | ---: | ---: | ---: | ---: | ---: |
| 0 | 108 | 54% | 6,59 | 0,76 | 0,21 |
| 1 | 92 | 46% | 1,99 | 0 | 0 |

Với profile này có thể mô tả cụm 0 tương tác mạnh hơn, cụm 1 chủ yếu xem ít. Tên “VIP”, “sắp rời bỏ” hoặc “chắc chắn mua” chưa được mô hình chứng minh. Giữ model version khi so sánh vì cluster 0 của model khác có thể mang profile khác.

### 6.3. Bảng Visitor ưu tiên theo điểm mô hình

| Cột | Ý nghĩa |
| --- | --- |
| VISITOR ID | Khóa chuỗi của người dùng; giữ số 0 đầu và toàn bộ chữ số |
| CLUSTER | Cụm hành vi theo K-Means đã khóa |
| ĐIỂM MÔ HÌNH | Điểm Random Forest trong [0,1], xếp hạng giảm dần; chưa hiệu chuẩn thành xác suất |
| NHÃN POLICY | Dự đoán mua khi điểm ≥ ngưỡng; còn lại Chưa dự đoán mua |
| NGƯỠNG | Threshold policy đang áp dụng cho run |
| XEM · 7D | Lượt xem trong cửa sổ bảy ngày trước cutoff |
| GIỎ · 7D | Lượt thêm giỏ trong cửa sổ đó |
| GIAO DỊCH · 7D | Transaction khác nhau của visitor trong cửa sổ đó |

Mỗi trang tối đa **50 visitor**, score giảm dần rồi visitor ID tăng dần khi bằng điểm. Bấm **Trang tiếp/Trang trước** để đọc tiếp. Batch mới, đổi run hoặc áp dụng bộ lọc sẽ quay về trang đầu; không coi việc danh sách đổi vị trí là visitor bị xóa.

### 6.4. Bộ lọc và tra visitor

- **Phân khúc:** chọn một cluster để lọc bảng xếp hạng.
- **Nhãn theo policy:** Tất cả visitor / Dự đoán mua / Chưa dự đoán mua.
- **Tra visitor ID:** dán ID dạng chuỗi chữ số, bấm **Áp dụng** để mở chi tiết visitor thuộc run/batch hiện tại.

Lọc cluster và nhãn có thể kết hợp. Chúng không tính lại biểu đồ phân bố cụm hoặc KPI toàn snapshot. Tra visitor là lookup riêng trên cùng run/batch; visitor được tìm không bắt buộc trùng bộ lọc cluster/nhãn của bảng xếp hạng.

ID dài và số 0 đầu phải được giữ nguyên; `001` khác `1`. Sao chép ID từ bảng/feed cùng run, không đổi sang số JavaScript/Excel hoặc ký pháp khoa học. Lookup không tìm thấy có thể do visitor thuộc run khác, chưa vào batch đang xem hoặc ID đã bị thay chữ số. ID trong feed lượt mới chưa chắc có prediction khi nội dung chính còn giữ snapshot lượt trước.

Trong ví dụ batch 15, score lớn nhất 0,8341 thấp hơn ngưỡng 0,96 nên lọc **Dự đoán mua** trả bảng trống là kết quả hợp lệ. Chuyển về Tất cả visitor để xem đủ danh sách.

<a id="interpretation"></a>

## 7. Hiểu mô hình, feature và kết quả dự báo

### 7.1. Hai mô hình và một luật gắn nhãn

K-Means trả lời **visitor này có hành vi giống nhóm nào**. Random Forest học mục tiêu **visitor có ít nhất một giao dịch mua trong 24 giờ sau `as_of` hay không**, dựa trên bảy ngày trước `as_of`. Policy so điểm với ngưỡng đã được lựa chọn/đánh giá để gắn nhãn ưu tiên.

RF dùng tám feature thô bên dưới; cluster K-Means không nằm trong vector RF. Cả hai model đã train được tải để inference, không train lại sau mỗi event.

| Feature | Ý nghĩa / đơn vị |
| --- | --- |
| `view_count_7d` | Số event xem |
| `cart_count_7d` | Số event thêm giỏ |
| `transaction_count_7d` | Số transaction khác nhau của visitor |
| `unique_items_7d` | Số sản phẩm khác nhau đã tương tác |
| `active_days_7d` | Số ngày UTC có hoạt động |
| `recency_hours` | Số giờ từ event cuối trước cutoff tới cutoff |
| `cart_view_ratio_7d` | Cart/view; bằng 0 nếu không có view |
| `transaction_view_ratio_7d` | Transaction/view; bằng 0 nếu không có view |

Cửa sổ feature là `[as_of − 7 ngày, as_of)`. Trong streaming, `as_of` lấy event-time lớn nhất đã accepted +1 ms; không dùng giờ máy hiện tại để thay ngày 2015. Feature có thể gồm seed lịch sử trước START và event đã accepted của run. Đếm feature bảy ngày vì vậy có thể khác tổng hành động chỉ trong run.

### 7.2. Ví dụ một visitor

Visitor `6498541375336314341117329260` trong snapshot tham chiếu có 20 lượt xem, ba lần thêm giỏ, hai transaction và tám sản phẩm trong cửa sổ feature. Kết quả: cluster 0, score **0,834149**, policy threshold **0,96**, nhãn **0**. Ngưỡng RF gốc 0,93 cũng cho nhãn 0.

Nhãn 0 nghĩa chưa được đưa vào nhóm dự đoán mua theo ngưỡng, không khẳng định chắc chắn visitor sẽ không mua. Điểm 0,834149 chưa phải xác suất mua 83,4149%: RF chưa được hiệu chuẩn sau negative sampling. Thêm event có thể làm score tăng hoặc giảm; không ép dự báo đi cùng số lượt xem.

**13 người đã mua** trong KPI mô tả quá khứ đã quan sát. **0 người được dự đoán mua** mô tả nhãn cho cửa sổ tương lai 24 giờ tính từ cutoff. Hai số này đo hai việc khác nhau; muốn đánh giá dự báo phải đợi đủ nhãn tương lai của đúng cohort/cutoff.

### 7.3. Đánh giá mô hình ở cuối trang

| Chỉ số | Cách hiểu | Điều kiện/giới hạn |
| --- | --- | --- |
| Precision test lịch sử | TP / (TP + FP): trong nhóm dự đoán mua, bao nhiêu thực sự mua | Thuộc tập test/policy lịch sử được hiển thị |
| Recall test lịch sử | TP / (TP + FN): trong người thực sự mua, tìm được bao nhiêu | Ngưỡng chặt có thể tăng precision và giảm recall |
| PR-AUC test lịch sử | Chất lượng xếp hạng qua các ngưỡng trên precision–recall | Phụ thuộc cohort và mức mất cân bằng nhãn |
| Ngưỡng policy | Điểm tối thiểu để mang nhãn 1 | Không phải precision hoặc tỷ lệ phần trăm người mua |
| Ngưỡng RF gốc | Threshold gốc trước khi áp policy riêng | Giữ lại phục vụ đối chiếu |

Policy tham chiếu 0,96 có precision test lịch sử khoảng **73,88%**, recall **13,64%**; đây là **đánh giá lại trên test từng được quan sát**, `fresh_holdout=false`. Không dùng nó làm precision hiện tại của simulator hoặc cam kết cho dữ liệu tương lai. Cohort chưa có positive prediction không đủ để suy ra precision bằng 0% hoặc 100%.

Phía dưới có version K-Means, Random Forest và policy; mở **Tham số và báo cáo mô hình đã khóa** để xem report chi tiết. Model đang chọn trong latest pointer của CLI không tự thay model của run cũ. Nội dung học sâu hơn: [feature](../ml/01-features.md), [dự đoán 24 giờ](../ml/05-purchase-data.md), [policy](../ml/09-purchase-policy.md).

<a id="pipeline"></a>

## 8. Màn hình Theo dõi pipeline

### 8.1. Bốn thẻ vận hành

| Chỉ số | Ý nghĩa | Đọc đúng phạm vi |
| --- | --- | --- |
| Backlog đã ACK | Chênh offset đã ACK và processed đã commit trên từng partition | Của RUN_ID đang xem, không phải tổng mọi event trên topic |
| Latency p95 | Khoảng thời gian phát → công bố kết quả của sample; bỏ batch đầu | Giây; `—` khi chưa đủ mẫu, không phải latency HTTP |
| Batch hiện tại | Batch commit của snapshot | `—` trước commit đầu tiên |
| Tốc độ producer | Event/giây ACK trung bình của run | Tốc độ đo được có thể thấp hơn RATE cấu hình |

Mục tiêu streaming p95 ≤60 giây và mục tiêu smoke API response p95 ≤1 giây là hai phép đo khác nhau. Browser poll một giây không tạo ra cam kết xử lý một giây. Batch đầu có startup/warm-up nên xem riêng.

### 8.2. Producer, Consumer và Scoring

| Trạng thái | Cách hiểu và hành động |
| --- | --- |
| Producer running | Đang phát/chờ backpressure; đối chiếu ACK có tăng |
| Producer completed | Đã gửi đủ nguồn hữu hạn; consumer có thể còn drain |
| Paused | Đã ngừng phần đang chạy để tiếp tục cùng cấu hình; chưa coi là đã đủ nguồn |
| Consumer running | Registry ghi consumer đang hoạt động; phải đối chiếu heartbeat/job nếu số liệu đứng lâu |
| Consumer completed | Run đã đi qua bước hoàn tất; đối chiếu backlog/report/fingerprint |
| Failed / interrupted | Job gặp lỗi hoặc bị ngắt; giữ log và snapshot đã commit để điều tra |
| Scoring waiting_for_events | Chưa có snapshot ML từ accepted event |
| Scoring ready | Snapshot ML đã công bố; chưa chứng minh toàn phiên live đã hoàn tất |

Panel phiên live còn có **Đang chuẩn bị lượt**, **Đang chuyển lượt**, **Đã dừng**, **Đã hoàn tất**, **Phiên gặp lỗi**, **Mất heartbeat**. Một consumer completed và backlog 0 là bằng chứng cho run đó; cần report phiên để đánh giá cả LIVE_ID. `stale` là trạng thái mất xác nhận heartbeat, không xác định nguyên nhân chết tiến trình.

### 8.3. Chất lượng xử lý

| Nhóm | Ý nghĩa |
| --- | --- |
| HỢP LỆ / accepted | Event thuộc run, hợp lệ, chưa trùng và chưa quá trễ; tham gia analytics/ML |
| TRÙNG / duplicate | Event đã được nhận diện trước; không cộng lại KPI |
| KHÔNG HỢP LỆ / invalid | Payload/giá trị không đạt hợp đồng; kiểm tra audit/DLQ |
| QUÁ TRỄ / late | Event bị loại theo quy tắc lateness của run; xem audit/DLQ |
| KHÁC RUN / foreign | Event cùng topic nhưng không thuộc RUN_ID; không cộng vào KPI run này |

Các số là tổng đến batch đang xem. Topic Kafka được chia sẻ nên foreign có thể xuất hiện hợp lệ. Tỷ lệ accepted cao không tự chứng minh model dự báo tốt; chất lượng xử lý dữ liệu và chất lượng ML là hai đánh giá riêng.

### 8.4. Batch duration, worker và UI hạ tầng

Biểu đồ **Thời gian xử lý mỗi batch** dùng giây từ bắt đầu xử lý tới bước công bố. Thời gian lớn hơn trigger 15 giây cho thấy batch đang chậm hơn nhịp kích hoạt; kiểm tra backlog và xu hướng qua nhiều batch. Worker đã tham gia phải được đọc theo run; chưa có batch có thể chưa có worker trong report.

**Mở Spark UI**: xem application đang active và worker; application không còn active trong khi registry còn running cần điều tra trạng thái cũ. **Mở HDFS UI**: xem dung lượng, DataNode và block. Nút mở Spark master không phải application UI 14040; application UI chỉ có khi driver còn chạy.

<a id="walkthrough"></a>

## 9. Bài thực hành từ xem lịch sử đến demo live

### 9.1. Đọc một run đã hoàn tất

Trên máy đã có ví dụ `demo-live-002-000001`:

1. Chọn RUN_ID này ở LƯỢT DỮ LIỆU; thao tác tắt Theo dõi live. Kiểm tra batch **15**.
2. Thu gọn feed. Tổng quan phải có 1.000 event, 200 visitor, 23 giao dịch, 13 người mua/6,5%; legend 895 xem, 82 giỏ, 23 mua.
3. Đọc funnel 200 → 57 → 13; giải thích mẫu số phần trăm trước khi so với buyer rate.
4. Sang Phân khúc & dự đoán: hai cụm 108/92, tương ứng 54%/46%.
5. Chọn cluster 0, bấm Áp dụng; bảng rank chỉ còn visitor cụm đó, biểu đồ tổng vẫn mô tả toàn snapshot.
6. Chọn nhãn Dự đoán mua, bấm Áp dụng: bảng rỗng vì score cao nhất 0,8341 <0,96. Trở về Tất cả visitor.
7. Dán visitor ví dụ ở mục 7.2, bấm Áp dụng và đọc đủ tám feature ở chi tiết. Xóa ô ID rồi Áp dụng để bỏ lookup.
8. Sang Theo dõi pipeline: producer/consumer completed, backlog 0, accepted 1.000, các nhóm trùng/lỗi/trễ/khác run bằng 0; p95 khoảng **42,269 giây**.

Máy mới chưa có ID này: chọn run completed do mình tạo, dùng số trong report của chính run để đối chiếu. Không cần tạo lại seed để ép số liệu giống ví dụ.

### 9.2. Quan sát hai lượt live hữu hạn

Điều kiện: ingest, hai model, policy eligible và API đã sẵn sàng; mọi Spark job khác kết thúc. Terminal API và Live Server giữ mở. Terminal job tại root:

```bash
make live-demo LIVE_ID=onboarding-live-check-001 RATE=5 LIVE_EVENTS=120 LIVE_VISITORS=24 LIVE_ITEMS=12 LIVE_SEGMENTS=2
```

ID này trùng ví dụ trong runbook để bạn xem chính phiên vừa tạo. Nếu đã hoàn tất thì nó chỉ xác minh/reuse; muốn một buổi demo mới dùng ID chưa có thay vì sửa request cũ.

1. Chọn Phiên live `onboarding-live-check-001`, bật Theo dõi live và Tự cập nhật KPI.
2. Sau READY, feed có event đã ACK; chờ Spark commit để KPI/prediction xuất hiện.
3. Khi lượt 1 kết thúc, quan sát tổng phiên không giảm. KPI có thể giữ lượt 1 trong lúc lượt 2 chuẩn bị.
4. Khi lượt 2 có snapshot, RUN_ID chính chuyển sang `onboarding-live-check-001-000002`.
5. Đợi terminal tự kết thúc, tổng phiên đủ **240 ACK/240 processed/hai lượt hoàn tất**.
6. Kiểm tra `artifacts/live/onboarding-live-check-001/report.json`: passed, completed và resource flags đều đạt. Mỗi run con có 120 event; KPI chính không cần hiển thị tổng 240 vì thuộc một run con.

Nguồn ngẫu nhiên có thể không có mua hoặc không ai vượt ngưỡng. Nghiệm thu dữ liệu/rotation dựa vào nguồn đã khóa, snapshot và đối soát, không dựa vào việc bắt buộc có nhãn mua.

### 9.3. Demo liên tục và xem lại lịch sử

Khi phiên hữu hạn đã kết thúc:

```bash
make live-demo LIVE_ID=onboarding-live-001 RATE=5
```

Chọn phiên mới trên web. Mặc định 1.000 event/lượt, 200 visitor, 100 sản phẩm; dữ liệu được model tính thật. Dành thời gian cho chuẩn bị seed/model, batch đầu và đối soát cuối lượt. Tổng số event đã ACK có thể chững khi backlog đạt 200 hoặc khi chuyển lượt.

Để xem lại một lượt: chọn RUN_ID thủ công, kiểm tra Theo dõi live đã tắt. Feed vẫn có thể mô tả phiên hiện tại trong khi các thẻ KPI đang xem lịch sử. Muốn quay lại realtime, chọn đúng phiên và bật lại hai checkbox. Không so tổng ACK toàn phiên với KPI một run rồi kết luận mất dữ liệu.

### 9.4. Checklist tự kiểm tra trên browser

- [ ] API health ready, không có banner kết nối lỗi.
- [ ] Chọn được đúng run và đọc được batch đã commit.
- [ ] KPI, funnel và prediction thuộc cùng run/batch.
- [ ] Thu gọn feed, tìm được cả ba màn hình.
- [ ] Lọc cluster/nhãn, phân trang và tra visitor đúng ID chuỗi.
- [ ] Hiểu nhãn 0, score chưa hiệu chuẩn và metrics lịch sử.
- [ ] Live có ACK mới, sau đó processed/snapshot mới.
- [ ] Chuyển lượt giữ snapshot trước rồi theo lượt mới, tổng phiên không giảm.
- [ ] Dừng/drain/resume được kiểm tra riêng; ghi pending nếu chỉ mới chạy hữu hạn.

<a id="troubleshooting"></a>

## 10. Lỗi thường gặp và cách đối chiếu

| Hiện tượng | Kiểm tra và hành động |
| --- | --- |
| Đang tải RUN_ID, báo Không kết nối được API | Giữ `make api` chạy; mở health từ chính browser; kiểm tra `apiBaseUrl`/port; reload sau cập nhật mã |
| Chỉ chạy api-indexes nhưng web chưa có dữ liệu | Chạy thêm `make api`; indexes không khởi động server |
| API ready nhưng bộ chọn rỗng | Cần run ML nghiệp vụ; ingest/model batch hoặc v2 không tự tạo run dashboard |
| Chưa thấy phân cụm/dự đoán | Chọn menu đúng, thu gọn feed/cuộn xuống; kiểm tra batch và scoring đã ready |
| Biểu đồ trống, KPI vẫn có | Kiểm tra bộ lọc event-time 2015, timezone và khoảng đầu/cuối; filter chỉ tác động biểu đồ |
| Lọc Dự đoán mua rỗng | So score/ngưỡng; bỏ lọc để xem toàn bộ visitor; không tự hạ policy cho có số |
| Visitor không tìm thấy | Đúng run/batch, giữ số 0 đầu; visitor feed mới có thể chưa được commit |
| Số liệu không tăng | Run completed là dữ liệu lịch sử; với live kiểm tra hai checkbox, tab đang hiện, ACK/backlog, heartbeat và application |
| Feed mới nhưng KPI cũ | ACK chưa commit hoặc lượt mới chưa có snapshot; xem trạng thái chờ và tổng từng phạm vi |
| Parent stale nhưng run completed | Snapshot cũ vẫn dùng được; kiểm tra lượt đang active và job, không gán trạng thái parent cho mọi run con |
| Registry running nhưng Spark không có active app | Trạng thái đã lưu có thể chưa được cập nhật khi process bị gián đoạn; giữ log/report và điều tra |
| API 503 hoặc thông báo snapshot/model không hợp lệ | Kiểm Mongo, model registry, checksum/publication; không sửa manifest hoặc fallback sang model khác |
| Bảng tự về trang đầu | Batch/run/filter thay đổi; đọc lại snapshot mới trước khi phân trang |
| Chưa có latency p95 | Chưa đủ sample; dấu — không phải 0 giây |
| Lỗi code/config changed khi chạy tiếp live | Giữ nguyên phiên bản để resume hoặc tạo LIVE_ID mới; không sửa code_sha256 trong request |

### 10.1. Đọc nguồn bằng chứng theo thứ tự

1. Ghi LIVE_ID, RUN_ID, batch và thời điểm quan sát trên web.
2. Kiểm tra health, rồi pipeline/live session API để phân biệt kết nối, backlog và heartbeat.
3. Đọc report/log đúng ID, đúng attempt; một report thành công cũ không chứng minh lần chạy mới thành công.
4. Kiểm tra Spark master: application có thật sự active không; đọc resources để phân biệt OOM/restart với lỗi ứng dụng/quan sát.
5. Chỉ khi đã xác định nguyên nhân mới retry hoặc khôi phục theo [quy trình chạy](../GETTING_STARTED.md#recovery).

Trên máy bàn giao, `live-check-002` có report passed, 240 event/hai lượt, monitor đầy đủ và không OOM/restart. `demo-live-002-000001` cũng đối soát thành công 1.000 event. Tuy nhiên parent `demo-live-002` từng kết thúc với report failed, stop_requested, mất heartbeat và 35 ACK chưa commit ở lượt 2. Monitor ghi không OOM/restart nhưng thiếu một quan sát tài nguyên; log chưa đủ để kết luận nguyên nhân kết thúc. Đây là ví dụ để đọc đúng trạng thái, không phải dấu đạt cho live liên tục.

### 10.2. Lệnh đọc trạng thái

```bash
make live-status LIVE_ID=onboarding-live-001
python3 -m json.tool artifacts/live/onboarding-live-001/report.json
```

Với replay/simulator ML kiểu cũ:

```bash
make stream-ml-status RUN_ID=ml-replay-001
```

Các lệnh status không mở thêm Spark application. File có thể chưa tồn tại khi job chưa tạo report hoặc chưa được chạy trên máy này; xem log tương ứng. Không xóa guard driver/producer để ép chạy thêm job.

<a id="daily"></a>

## 11. Dừng, mở lại và lưu bằng chứng

### 11.1. Dừng buổi demo

1. Tại terminal live, Ctrl+C **một lần**, giữ terminal để chờ drain; hoặc gửi `make live-stop LIVE_ID=onboarding-live-001` từ terminal khác.
2. Chờ lệnh live kết thúc, kiểm tra status/report/backlog và job đã dừng. Ctrl+C lần hai có thể kích hoạt cancel cưỡng bức, không thay cho drain bình thường.
3. Ctrl+C terminal `make api` nếu không cần API nữa.
4. Trong VS Code dùng **Live Server: Stop Live Server** hoặc nút cổng trạng thái.
5. Khi không còn job, `make down` nếu muốn dừng hạ tầng và giữ volume. Web/API là tiến trình host nên cần dừng riêng.

Snapshot committed vẫn được giữ. Nếu drain không được xác nhận, giữ token/guard và theo [hướng dẫn phục hồi](../streaming/04-testing-and-recovery.md); không xóa marker để giả lập job đã dừng.

### 11.2. Mở lại ngày hôm sau

Trong cùng clone, Docker daemon/Desktop đã hoạt động, giữ nguyên `.env`/volume:

```bash
make up
make doctor
make api
```

Giữ API, mở lại Live Server rồi chọn run cũ. Không cần train lại để xem snapshot. Nếu đã dừng/drain đúng và muốn tiếp tục cùng live ID/mã/config, chạy `make live-demo LIVE_ID=onboarding-live-001` ở terminal job. Muốn nguồn/tốc độ/model mới, tạo LIVE_ID khác; không tái sử dụng ID với tham số đã đổi.

### 11.3. Lưu một kết quả để bàn giao

Ghi commit, LIVE_ID/RUN_ID, batch, event-time cutoff, model/policy version, các bộ lọc, số liệu và thời điểm đọc. Giữ report, log UUID và resources đúng lượt; snapshot/model/checkpoint nằm trong Docker volumes, còn pointer/report host ở `artifacts/`.

| Artifact | Nội dung |
| --- | --- |
| `artifacts/live/<LIVE_ID>/report.json` | Kết quả vòng đời phiên và tổng lượt/event hoàn tất |
| `artifacts/live/<LIVE_ID>/status.json` | Lần đọc status gần nhất, không phải nguồn heartbeat tự cập nhật |
| `artifacts/live/<LIVE_ID>/logs/live-demo-<UUID>.log` | Batch, kết quả từng run con, lỗi của attempt |
| `artifacts/live/<LIVE_ID>/resources.json` | Quan sát tài nguyên gần nhất; cần đối chiếu UUID khi có nhiều attempt |
| `artifacts/streaming-ml/<RUN_ID>/` | Config/report/status/log của replay/simulator ML kiểu cũ |
| `artifacts/serving/` | Test API và smoke HTTP; không thay báo cáo xử lý Spark |

Run con của live được quản lý trong registry/HDFS và log của phiên; không giả định mỗi run con có sẵn thư mục host `artifacts/streaming-ml/`. `control.json` chứa owner token, `.env` chứa credentials: không đưa vào ảnh chụp/ticket công khai. [Runbook cleanup](../GETTING_STARTED.md#cleanup) phân biệt dừng giữ dữ liệu và reset phá hủy volume.

<a id="reference"></a>

## 12. Tra cứu nhanh API và thuật ngữ

API chỉ đọc; không có endpoint web để tự train hoặc phát stream. Các ví dụ sau dùng run/batch tham chiếu chỉ có trên máy đã tạo nó:

```bash
curl -sS 'http://127.0.0.1:8000/api/v1/runs'
curl -sS 'http://127.0.0.1:8000/api/v1/overview?run_id=demo-live-002-000001&batch_id=15'
curl -sS 'http://127.0.0.1:8000/api/v1/segments?run_id=demo-live-002-000001&batch_id=15'
curl -sS 'http://127.0.0.1:8000/api/v1/predictions?run_id=demo-live-002-000001&batch_id=15&limit=5'
curl -sS 'http://127.0.0.1:8000/api/v1/pipeline?run_id=demo-live-002-000001&batch_id=15'
curl -sS 'http://127.0.0.1:8000/api/v1/live/sessions/demo-live-002'
```

Giữ URL trong dấu nháy để Bash không xử lý `&`. Thay đúng ID/batch trên máy mình, và API port nếu đã đổi. Bỏ batch cho pipeline để tìm committed batch hiện tại, sau đó dùng cùng batch khi đối chiếu endpoint khác. Hợp đồng đầy đủ: [02 — API](02-api.md).

| Thuật ngữ | Nghĩa trong dự án |
| --- | --- |
| Visitor | Người dùng theo ID của dataset/simulator |
| Session | Phiên tương tác; tách khi khoảng không hoạt động quá 30 phút |
| Cohort | Tập visitor được phân tích/score tại một cutoff |
| Feature | Đại lượng hành vi tính từ dữ liệu trước cutoff |
| `as_of` / cutoff | Mốc chốt feature và bắt đầu cửa sổ dự báo |
| Cluster | Nhóm hành vi của cùng model K-Means |
| Score | Điểm RF phục vụ xếp hạng, chưa là xác suất hiệu chuẩn |
| Threshold / policy | Ngưỡng và quy tắc gắn nhãn mua |
| Prediction | Điểm/nhãn cho visitor và cutoff, gắn đúng model/policy |
| Commit / published | Kết quả đã qua bước công bố để reader được phép sử dụng |
| Checkpoint | Trạng thái Spark phục vụ tiếp tục xử lý offset/batch |
| Backlog | Phần đã ACK nhưng chưa nằm trong processed offset đã commit |
| Reused | Kiểm tra và sử dụng lại kết quả đã có; không chứng minh có dữ liệu mới |

Sau khi đọc tài liệu này, bạn có thể quay về [runbook chạy đầy đủ](../GETTING_STARTED.md#live-demo) để tự tạo một phiên mới hoặc [live runbook](05-live-demo.md) để điều chỉnh quy mô trong giới hạn hỗ trợ.
