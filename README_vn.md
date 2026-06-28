# MeowIce's Advanced Chatlogging Bot (MACB) 2.0  
Bot ghi nhật ký trò chuyện siêu việt, hiệu năng cao và siêu tiết kiệm tài nguyên cho Discord. Được thiết kế chuyên biệt cho các cộng đồng quy mô lớn.  

---
Languages: `[Tiếng Việt]` | [[English]](https://github.com/MeowIce/macb/blob/main/README.md)  
## Điều gì tạo nên sự khác biệt của MACB ?

### Dành cho Thành viên & Đội ngũ quản trị
* **Ghi nhật ký tin nhắn toàn diện** - Khác với các bot chatlog thông thường, MACB có khả năng phát hiện chính xác mọi hành vi chỉnh sửa hoặc xóa bỏ đối với cả tin nhắn mới lẫn tin nhắn cũ trong lịch sử, ngay cả khi chúng đã được gửi từ nhiều tháng trước.
* **Lưu vết toàn bộ lịch sử chỉnh sửa** - MACB bảo toàn cả nội dung gốc lẫn nội dung mới sau khi sửa, giúp ban quản trị dễ dàng đối chiếu mọi phiên bản.
* **Khôi phục dữ liệu ngoại tuyến** - MACB sẽ tự động quét và tái dựng lại các hành vi sửa hoặc xóa tin nhắn diễn ra trong suốt quãng thời gian bot offline.
* **Nhận diện xóa tin nhắn hàng loạt** - Phân biệt chính xác giữa các lượt xóa thông thường, lệnh dọn dẹp kênh (purge) của admin, hoặc các sự kiện xóa hàng loạt nhờ việc đối chiếu trực tiếp với Nhật ký kiểm tra (Audit Logs) của Discord.
* **Giữ trọn vẹn ngữ cảnh trò chuyện** - Lưu trữ đầy đủ thông tin về tin nhắn phản hồi (reply), embed, tệp đính kèm, định dạng MIME cùng profile người gửi để cung cấp ngữ cảnh rõ ràng nhất.
* **Sao lưu và bảo tồn media vĩnh viễn** - Tự động tải toàn bộ tệp đính kèm về máy và nạp lại một bản sao vào kênh log. Điều này giúp hình ảnh hoặc video không bị mất đi ngay cả khi Discord xóa vĩnh viễn file trên hệ thống CDN.
* **Tối ưu hóa hiển thị media** - Hình ảnh và ảnh động GIF được chèn trực tiếp vào embed; video được nạp dưới dạng tệp đính kèm có thể phát ngay, đảm bảo hiển thị mượt mà trên cả giao diện máy tính và điện thoại.

### Hiệu năng & Tối ưu hóa
* **Chat indexing siêu tốc** - Tốc độ đồng bộ & indexing lịch sử server đạt hơn **820 tin nhắn/giây**, xử lý xong **hơn 27.000 tin nhắn** chỉ trong vòng chưa đầy **33 giây**.
* **Kiến trúc SQLite WAL tối ưu** - Tách biệt hoàn toàn kết nối đọc và ghi chuyên biệt kết hợp chế độ WAL, giúp xử lý các tác vụ cơ sở dữ liệu đồng thời với độ trễ cực thấp.
* **Quét khởi động theo phân đoạn (Delta Scan)** - MACB chỉ quét những kênh văn bản có phát sinh thay đổi kể từ lần tắt bot trước đó, giúp rút ngắn tối đa thời gian khởi động trên các máy chủ quy mô lớn.
* **Ghi cơ sở dữ liệu theo lô (Batching)** - Gom cụm và ghi dữ liệu hàng loạt nhằm giảm thiểu số lần đọc hoặc ghi đĩa (I/O Disk), tối đa hóa thông lượng hệ thống.
* **Siêu tiết kiệm bộ nhớ** - Duy trì mức chiếm dụng RAM dưới **80 MB** ngay cả sau khi nạp chỉ mục cho hàng chục nghìn tin nhắn.
* **Chuyên dụng cho các cộng đồng lớn** - Được tinh chỉnh để vận hành trên các server Discord có mật độ tương tác cao với mức tiêu hao tài nguyên CPU, RAM và DB ở ngưỡng tối thiểu.

### Quản trị & Độ tin cậy hệ thống
* **Slash Command** - Hiển thị thời gian chạy thực tế (uptime), thống kê DB, tổng số tin nhắn lưu trữ, mức RAM thực tế, thời gian quét và thông tin server.
* **Gộp log khi khởi động** - Gom hàng nghìn sự kiện sửa hoặc xóa ngoại tuyến vào các tệp báo cáo lô nhỏ gọn. Cơ chế này giảm lượng request REST API gửi tới Discord hơn **95%**, loại bỏ hoàn toàn rủi ro dính giới hạn Rate Limit (HTTP 429).
* **Báo cáo hoạt động định kỳ** - Tự động tổng hợp và gửi báo cáo kiểm soát hàng giờ hoặc hàng ngày về kênh log được chỉ định.
* **Hỗ trợ đa ngôn ngữ** - Chuyển đổi linh hoạt giữa tiếng Anh (`en`) và tiếng Việt (`vi`) qua một tùy chọn cấu hình duy nhất.
* **Giám sát sức khỏe tự động (Watchdog)** - Liên tục kiểm tra trạng thái của các tác vụ nền, tự động khôi phục các worker bị treo hoặc đơ luồng nhằm đảm bảo hệ thống vận hành ổn định 24/7.
* **Thiết kế dạng mô-đun độc lập** - Các tính năng cốt lõi được chia tách rõ ràng, giúp việc bảo trì, nâng cấp hoặc thay thế cấu phần trở nên dễ dàng hơn.
* **Sẵn sàng cho môi trường Production** - Hoạt động bền bỉ dài hạn nhờ thiết kế kiến trúc xử lý bất đồng bộ kết hợp phân tách quyền truy cập DB.

## Hình ảnh Thực tế
### Khởi động lần đầu - Quét toàn diện
Chỉ mất ~33 giây cho 27.205 tin nhắn !
<img width="1066" height="201" alt="image" src="https://github.com/user-attachments/assets/19cec69d-67cb-42ad-9f10-d6da0bb0e021" />  

### Các lần khởi động tiếp theo - Quét thông minh
MACB chỉ cần ~3 giây để chuẩn bị cơ sở dữ liệu và hoàn tất đồng bộ hóa tin nhắn.
<img width="1044" height="205" alt="{1F424276-8075-4794-92D6-43DA0715563F}" src="https://github.com/user-attachments/assets/81cbf592-d7f9-4761-928f-dd02a9070e6f" />  

### Nhật ký xóa tin nhắn
<img width="411" height="711" alt="{B0D80459-89A5-4CFD-A0B5-3EC362B382AF}" src="https://github.com/user-attachments/assets/932d517f-e748-4bd4-8c8d-f3aa10af0f54" />  
<img width="392" height="453" alt="{192F5F05-DFDC-423F-8164-D1C2640B68A3}" src="https://github.com/user-attachments/assets/976914d9-3e3b-4ecd-86e4-3723403cce0d" />  

### Nhật ký chỉnh sửa tin nhắn
<img width="304" height="422" alt="{32E18CA3-63AA-4EEE-8B5C-DF07D17F3E6C}" src="https://github.com/user-attachments/assets/5e546aa9-e393-4fdc-bf9e-386d84012631" />  

### Sao lưu tệp đính kèm & Bảo tồn media
<img width="381" height="664" alt="{5D59D99D-E46C-4A44-B1C3-21D226BBE93A}" src="https://github.com/user-attachments/assets/05877ab7-1cf3-4409-86bf-f49eb5aff543" />  
<img width="639" height="624" alt="{F3212D74-04B3-4E8D-A4DC-C5689E203625}" src="https://github.com/user-attachments/assets/55ffdc92-9090-41e0-a8cf-a803a7d85487" />  
<img width="1008" height="666" alt="{CE195510-DEA6-45FD-AE63-1A821E103620}" src="https://github.com/user-attachments/assets/4d3a8b8c-37e0-4b92-8df4-22f44cdcd2f8" />  


### Khôi phục dữ liệu ngoại tuyến
<img width="391" height="433" alt="{0C3BCE78-3437-4604-9305-E2DADDB31B24}" src="https://github.com/user-attachments/assets/e5c4b8f0-b85b-4186-8fae-620e6333c3f5" />  

### Phát hiện xóa hàng loạt
<img width="705" height="387" alt="{A01ABCB4-B104-4EEB-81E6-9E53ED36F0D6}" src="https://github.com/user-attachments/assets/716793cd-4d90-45f1-b9f0-4ef1a105fd7d" />

### Slash Command
<img width="497" height="512" alt="{7F49016C-D713-4577-9D39-C467F4F9A072}" src="https://github.com/user-attachments/assets/b8017aa6-41f9-4de7-b92c-507d4f832b7f" />  
<img width="495" height="484" alt="{0572E212-E35B-4229-AFE6-58F116164A74}" src="https://github.com/user-attachments/assets/f813ab0f-4eaa-4639-a8fe-6381127d0f06" />  



### Hỗ trợ đa ngôn ngữ
<img width="322" height="426" alt="{E0B78561-1797-4318-A18B-646C18A32661}" src="https://github.com/user-attachments/assets/9b327c7a-e84c-49d5-96cc-1e279ff7034e" />

## Hướng dẫn Cài đặt & Thiết lập

### Yêu cầu
* **Python 3.10+** (Đảm bảo đã thêm Python vào biến môi trường PATH của hệ thống)
* Một mã Token Bot Discord (khởi tạo trực tiếp tại [Discord Developer Portal](https://discord.com/developers/applications))
* **Discord Gateway Intents**: Đảm bảo đã bật kích hoạt **Message Content Intent** và **Server Members Intent** trong mục *Bot -> Privileged Gateway Intents* tại trang quản lý ứng dụng.
* **Phân quyền (Permissions)**: Vai trò (Role) cao nhất của Bot cần được cấp các quyền `Read Message History`, `View Audit Log`, và `View Channels` trên toàn bộ các kênh văn bản cần giám sát.

### 1. Tải mã nguồn về máy (Clone)
```bash
git clone [https://github.com/MeowIce/macb.git](https://github.com/MeowIce/macb.git)
cd macb

```

### 2. Cài đặt các thư viện phụ thuộc

Cài đặt các gói thư viện cần thiết thông qua công cụ `pip`:

```bash
pip install discord.py psutil aiohttp

```

### 3. Thiết lập cấu hình (`config.py`)

Mở tệp `config.py` và điều chỉnh các thông số sau:

* `botToken`: Mã Token Bot Discord của bạn.
* `targetGuildId`: Mã ID của Máy chủ Discord cần giám sát.
* `logChannelId`: Mã ID của kênh văn bản dùng để nhận nội dung log.
* `botLang`: Lựa chọn ngôn ngữ hiển thị; nhập `"en"` cho tiếng Anh hoặc `"vi"` cho tiếng Việt.

Cấu trúc tệp mẫu `config.py`:

```python
botToken = "YOUR_DISCORD_BOT_TOKEN"
targetGuildId = 123456789012345678    # ID Server mục tiêu
logChannelId = 987654321098765432     # ID Kênh nhận Log mục tiêu
botLang = "en"                        # Cấu hình ngôn ngữ ('en' hoặc 'vi')

```

> [!NOTE]
> **Về thông số maxParallelScans = 36 và maxParallelDownloads = 16**: Các option này được tối ưu hóa cho các hệ thống có băng thông lớn. Nếu môi trường vận hành (VPS/Home server) có cấu hình CPU hạn chế hoặc đường truyền mạng yếu, bạn nên giảm các giá trị này xuống ngưỡng `15` hoặc `20` để tránh gây nghẽn mạng.  
>
> Các thông số còn lại nên giữ nguyên theo mặc định, trừ khi bạn hiểu rõ cấu trúc hệ thống.

## Chạy bot

Kích hoạt và chạy bot trực tiếp thông qua cửa sổ terminal:

```bash
python bot.py

```

---

> [!NOTE]
> Nếu MACB giúp ích cho quy trình quản lý server của bạn, đừng quên tặng star cho repo nhé. Thank you ^^  
> <img width="207" height="81" alt="{9EC984E8-81B6-47CE-B7AD-9CD3A5EAA942}" src="https://github.com/user-attachments/assets/c75b2695-a56d-44a0-9f38-831d1da38378" />

---

## Giấy phép và Điều khoản Sử dụng

Bản quyền (c) 2026 thuộc về MeowIce. Mã nguồn này được cấp phép sử dụng, chỉnh sửa và phân phối hoàn toàn miễn phí nhưng chỉ áp dụng cho các mục đích phi thương mại. Nghiêm cấm mọi hành vi thương mại hóa, mua bán phần mềm này hoặc các sản phẩm phái sinh từ nó khi chưa có sự đồng ý bằng văn bản từ tác giả. Nghiêm cấm hành vi xóa bỏ hoặc thay đổi thông tin ghi nhận tác giả gốc.
