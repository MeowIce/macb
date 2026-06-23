# MeowIce's Advanced Chatlogging Bot
## Một chiếc Bot theo dõi tin nhắn cực kỳ mạnh mẽ trên Discord !

## Giới thiệu
Quá mệt mỏi với việc các thành viên trong server Discord của bạn sửa/xoá tin nhắn mà không thể đọc lại được bản gốc ? Hay các bot hiện tại chưa đủ mạnh mẽ để khiến bạn hài lòng ?  
Hãy thử MeowIce's Advanced ChatLogging Bot !

### ✨ Tính năng nổi bật ✨
Điều gì khiến MACB "built different" so với bot khác trên thị trường ?  
- **Cảnh báo thời gian thực:** MACB tự động phát hiện và gửi thông báo khi có tin nhắn bị xoá hoặc chỉnh sửa trong các kênh văn bản;  
- **Tự động nhận diện định dạng:** MACB sẽ tự phân tích cấu trúc tin nhắn để phân loại giữa văn bản thô (plain text), ảnh tĩnh (jpg, png, jepg, webp), ảnh động GIF, video (mp4, webm, mov) hay Sticker để set giao diện hiển thị log tương ứng;  
- **Lưu ảnh, video và phương tiện đã xoá vĩnh viễn**: MACB sẽ tự động upload 1 bản copy của file phương tiện gốc lên thay vì phục thuộc hoàn toàn vào link media của file gốc như bot Auttaja -> không còn tình trạng file không khả dụng sau vài giờ;  
- **Lưu trữ Database mạnh mẽ**: MACB sẽ không quên các tin nhắn đã gửi. Các tin nhắn đều được lưu trữ trong Database SQLite cực kỳ gọn nhẹ, thực tế cho thấy 13k tin nhắn từ 2020 đến 2026 chỉ tốn ~5.7MB;  
- **Tốc độ nạp dữ liệu tin nhắn đỉnh cao**: Nhờ sự tối ưu hoá tài tình của Mèo ta mà cơ chế tìm & nạp dữ liệu chat diễn ra nhanh bứt tốc, lên đến 420 tin nhắn/giây. Test thực tế cho thấy việc tìm & nạp thành công 13121 tin nhắn trong 54 kênh ở server [MeowHouse](https://dsc.gg/meowsmp) với thời gian chỉ khoảng hơn 30 giây* !  
- **Slash Command**: MACB tích hợp sẵn lệnh `/getstats` để bạn check nhanh thông tin bot trong Discord mà không cần mở console/terminal;  
- **Khởi động nhanh**: Toàn bộ tiến trình tìm & nạp chat cũ được xử lý async, tự động đẩy xuống luồng xử lý ngầm, giải phóng hoàn toàn luồng chính để MACB sẵn sàng sử dụng ngay khi vừa online;  
- **Phát hiện thay đổi khi offline****: Khác với các bot khác trên thị trường, MACB tự động đối chiếu các tin nhắn trên Discord với dữ liệu trong Database ngay khi khởi động và notify các tin nhắn bị xóa hoặc sửa tin nhắn trong lúc bot offline;  
- **Thống kê Định kỳ**: Bạn có thể cho MACB report trạng thái thu thập tin nhắn theo giờ hoặc theo ngày trong config.  
- **Bảo vệ Rate Limit an toàn**: Quản lý mật độ kết nối nghiêm ngặt bằng Semaphore giúp bảo vệ tuyệt đối địa chỉ IP máy chủ của bạn khỏi các bộ lọc chặn truy cập từ Discord.  
> [!NOTE]
> <small>(*) Tốc độ nạp tin nhắn phụ thuộc vào máy của bạn. Dữ liệu trên được lấy khi cho bot chạy trên OS Ubuntu Server 24.04, i7 3770, 32GB RAM DDR3-1600 với SSD SATA 3 (500MB/s)</small>  
> <small>(**) Nếu tin nhắn đó đã được MACB lưu trữ. Không có tác dụng đối với tin nhắn mới. Khi khởi động lên lại, MACB sẽ tự động fetch tin nhắn mới về.</small>  

### Showcase
<img width="889" height="496" alt="image" src="https://github.com/user-attachments/assets/1e8b3144-2416-46f0-9722-edac28e529da" />
<img width="490" height="601" alt="image" src="https://github.com/user-attachments/assets/5508d138-45a8-4dd0-9bb1-ad114adf1c78" />
<img width="579" height="782" alt="{ACDA0760-B891-4D28-A807-9563EF1EE400}" src="https://github.com/user-attachments/assets/177940fd-3e45-4b1a-a0c2-978e05d8968b" />
<img width="484" height="805" alt="{77E84663-6398-4D18-A4CA-8ADDBD5CBA7A}" src="https://github.com/user-attachments/assets/f6784b9c-3a8b-4bdb-9dc7-6d7d97a714db" />
<img width="534" height="622" alt="{317F9478-00D7-4685-B0E0-189DD378D52C}" src="https://github.com/user-attachments/assets/40f5ef4e-9e2b-4809-bcc8-a12703c6d42f" />
<img width="389" height="692" alt="{E66A52EA-15AE-4F04-A9EB-03F8F8CDAB31}" src="https://github.com/user-attachments/assets/322c5868-f07b-49aa-9ae0-5e6cc4bcb210" />
<img width="696" height="633" alt="{35AAE47F-98CD-42C5-83E2-5747C51C818F}" src="https://github.com/user-attachments/assets/5e5c83b7-39e1-486b-846d-0283666234cc" />


## Cài đặt & Chạy
### 1. Cấu hình trên Discord Developer Portal

* Truy cập vào giao diện quản lý ứng dụng của Discord.
* Tạo một ứng dụng mới và thiết lập tài khoản Bot.
* Di chuyển đến mục **Bot**, tìm phần **Privileged Gateway Intents** và kích hoạt cả 3 options: `Presence Intent`, `Server Members Intent`, và `Message Content Intent`.
* Mời bot vào máy chủ với quyền `Xem lịch sử tin nhắn`.

### 2. Cài đặt thư viện trên server

Mở cửa sổ Terminal mới tại thư mục chứa src của bot và chạy lệnh sau để cài đặt các dependencies:

```bash
pip install discord.py aiohttp psutil
```

### 3. Set guild & kênh để theo dõi và gửi notify

Mở file `bot.py` và tiến hành thay thế ID tương ứng với thông tin server Discord của bạn:

```python
botToken = "YOUR_BOT_TOKEN"
targetGuildId = 708718758616760339
logChannelId = 879961838043922432
reportTaskSched = "hourly"
alsoSendToLogChannel = True
```
`botToken` là chỗ để điền token của bot
`targetGuildId` là ID server Dis cần theo dõi. Var này nhận số nguyên (int)  
`logChannelId` là ID kênh ở cái server đó để gửi notify vô. Var này nhận số nguyên (int)   
`reportTaskSched` là thời gian định kỳ mà bot sẽ gửi thống kê. Var này nhận 2 strings (`hourly`/`daily`).  
`alsoSendToLogChannel` Có cho phép bot gửi bản copy trạng thái từ terminal vào kênh `logChannelId` không. Var này nhận giá trị Boolean (`True`/`False`)  

### 4. Chạy bot

Chạy lệnh sau để kích hoạt bot:

```bash
python bot.py
```

Khi terminal hiển thị thông báo kết nối thành công, bot sẽ tự động chạy tiến trình đồng bộ dữ liệu chat.

## Hỗ trợ
- Discord: `@meowice`
- MeowHouse: https://dsc.gg/meowsmp

## License
MeowIce's Advanced Chatlogging Bot  
Copyright (c) 2026 MeowIce  

Được phép sử dụng, sửa đổi và phân phối phần mềm này chỉ cho mục đích phi thương mại.  
Việc bán phần mềm này hoặc bất kỳ tác phẩm phái sinh nào đều bị nghiêm cấm nếu không có sự cho phép rõ ràng bằng văn bản.  
Nghiêm cấm xóa hoặc thay đổi thông tin tác giả.  
