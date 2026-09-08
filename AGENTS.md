# AGENTS.md - Bối cảnh Dự án & Quy định Vận hành Co-Pilot

## 1. Bối cảnh Dự án (MACB - MeowIce's Advanced Chatlogging Bot 2.0)

### 1.1 Mục tiêu & Phạm vi
- Bot Discord chuyên dụng ghi nhật ký (chatlog), kiểm toán và bảo tồn dữ liệu cho các cộng đồng lớn.
- Phát hiện và ghi vết tin nhắn bị chỉnh sửa, bị xóa (cả realtime và trong lúc bot offline).
- Nhận diện xóa tin nhắn hàng loạt (purge/bulk delete) đối chiếu qua Discord Audit Logs.
- Lưu trữ ngữ cảnh tin nhắn (reply, attachment, sticker, embed, profile).
- Báo cáo định kỳ (hourly/daily) và cung cấp slash command tra cứu trạng thái hệ thống.

### 1.2 Kiến trúc Hệ thống & Các Thành phần Cốt lõi
- **`bot.py`**: Điểm khởi chạy chính (`commands.Bot`). Quản lý vòng đời bot, `MetricsTracker`, `HealthWatchdog` giám sát các worker chạy nền.
- **`config.py`**: Cấu hình tập trung (token, target guild, log channel, sqlite path, ngưỡng worker, kích thước batch/queue).
- **`database.py`**: `DatabaseManager` vận hành SQLite ở chế độ WAL (`PRAGMA journal_mode=WAL`). Phân tách luồng đọc/ghi (`writeConn` xử lý hàng đợi theo lô qua worker bất đồng bộ, `sharedReadConn` / per-thread connection phục vụ truy vấn).
- **`events.py`**: `BotEvents` lắng nghe và điều phối các sự kiện Discord Gateway (`on_ready`, `on_message`, `on_message_edit`, `on_message_delete`, `on_raw_bulk_message_delete`, slash commands).
- **`scanner.py`**: `StartupScanner` quét đồng bộ hóa lịch sử tin nhắn khi khởi động (quét kênh phân đoạn delta scan hoặc full scan ban đầu).
- **`log_dispatcher.py`**: `LogDispatcher` quản lý hàng đợi gửi log (`logQueue`) với pool worker đa luồng bất đồng bộ (`logConsumerWorkersCount`), cơ chế gộp batch và chống rate-limit Discord API.
- **`media_manager.py`**: Xử lý tải xuống, lưu tạm/chuyển đổi tệp đính kèm và media để gửi sang kênh lưu trữ.
- **`localization.py`**: Quản lý chuỗi văn bản hiển thị đa ngôn ngữ (`en` và `vi`).
- **`Issues.md`**: Tài liệu theo dõi các vấn đề kỹ thuật và điểm nghẽn cần tối ưu (idempotency khi reconnect, xử lý task_done trong queue, pagination quét lịch sử, đồng nhất media cache).

### 1.3 Tech Stack & Ràng buộc Kỹ thuật
- Ngôn ngữ: Python 3.10+
- Thư viện chính: `discord.py`, `aiohttp`, `psutil`, `sqlite3`
- Mô hình I/O: Xử lý bất đồng bộ (`asyncio`) kết hợp queue phân tách (`dbQueue`, `logQueue`).

---

## 2. Quy chuẩn Lập trình Dự án

- **Quy tắc đặt tên**: Sử dụng duy nhất kiểu camelCase cho tất cả tên biến và tên hàm (ngoại trừ các hàm hook/event bắt buộc của framework như `on_ready`, `setup_hook`).
- **Ghi chú trong mã nguồn**: Tuyệt đối không để lại ghi chú (comment) bên trong khối mã nguồn. Tất cả mã nguồn phải hoàn toàn sạch ghi chú, trừ khi được yêu cầu rõ ràng.
- **Không sử dụng emoji**: Tuyệt đối không được sử dụng emoji trong mã nguồn hoặc log xuất bản, trừ khi được yêu cầu rõ ràng.
- **Triết lý thiết kế (Ponytail & Minimal)**:
  - Tận dụng tối đa thư viện chuẩn (stdlib) và tính năng có sẵn của nền tảng trước khi thêm code hoặc thư viện ngoài.
  - Không tạo abstraction dư thừa (interface 1 implementation, helper 1 lần dùng).
  - Khắc phục lỗi tại gốc rễ (root cause) thay vì vá triệu chứng bề mặt.
  - Diff ngắn nhất, hiệu quả nhất, đảm bảo tính đúng đắn trên edge cases.

---

## 3. Nguyên tắc và Ràng buộc Vận hành Co-Pilot

- **Ngôn ngữ phản hồi**: Phản hồi theo đúng ngôn ngữ của câu hỏi (Tiếng Anh hoặc Tiếng Việt).
- **Xử lý thông tin mơ hồ**: Nếu các yêu cầu hoặc thông số kỹ thuật chưa rõ ràng, phải yêu cầu làm rõ trước khi thực thi.
- **Phong cách thực thi**: Đưa ra nhiều góc nhìn hoặc giải pháp cho các vấn đề kỹ thuật phức tạp. Giữ kết quả trực tiếp, không chứa các thành phần giao tiếp thừa, dấu gạch ngang dài, biểu tượng cảm xúc hoặc lời chào kết thúc.
- **Phong cách giao tiếp (Caveman)**: Giữ văn phong súc tích, ngắn gọn, đi thẳng vào trọng tâm kỹ thuật.

---

## 4. Quy định Thông báo Webhook

- Mỗi khi hoàn thành xong bất kỳ tác vụ hoặc công việc nào, phải chủ động gọi script `c:\Users\MeowIce\Documents\MeowBots\Webhooks\Webhook_Gemini.py` để gửi thông báo Discord webhook cho người dùng.
- **LƯU Ý**: Script `Webhook_Gemini.py` đã tự động chèn `<@666824403216105483> Workspace [tên ws]` ở đầu tin nhắn. Khi truyền tham số nội dung vào script, **chỉ truyền phần [nội dung output tóm tắt công việc đã xong]**, KHÔNG truyền lặp lại tag user hay `Workspace ...`.
- Lệnh mẫu thực thi qua PowerShell (`powershell.exe`):
  `$OutputEncoding = [System.Text.Encoding]::UTF8; [Console]::OutputEncoding = [System.Text.Encoding]::UTF8; $env:WORKSPACE_NAME="MACB"; C:\Python314\python.exe c:\Users\MeowIce\Documents\MeowBots\Webhooks\Webhook_Gemini.py "[nội dung output]"`
