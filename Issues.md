# Production review issues

Phạm vi: review tĩnh các file Python hiện có; chưa chạy bot và chưa sửa code ứng dụng.

## Critical / High

### 1. `on_ready` không idempotent, có thể khởi động trùng sau reconnect

- Vị trí: `events.py:25-74`.
- Mỗi lần Discord reconnect và phát lại `on_ready`, code lại gọi `initDatabase()`, tạo thêm SQLite connections, tạo startup scan mới và gọi `startWorkers()`.
- `LogDispatcher.startLoops()` chỉ gán lại danh sách task mà không cancel worker cũ; các worker cũ vẫn chạy. Startup scan cũng có thể chạy đồng thời và ghi/đếm dữ liệu trùng.
- Tác động production: rò rỉ connection/task, tranh chấp SQLite, duplicate log, tăng tải API và trạng thái thống kê sai.
- Cần sửa: dùng cờ/lifecycle lock để chỉ initialize một lần; khi reconnect chỉ khôi phục kết nối cần thiết, không chạy lại scan/worker nếu chúng còn sống.

### 2. Queue item không luôn được đánh dấu `task_done`, làm shutdown có thể treo vô hạn

- Vị trí: `database.py:83-160`, `log_dispatcher.py:74-96` và `database.py:258-265`, `log_dispatcher.py:343-352`.
- `flushAndClose()` chờ `queue.join()`, nhưng nếu xử lý một action phát sinh exception thì worker đi vào `except` trước khi gọi `task_done()`.
- Với log worker, exception cũng bỏ qua `task_done()` cho item hiện tại. Với database worker, cả batch có thể bị kẹt ở trạng thái unfinished.
- Tác động production: khi shutdown, deploy hoặc crash recovery, process có thể không thoát; dữ liệu còn lại không được flush và service manager có thể force-kill process.
- Cần sửa: bảo đảm `task_done()` bằng `finally` cho từng item/batch; định nghĩa rõ retry/dead-letter khi một item lỗi.

### 3. Startup scan không quét toàn bộ lịch sử dù được mô tả là full scan

- Vị trí: `scanner.py:103-108`, cấu hình `config.py:54`.
- First scan chỉ gọi `channel.history(limit=config.scanSize)` với `scanSize = 3500`, sau đó đánh dấu scan hoàn tất. Các tin nhắn cũ hơn 3500 tin/kênh không được lưu.
- Tác động production: mất dữ liệu lịch sử ngay từ lần triển khai đầu; các delete/edit cũ ngoài phạm vi này không thể phục hồi hoặc log.
- Cần sửa: paginate cho tới hết lịch sử hoặc ghi rõ/kiểm soát retention; không đánh dấu full scan thành công khi còn trang chưa xử lý.

### 4. Incremental scan có thể bỏ sót message mới và thay đổi trong khoảng giữa các lần lấy

- Vị trí: `scanner.py:111-119`.
- Mỗi kênh chỉ lấy 100 message gần nhất và tối đa 200 message sau `maxLocalId`. Nếu kênh có hơn 200 message mới trong thời gian bot offline, phần dư bị bỏ qua.
- Vì bước đối chiếu offline chỉ dựa trên tập message vừa fetch, các message mới hơn nhưng ngoài giới hạn cũng không được lưu; các edit/delete trong vùng bỏ sót cũng không được phát hiện.
- Cần sửa: phân trang từ `maxLocalId` tới message mới nhất, tiếp tục cho đến khi hết dữ liệu; đặt checkpoint theo từng trang.

### 5. Huỷ worker khi còn queue item làm mất trạng thái hoàn tất và có thể làm kẹt lần flush sau

- Vị trí: `bot.py:105-126`, `log_dispatcher.py:343-352`, `database.py:250-265`.
- Watchdog có thể cancel worker rồi tạo worker mới. Nếu worker bị cancel sau khi đã lấy item khỏi queue nhưng trước `task_done()`, unfinished-task counter không bao giờ giảm.
- Tác động production: watchdog tưởng đã phục hồi nhưng shutdown sau đó vẫn bị treo; action đang xử lý có thể mất hoặc bị xử lý không nhất quán.
- Cần sửa: graceful drain trước khi cancel, hoặc đảm bảo `task_done()` trong `finally` và có cơ chế retry/ack rõ ràng.

## Medium

### 6. Có hai cơ chế media manager; implementation đang được dùng không lưu cache media

- Vị trí: `bot.py:137-172`, `media_manager.py:10-119`.
- `bot.py` dùng `DummyMediaManager`, còn `media_manager.py` định nghĩa `MediaManager` khác nhưng không được import/dùng. Dummy manager tải bytes trực tiếp và không dùng `cacheDir`.
- Tác động: chức năng “permanent media preservation”/cache như README mô tả không phản ánh implementation thực tế; media lớn bị tải lại khi retry/log và làm tăng RAM/network.
- Ngoài ra `MediaManager.cleanCacheTask()` trong file riêng chỉ chạy một vòng rồi `return False`, nên không thực sự là task dọn cache định kỳ nếu implementation này được dùng.
- Cần sửa: chọn một implementation duy nhất, giới hạn kích thước response/stream ra disk, và giữ cleanup loop chạy liên tục.

### 7. Task cleanup của media được tạo trùng và một task không được quản lý

- Vị trí: `bot.py:209-214`, `events.py:59-63`.
- `setup_hook()` tạo `cacheTask` trước khi session được initialize; `on_ready()` lại tạo thêm một task cleanup nhưng không lưu handle. Khi shutdown chỉ task trong `cacheTask` được cancel.
- Tác động: task mồ côi sau reconnect/shutdown và hành vi cleanup không xác định.
- Cần sửa: initialize session và tạo đúng một task trong cùng lifecycle; lưu/cancel mọi task đã tạo.

### 8. Lỗi database init không chặn event handlers và scan tiếp tục ghi vào trạng thái chưa sẵn sàng

- Vị trí: `events.py:29-74`, `database.py:66-71`.
- Khi `initDatabase()` thất bại, code không dừng lifecycle chung; các event vẫn có thể gọi `enqueueAction`, còn startup scan vẫn được tạo nếu guild tồn tại.
- Tác động: action bị drop âm thầm hoặc scan chạy với read connection không hợp lệ, dẫn tới thiếu dữ liệu.
- Cần sửa: fail fast hoặc retry có backoff; chỉ đăng ký/khởi động ingestion và scan sau khi DB ready.

### 9. Backpressure database làm mất action thay vì bảo toàn dữ liệu

- Vị trí: `database.py:77-81`.
- `put_nowait()` trên queue giới hạn 10.000; khi đầy, exception chỉ được log rồi action bị bỏ. Các sự kiện message/delete/edit vì vậy có thể mất vĩnh viễn.
- Tác động: database không còn là bản ghi đầy đủ trong giờ cao điểm.
- Cần sửa: dùng await có timeout và retry/backpressure, hoặc durable spool/dead-letter; cảnh báo/metric phải đủ nổi bật để paging.

### 10. Cấu hình production còn chứa token placeholder và secret không tách khỏi source

- Vị trí: `config.py:5-7`.
- `botToken` vẫn là `YOUR_DISCORD_BOT_TOKEN`; bot sẽ không đăng nhập nếu triển khai nguyên trạng. Cấu hình ID/database cũng hardcode trong source.
- Cần sửa: đọc token từ environment/secret manager, validate khi startup và không commit secret vào repository.

## Low / Operational

### 11. Timestamp audit log dùng UTC naive có thể sai lệch khi so sánh

- Vị trí: `events.py:175-177`.
- `datetime.utcnow()` là datetime không timezone, trong khi Discord thường trả datetime aware. Tuỳ phiên bản/runtime, phép trừ có thể lỗi hoặc logic cửa sổ 12 giây không đáng tin.
- Cần sửa: dùng `datetime.now(timezone.utc)` và chuẩn hoá toàn bộ timestamp về aware UTC.

### 12. Thiếu giới hạn kích thước response khi tải attachment

- Vị trí: `bot.py:158-161`, được gọi từ `log_dispatcher.py:107-126`.
- `response.read()` đọc toàn bộ response vào RAM; `maxPayloadBytesLimit` chỉ được dùng khi chia file gửi Discord, không bảo vệ bước download.
- Tác động: attachment bất thường hoặc response không đúng content-type có thể gây memory spike/OOM khi có nhiều worker tải song song.
- Cần sửa: stream theo chunk, enforce Content-Length và byte budget trước khi giữ dữ liệu trong memory.

