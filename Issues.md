# Codebase Cleanup & Logic-Fix Instructions

Mục tiêu của file này là hướng dẫn AI implementer sửa code. Không đánh dấu issue là hoàn tất nếu chưa có test/regression check tương ứng.

## 1. [HIGH][VERIFIED] Startup scan task không được quản lý khi shutdown

### Vị trí

- `events.py`, nơi gán `self.startupScanTask = asyncio.create_task(...)`.
- `bot.py:main()`, cleanup lifecycle.

### Nguyên nhân

Startup scan chạy background nhưng `main()` không await, cancel hoặc drain `startupScanTask`. Khi process shutdown, code đóng log dispatcher/database trong khi scanner vẫn có thể enqueue DB/log actions. Những action này có thể bị drop, gặp connection đã đóng hoặc làm queue drain không phản ánh đúng trạng thái.

### Cách sửa

1. Expose task qua `bot.botEvents.startupScanTask` hoặc một field chính thức trên `MACB`.
2. Trước shutdown, stop nhận event mới hoặc đặt `isShuttingDown=True`.
3. Await scan với deadline; nếu quá hạn thì cancel và await cancellation.
4. Chỉ sau đó mới flush log queue, DB queue, đóng media session và đóng bot.
5. Nếu scanner bị cancel giữa channel, ghi checkpoint để lần sau tiếp tục.

Ví dụ lifecycle:

```python
if bot.startupScannerTask and not bot.startupScannerTask.done():
    try:
        await asyncio.wait_for(bot.startupScannerTask, timeout=30)
    except asyncio.TimeoutError:
        bot.startupScannerTask.cancel()
        await bot.startupScannerTask

await bot.logDispatcher.flushAndClose()
await bot.databaseManager.flushAndClose()
```

Test: khởi động scan lớn, gửi SIGTERM/Ctrl+C giữa scan, xác nhận không có enqueue sau khi DB đóng.

## 2. [HIGH][VERIFIED] Attachment-only message edits bị bỏ qua

### Vị trí

- `events.py`, `on_raw_message_edit()`.

### Nguyên nhân

Handler return ngay nếu `"content" not in payload.data`. Discord raw edit có thể chỉ chứa thay đổi attachment/embed hoặc metadata; code không fetch message mới và không cập nhật `attachments`, `contentTypes`, `embeds` trong DB.

### Cách sửa

1. Không chỉ kiểm tra `content`.
2. Khi payload thiếu field cần thiết, fetch message bằng channel/message ID nếu có thể.
3. So sánh content, attachment URLs, MIME types, embeds và reply reference với DB.
4. Tạo một action update đầy đủ, kèm `updatedAt`.
5. Log edit với before/after data; nếu attachment cũ đã mất, ghi rõ trạng thái unavailable.

Ví dụ logic:

```python
message = await channel.fetch_message(payload.message_id)
newFields = extract_message_fields(message)
oldRow = await load_message(payload.message_id)
if newFields != oldRow:
    accepted = await enqueueAction("updateFields", (message.id, newFields))
    if accepted:
        await enqueueLogAction(build_edit_log(oldRow, newFields))
```

Test: content giữ nguyên nhưng thêm/xoá attachment; thay embed; attachment-only edit khi Message Content Intent vẫn hoạt động.

## 3. [HIGH][VERIFIED] Delete log có thể được gửi dù database delete enqueue thất bại

### Vị trí

- `events.py`, `on_raw_message_delete()` và `on_raw_bulk_message_delete()`.

### Nguyên nhân

Code kiểm tra kết quả `enqueueAction()` để cập nhật counters nhưng vẫn enqueue log delete/bulk-delete ngay cả khi DB action trả `False`. Audit log có thể nói message đã được xử lý trong khi DB record vẫn còn; retry/reconciliation sau đó có thể tạo log trùng.

### Cách sửa

1. Quyết định semantics rõ: log chỉ gửi sau khi DB delete được commit, hoặc log phải ghi trạng thái `pending`.
2. Không dùng “đã enqueue” như “đã commit”; đổi `enqueueAction()` thành action receipt/future trả kết quả commit.
3. Với delete, persist event vào durable pending table trước khi gửi Discord log.
4. Worker xử lý DB delete thành công thì phát signal cho log worker.
5. Nếu log gửi trước DB commit vì yêu cầu latency, embed phải ghi `storageStatus=pending` và có reconciliation.

Test: force DB queue full/SQLite lock trong single delete và bulk delete; kiểm tra không có audit record misleading.

## 4. [HIGH][VERIFIED] Live edit chỉ cập nhật content, không cập nhật toàn bộ message fields

### Vị trí

- `events.py`, `on_raw_message_edit()`.
- `database.py`, nhánh `updateFields`.

### Nguyên nhân

Handler hiện tạo `{"content": newContent, "updatedAt": ...}`. Attachment, content types, embeds và reply reference mới không được ghi. Startup scanner có logic so sánh các field này nhưng live path lại bỏ qua, dẫn tới database không nhất quán tùy message được sửa lúc bot online hay offline.

### Cách sửa

1. Chuẩn hóa một hàm `extract_message_fields()` dùng chung cho `on_message`, raw edit và scanner.
2. Raw edit fetch message mới rồi tạo full update payload.
3. Dùng `updatedAt` trong điều kiện SQL để stale update không ghi đè live update.
4. Log before/after theo field changed, không chỉ content.

Ví dụ SQL:

```sql
UPDATE cachedMessages
SET content=?, attachments=?, embeds=?, contentTypes=?, updatedAt=?
WHERE messageId=? AND updatedAt <= ?;
```

Test: edit content, attachment, embed và nhiều edit liên tiếp khi scanner đang chạy.

## 5. [MEDIUM][VERIFIED] `normalizeTimestamp()` và `getMessagesInIdRange()` không có caller

### Vị trí

- `scanner.py:18-21`, `normalizeTimestamp()`.
- `database.py`, `getMessagesInIdRange()`.

### Xác minh

Search toàn repo chỉ thấy định nghĩa, không thấy call site. Đây là dead function, làm tăng surface area và gây hiểu nhầm rằng scanner dùng range query.

### Cách sửa

1. Trước khi xóa, kiểm tra external entrypoint/plugin có import trực tiếp không.
2. Nếu không có public API dependency, xóa function và import liên quan.
3. Nếu muốn giữ API, viết test/call site thật và ghi rõ contract; không giữ hàm “dự phòng” không dùng.
4. Chạy static check sau khi xóa.

## 6. [MEDIUM][VERIFIED] Cache metrics không bao giờ được cập nhật

### Vị trí

- `bot.py:21-80`, các field `cacheHits`, `cacheMisses` và methods `incrementCacheHit()`/`incrementCacheSubMiss()`.
- `metricsReportingTask()` chỉ in các giá trị này.

### Nguyên nhân

Không có caller thực tế cho hai increment methods. Dashboard luôn hiển thị 0, tạo false confidence về hiệu quả cache.

### Cách sửa

Chọn một trong hai hướng, không giữ trạng thái giả:

- Nếu không có cache thật: xóa fields, methods và output `Hits/Misses` khỏi metrics/health report.
- Nếu muốn đo cache: định nghĩa rõ hit/miss tại `getMessage`, `getMessagesBulk`, media download hoặc reply cache; increment đúng nơi lookup trả hit/miss và viết test counter.

Test: lookup hit và miss phải thay đổi metric đúng một lần, không double count khi retry.

## 7. [MEDIUM][VERIFIED] Một số state/parameter chỉ được ghi nhưng không có tác dụng

### Vị trí

- `bot.py`: `scanComplete`, `startupScanTask` lifecycle chưa được consume đầy đủ.
- `generateAndSendReport(isManual)`: parameter `isManual` không được dùng.
- `scanner.py`: `normalizeTimestamp()` không được gọi.

### Cách sửa

1. Dùng `rg` để xác định mọi writer/reader trước khi thay đổi.
2. Với `scanComplete`, dùng nó để chặn report/commands trước khi scan hoàn tất hoặc xóa state nếu không cần.
3. Xóa `isManual` nếu hai mode có cùng behavior; nếu cần phân biệt manual/scheduled, dùng nó để ghi label/log/permission rõ ràng.
4. Không giữ biến chỉ để “có vẻ hữu ích”; mỗi state phải có invariant và test.

## 8. [MEDIUM][VERIFIED] Replay dead letters có thể cạnh tranh với shutdown/normal workers

### Vị trí

- `log_dispatcher.py`, `replayWorker()` và `replayDeadLetters()`.
- `events.py`, command replay dead letters.

### Nguyên nhân

Replay gọi trực tiếp `executeLogPipeline()` trong khi normal log workers cũng đang gửi log. Không có lease/claim trên DB row; command và replay worker có thể lấy cùng item, gửi duplicate hoặc xóa row khi worker khác vừa retry.

### Cách sửa

1. Thêm trạng thái `claimedAt`, `claimOwner`, `nextRetryAt` vào `logDeadLetters`.
2. Claim row atomic trong transaction trước khi replay.
3. Chỉ delete row bởi owner sau success; nếu crash, lease hết hạn để worker khác claim lại.
4. Dùng một dispatcher path chung thay vì gọi pipeline trực tiếp không qua concurrency control.
5. Shutdown phải cancel replay task trước khi flush queue.

Test: replay command đồng thời với replay worker, kill process sau claim trước send, và retry duplicate prevention.
