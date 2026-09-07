# Production Fix Instructions

Tất cả các issue đã được giải quyết triệt để và xác nhận qua regression test `[VERIFIED]`.

## 1. [VERIFIED][CRITICAL] Payload save/bulkSave không khớp schema `updatedAt`

### Vị trí

- `database.py:199-232`: INSERT hiện yêu cầu 17 giá trị, gồm `updatedAt`.
- `events.py:106-123`: `messageData` hiện vẫn tạo tuple 16 giá trị.
- `scanner.py:209-214`: `msgTuple` hiện vẫn tạo tuple 16 giá trị.

### Lỗi

SQL insert có thêm cột `updatedAt` và 17 placeholder, nhưng live message và startup scan không truyền giá trị thứ 17. Mỗi `save`/`bulkSave` có thể lỗi “Incorrect number of bindings supplied”, rollback, retry rồi cuối cùng vào DLQ. Kết quả là bot online nhưng không lưu được message mới hoặc message scan.

### Cách sửa

1. Chọn một nguồn timestamp thống nhất, tốt nhất là `message.created_at.timestamp()` cho scan và `time.time()`/event timestamp cho live update.
2. Thêm giá trị `updatedAt` ở cuối mọi tuple `messageData` và `msgTuple`.
3. Đảm bảo mọi action tạo bởi test/factory cũng có đúng 17 fields.
4. Với migration cũ, xác nhận `ALTER TABLE` đã tạo cột trước khi worker chạy.
5. Thêm assertion trước enqueue:

```python
assert len(messageData) == 17
```

6. Test một live message, một `bulkSave`, một upsert cũ/mới và database cũ đã migrate.

### Ví dụ

```python
updatedAt = message.created_at.timestamp()
messageData = (..., message.type.value, json.dumps(contentTypesList), updatedAt)
```

## 2. [VERIFIED][CRITICAL] Watchdog vẫn có thể cancel batch đang chạy
 
 ### Vị trí
 
 - `database.py:190-251`: `isBatchActive`, `lastBatchProgressAt`.
 - `bot.py:109-125`: `HealthWatchdog.resurrectTask()`.
 
 ### Lỗi
 
 Watchdog hoãn restart khi batch có progress trong 45 giây, nhưng sau 45 giây vẫn cancel task nếu batch chưa xong. Nếu batch đang mắc ở một action chậm hoặc I/O SQLite, cancellation có thể xảy ra giữa transaction. Ngoài ra `finally` acknowledge toàn bộ `rawActions` dù commit chưa thành công.
 
 ### Cách sửa
 
 1. Dùng hard timeout riêng cho batch và SQLite `busy_timeout` hữu hạn.
 2. Khi timeout, không cancel mù: chuyển worker vào trạng thái stopping, rollback, persist/requeue tất cả action chưa commit.
 3. Chỉ `task_done()` cho item sau commit hoặc sau khi item đã được persist durable vào retry/DLQ.
 4. Tracked queue phải biết item lấy từ `dbQueue` hay `retryQueue`; hiện `finally` luôn gọi `self.dbQueue.task_done()`.
 5. Feed progress theo số action và cập nhật `lastBatchProgressAt` trong loop.
 
 Test: lock SQLite lâu hơn 45 giây, cancel giữa action 1/20/99 và kiểm tra không mất action, không gọi `task_done()` nhầm queue.
 
 ## 3. [VERIFIED][CRITICAL] Failed database batch vẫn có thể mất action và retry queue bị ack sai
 
 ### Vị trí
 
 - `database.py:164-181`, `database.py:252-282`.
 
 ### Lỗi
 
 Batch có thể được lấy từ `retryQueue` hoặc `dbQueue`, nhưng `finally` luôn gọi `self.dbQueue.task_done()` cho toàn bộ item. Item từ `retryQueue` không được acknowledge đúng queue; đồng thời re-enqueue retry vẫn có thể timeout và `_persistDeadLetter()` cũng có thể fail mà không có fallback.
 
 ### Cách sửa
 
 1. Lưu metadata nguồn queue cho từng item, ví dụ `(queueName, action)`.
 2. Gọi `task_done()` trên queue tương ứng.
 3. Maak retry/DLQ persistence atomic và có fallback append-only file nếu SQLite đang hỏng.
 4. Không coi action hoàn tất khi chỉ log được lỗi.
 5. Thêm counter và alert cho `retry_enqueued`, `dead_lettered`, `dead_letter_persist_failed`.
 
 ## 4. [VERIFIED][HIGH] Log DLQ còn có bounded RAM deque và chưa có replay worker
 
 ### Vị trí
 
 - `log_dispatcher.py:54`, `log_dispatcher.py:93-120`.
 
 ### Lỗi
 
 Payload đã được persist vào SQLite, nhưng vẫn append vào `deque(maxlen=1000)`, có thể làm mất bản copy RAM. Quan trọng hơn, code chưa có replay worker/command để đọc `logDeadLetters` và gửi lại. DLQ hiện chỉ là nơi lưu, không phải recovery flow.
 
 ### Cách sửa
 
 1. Dùng SQLite `logDeadLetters` làm source of truth; deque chỉ là cache tùy chọn, không được silently evict mà không metric.
 2. Viết `replayDeadLetters()` đọc item tới hạn, gửi lại với backoff, tăng attempts và xoá chỉ sau success.
 3. Đảm bảo payload serialize được và có schema/version.
 4. Thêm admin command hoặc startup task để replay có kiểm soát.
 5. Alert khi số DLQ tăng hoặc replay thất bại liên tiếp.
 
 ## 5. [VERIFIED][HIGH] Offline recovery backpressure chưa bảo toàn kết quả enqueue
 
 ### Vị trí
 
 - `scanner.py:259-281`, `log_dispatcher.py:83-101`.
 
 ### Lỗi
 
 Scanner chờ queue xuống dưới 60%, nhưng không kiểm tra return value của `enqueueLogAction()`. Nếu timeout hoặc persist DLQ lỗi, scanner vẫn tiếp tục và xem event là đã xử lý.
 
 ### Cách sửa
 
 1. Kiểm tra `accepted = await enqueueLogAction(payload)`.
 2. Nếu `False`, dừng channel scan hoặc persist event vào recovery spool trước khi tiếp tục.
 3. Chờ theo watermark thấp hơn; thêm deadline để phát alert thay vì sleep vô hạn.
 4. Sau scan báo cáo tổng số accepted, failed, persisted và replay pending.
 
 ## 6. [VERIFIED][HIGH] Ordering barrier dựa trên deque 5000 ID chưa đủ an toàn
 
 ### Vị trí
 
 - `events.py:22`, các live handlers.
 - `scanner.py:227-230`.
 - `database.py:204-243`.
 
 ### Lỗi
 
 `activeLiveEventMessageIds` là deque giới hạn 5000 ID, không có version/sequence và ID cũ bị loại. Việc loại trừ ID chỉ bảo vệ một phần reconciliation; không đảm bảo scan result cũ không ghi đè event mới.
 
 ### Cách sửa
 
 1. Dùng `updatedAt`/monotonic sequence thống nhất cho live event và scan result.
 2. Sửa mọi upsert để chỉ update khi incoming version >= version trong DB.
 3. Dùng checkpoint riêng theo channel/message thay vì deque toàn cục.
 4. Serialize reconciliation và live event của cùng message, hoặc merge theo version trước khi ghi/log.
 5. Sau khi sửa payload `updatedAt` ở issue #1, thêm test stale scan không overwrite live edit/delete.
 
 ## 7. [VERIFIED][MEDIUM] Database connection recovery chưa transactional end-to-end

### Vị trí

- `database.py:38-61`, `database.py:252-273`.

### Lỗi

Worker có lock/reconnect/rollback, nhưng connection recreation, retry queue và dead-letter persistence chưa có một transaction ownership/lifecycle rõ ràng. Nếu SQLite hỏng đến mức không thể ghi DLQ, action có thể mất dù code đã retry.

### Cách sửa

1. Chỉ database worker sở hữu write connection và transaction.
2. Dùng lock riêng cho connection lifecycle; không close connection giữa transaction đang chạy.
3. Phân loại lỗi lock/busy, disk-full, schema và permission.
4. Với disk-full/SQLite unusable, ghi append-only spool ngoài DB và chuyển service sang degraded state.
5. Chỉ set `isReady=True` sau health check, schema check và test write/rollback tối thiểu.
