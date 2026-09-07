# Production Review Findings

Follow-up review of the current working tree on branch `code-cleanup`.

Status legend: `[VERIFIED]` fixed correctly; `[PARTIAL]` remains partly fixed or still risky; `[NEW]` newly discovered during this follow-up.

## Critical / High

### 1. [VERIFIED] `on_ready` is not fully idempotent after reconnect

- `events.py`: `isInitialized` guard dời xuống sau khi moi khoi tao thanh cong. `initDatabase()` van chay moi on_ready de kiem tra suc khoe connection (dung `_isConnectionHealthy`).
- `setup_hook()` bo cacheTask trung. `on_ready` tao va luu handle duy nhat vao `mediaManager.cacheTask`.
- Neu DB init that bai, `isInitialized` khong set, on_ready tiep theo se thu lai.

### 2. [VERIFIED] Queue items can remain unfinished and hang shutdown

- `database.py dbWorker`: batch processing boc trong `try/finally`, `task_done()` luon duoc goi du exception.
- `log_dispatcher.py logConsumerWorker`: `executeLogPipeline` boc `try/finally`, `task_done()` luon goi.

### 3. [VERIFIED] First startup scan still does not cover full history

- `scanner.py`: `channel.history(limit=None)` — paginate toan bo lich su, khong gioi han.

### 4. [VERIFIED] Incremental scan can miss large offline gaps

- `scanner.py`: `channel.history(after=..., limit=None)` — paginate tu checkpoint toi tin nhan moi nhat, khong gioi han 200.

### 5. [VERIFIED] Cancelling workers can strand queue items

- Giai quyet qua fix Issue 2: `task_done()` trong `finally` dam bao du task bi cancel mid-batch, cac item da dequeue se duoc ack.

## Medium

### 6. [VERIFIED] Two media-manager implementations remain

- `media_manager.py` đã xóa (dead code, không được import ở đâu).
- `DummyMediaManager` trong `bot.py` là implementation duy nhất, phù hợp với flow download-to-RAM của `log_dispatcher.py`.

### 7. [VERIFIED] Media cleanup task is still created twice

- `setup_hook()` bo cacheTask. `on_ready` init guard tao duy nhat 1 task, luu vao `mediaManager.cacheTask`.

### 8. [VERIFIED] Database initialization failure is not handled safely

- `enqueueAction()` kiem tra `isReady` truoc khi enqueue, drop va log neu DB chua san sang.
- Startup scan va workers chi khoi dong neu `dbSuccess=True` trong on_ready.

### 9. [VERIFIED] Database backpressure drops actions

- `enqueueAction()` doi tu `put_nowait()` sang `await asyncio.wait_for(put(), timeout=5.0)`.

### 10. [VERIFIED] Production configuration still uses a token placeholder

- `config.py`: `botToken = os.environ.get("MACB_TOKEN", "YOUR_DISCORD_BOT_TOKEN")`.

## Low / Operational

### 11. [VERIFIED] Audit-log timestamp comparison uses naive UTC

- `events.py`: `datetime.utcnow()` thay bang `datetime.now(timezone.utc)`.

### 12. [VERIFIED] Attachment downloads have no response-size guard

- `DummyMediaManager.downloadMediaBytes()`: kiem tra `Content-Length`, stream 64KB chunk, abort neu vuot `maxPayloadBytesLimit`.

## Newly found

### 13. [VERIFIED] Initialization guard can permanently suppress recovery after DB failure

- `events.py`: `isInitialized = True` chi set sau khi toan bo khoi tao hoan tat.

### 14. [VERIFIED] Database connection guard does not verify connection health

- `database.py`: `_isConnectionHealthy()` chay `SELECT 1`, neu broken thi close va tao lai.
