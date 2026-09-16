import os
import sys
from dotenv import load_dotenv

if sys.platform == "win32":
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    if hasattr(sys.stderr, "reconfigure"):
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")

load_dotenv()

botToken = os.getenv("BOT_TOKEN") or "YOUR_DISCORD_BOT_TOKEN"
targetGuildId = int(os.getenv("TARGET_GUILD_ID") or "708718758616760339")
logChannelId = int(os.getenv("LOG_CHANNEL_ID") or "879961838043922432")
dbPath = os.getenv("DB_PATH") or "chatlog.db"
cacheDir = os.getenv("CACHE_DIR") or "mediacache"

botLang = os.getenv("BOT_LANG") or "vi"
reportTaskSched = os.getenv("REPORT_TASK_SCHED") or "hourly"
alsoSendToLogChannel = (os.getenv("ALSO_SEND_TO_LOG_CHANNEL") or "True").lower() in ("true", "1", "yes")

maxDbQueueSize = int(os.getenv("MAX_DB_QUEUE_SIZE") or "10000")
maxLogQueueSize = int(os.getenv("MAX_LOG_QUEUE_SIZE") or "5000")
logConsumerWorkersCount = int(os.getenv("LOG_CONSUMER_WORKERS_COUNT") or "3")
maxParallelDownloads = int(os.getenv("MAX_PARALLEL_DOWNLOADS") or "16")
maxParallelScans = int(os.getenv("MAX_PARALLEL_SCANS") or "30")
scanSize = int(os.getenv("SCAN_SIZE") or "3500")
maxPayloadBytesLimit = int(os.getenv("MAX_PAYLOAD_BYTES_LIMIT") or "7800000")
