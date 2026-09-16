import os
from dotenv import load_dotenv

load_dotenv()

botToken = os.getenv("BOT_TOKEN", "YOUR_DISCORD_BOT_TOKEN")
targetGuildId = int(os.getenv("TARGET_GUILD_ID", "708718758616760339"))
logChannelId = int(os.getenv("LOG_CHANNEL_ID", "879961838043922432"))
dbPath = os.getenv("DB_PATH", "chatlog.db")
cacheDir = os.getenv("CACHE_DIR", "mediacache")

botLang = os.getenv("BOT_LANG", "vi")
reportTaskSched = os.getenv("REPORT_TASK_SCHED", "hourly")
alsoSendToLogChannel = os.getenv("ALSO_SEND_TO_LOG_CHANNEL", "True").lower() in ("true", "1", "yes")

maxDbQueueSize = int(os.getenv("MAX_DB_QUEUE_SIZE", "10000"))
maxLogQueueSize = int(os.getenv("MAX_LOG_QUEUE_SIZE", "5000"))
logConsumerWorkersCount = int(os.getenv("LOG_CONSUMER_WORKERS_COUNT", "3"))
maxParallelDownloads = int(os.getenv("MAX_PARALLEL_DOWNLOADS", "16"))
maxParallelScans = int(os.getenv("MAX_PARALLEL_SCANS", "30"))
scanSize = int(os.getenv("SCAN_SIZE", "3500"))
maxPayloadBytesLimit = int(os.getenv("MAX_PAYLOAD_BYTES_LIMIT", "7800000"))
