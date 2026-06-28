# ==============================================================================
# 1. CONNECTION & DATABASE CONFIGURATION / CẤU HÌNH KẾT NỐI VÀ CƠ SỞ DỮ LIỆU
# ==============================================================================

# Discord Bot authentication token (obtained from Discord Developer Portal)
# Token xác thực Bot Discord (lấy từ Discord Developer Portal)
botToken = "YOUR_DISCORD_BOT_TOKEN"

# Target Discord Guild (Server) ID to be monitored
# ID của máy chủ Discord cần giám sát nhật ký
targetGuildId = 708718758616760339

# Channel ID where log embeds will be dispatched
# ID của kênh văn bản dùng để gửi nhật ký log
logChannelId = 879961838043922432

# Path to the local SQLite database file
# Đường dẫn cục bộ dẫn tới tệp tin cơ sở dữ liệu
dbPath = "chatlog.db"

# Directory for transient media caching
# Thư mục lưu trữ tạm thời phục vụ tải tệp đa phương tiện
cacheDir = "mediacache"


# ==============================================================================
# 2. SYSTEM LANGUAGE & REPORT MODES / CẤU HÌNH NGÔN NGỮ VÀ CHẾ ĐỘ BÁO CÁO
# ==============================================================================

# Central localization language setting ('en' or 'vi')
# Cấu hình ngôn ngữ vận hành của bot ('en' hoặc 'vi')
botLang = "vi"

# Execution cycle for automated reports ('hourly' or 'daily')
# Chu kỳ thời gian tự động gửi báo cáo ('hourly' hoặc 'daily')
reportTaskSched = "hourly"

# Toggle for synchronizing periodic reports to the main log channel
# Tùy chọn gửi báo cáo định kỳ vào thẳng kênh log chính
alsoSendToLogChannel = True


# ==============================================================================
# 3. CRITICAL PERFORMANCE LIMITS - DO NOT MODIFY / CẤU HÌNH HỆ THỐNG - KHÔNG ĐỤNG VÀO
# ==============================================================================
# WARNING: Changing these values may cause rate limits, memory leaks, or crashes.
# CẢNH BÁO: Thay đổi các giá trị này có thể gây lỗi nghẽn mạch, tràn bộ nhớ hoặc sập bot.

maxDbQueueSize = 10000
maxLogQueueSize = 5000
logConsumerWorkersCount = 3
maxParallelDownloads = 16
maxParallelScans = 36
scanSize = 3000
maxPayloadBytesLimit = 7800000