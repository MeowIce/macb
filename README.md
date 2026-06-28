# MeowIce's Advanced Chatlogging Bot (MACB) 2.0  
The most advanced, high-performance, and memory-efficient Discord chat logging bot. Built for large-scale communities.

---

## What Makes MACB Stand Out ?

### For Users & Moderators
* **Complete Message Logging** - Captures message creation, edits, and deletions across all monitored channels in real time.
* **Full Edit History** - Preserves both original and updated message content, allowing moderators to review every revision.
* **Offline Recovery** - Automatically detects and reconstructs message edits and deletions that occurred while the bot was offline.
* **Bulk Delete Detection** - Differentiates between normal deletions, moderator purges, and bulk delete events using Discord Audit Logs.
* **Rich Message Context** - Stores replies, embeds, attachments, author information, and attachment MIME types for complete context.
* **Permanent Media Preservation** - Automatically downloads every attachments and re-upload a copy of them to the log channel, ensuring deleted media remains available even after Discord permanently removes CDN files.
* **Optimized Media Rendering** - Displays images and GIFs directly inside embeds while presenting videos as playable attachments for reliable desktop and mobile viewing.

### Performance & Efficiency
* **Fast First-Startup Indexing** - Synchronizes existing server history at over **820 messages/sec**, indexing **27,000+ messages** in under **33 seconds**.
* **SQLite WAL Architecture** - Uses dedicated read and write connections with WAL mode to support concurrent database operations efficiently.
* **Delta-Based Startup Scan** - Only scans channels that have changed since the previous shutdown, dramatically reducing startup time on large servers.
* **Batch Database Writes** - Buffers and commits messages in large batches to minimize disk I/O and maximize throughput.
* **Memory Efficient** - Maintains under **80 MB RAM** usage even after indexing tens of thousands of messages.
* **Designed for Large Communities** - Optimized to process large Discord servers with minimal CPU, memory, and database overhead.

### Administration & Reliability
* **`/getstats`** - Displays live uptime, database statistics, indexed message count, RAM usage, scan duration, and server information.
* **Startup Log Batching** - Groups thousands of offline edits and deletions into compact batch reports, reducing Discord REST requests by over **95%** and virtually eliminating HTTP 429 rate limits.
* **Scheduled Activity Reports** - Automatically sends hourly or daily moderation summaries to your configured log channel.
* **Multi-language Support** - Supports both English (`en`) and Vietnamese (`vi`) through a single configuration option.
* **Health Watchdog** - Continuously monitors background workers and automatically recovers stalled tasks for stable 24/7 operation.
* **Modular Design** - Separates core functionality into independent modules, enabling easier maintenance, customization, and component replacement.
* **Production Ready** - Built for long-term deployment with asynchronous workers, concurrent database access, and resilient startup synchronization.


## Showcase
### First startup - Full scan
Only took ~33s for 27,205 messages !
<img width="1066" height="201" alt="image" src="https://github.com/user-attachments/assets/19cec69d-67cb-42ad-9f10-d6da0bb0e021" />  

### Next startup and onwards - Smart Scan
MACB only took ~3s to prepare the database and finish messages synching.
<img width="1044" height="205" alt="{1F424276-8075-4794-92D6-43DA0715563F}" src="https://github.com/user-attachments/assets/81cbf592-d7f9-4761-928f-dd02a9070e6f" />  

### Message Deletion
<img width="411" height="711" alt="{B0D80459-89A5-4CFD-A0B5-3EC362B382AF}" src="https://github.com/user-attachments/assets/932d517f-e748-4bd4-8c8d-f3aa10af0f54" />  
<img width="392" height="453" alt="{192F5F05-DFDC-423F-8164-D1C2640B68A3}" src="https://github.com/user-attachments/assets/976914d9-3e3b-4ecd-86e4-3723403cce0d" />  

### Message Edit
<img width="304" height="422" alt="{32E18CA3-63AA-4EEE-8B5C-DF07D17F3E6C}" src="https://github.com/user-attachments/assets/5e546aa9-e393-4fdc-bf9e-386d84012631" />  

### Attachments & Media Preservation
<img width="381" height="664" alt="{5D59D99D-E46C-4A44-B1C3-21D226BBE93A}" src="https://github.com/user-attachments/assets/05877ab7-1cf3-4409-86bf-f49eb5aff543" />  
<img width="639" height="624" alt="{F3212D74-04B3-4E8D-A4DC-C5689E203625}" src="https://github.com/user-attachments/assets/55ffdc92-9090-41e0-a8cf-a803a7d85487" />  
<img width="1008" height="666" alt="{CE195510-DEA6-45FD-AE63-1A821E103620}" src="https://github.com/user-attachments/assets/4d3a8b8c-37e0-4b92-8df4-22f44cdcd2f8" />  


### Offline Recovery
<img width="391" height="433" alt="{0C3BCE78-3437-4604-9305-E2DADDB31B24}" src="https://github.com/user-attachments/assets/e5c4b8f0-b85b-4186-8fae-620e6333c3f5" />  

### Bulk Delete Detection
<img width="705" height="387" alt="{A01ABCB4-B104-4EEB-81E6-9E53ED36F0D6}" src="https://github.com/user-attachments/assets/716793cd-4d90-45f1-b9f0-4ef1a105fd7d" />

### Slash Command
<img width="354" height="770" alt="{621365D9-9E31-47B8-B132-0F9BD872244C}" src="https://github.com/user-attachments/assets/52aa59bb-76a8-4c5f-bcf5-77b91ca6da19" />

### Multi-Languages
<img width="322" height="426" alt="{E0B78561-1797-4318-A18B-646C18A32661}" src="https://github.com/user-attachments/assets/9b327c7a-e84c-49d5-96cc-1e279ff7034e" />

## Installation & Setup

### Prerequisites
*   **Python 3.10+** (Ensure Python is added to your system PATH)
*   A Discord Bot Token (created via the [Discord Developer Portal](https://discord.com/developers/applications))
*   **Discord Gateway Intents**: Ensure **Message Content Intent** and **Server Members Intent** are toggled **ON** in the *Discord Developer Portal* under *Bot -> Privileged Gateway Intents*.
*   **Permissions**: Ensure the Bot's highest role has the `Read Message History`, `View Audit Log`, and `View Channels` permissions for all monitored text channels.

### 1. Clone the Repository
```bash
git clone https://github.com/MeowIce/macb.git
cd macb
```

### 2. Install Dependencies
Install the required packages using `pip`:
```bash
pip install discord.py psutil aiohttp
```

### 3. Configuration (`config.py`)

Open `config.py` and configure your settings:

*   `botToken`: Your Discord Bot Token.
*   `targetGuildId`: The ID of the Discord Guild (Server) you want to monitor.
*   `logChannelId`: The ID of the text channel where log embeds should be sent.
*   `botLang`: Choose `"en"` for English or `"vi"` for Vietnamese.

Example `config.py` structure:
```python
botToken = "YOUR_DISCORD_BOT_TOKEN"
targetGuildId = 123456789012345678   # Target Server ID
logChannelId = 987654321098765432    # Target Log Channel ID
botLang = "en"                       # Language ('en' or 'vi')
```
> [!NOTE]
> **On `maxParallelScans = 36` and `maxParallelDownloads = 16`**: These settings are optimized for high-throughput systems. If your hosting environment (VPS/Home server) has limited CPU or network speed, reduce these value to `15` or `20` to prevent network congestion.  
>
> The rest should be left intact, unless you know what you're doing.
## How to Run

Run the bot directly via the terminal:

```bash
python bot.py
```
---
> [!NOTE]
> If MACB has been useful for your moderation workflow, consider giving the repository a star. Thank you ^^  
> <img width="207" height="81" alt="{9EC984E8-81B6-47CE-B7AD-9CD3A5EAA942}" src="https://github.com/user-attachments/assets/c75b2695-a56d-44a0-9f38-831d1da38378" />

---

## License and Terms of Use

Copyright (c) 2026 MeowIce. Permission is granted to use, modify, and distribute this software for non-commercial purposes only. Selling this software or any derivative works is prohibited without explicit written permission. Removing or altering author credits is prohibited.
