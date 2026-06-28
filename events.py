import discord
import json
import logging
import asyncio
import os
import psutil
from datetime import datetime
import config
from localization import getLocaleString
import collections

logger = logging.getLogger("BotLogger")

class BotEvents:
    def __init__(self, bot, metricsTracker=None):
        self.bot = bot
        self.metricsTracker = metricsTracker
        self.bulkDeletedIds = set()
        self.bulkDeletedQueue = collections.deque(maxlen=20000)
        self.bulkLock = asyncio.Lock()

    def setupEvents(self):
        @self.bot.event
        async def on_ready():
            if self.bot.bootTime is None:
                self.bot.bootTime = datetime.now()
            
            targetGuild = self.bot.get_guild(config.targetGuildId)
            dbSuccess = self.bot.databaseManager.initDatabase()
            
            dbSizeStr = "0.00 KB"
            if os.path.exists(config.dbPath):
                dbSizeBytes = os.path.getsize(config.dbPath)
                if dbSizeBytes >= 1024 * 1024:
                    dbSizeStr = f"{dbSizeBytes / (1024 * 1024):.2f} MB"
                else:
                    dbSizeStr = f"{dbSizeBytes / 1024:.2f} KB"
            
            monitoredChannels = 0
            totalMembers = 0
            if targetGuild:
                monitoredChannels = len([ch for ch in targetGuild.text_channels if ch.permissions_for(targetGuild.me).read_message_history])
                totalMembers = targetGuild.member_count
            
            print(getLocaleString("botUsername", name=self.bot.user.name))
            print(getLocaleString("botId", id=self.bot.user.id))
            print(getLocaleString("reportSched", sched=getattr(config, 'reportTaskSched', 'hourly')))
            statusKey = "yes" if getattr(config, 'alsoSendToLogChannel', True) else "no"
            print(getLocaleString("sendReportLog", status=getLocaleString(statusKey)))
            print()
            print(getLocaleString("dbSuccess") if dbSuccess else getLocaleString("dbFail"))
            print(getLocaleString("dbSize", size=dbSizeStr))
            print()
            print(getLocaleString("monitoredChannels", count=monitoredChannels))
            print(getLocaleString("totalMembers", count=totalMembers))
            print()
            
            if dbSuccess:
                self.bot.databaseManager.startWorker(asyncio.get_running_loop())
                self.bot.mediaManager.initialize()
                asyncio.get_running_loop().create_task(self.bot.mediaManager.cleanCacheTask())
            
            if targetGuild:
                try:
                    await self.bot.tree.sync(guild=discord.Object(id=config.targetGuildId))
                    print(getLocaleString("syncSlash"))
                    print()
                    print()
                except Exception as syncError:
                    logger.error(f"Slash sync error: {str(syncError)}")
                
                self.bot.logDispatcher.startWorkers(asyncio.get_running_loop())
                asyncio.create_task(self.bot.startupScanner.executeScan(targetGuild))

        @self.bot.event
        async def on_message(message):
            if message.guild is None or message.guild.id != config.targetGuildId:
                return
            if message.author.bot:
                return
            attachmentsList = [att.url for att in message.attachments]
            contentTypesList = [att.content_type for att in message.attachments]
            stickersList = [st.url for st in message.stickers] if hasattr(message, "stickers") else []
            embedsData = [emb.to_dict() for emb in message.embeds]
            replyRef = {}
            if message.reference and message.reference.message_id:
                replyRef = {"messageId": message.reference.message_id, "channelId": message.reference.channel_id}
            avatarUrl = message.author.display_avatar.url if message.author.display_avatar else ""
            authorDisplayName = getattr(message.author, "display_name", "")
            authorGlobalName = getattr(message.author, "global_name", "")
            messageData = (
                message.id,
                message.author.id,
                message.author.name,
                authorDisplayName,
                authorGlobalName,
                avatarUrl,
                message.channel.id,
                message.channel.name,
                getattr(message.channel, "parent", None).name if hasattr(message.channel, "parent") and message.channel.parent else "",
                message.content,
                json.dumps(attachmentsList),
                json.dumps(stickersList),
                json.dumps(embedsData),
                json.dumps(replyRef),
                message.type.value if hasattr(message.type, "value") else 0,
                json.dumps(contentTypesList)
            )
            await self.bot.databaseManager.enqueueAction("save", messageData)
            self.bot.totalScannedMessages += 1
            self.bot.hourlyNewMessages += 1

        @self.bot.event
        async def on_raw_message_delete(payload):
            if payload.guild_id != config.targetGuildId:
                return
            await asyncio.sleep(0.3)
            async with self.bulkLock:
                if payload.message_id in self.bulkDeletedIds:
                    return
            dbData = await asyncio.to_thread(self.bot.databaseManager.getMessage, payload.message_id)
            if not dbData:
                return
            self.bot.totalScannedMessages -= 1
            self.bot.hourlyDeletedMessages += 1
            attachmentsList = []
            try:
                attachmentsList = json.loads(dbData[10])
            except Exception:
                pass
            replyRef = {}
            try:
                replyRef = json.loads(dbData[13])
            except Exception:
                pass
            msgTransformed = {
                "messageId": dbData[0],
                "authorId": dbData[1],
                "authorName": dbData[2],
                "authorAvatar": dbData[5],
                "content": dbData[9],
                "attachments": attachmentsList,
                "replyReference": replyRef,
                "isOffline": False,
                "contentTypes": dbData[15]
            }
            await self.bot.databaseManager.enqueueAction("delete", payload.message_id)
            await self.bot.logDispatcher.enqueueLogAction({
                "logType": "singleDelete",
                "channelId": payload.channel_id,
                "messageData": msgTransformed
            })

        @self.bot.event
        async def on_raw_bulk_message_delete(payload):
            if payload.guild_id != config.targetGuildId:
                return
            messageIds = list(payload.message_ids)
            async with self.bulkLock:
                for mId in messageIds:
                    if len(self.bulkDeletedQueue) >= self.bulkDeletedQueue.maxlen:
                        oldest = self.bulkDeletedQueue.popleft()
                        self.bulkDeletedIds.discard(oldest)
                    self.bulkDeletedQueue.append(mId)
                    self.bulkDeletedIds.add(mId)
            asyncio.get_running_loop().create_task(self.clearBulkCacheDelayed(messageIds))
            guild = self.bot.get_guild(payload.guild_id)
            reason = getLocaleString("bulkUnknown")
            if guild and guild.me.guild_permissions.view_audit_log:
                try:
                    await asyncio.sleep(0.5)
                    async for entry in guild.audit_logs(action=discord.AuditLogAction.message_bulk_delete, limit=3):
                        if entry.extra.channel.id == payload.channel_id and (datetime.utcnow() - entry.created_at).total_seconds() < 12:
                            reason = getLocaleString("purgeReason", name=entry.user.name, id=entry.user.id)
                            break
                except Exception as auditEx:
                    logger.error(f"Audit log fetch failed: {str(auditEx)}")
            dbRecords = await asyncio.to_thread(self.bot.databaseManager.getMessagesBulk, messageIds)
            if not dbRecords:
                return
            await self.bot.databaseManager.enqueueAction("bulkDelete", messageIds)
            self.bot.totalScannedMessages -= len(dbRecords)
            self.bot.hourlyDeletedMessages += len(dbRecords)
            validMessages = []
            for mId in messageIds:
                if mId in dbRecords:
                    dbData = dbRecords[mId]
                    attachmentsList = []
                    try:
                        attachmentsList = json.loads(dbData[10])
                    except Exception:
                        pass
                    replyRef = {}
                    try:
                        replyRef = json.loads(dbData[13])
                    except Exception:
                        pass
                    validMessages.append({
                        "messageId": dbData[0],
                        "authorId": dbData[1],
                        "authorName": dbData[2],
                        "authorAvatar": dbData[5],
                        "content": dbData[9],
                        "attachments": attachmentsList,
                        "replyReference": replyRef,
                        "contentTypes": dbData[15]
                    })
            if validMessages:
                await self.bot.logDispatcher.enqueueLogAction({
                    "logType": "bulkDeleteEvent",
                    "channelId": payload.channel_id,
                    "messagesList": validMessages,
                    "reason": reason
                })

        @self.bot.event
        async def on_raw_message_edit(payload):
            if payload.guild_id != config.targetGuildId:
                return
            if "content" not in payload.data:
                return
            newContent = payload.data["content"]
            dbData = await asyncio.to_thread(self.bot.databaseManager.getMessage, payload.message_id)
            if not dbData or dbData[9] == newContent:
                return
            self.bot.hourlyEditedMessages += 1
            await self.bot.databaseManager.enqueueAction("updateFields", (payload.message_id, {"content": newContent}))
            await self.bot.logDispatcher.enqueueLogAction({
                "logType": "singleEdit",
                "messageId": payload.message_id,
                "authorId": dbData[1],
                "authorName": dbData[2],
                "authorAvatar": dbData[5],
                "channelId": dbData[6],
                "oldContent": dbData[9],
                "newContent": newContent,
                "contentTypes": dbData[15]
            })

        @self.bot.tree.command(name="getstats", description="Get operational statistics of MACB", guild=discord.Object(id=config.targetGuildId))
        async def getStatsCommand(interaction: discord.Interaction):
            await interaction.response.defer(ephemeral=False)
            uptimeDelta = datetime.now() - self.bot.bootTime
            days = uptimeDelta.days
            hours, remainder = divmod(uptimeDelta.seconds, 3600)
            minutes, seconds = divmod(remainder, 60)
            uptimeText = getLocaleString("uptimeStr", d=days, h=hours, m=minutes, s=seconds)
            dbConnectedStatus = getLocaleString("dbSuccess") if self.bot.databaseManager.isReady else getLocaleString("dbFail")
            dbSizeBytes = os.path.getsize(config.dbPath) if os.path.exists(config.dbPath) else 0
            if dbSizeBytes >= 1024 * 1024:
                dbSizeText = f"{dbSizeBytes / (1024 * 1024):.2f} MB"
            else:
                dbSizeText = f"{dbSizeBytes / 1024:.2f} KB"
                
            loadedMessagesCount = await asyncio.to_thread(self.bot.databaseManager.getTotalMessageCount)
            
            totalScanTimeStr = f"{self.bot.totalScanTimeStr} {getLocaleString('empty') if not self.bot.totalScanTimeStr else ''}"
            if "giây" in totalScanTimeStr or "seconds" in totalScanTimeStr:
                pass
            else:
                totalScanTimeStr = f"{self.bot.totalScanTimeStr} giây" if config.botLang == "vi" else f"{self.bot.totalScanTimeStr} seconds"
            oldestDateStr = getLocaleString("empty")
            if self.bot.globalOldestDate:
                oldestDateStr = self.bot.globalOldestDate.astimezone().strftime("%d/%m/%Y %H:%M:%S")
            targetGuild = interaction.guild
            monitoredChannelsCount = 0
            totalMembersCount = 0
            if targetGuild:
                monitoredChannelsCount = len([ch for ch in targetGuild.text_channels if ch.permissions_for(targetGuild.me).read_message_history])
                totalMembersCount = targetGuild.member_count
            ramUsageMb = psutil.Process(os.getpid()).memory_info().rss / (1024 * 1024)
            descriptionLines = [
                f"### {getLocaleString('genStats')}",
                f"**Uptime:** {uptimeText}",
                "",
                f"### {getLocaleString('botInfo')}",
                f"**Username:** {self.bot.user.name}",
                f"**ID:** {self.bot.user.id}",
                "",
                f"### {getLocaleString('dbSection')}",
                f"**{getLocaleString('dbStatus')}:** {dbConnectedStatus.split(':')[-1].strip() if ':' in dbConnectedStatus else dbConnectedStatus}",
                f"**{getLocaleString('dbFileSize')}:** {dbSizeText}",
                f"**{getLocaleString('loadedMsgs')}:** {loadedMessagesCount}",
                f"**{getLocaleString('scanTimeField')}:** {totalScanTimeStr}",
                f"**{getLocaleString('oldestMsgField')}:** {oldestDateStr}",
                "",
                f"### {getLocaleString('serverStats')}",
                f"**{getLocaleString('monitoredChannelsField')}:** {monitoredChannelsCount}",
                f"**{getLocaleString('totalMembersField')}:** {totalMembersCount}",
                "",
                f"### {getLocaleString('resourcesSection')}",
                f"**{getLocaleString('ramUsageField')}:** {ramUsageMb:.2f} MB",
                "",
                f"### {getLocaleString('supportDiscord')}",
                "https://dsc.gg/meowsmp",
                "",
                "Bot by @meowice",
                "Source: https://github.com/MeowIce/macb"
            ]
            embedStatus = discord.Embed(
                title=getLocaleString("statsTitle"),
                color=discord.Color.blue(),
                description="\n".join(descriptionLines)
            )
            await interaction.followup.send(embed=embedStatus)

    async def clearBulkCacheDelayed(self, messageIds):
        await asyncio.sleep(12)
        async with self.bulkLock:
            for mId in messageIds:
                self.bulkDeletedIds.discard(mId)