import discord
import json
import logging
import asyncio
import os
import psutil
import time
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

        @self.bot.tree.command(name="getstats", description=getLocaleString("statsDescription"), guild=discord.Object(id=config.targetGuildId))
        async def getStatsCommand(interaction: discord.Interaction):
            await interaction.response.defer(ephemeral=False)
            bootTime = self.bot.bootTime or datetime.now()
            uptimeDelta = datetime.now() - bootTime
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
            
            totalScanTimeRaw = getattr(self.bot, "totalScanTimeStr", "")
            totalScanTimeStr = getLocaleString("secondsSuffix", value=totalScanTimeRaw) if totalScanTimeRaw else getLocaleString("empty")
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
            scanCompleted = self.bot.startupScanner.channelsCompleted
            scanTotal = self.bot.startupScanner.totalChannels
            scanPercent = (scanCompleted / scanTotal * 100) if scanTotal else 100.0
            dbQueueSize = self.bot.databaseManager.dbQueue.qsize()
            logQueueSize = self.bot.logDispatcher.logQueue.qsize()
            nowPerf = time.perf_counter()
            heartbeatAges = [nowPerf - lastBeat for lastBeat in self.bot.watchdog.lastHeartbeats.values()]
            watchdogHealthy = all(age <= 60 for age in heartbeatAges) if heartbeatAges else True
            watchdogStatus = getLocaleString("healthyStatus") if watchdogHealthy else getLocaleString("warningStatus")
            schedType = getattr(config, "reportTaskSched", "hourly")
            scheduleText = getLocaleString("reportScheduleHourlyValue") if schedType == "hourly" else getLocaleString("reportScheduleDailyValue")
            embedStatus = discord.Embed(
                title=getLocaleString("statsTitle"),
                color=discord.Color.blue(),
                timestamp=datetime.now()
            )
            embedStatus.add_field(
                name=getLocaleString("generalSection"),
                value=(
                    f"**{getLocaleString('uptimeField')}:** {uptimeText}\n"
                    f"**{getLocaleString('botField')}:** {self.bot.user.name} (`{self.bot.user.id}`)\n"
                    f"**{getLocaleString('scheduleField')}:** {scheduleText}\n"
                    f"**{getLocaleString('watchdogField')}:** {watchdogStatus}"
                ),
                inline=False
            )
            embedStatus.add_field(
                name=getLocaleString("dbSection"),
                value=(
                    f"**{getLocaleString('dbStatus')}:** {dbConnectedStatus.split(':')[-1].strip() if ':' in dbConnectedStatus else dbConnectedStatus}\n"
                    f"**{getLocaleString('dbFileSize')}:** {dbSizeText}\n"
                    f"**{getLocaleString('loadedMsgs')}:** {loadedMessagesCount:,}\n"
                    f"**{getLocaleString('queueField')}:** {dbQueueSize}/{config.maxDbQueueSize}"
                ),
                inline=True
            )
            embedStatus.add_field(
                name=getLocaleString("activitySection"),
                value=(
                    f"**{getLocaleString('newField')}:** {self.bot.hourlyNewMessages:,}\n"
                    f"**{getLocaleString('editedField')}:** {self.bot.hourlyEditedMessages:,}\n"
                    f"**{getLocaleString('deletedField')}:** {self.bot.hourlyDeletedMessages:,}\n"
                    f"**{getLocaleString('logQueueField')}:** {logQueueSize}/{config.maxLogQueueSize}"
                ),
                inline=True
            )
            embedStatus.add_field(
                name=getLocaleString("startupScanSection"),
                value=(
                    f"**{getLocaleString('progressField')}:** {scanCompleted}/{scanTotal} ({scanPercent:.1f}%)\n"
                    f"**{getLocaleString('bootScannedField')}:** {self.bot.currentBootScanned:,}\n"
                    f"**{getLocaleString('scanTimeField')}:** {totalScanTimeStr}\n"
                    f"**{getLocaleString('oldestMsgField')}:** {oldestDateStr}"
                ),
                inline=False
            )
            embedStatus.add_field(
                name=getLocaleString("serverStats"),
                value=(
                    f"**{getLocaleString('monitoredChannelsField')}:** {monitoredChannelsCount}\n"
                    f"**{getLocaleString('totalMembersField')}:** {totalMembersCount}\n"
                    f"**{getLocaleString('ramUsageField')}:** {ramUsageMb:.2f} MB"
                ),
                inline=True
            )
            embedStatus.add_field(
                name=getLocaleString("runtimeSection"),
                value=(
                    f"**{getLocaleString('retriesField')}:** {self.metricsTracker.retryCount:,}\n"
                    f"**{getLocaleString('droppedField')}:** {self.metricsTracker.queueDroppedCount:,}\n"
                    f"**{getLocaleString('throughputField')}:** {self.metricsTracker.currentThroughputRps:.2f}/s"
                ),
                inline=True
            )
            embedStatus.set_footer(text=getLocaleString("statsFooter"))
            await interaction.followup.send(embed=embedStatus)

        @self.bot.tree.command(name="gethealth", description=getLocaleString("healthDescription"), guild=discord.Object(id=config.targetGuildId))
        async def getHealthCommand(interaction: discord.Interaction):
            await interaction.response.defer(ephemeral=False)
            nowPerf = time.perf_counter()

            def taskState(task):
                if task is None:
                    return getLocaleString("taskMissing")
                if task.cancelled():
                    return getLocaleString("taskCancelled")
                if task.done():
                    return getLocaleString("taskStopped")
                return getLocaleString("taskRunning")

            heartbeatLines = []
            expectedTasks = ["DatabaseWorker", "LogDispatcherWorker", "MediaManager", "MetricsTask", "PeriodicReportTask"]
            for taskName in expectedTasks:
                lastBeat = self.bot.watchdog.lastHeartbeats.get(taskName)
                if lastBeat is None:
                    heartbeatLines.append(f"**{taskName}:** {getLocaleString('noHeartbeatStatus')}")
                    continue
                age = nowPerf - lastBeat
                status = getLocaleString("okStatus") if age <= 60 else getLocaleString("staleStatus")
                heartbeatLines.append(f"**{taskName}:** {status} ({getLocaleString('heartbeatAgeSuffix', value=age)})")

            logWorkerStates = [taskState(task) for task in self.bot.logDispatcher.workerTasks]
            taskLines = [
                f"**{getLocaleString('watchdogField')}:** {taskState(self.bot.watchdog.watchdogTask)}",
                f"**{getLocaleString('metricsTaskField')}:** {taskState(self.bot.reportingTask)}",
                f"**{getLocaleString('periodicReportTaskField')}:** {taskState(self.bot.periodicTask)}",
                f"**{getLocaleString('databaseWorkerField')}:** {taskState(self.bot.databaseManager.workerTask)}",
                f"**{getLocaleString('mediaCacheTaskField')}:** {taskState(self.bot.mediaManager.cacheTask)}",
                f"**{getLocaleString('logWorkersField')}:** {', '.join(logWorkerStates) if logWorkerStates else getLocaleString('taskMissing')}"
            ]

            embedHealth = discord.Embed(
                title=getLocaleString("healthTitle"),
                color=discord.Color.green(),
                timestamp=datetime.now()
            )
            embedHealth.add_field(name=getLocaleString("heartbeatsSection"), value="\n".join(heartbeatLines), inline=False)
            embedHealth.add_field(name=getLocaleString("tasksSection"), value="\n".join(taskLines), inline=False)
            embedHealth.add_field(
                name=getLocaleString("queuesSection"),
                value=(
                    f"**{getLocaleString('databaseShortField')}:** {self.bot.databaseManager.dbQueue.qsize()}/{config.maxDbQueueSize}\n"
                    f"**{getLocaleString('logShortField')}:** {self.bot.logDispatcher.logQueue.qsize()}/{config.maxLogQueueSize}\n"
                    f"**{getLocaleString('retriesField')}:** {self.metricsTracker.retryCount:,}\n"
                    f"**{getLocaleString('droppedField')}:** {self.metricsTracker.queueDroppedCount:,}"
                ),
                inline=True
            )
            embedHealth.add_field(
                name=getLocaleString("latencySection"),
                value=(
                    f"**{getLocaleString('databaseShortField')}:** {self.metricsTracker.dbLatencyEwma * 1000:.2f} ms\n"
                    f"**{getLocaleString('sendLatencyField')}:** {self.metricsTracker.sendLatencyEwma * 1000:.2f} ms\n"
                    f"**{getLocaleString('downloadLatencyField')}:** {self.metricsTracker.downloadTimeEwma * 1000:.2f} ms\n"
                    f"**{getLocaleString('throughputField')}:** {self.metricsTracker.currentThroughputRps:.2f}/s"
                ),
                inline=True
            )
            embedHealth.set_footer(text=getLocaleString("statsFooter"))
            await interaction.followup.send(embed=embedHealth)

        @self.bot.tree.command(name="getreport", description=getLocaleString("reportDescription"), guild=discord.Object(id=config.targetGuildId))
        async def getReportCommand(interaction: discord.Interaction):
            if not interaction.user.guild_permissions.manage_guild:
                await interaction.response.send_message(getLocaleString("reportPermissionDenied"), ephemeral=True)
                return

            await interaction.response.defer(ephemeral=True)
            try:
                reportSent = await self.bot.generateAndSendReport(True)
            except Exception as reportError:
                logger.error(f"Manual report command failed: {str(reportError)}", exc_info=True)
                reportSent = False

            if reportSent:
                await interaction.followup.send(getLocaleString("reportSent", channelId=config.logChannelId), ephemeral=True)
            else:
                await interaction.followup.send(getLocaleString("reportFailed"), ephemeral=True)

    async def clearBulkCacheDelayed(self, messageIds):
        await asyncio.sleep(12)
        async with self.bulkLock:
            for mId in messageIds:
                self.bulkDeletedIds.discard(mId)
