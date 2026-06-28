import discord
from discord.ext import commands
import asyncio
import logging
import sys
import psutil
import time
import os
import aiohttp
from datetime import datetime
import config
from database import DatabaseManager
from log_dispatcher import LogDispatcher
from events import BotEvents
from scanner import StartupScanner
from localization import getLocaleString

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("BotLogger")

class MetricsTracker:
    def __init__(self):
        self.dbQueueLength = 0
        self.logQueueLength = 0
        self.dbLatencyEwma = 0.0
        self.sendLatencyEwma = 0.0
        self.downloadTimeEwma = 0.0
        self.queueDroppedCount = 0
        self.retryCount = 0
        self.cacheHits = 0
        self.cacheMisses = 0
        self.throughputCount = 0
        self.alpha = 0.2
        self.lastThroughputCheck = time.perf_counter()
        self.currentThroughputRps = 0.0

    def updateDbQueue(self, length):
        self.dbQueueLength = length

    def updateLogQueue(self, length):
        self.logQueueLength = length

    def recordDbLatency(self, val):
        if self.dbLatencyEwma == 0.0:
            self.dbLatencyEwma = val
        else:
            self.dbLatencyEwma = (self.alpha * val) + ((1.0 - self.alpha) * self.dbLatencyEwma)

    def recordSendLatency(self, val):
        if self.sendLatencyEwma == 0.0:
            self.sendLatencyEwma = val
        else:
            self.sendLatencyEwma = (self.alpha * val) + ((1.0 - self.alpha) * self.sendLatencyEwma)

    def recordDownloadTime(self, val):
        if self.downloadTimeEwma == 0.0:
            self.downloadTimeEwma = val
        else:
            self.downloadTimeEwma = (self.alpha * val) + ((1.0 - self.alpha) * self.downloadTimeEwma)

    def incrementQueueDropped(self):
        self.queueDroppedCount += 1

    def incrementRetry(self):
        self.retryCount += 1

    def incrementCacheHit(self):
        self.cacheHits += 1

    def incrementCacheSubMiss(self):
        self.cacheMisses += 1

    def recordThroughput(self, count):
        self.throughputCount += count
        now = time.perf_counter()
        diff = now - self.lastThroughputCheck
        if diff >= 1.0:
            self.currentThroughputRps = self.throughputCount / diff
            self.throughputCount = 0
            self.lastThroughputCheck = now

class HealthWatchdog:
    def __init__(self, bot):
        self.bot = bot
        self.lastHeartbeats = {}
        self.watchdogTask = None

    def feedHeartbeat(self, taskName):
        self.lastHeartbeats[taskName] = time.perf_counter()

    async def startWatchdogLoop(self):
        await self.bot.wait_until_ready()
        while True:
            try:
                await asyncio.sleep(15)
                now = time.perf_counter()
                for taskName, lastTime in list(self.lastHeartbeats.items()):
                    if now - lastTime > 60:
                        logger.critical(f"Watchdog detected task stagnation or crash: {taskName}! Safe reconstruction in progress...")
                        await self.resurrectTask(taskName)
            except asyncio.CancelledError:
                break
            except Exception as watchdogEx:
                logger.error(f"Watchdog loop execution exception: {str(watchdogEx)}")

    async def resurrectTask(self, taskName):
        loop = asyncio.get_running_loop()
        if taskName == "DatabaseWorker":
            oldTask = self.bot.databaseManager.workerTask
            if oldTask and not oldTask.done():
                oldTask.cancel()
                try:
                    await oldTask
                except asyncio.CancelledError:
                    pass
            self.bot.databaseManager.workerTask = loop.create_task(self.bot.databaseManager.dbWorker())
            self.feedHeartbeat("DatabaseWorker")
        elif taskName == "LogDispatcherWorker":
            for task in list(self.bot.logDispatcher.workerTasks):
                if not task.done():
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass
            self.bot.logDispatcher.startLoops(loop)
            self.feedHeartbeat("LogDispatcherWorker")
        elif taskName == "PeriodicReportTask":
            oldTask = self.bot.periodicTask
            if oldTask and not oldTask.done():
                oldTask.cancel()
                try:
                    await oldTask
                except asyncio.CancelledError:
                    pass
            self.bot.periodicTask = loop.create_task(self.bot.periodicReportTask())
            self.feedHeartbeat("PeriodicReportTask")

class DummyMediaManager:
    def __init__(self, bot):
        self.bot = bot
        self.session = None
        self.cacheTask = None

    def initialize(self):
        self.session = aiohttp.ClientSession()

    async def cleanCacheTask(self):
        while True:
            try:
                if self.bot and hasattr(self.bot, "watchdog"):
                    self.bot.watchdog.feedHeartbeat("MediaManager")
                await asyncio.sleep(30)
            except asyncio.CancelledError:
                break

    async def downloadMediaBytes(self, url):
        if not self.session:
            return None
        async with self.session.get(url, timeout=15) as response:
            response.raise_for_status()
            return await response.read()

    async def close(self):
        if self.cacheTask and not self.cacheTask.done():
            self.cacheTask.cancel()
            try:
                await self.cacheTask
            except asyncio.CancelledError:
                pass
        if self.session:
            await self.session.close()

metricsTracker = MetricsTracker()
intents = discord.Intents.default()
intents.message_content = True
intents.guilds = True
intents.messages = True
intents.members = True

class AdvancedChatBot(commands.Bot):
    def __init__(self):
        super().__init__(
            command_prefix="!", 
            intents=intents,
            max_messages=10,
            chunk_guilds_at_startup=False
        )
        self.databaseManager = DatabaseManager(self, metricsTracker)
        self.logDispatcher = LogDispatcher(self, metricsTracker)
        self.startupScanner = StartupScanner(self)
        self.botEvents = BotEvents(self, metricsTracker)
        self.watchdog = HealthWatchdog(self)
        self.mediaManager = DummyMediaManager(self)
        self.totalScannedMessages = 0
        self.globalOldestDate = None
        self.bootTime = None
        self.hourlyNewMessages = 0
        self.hourlyEditedMessages = 0
        self.hourlyDeletedMessages = 0
        self.currentBootScanned = 0
        self.scanComplete = False
        self.reportingTask = None
        self.periodicTask = None

    async def setup_hook(self):
        self.botEvents.setupEvents()
        self.watchdog.watchdogTask = self.loop.create_task(self.watchdog.startWatchdogLoop())
        self.reportingTask = self.loop.create_task(self.metricsReportingTask())
        self.mediaManager.cacheTask = self.loop.create_task(self.mediaManager.cleanCacheTask())
        self.periodicTask = self.loop.create_task(self.periodicReportTask())

    async def metricsReportingTask(self):
        while True:
            try:
                await asyncio.sleep(30)
                self.watchdog.feedHeartbeat("MetricsTask")
                dbAvg = self.databaseManager.metricsTracker.dbLatencyEwma * 1000
                sendAvg = self.logDispatcher.metricsTracker.sendLatencyEwma * 1000
                dlAvg = self.logDispatcher.metricsTracker.downloadTimeEwma * 1000
                logger.debug(
                    f"[METRICS-EWMA] DB Queue: {metricsTracker.dbQueueLength} | Log Queue: {metricsTracker.logQueueLength} | "
                    f"DB Latency: {dbAvg:.2f}ms | Send Latency: {sendAvg:.2f}ms | Download Time: {dlAvg:.2f}ms | "
                    f"Throughput: {metricsTracker.currentThroughputRps:.2f} rps | Retries: {metricsTracker.retryCount} | "
                    f"Hits: {metricsTracker.cacheHits} | Misses: {metricsTracker.cacheMisses} | Dropped: {metricsTracker.queueDroppedCount}"
                )
            except asyncio.CancelledError:
                break
            except Exception as ex:
                logger.error(f"Metrics reporting task error: {str(ex)}")

    async def periodicReportTask(self):
        await self.wait_until_ready()
        schedType = getattr(config, "reportTaskSched", "hourly")
        sleepInterval = 3600 if schedType == "hourly" else 86400
        while True:
            try:
                remainingSleep = sleepInterval
                while remainingSleep > 0:
                    if self.watchdog:
                        self.watchdog.feedHeartbeat("PeriodicReportTask")
                    sleepChunk = min(30, remainingSleep)
                    await asyncio.sleep(sleepChunk)
                    remainingSleep -= sleepChunk

                if self.watchdog:
                    self.watchdog.feedHeartbeat("PeriodicReportTask")
                if getattr(config, "alsoSendToLogChannel", True):
                    await self.generateAndSendReport(False)
                if self.watchdog:
                    self.watchdog.feedHeartbeat("PeriodicReportTask")
            except asyncio.CancelledError:
                break
            except Exception as ex:
                logger.error(f"Periodic report task error: {str(ex)}", exc_info=True)
                if self.watchdog:
                    self.watchdog.feedHeartbeat("PeriodicReportTask")

    async def generateAndSendReport(self, isManual):
        logChannel = self.get_channel(config.logChannelId)
        if not logChannel:
            try:
                logChannel = await self.fetch_channel(config.logChannelId)
            except Exception as fetchError:
                logger.error(f"Failed to fetch log channel via API: {str(fetchError)}")
                return False
        schedType = getattr(config, "reportTaskSched", "hourly")
        titleKey = "periodicReportHourly" if schedType == "hourly" else "periodicReportDaily"
        titleText = getLocaleString(titleKey)
        currentTimeStr = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        
        # Query total message count directly from database to match /getstats
        totalMsgsCount = await asyncio.to_thread(self.databaseManager.getTotalMessageCount)
        totalMsgsStr = getLocaleString("msgCountSuffix", count=totalMsgsCount)
        newMsgsStr = getLocaleString("msgCountSuffix", count=self.hourlyNewMessages)
        editedMsgsStr = getLocaleString("msgCountSuffix", count=self.hourlyEditedMessages)
        deletedMsgsStr = getLocaleString("msgCountSuffix", count=self.hourlyDeletedMessages)
        descriptionContent = (
            f"**{getLocaleString('publishTime')}**\n{currentTimeStr}\n\n"
            f"**{getLocaleString('totalCurrentMessages')}**\n{totalMsgsStr}\n\n"
            f"**{getLocaleString('newMessagesGenerated')}**\n{newMsgsStr}\n\n"
            f"**{getLocaleString('editedMessagesField')}**\n{editedMsgsStr}\n\n"
            f"**{getLocaleString('deletedMessagesField')}**\n{deletedMsgsStr}"
        )
        embedReport = discord.Embed(
            title=titleText,
            color=discord.Color.blue(),
            description=descriptionContent
        )
        await logChannel.send(embed=embedReport)
        self.hourlyNewMessages = 0
        self.hourlyEditedMessages = 0
        self.hourlyDeletedMessages = 0
        return True

bot = AdvancedChatBot()

async def main():
    try:
        async with bot:
            print(getLocaleString("botStarting"))
            await bot.start(config.botToken)
    except asyncio.CancelledError:
        pass
    finally:
        print(getLocaleString("shuttingDown"))
        if bot.reportingTask and not bot.reportingTask.done():
            bot.reportingTask.cancel()
            try:
                await bot.reportingTask
            except asyncio.CancelledError:
                pass
        if bot.watchdog.watchdogTask and not bot.watchdog.watchdogTask.done():
            bot.watchdog.watchdogTask.cancel()
            try:
                await bot.watchdog.watchdogTask
            except asyncio.CancelledError:
                pass
        if bot.periodicTask and not bot.periodicTask.done():
            bot.periodicTask.cancel()
            try:
                await bot.periodicTask
            except asyncio.CancelledError:
                pass
        await bot.logDispatcher.flushAndClose()
        await bot.databaseManager.flushAndClose()
        await bot.mediaManager.close()
        await bot.close()
        print(getLocaleString("shutdownComplete"))

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        sys.exit(0)
