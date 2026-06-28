import discord
import asyncio
import io
import logging
import time
import json
import aiohttp
from datetime import datetime
import config
from localization import getLocaleString

logger = logging.getLogger("BotLogger")

def determineMessageType(content, contentTypesRaw):
    hasText = bool(content and str(content).strip())
    if not contentTypesRaw:
        return getLocaleString("text")
    try:
        cTypes = json.loads(contentTypesRaw)
    except Exception:
        cTypes = []
    if not cTypes:
        return getLocaleString("text")
    mTypes = []
    for ct in cTypes:
        if not ct:
            mTypes.append(getLocaleString("attachment"))
        elif "image" in ct:
            if "gif" in ct:
                mTypes.append(getLocaleString("gif"))
            else:
                mTypes.append(getLocaleString("image"))
        elif "video" in ct:
            mTypes.append(getLocaleString("video"))
        else:
            mTypes.append(getLocaleString("attachment"))
    uniqueMTypes = []
    for m in mTypes:
        if m not in uniqueMTypes:
            uniqueMTypes.append(m)
    mediaStr = ", ".join(uniqueMTypes)
    if hasText:
        return getLocaleString("textMedia", mediaStr=mediaStr)
    return getLocaleString("mediaOnly", mediaStr=mediaStr)

class LogDispatcher:
    def __init__(self, bot, metricsTracker=None):
        self.bot = bot
        self.logQueue = asyncio.Queue(maxsize=config.maxLogQueueSize)
        self.downloadSemaphore = asyncio.Semaphore(config.maxParallelDownloads)
        self.metricsTracker = metricsTracker
        self.workerTasks = []

    def startLoops(self, loop):
        self.workerTasks = []
        for _ in range(config.logConsumerWorkersCount):
            self.workerTasks.append(loop.create_task(self.logConsumerWorker()))

    def startWorkers(self, loop):
        """Alias for startLoops for backward compatibility."""
        self.startLoops(loop)

    async def enqueueLogAction(self, actionPayload):
        try:
            await asyncio.wait_for(self.logQueue.put(actionPayload), timeout=5.0)
            if self.metricsTracker:
                self.metricsTracker.updateLogQueue(self.logQueue.qsize())
        except asyncio.TimeoutError:
            logger.warning("Log queue is full; operation timed out.")
            if self.metricsTracker:
                self.metricsTracker.incrementQueueDropped()
        except Exception as ex:
            logger.error(f"Error enqueuing log action: {str(ex)}")

    async def logConsumerWorker(self):
        while True:
            try:
                if self.bot and hasattr(self.bot, "watchdog"):
                    self.bot.watchdog.feedHeartbeat("LogDispatcherWorker")
                try:
                    actionPayload = await asyncio.wait_for(self.logQueue.get(), timeout=1.0)
                except asyncio.TimeoutError:
                    continue
                startTime = time.perf_counter()
                await self.executeLogPipeline(actionPayload)
                if self.metricsTracker:
                    self.metricsTracker.recordSendLatency(time.perf_counter() - startTime)
                    self.metricsTracker.updateLogQueue(self.logQueue.qsize())
                self.logQueue.task_done()
            except asyncio.CancelledError:
                break
            except Exception as workerError:
                logger.error(f"Error in log consumer worker: {str(workerError)}", exc_info=True)
                await asyncio.sleep(1)

    async def executeLogPipeline(self, payload):
        logType = payload.get("logType")
        channelId = payload.get("channelId")
        if logType in ("singleDelete", "offlineDelete"):
            msgDataList = [payload["messageData"]]
            preloadedReplies = await self.preloadReplyCache(msgDataList)
            await self.processSingleDeletePipeline(channelId, payload["messageData"], preloadedReplies)
        elif logType == "bulkDeleteEvent":
            await self.processBulkDeletePipeline(channelId, payload["messagesList"], payload["reason"])
        elif logType in ("singleEdit", "offlineEdit"):
            await self.processSingleEditPipeline(payload)

    async def preloadReplyCache(self, messagesList):
        replyIds = []
        for m in messagesList:
            if m.get("replyReference") and m["replyReference"].get("messageId"):
                replyIds.append(m["replyReference"]["messageId"])
        if not replyIds:
            return {}
        replyIds = list(set(replyIds))
        return await asyncio.to_thread(self.bot.databaseManager.getMessagesBulk, replyIds)

    async def downloadAttachmentSafe(self, url):
        async with self.downloadSemaphore:
            startTime = time.perf_counter()
            try:
                fileBytes = await self.bot.mediaManager.downloadMediaBytes(url)
                if self.metricsTracker:
                    self.metricsTracker.recordDownloadTime(time.perf_counter() - startTime)
                    self.metricsTracker.recordThroughput(1)
                return fileBytes
            except aiohttp.ClientResponseError as responseError:
                if responseError.status in [404, 410]:
                    logger.info(f"Attachment expired or missing (HTTP {responseError.status}): {url}")
                else:
                    logger.warning(f"HTTP error downloading attachment {url}: {str(responseError)}")
                return None
            except Exception as dlEx:
                logger.debug(f"Non-critical error downloading attachment {url}: {str(dlEx)}")
                return None

    async def processSingleDeletePipeline(self, channelId, msg, replyCache):
        createdUtc = discord.utils.snowflake_time(msg["messageId"])
        sentTimeStr = createdUtc.astimezone().strftime("%d/%m/%Y %H:%M:%S")
        descriptionText = f"{getLocaleString('author')}: <@{msg['authorId']}> ({msg['authorId']})\n{getLocaleString('channel')}: <#{channelId}>\n{getLocaleString('sentTime')}: {sentTimeStr}\nID: {msg['messageId']}"
        replyRef = msg.get("replyReference")
        if replyRef and replyRef.get("messageId"):
            replyId = replyRef["messageId"]
            replyData = replyCache.get(replyId)
            replyContent = replyData[9] if replyData else getLocaleString("noDbContent")
            if not replyContent.strip():
                replyContent = getLocaleString("noTextContent")
            if len(replyContent) > 500:
                replyContent = replyContent[:500] + "..."
            descriptionText += f"\n{getLocaleString('replyTo')}: {replyId}"
        isOffline = msg.get("isOffline", False)
        titleText = getLocaleString("msgDeletedOffline") if isOffline else getLocaleString("msgDeleted")
        colorValue = discord.Color.dark_red() if isOffline else discord.Color.red()
        embed = discord.Embed(title=titleText, color=colorValue, description=descriptionText)
        embed.set_author(name=msg["authorName"], icon_url=msg["authorAvatar"])
        embed.set_footer(text=f"{getLocaleString('sentTime')}: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
        msgType = determineMessageType(msg["content"], msg.get("contentTypes"))
        embed.add_field(name=getLocaleString("msgType"), value=msgType, inline=False)
        if replyRef and replyRef.get("messageId"):
            embed.add_field(name=f"{getLocaleString('replyTo')}: {replyId}", value=f"```\n{replyContent}\n```", inline=False)
        txtFile = None
        if msg["content"]:
            if len(msg["content"]) > 1024:
                txtFile = discord.File(io.BytesIO(msg["content"].encode("utf-8")), filename=f"{msg['messageId']}_content.txt")
                embed.add_field(name=getLocaleString("content"), value=getLocaleString("overLimit"), inline=False)
            else:
                embed.add_field(name=getLocaleString("content"), value=f"```\n{msg['content']}\n```", inline=False)
        else:
            embed.add_field(name=getLocaleString("content"), value=getLocaleString("noText"), inline=False)

        attachmentsList = msg.get("attachments", [])
        downloadTasks = [self.downloadAttachmentSafe(url) for url in attachmentsList]
        downloadedBytesList = await asyncio.gather(*downloadTasks)
        cTypes = []
        if msg.get("contentTypes"):
            try:
                cTypes = json.loads(msg["contentTypes"])
            except Exception:
                pass
        validFiles = []
        if len(attachmentsList) == 1 and downloadedBytesList[0]:
            fBytes = downloadedBytesList[0]
            origUrl = attachmentsList[0]
            ext = "png"
            if cTypes and len(cTypes) == 1 and cTypes[0]:
                ct = cTypes[0].lower()
                if "gif" in ct:
                    ext = "gif"
                elif "jpeg" in ct or "jpg" in ct:
                    ext = "jpg"
                elif "webp" in ct:
                    ext = "webp"
                elif "png" in ct:
                    ext = "png"
                elif "mp4" in ct:
                    ext = "mp4"
                elif "quicktime" in ct or "mov" in ct:
                    ext = "mov"
                elif "webm" in ct:
                    ext = "webm"
            else:
                urlPart = origUrl.lower().split("?")[0]
                for possibleExt in ['.gif', '.png', '.jpg', '.jpeg', '.webp', '.mp4', '.mov', '.webm', '.mkv']:
                    if urlPart.endswith(possibleExt):
                        ext = possibleExt.lstrip('.')
                        break
            isImageOrGif = ext in ['gif', 'png', 'jpg', 'jpeg', 'webp']
            if isImageOrGif:
                cleanFilename = f"deleted_media.{ext}"
                embed.set_image(url=f"attachment://{cleanFilename}")
                validFiles.append((cleanFilename, fBytes))
            else:
                origFilename = origUrl.split("/")[-1].split("?")[0] or f"deleted_video.{ext}"
                validFiles.append((origFilename, fBytes))
        else:
            for index, fBytes in enumerate(downloadedBytesList):
                if fBytes:
                    origUrl = attachmentsList[index]
                    filename = origUrl.split("/")[-1].split("?")[0] or f"file_{index}"
                    validFiles.append((filename, fBytes))
        if txtFile:
            validFiles.append((txtFile.filename, txtFile.fp.read()))
        await self.dispatchPayloadChunked(embed, validFiles)

    async def processBulkDeletePipeline(self, channelId, messagesList, reason):
        logReport = [
            getLocaleString("bulkReportHeader", channelId=channelId),
            getLocaleString("bulkReason", reason=reason),
            getLocaleString("bulkDetectTime", time=datetime.now().strftime('%d/%m/%Y %H:%M:%S')) + "\n"
        ]
        for msg in messagesList:
            createdUtc = discord.utils.snowflake_time(msg["messageId"])
            sentTimeStr = createdUtc.astimezone().strftime("%d/%m/%Y %H:%M:%S")
            logReport.append(getLocaleString("bulkMsgAuthor", time=sentTimeStr, authorId=msg['authorId'], name=msg['authorName']))
            logReport.append(getLocaleString("bulkMsgId", id=msg['messageId']))
            logReport.append(getLocaleString("bulkMsgContent", content=msg['content'] or getLocaleString("empty")))
            logReport.append("-" * 40)
        reportBytes = "\n".join(logReport).encode("utf-8")
        reportFile = discord.File(io.BytesIO(reportBytes), filename=f"bulk_delete_{channelId}.txt")
        embed = discord.Embed(
            title=getLocaleString("bulkTitle"),
            color=discord.Color.dark_magenta(),
            description=getLocaleString("bulkDesc", count=len(messagesList), channelId=channelId, reason=reason)
        )
        embed.set_footer(text=f"{getLocaleString('sentTime')}: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
        await self.sendLogWithRetry(embed=embed, file=reportFile)

    async def processSingleEditPipeline(self, payload):
        createdUtc = discord.utils.snowflake_time(payload["messageId"])
        sentTimeStr = createdUtc.astimezone().strftime("%d/%m/%Y %H:%M:%S")
        descriptionText = f"{getLocaleString('author')}: <@{payload['authorId']}>\n{getLocaleString('channel')}: <#{payload['channelId']}>\n{getLocaleString('sentTime')}: {sentTimeStr}\nID: {payload['messageId']}"
        isOffline = payload.get("logType") == "offlineEdit"
        titleText = f"{getLocaleString('msgEdited')} (Offline)" if isOffline else getLocaleString("msgEdited")
        colorValue = discord.Color.dark_orange() if isOffline else discord.Color.orange()
        embed = discord.Embed(title=titleText, color=colorValue, description=descriptionText)
        embed.set_author(name=payload["authorName"], icon_url=payload["authorAvatar"])
        msgType = determineMessageType(payload["newContent"], payload.get("contentTypes"))
        embed.add_field(name=getLocaleString("msgType"), value=msgType, inline=False)
        oldContentText = payload["oldContent"]
        newContentText = payload["newContent"]
        txtFiles = []
        if oldContentText and len(oldContentText) > 1024:
            txtFiles.append(("old_content.txt", oldContentText.encode("utf-8")))
            embed.add_field(name=getLocaleString("beforeEdit"), value=getLocaleString("overLimit"), inline=False)
        else:
            embed.add_field(name=getLocaleString("beforeEdit"), value=f"```\n{oldContentText or getLocaleString('empty')}\n```", inline=False)
        if newContentText and len(newContentText) > 1024:
            txtFiles.append(("new_content.txt", newContentText.encode("utf-8")))
            embed.add_field(name=getLocaleString("afterEdit"), value=getLocaleString("overLimit"), inline=False)
        else:
            embed.add_field(name=getLocaleString("afterEdit"), value=f"```\n{newContentText or getLocaleString('empty')}\n```", inline=False)
        embed.set_footer(text=f"{getLocaleString('sentTime')}: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
        fileObjects = [discord.File(io.BytesIO(b), filename=n) for n, b in txtFiles]
        await self.sendLogWithRetry(embed=embed, files=fileObjects if fileObjects else None)

    async def dispatchPayloadChunked(self, embed, validFiles):
        if not validFiles:
            await self.sendLogWithRetry(embed=embed)
            return
        currentChunk = []
        currentChunkSize = 0
        maxFilesPerMsg = 10
        maxBytesPerMsg = config.maxPayloadBytesLimit
        isFirstMessage = True
        for filename, fBytes in validFiles:
            fSize = len(fBytes)
            if fSize > maxBytesPerMsg:
                embed.add_field(name=getLocaleString("sysWarning"), value=getLocaleString("fileSizeError", filename=filename), inline=False)
                continue
            if len(currentChunk) >= maxFilesPerMsg or (currentChunkSize + fSize) > maxBytesPerMsg:
                await self.sendChunk(embed if isFirstMessage else None, currentChunk)
                isFirstMessage = False
                currentChunk = []
                currentChunkSize = 0
            currentChunk.append(discord.File(io.BytesIO(fBytes), filename=filename))
            currentChunkSize += fSize
        if currentChunk:
            await self.sendChunk(embed if isFirstMessage else None, currentChunk)

    async def sendChunk(self, embed, fileObjects):
        await self.sendLogWithRetry(embed=embed, files=fileObjects)

    async def sendLogWithRetry(self, embed=None, file=None, files=None):
        logChannel = self.bot.get_channel(config.logChannelId)
        if not logChannel:
            return False
        maxRetries = 3
        for attempt in range(maxRetries):
            try:
                if files:
                    await asyncio.wait_for(logChannel.send(embed=embed, files=files), timeout=15)
                elif file:
                    await asyncio.wait_for(logChannel.send(embed=embed, file=file), timeout=15)
                else:
                    await asyncio.wait_for(logChannel.send(embed=embed), timeout=15)
                return True
            except discord.Forbidden:
                logger.error("Permanent Error: Missing Permissions / Forbidden to send log messages.")
                return False
            except discord.NotFound:
                logger.error("Permanent Error: Log channel not found.")
                return False
            except discord.HTTPException as httpEx:
                if httpEx.status in [400, 401, 403, 404]:
                    logger.error(f"Permanent HTTP Error {httpEx.status}; aborting retry loop.")
                    return False
                if self.metricsTracker:
                    self.metricsTracker.incrementRetry()
                sleepTime = httpEx.retry_after if (httpEx.status == 429 and hasattr(httpEx, "retry_after")) else (1.5 ** attempt)
                await asyncio.sleep(sleepTime)
            except (asyncio.TimeoutError, aiohttp.ClientError) as transientError:
                if self.metricsTracker:
                    self.metricsTracker.incrementRetry()
                logger.warning(f"Transient network exception encountered ({str(transientError)}); retrying in worker ({attempt + 1}/{maxRetries}).")
                await asyncio.sleep(1.5 ** attempt)
            except Exception as ex:
                if self.metricsTracker:
                    self.metricsTracker.incrementRetry()
                logger.warning(f"Unexpected transmission exception ({str(ex)}); retrying in worker ({attempt + 1}/{maxRetries}).")
                await asyncio.sleep(1.5 ** attempt)
        return False

    async def flushAndClose(self):
        logger.info("Joining log queue...")
        await self.logQueue.join()
        for task in self.workerTasks:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        logger.info("Log dispatcher workers stopped cleanly.")