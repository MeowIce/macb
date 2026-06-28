import discord
import asyncio
import logging
import time
import json
from datetime import datetime, timezone
import config
from localization import getLocaleString
import gc

logger = logging.getLogger("BotLogger")

class StartupScanner:
    def __init__(self, bot):
        self.bot = bot
        self.totalChannels = 0
        self.channelsCompleted = 0
        self.lastLogTime = 0

    async def executeScan(self, guild):
        startTime = time.perf_counter()
        
        oldestId, latestMessagesMap = await asyncio.to_thread(self.bot.databaseManager.getStartupMetadata)
        isFirstScan = not latestMessagesMap
        
        if isFirstScan:
            print(getLocaleString("loadingDbFirst"))
        else:
            print(getLocaleString("loadingDbSync"))
            
        if oldestId:
            timestampMs = (oldestId >> 22) + 1420070400000
            self.bot.globalOldestDate = datetime.fromtimestamp(timestampMs / 1000, tz=timezone.utc)
            
        textChannels = [ch for ch in guild.text_channels if ch.permissions_for(guild.me).read_message_history]
        self.bot.totalScannedMessages = 0
        self.bot.currentBootScanned = 0
        self.totalChannels = len(textChannels)
        self.channelsCompleted = 0
        self.lastLogTime = time.perf_counter()
        
        channelQueue = asyncio.Queue()
        for channel in textChannels:
            channelQueue.put_nowait(channel)
            
        scanSemaphore = asyncio.Semaphore(config.maxParallelScans)
        workerCount = min(config.maxParallelScans, 32 if not isFirstScan else 16)
        
        async def worker():
            while not channelQueue.empty():
                try:
                    channel = channelQueue.get_nowait()
                except asyncio.QueueEmpty:
                    break
                async with scanSemaphore:
                    await self.scanChannelAdaptive(channel, latestMessagesMap.get(channel.id), isFirstScan)
                channelQueue.task_done()
                
        workers = [asyncio.create_task(worker()) for _ in range(workerCount)]
        await asyncio.gather(*workers)
        
        print()
        duration = time.perf_counter() - startTime
        self.bot.totalScanTimeStr = f"{duration:.2f}"
        self.bot.scanComplete = True
        
        # Sync the memory variable with actual DB count
        self.bot.totalScannedMessages = await asyncio.to_thread(self.bot.databaseManager.getTotalMessageCount)
        
        if isFirstScan:
            print(getLocaleString("scanCompleteFirstStr", count=self.bot.currentBootScanned, time=self.bot.totalScanTimeStr))
        else:
            print(getLocaleString("scanCompleteSyncStr", count=self.bot.currentBootScanned, time=self.bot.totalScanTimeStr))
            
        oldestDateText = getLocaleString("empty")
        if self.bot.globalOldestDate:
            oldestDateText = self.bot.globalOldestDate.astimezone().strftime("%d/%m/%Y %H:%M:%S")
        print(getLocaleString("oldestMsgStr", date=oldestDateText))
        print()
        
        # Trigger Garbage Collection to free memory occupied by transient startup data
        gc.collect()

    async def scanChannelDelta(self, channel, maxLocalId):
        pass

    async def scanChannelAdaptive(self, channel, maxLocalId, isFirstScan):
        localMsgCount = 0
        try:
            if isinstance(channel, discord.Thread) and channel.archived:
                return
                
            fetchedMessagesList = []
            
            if isFirstScan:
                async for msg in channel.history(limit=config.scanSize, oldest_first=False):
                    if msg.author.bot:
                        continue
                    localMsgCount += 1
                    fetchedMessagesList.append(msg)
            else:
                # Quét 100 tin nhắn mới nhất để phục vụ phát hiện sửa/xóa vùng hoạt náo gần đây
                async for msg in channel.history(limit=100, oldest_first=False):
                    if msg.author.bot:
                        continue
                    localMsgCount += 1
                    fetchedMessagesList.append(msg)
                    
                # Quét bám đuổi từ mốc cục bộ để lấy toàn bộ tin nhắn mới tinh gửi trong lúc bot offline
                if maxLocalId:
                    async for extraMsg in channel.history(after=discord.Object(id=maxLocalId), limit=200, oldest_first=True):
                        if extraMsg.author.bot:
                            continue
                        localMsgCount += 1
                        fetchedMessagesList.append(extraMsg)
                        
            if not fetchedMessagesList:
                return
                
            minFetchedId = min(m.id for m in fetchedMessagesList)
            
            localCacheMap = {}
            if maxLocalId:
                localCacheMap = await asyncio.to_thread(self.bot.databaseManager.getMessagesFromId, channel.id, minFetchedId)
                
            channelMessagesList = []
            dbUpdatesList = []
            offlineEditsCollected = []
            offlineDeletesCollected = []
            fetchedIdsSet = set()
            
            for msg in fetchedMessagesList:
                msgId = msg.id
                fetchedIdsSet.add(msgId)
                
                if maxLocalId and msgId <= maxLocalId:
                    dbRow = localCacheMap.get(msgId)
                    if dbRow:
                        dbAuthorId, dbAuthorName, dbAuthorAvatar, dbContent, dbAttachmentsRaw, dbReplyRefRaw, dbContentTypesRaw = dbRow
                        
                        if not msg.attachments and dbAttachmentsRaw == "[]" and dbContentTypesRaw == "[]":
                            currentAttachmentsRaw = "[]"
                            currentContentTypesRaw = "[]"
                        else:
                            currentAttachmentsRaw = json.dumps([att.url for att in msg.attachments])
                            currentContentTypesRaw = json.dumps([att.content_type for att in msg.attachments])
                            
                        if not msg.reference and dbReplyRefRaw == "{}":
                            currentReplyRefRaw = "{}"
                        else:
                            currentReplyRefRaw = json.dumps({"messageId": msg.reference.message_id, "channelId": msg.reference.channel_id} if msg.reference and msg.reference.message_id else {})
                            
                        isContentEqual = (dbContent == msg.content)
                        isAttachmentsEqual = (dbAttachmentsRaw == currentAttachmentsRaw)
                        isContentTypesEqual = (dbContentTypesRaw == currentContentTypesRaw)
                        isReplyEqual = (dbReplyRefRaw == currentReplyRefRaw)
                        
                        if not (isContentEqual and isAttachmentsEqual and isContentTypesEqual and isReplyEqual):
                            dbUpdatesList.append((msg.content, currentAttachmentsRaw, currentContentTypesRaw, msgId))
                            avatarUrl = msg.author.display_avatar.url if msg.author.display_avatar else ""
                            offlineEditsCollected.append({
                                "messageId": msgId,
                                "authorId": msg.author.id,
                                "authorName": msg.author.name,
                                "authorAvatar": avatarUrl,
                                "oldContent": dbContent,
                                "newContent": msg.content,
                                "contentTypes": currentContentTypesRaw
                            })
                else:
                    attachmentsList = [att.url for att in msg.attachments]
                    contentTypesList = [att.content_type for att in msg.attachments]
                    stickersList = [st.url for st in msg.stickers] if hasattr(msg, "stickers") else []
                    embedsData = [emb.to_dict() for emb in msg.embeds]
                    replyRef = {"messageId": msg.reference.message_id, "channelId": msg.reference.channel_id} if msg.reference and msg.reference.message_id else {}
                    
                    avatarUrl = msg.author.display_avatar.url if msg.author.display_avatar else ""
                    authorDisplayName = getattr(msg.author, "display_name", "")
                    authorGlobalName = getattr(msg.author, "global_name", "")
                    
                    msgTuple = (
                        msgId, msg.author.id, msg.author.name, authorDisplayName, authorGlobalName, avatarUrl,
                        channel.id, channel.name, getattr(channel, "parent", None).name if getattr(channel, "parent", None) else "",
                        msg.content, json.dumps(attachmentsList), json.dumps(stickersList), json.dumps(embedsData),
                        json.dumps(replyRef), msg.type.value if hasattr(msg.type, "value") else 0, json.dumps(contentTypesList)
                    )
                    channelMessagesList.append(msgTuple)
                    
                    if self.bot.globalOldestDate is None or msg.created_at < self.bot.globalOldestDate:
                        self.bot.globalOldestDate = msg.created_at
                        
            if channelMessagesList:
                await self.bot.databaseManager.enqueueAction("bulkSave", channelMessagesList)
            if dbUpdatesList:
                await self.bot.databaseManager.enqueueAction("bulkUpdateOffline", dbUpdatesList)
                self.bot.hourlyEditedMessages += len(dbUpdatesList)
                
            if maxLocalId and localCacheMap:
                for cId in localCacheMap.keys():
                    if cId not in fetchedIdsSet:
                        dbRow = localCacheMap[cId]
                        # dbRow: (authorId, authorName, authorAvatar, content, attachments, replyReference, contentTypes)
                        attachmentsList = []
                        try:
                            attachmentsList = json.loads(dbRow[4]) if dbRow[4] else []
                        except Exception:
                            pass
                        replyRef = {}
                        try:
                            replyRef = json.loads(dbRow[5]) if dbRow[5] else {}
                        except Exception:
                            pass
                        offlineDeletesCollected.append({
                            "messageId": cId,
                            "authorId": dbRow[0],
                            "authorName": dbRow[1],
                            "authorAvatar": dbRow[2],
                            "content": dbRow[3],
                            "attachments": attachmentsList,
                            "replyReference": replyRef,
                            "contentTypes": dbRow[6],
                            "isOffline": True
                        })
                        
            if offlineDeletesCollected:
                await self.bot.databaseManager.enqueueAction("bulkDelete", [d["messageId"] for d in offlineDeletesCollected])
                self.bot.hourlyDeletedMessages += len(offlineDeletesCollected)
                
            if offlineEditsCollected:
                for edit in offlineEditsCollected:
                    await self.bot.logDispatcher.enqueueLogAction({
                        "logType": "offlineEdit",
                        "channelId": channel.id,
                        "messageId": edit["messageId"],
                        "authorId": edit["authorId"],
                        "authorName": edit["authorName"],
                        "authorAvatar": edit["authorAvatar"],
                        "oldContent": edit["oldContent"],
                        "newContent": edit["newContent"],
                        "contentTypes": edit.get("contentTypes", "[]")
                    })
            if offlineDeletesCollected:
                for item in offlineDeletesCollected:
                    await self.bot.logDispatcher.enqueueLogAction({
                        "logType": "offlineDelete",
                        "channelId": channel.id,
                        "messageData": item
                    })
        except discord.Forbidden:
            pass
        except Exception as scanEx:
            logger.error(f"Error scanning channel history for {channel.id}: {str(scanEx)}")
        finally:
            self.channelsCompleted += 1
            self.bot.currentBootScanned += localMsgCount
            currentTime = time.perf_counter()
            if currentTime - self.lastLogTime >= 0.25 or self.channelsCompleted == self.totalChannels:
                self.lastLogTime = currentTime
                bootTimeDiff = getattr(self.bot, "bootTime", None)
                bootSeconds = (datetime.now() - bootTimeDiff).total_seconds() if bootTimeDiff else 1.0
                speed = self.bot.currentBootScanned / bootSeconds
                print(f"{getLocaleString('channelsProgressStr', completed=self.channelsCompleted, total=self.totalChannels, count=self.bot.currentBootScanned, speed=speed)}   ", end="", flush=True)