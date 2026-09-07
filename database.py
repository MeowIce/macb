import sqlite3
import asyncio
import logging
import time
import os
import json
import config

logger = logging.getLogger("MacbLogger")

class DatabaseManager:
    def __init__(self, bot, metricsTracker=None):
        self.bot = bot
        self.metricsTracker = metricsTracker
        self.writeConn = None
        self.sharedReadConn = None
        self.dbQueue = asyncio.Queue(maxsize=config.maxDbQueueSize)
        self.retryQueue = asyncio.Queue(maxsize=config.maxDbQueueSize)
        self.writeLock = asyncio.Lock()
        self.workerTask = None
        self.isReady = False
        self.isBatchActive = False
        self.lastBatchProgressAt = 0.0

    def getReadConnection(self):
        conn = sqlite3.connect(config.dbPath, check_same_thread=False)
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA busy_timeout=5000;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        return conn

    def _isConnectionHealthy(self, conn):
        if not conn:
            return False
        try:
            conn.execute("SELECT 1")
            return True
        except Exception:
            return False

    def initDatabase(self):
        try:
            if not self._isConnectionHealthy(self.writeConn):
                if self.writeConn:
                    try:
                        self.writeConn.close()
                    except Exception:
                        pass
                self.writeConn = sqlite3.connect(config.dbPath, check_same_thread=False)
                self.writeConn.execute("PRAGMA journal_mode=WAL;")
                self.writeConn.execute("PRAGMA busy_timeout=5000;")
                self.writeConn.execute("PRAGMA synchronous=NORMAL;")
            
            if not self._isConnectionHealthy(self.sharedReadConn):
                if self.sharedReadConn:
                    try:
                        self.sharedReadConn.close()
                    except Exception:
                        pass
                self.sharedReadConn = sqlite3.connect(config.dbPath, isolation_level=None, check_same_thread=False)
                self.sharedReadConn.execute("PRAGMA journal_mode=WAL;")
                self.sharedReadConn.execute("PRAGMA busy_timeout=5000;")
                self.sharedReadConn.execute("PRAGMA synchronous=NORMAL;")
                self.sharedReadConn.execute("PRAGMA mmap_size=268435456;")
                self.sharedReadConn.execute("PRAGMA cache_size=-64000;")
                self.sharedReadConn.execute("PRAGMA temp_store=MEMORY;")
            
            cursor = self.writeConn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS cachedMessages (
                    messageId INTEGER PRIMARY KEY,
                    authorId INTEGER,
                    authorName TEXT,
                    authorDisplayName TEXT,
                    authorGlobalName TEXT,
                    authorAvatar TEXT,
                    channelId INTEGER,
                    channelName TEXT,
                    parentChannelName TEXT,
                    content TEXT,
                    attachments TEXT,
                    stickers TEXT,
                    embeds TEXT,
                    replyReference TEXT,
                    messageType INTEGER,
                    contentTypes TEXT,
                    updatedAt REAL DEFAULT 0.0
                )
            """)
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_channel_msg ON cachedMessages(channelId, messageId DESC);")
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS dbDeadLetters (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    actionType TEXT,
                    actionData TEXT,
                    attempts INTEGER,
                    lastError TEXT,
                    createdAt REAL
                )
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS logDeadLetters (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    logType TEXT,
                    payload TEXT,
                    attempts INTEGER,
                    lastError TEXT,
                    createdAt REAL
                )
            """)
            try:
                cursor.execute("ALTER TABLE cachedMessages ADD COLUMN updatedAt REAL DEFAULT 0.0")
            except sqlite3.OperationalError:
                pass
            cursor.execute("SELECT messageId, updatedAt FROM cachedMessages LIMIT 1")
            cursor.execute("INSERT INTO dbDeadLetters (actionType, actionData, attempts, lastError, createdAt) VALUES ('__health_check__', '{}', 0, '', ?)", (time.time(),))
            self.writeConn.commit()
            cursor.execute("DELETE FROM dbDeadLetters WHERE actionType = '__health_check__'")
            self.writeConn.commit()
            cursor.close()
            self.isReady = True
            return True
        except Exception as ex:
            logger.critical(f"Database initialization failed: {str(ex)}")
            self.isReady = False
            return False

    def startWorker(self, loop):
        if self.isReady and (self.workerTask is None or self.workerTask.done()):
            self.workerTask = loop.create_task(self.dbWorker())

    async def enqueueAction(self, actionType, actionData):
        if not self.isReady:
            logger.warning(f"DB not ready; dropping action {actionType}")
            return False
        try:
            await asyncio.wait_for(self.dbQueue.put((actionType, actionData)), timeout=5.0)
            if self.metricsTracker:
                self.metricsTracker.updateDbQueue(self.dbQueue.qsize())
            return True
        except asyncio.TimeoutError:
            logger.error(f"DB queue full; dropping action {actionType}")
            if self.metricsTracker:
                self.metricsTracker.incrementQueueDropped()
            return False
        except Exception as ex:
            logger.error(f"Error enqueuing database action: {str(ex)}")
            return False

    async def _persistDeadLetter(self, actionType, actionData, attempts, errorMsg):
        persisted = False
        try:
            if self.writeConn:
                cursor = self.writeConn.cursor()
                dataStr = json.dumps(actionData) if not isinstance(actionData, str) else actionData
                cursor.execute(
                    "INSERT INTO dbDeadLetters (actionType, actionData, attempts, lastError, createdAt) VALUES (?, ?, ?, ?, ?)",
                    (actionType, dataStr, attempts, str(errorMsg), time.time())
                )
                self.writeConn.commit()
                cursor.close()
                persisted = True
                if self.metricsTracker:
                    self.metricsTracker.incrementDeadLettered()
                logger.critical(f"Persisted dead-letter DB action: {actionType} (attempts: {attempts})")
        except Exception as ex:
            logger.critical(f"Failed to persist dead-letter action to SQLite: {str(ex)}")

        if not persisted:
            try:
                spoolEntry = {
                    "actionType": actionType,
                    "actionData": actionData,
                    "attempts": attempts,
                    "lastError": str(errorMsg),
                    "createdAt": time.time()
                }
                with open("db_dead_letters_spool.jsonl", "a", encoding="utf-8") as f:
                    f.write(json.dumps(spoolEntry, ensure_ascii=False) + "\n")
                if self.metricsTracker:
                    self.metricsTracker.incrementDeadLettered()
                logger.critical(f"Spolled dead-letter DB action to disk: {actionType}")
            except Exception as spoolEx:
                if self.metricsTracker:
                    self.metricsTracker.incrementDeadLetterPersistFailed()
                logger.critical(f"Disk spool fallback failed: {str(spoolEx)}")

    async def dbWorker(self):
        while True:
            trackedItems = []
            try:
                try:
                    actionItem = self.retryQueue.get_nowait()
                    trackedItems.append(("retry", actionItem))
                except asyncio.QueueEmpty:
                    try:
                        actionItem = await asyncio.wait_for(self.dbQueue.get(), timeout=1.0)
                        trackedItems.append(("db", actionItem))
                    except asyncio.TimeoutError:
                        if getattr(self.bot, "watchdog", None):
                            self.bot.watchdog.feedHeartbeat("DatabaseWorker")
                        continue

                while len(trackedItems) < 100:
                    try:
                        trackedItems.append(("retry", self.retryQueue.get_nowait()))
                    except asyncio.QueueEmpty:
                        try:
                            trackedItems.append(("db", self.dbQueue.get_nowait()))
                        except asyncio.QueueEmpty:
                            break

                normalizedActions = []
                for qName, item in trackedItems:
                    if len(item) == 3:
                        normalizedActions.append((qName, item[0], item[1], item[2]))
                    else:
                        normalizedActions.append((qName, item[0], item[1], 0))

                self.isBatchActive = True
                self.lastBatchProgressAt = time.perf_counter()
                cursor = None
                async with self.writeLock:
                    try:
                        cursor = self.writeConn.cursor()
                        for index, (qName, actType, actData, attempts) in enumerate(normalizedActions, start=1):
                            if actType == "save":
                                cursor.execute("""
                                    INSERT INTO cachedMessages (
                                        messageId, authorId, authorName, authorDisplayName, authorGlobalName, authorAvatar,
                                        channelId, channelName, parentChannelName, content, attachments, stickers, embeds,
                                        replyReference, messageType, contentTypes, updatedAt
                                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                                    ON CONFLICT(messageId) DO UPDATE SET
                                        authorName=excluded.authorName,
                                        authorDisplayName=excluded.authorDisplayName,
                                        authorGlobalName=excluded.authorGlobalName,
                                        authorAvatar=excluded.authorAvatar,
                                        content=excluded.content,
                                        attachments=excluded.attachments,
                                        contentTypes=excluded.contentTypes,
                                        updatedAt=excluded.updatedAt
                                    WHERE excluded.updatedAt >= cachedMessages.updatedAt
                                """, actData)
                            elif actType == "bulkSave":
                                cursor.executemany("""
                                    INSERT INTO cachedMessages (
                                        messageId, authorId, authorName, authorDisplayName, authorGlobalName, authorAvatar,
                                        channelId, channelName, parentChannelName, content, attachments, stickers, embeds,
                                        replyReference, messageType, contentTypes, updatedAt
                                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                                    ON CONFLICT(messageId) DO UPDATE SET
                                        authorName=excluded.authorName,
                                        authorDisplayName=excluded.authorDisplayName,
                                        authorGlobalName=excluded.authorGlobalName,
                                        authorAvatar=excluded.authorAvatar,
                                        content=excluded.content,
                                        attachments=excluded.attachments,
                                        contentTypes=excluded.contentTypes,
                                        updatedAt=excluded.updatedAt
                                    WHERE excluded.updatedAt >= cachedMessages.updatedAt
                                """, actData)
                            elif actType == "bulkUpdateOffline":
                                cursor.executemany("UPDATE cachedMessages SET content=?, attachments=?, contentTypes=?, updatedAt=? WHERE messageId=? AND updatedAt <= ?", actData)
                            elif actType == "bulkDelete":
                                cursor.executemany("DELETE FROM cachedMessages WHERE messageId = ?", [(mId,) for mId in actData])
                            elif actType == "delete":
                                cursor.execute("DELETE FROM cachedMessages WHERE messageId = ?", (actData,))
                            elif actType == "updateFields":
                                mId, fields = actData
                                setClauses = ", ".join([f"{k} = ?" for k in fields.keys()])
                                values = list(fields.values()) + [mId]
                                if "updatedAt" in fields:
                                    values.append(fields["updatedAt"])
                                    cursor.execute(f"UPDATE cachedMessages SET {setClauses} WHERE messageId = ? AND updatedAt <= ?", values)
                                else:
                                    cursor.execute(f"UPDATE cachedMessages SET {setClauses} WHERE messageId = ?", values)
                            if index % 20 == 0:
                                self.lastBatchProgressAt = time.perf_counter()
                                if getattr(self.bot, "watchdog", None):
                                    self.bot.watchdog.feedHeartbeat("DatabaseWorker")
                        self.writeConn.commit()
                        self.lastBatchProgressAt = time.perf_counter()
                        if getattr(self.bot, "watchdog", None):
                            self.bot.watchdog.feedHeartbeat("DatabaseWorker")
                    except Exception as batchError:
                        logger.error(f"Error processing database batch: {str(batchError)}")
                        try:
                            self.writeConn.rollback()
                        except Exception:
                            pass
                        try:
                            if not self._isConnectionHealthy(self.writeConn):
                                self.writeConn.close()
                                self.writeConn = None
                                self.initDatabase()
                        except Exception:
                            pass
                        for qName, actType, actData, attempts in normalizedActions:
                            nextAttempts = attempts + 1
                            if nextAttempts <= 3:
                                try:
                                    await asyncio.wait_for(self.retryQueue.put((actType, actData, nextAttempts)), timeout=2.0)
                                    if self.metricsTracker:
                                        self.metricsTracker.incrementRetryEnqueued()
                                except Exception:
                                    await self._persistDeadLetter(actType, actData, nextAttempts, batchError)
                            else:
                                await self._persistDeadLetter(actType, actData, nextAttempts, batchError)
                    finally:
                        self.isBatchActive = False
                        if cursor:
                            try:
                                cursor.close()
                            except Exception:
                                pass
                        for qName, _ in trackedItems:
                            if qName == "retry":
                                self.retryQueue.task_done()
                            else:
                                self.dbQueue.task_done()
            except asyncio.CancelledError:
                break
            except Exception as workerError:
                logger.error(f"Error in database worker loop: {str(workerError)}")
                await asyncio.sleep(1)

    def getStartupMetadata(self):
        try:
            cursor = self.sharedReadConn.cursor()
            cursor.execute("SELECT MIN(messageId) FROM cachedMessages")
            oldestRow = cursor.fetchone()
            oldestId = oldestRow[0] if oldestRow else None
            cursor.execute("SELECT channelId, MAX(messageId) FROM cachedMessages GROUP BY channelId")
            rows = cursor.fetchall()
            cursor.close()
            return oldestId, {row[0]: row[1] for row in rows}
        except Exception as ex:
            logger.error(f"Error fetching startup metadata combo: {str(ex)}")
            return None, {}

    def getMessagesInIdRange(self, channelId, minId, maxId):
        try:
            cursor = self.sharedReadConn.cursor()
            query = "SELECT authorId, authorName, authorAvatar, content, attachments, replyReference, contentTypes, messageId FROM cachedMessages WHERE channelId = ? AND messageId >= ? AND messageId <= ?"
            cursor.execute(query, (channelId, minId, maxId))
            rows = cursor.fetchall()
            cursor.close()
            cacheMap = {}
            for row in rows:
                cacheMap[row[7]] = row[:7]
            return cacheMap
        except Exception as ex:
            logger.error(f"Error reading message range for channel {channelId}: {str(ex)}")
            return {}

    def getMessagesFromId(self, channelId, minId):
        try:
            cursor = self.sharedReadConn.cursor()
            query = "SELECT authorId, authorName, authorAvatar, content, attachments, replyReference, contentTypes, messageId FROM cachedMessages WHERE channelId = ? AND messageId >= ?"
            cursor.execute(query, (channelId, minId))
            rows = cursor.fetchall()
            cursor.close()
            cacheMap = {}
            for row in rows:
                cacheMap[row[7]] = row[:7]
            return cacheMap
        except Exception as ex:
            logger.error(f"Error reading messages from ID {minId} for channel {channelId}: {str(ex)}")
            return {}

    def getMessage(self, messageId):
        conn = None
        try:
            conn = self.getReadConnection()
            dbCursor = conn.cursor()
            dbCursor.execute("SELECT messageId, authorId, authorName, authorDisplayName, authorGlobalName, authorAvatar, channelId, channelName, parentChannelName, content, attachments, stickers, embeds, replyReference, messageType, contentTypes FROM cachedMessages WHERE messageId = ?", (messageId,))
            dataRow = dbCursor.fetchone()
            dbCursor.close()
            return dataRow
        except Exception as executionException:
            logger.error(f"Error reading message {messageId}: {str(executionException)}")
            return None
        finally:
            if conn:
                conn.close()

    def getMessagesBulk(self, messageIds):
        if not messageIds:
            return {}
        conn = None
        try:
            conn = self.getReadConnection()
            dbCursor = conn.cursor()
            result = {}
            chunkSize = 900
            idList = list(messageIds)
            for i in range(0, len(idList), chunkSize):
                chunk = idList[i:i + chunkSize]
                sqlPlaceholders = ", ".join(["?"] * len(chunk))
                sqlQuery = f"SELECT messageId, authorId, authorName, authorDisplayName, authorGlobalName, authorAvatar, channelId, channelName, parentChannelName, content, attachments, stickers, embeds, replyReference, messageType, contentTypes FROM cachedMessages WHERE messageId IN ({sqlPlaceholders})"
                dbCursor.execute(sqlQuery, chunk)
                for row in dbCursor.fetchall():
                    result[row[0]] = row
            dbCursor.close()
            return result
        except Exception as executionException:
            logger.error(f"Error bulk reading messages: {str(executionException)}")
            return {}
        finally:
            if conn:
                conn.close()

    def getTotalMessageCount(self):
        try:
            cursor = self.sharedReadConn.cursor()
            cursor.execute("SELECT COUNT(*) FROM cachedMessages")
            row = cursor.fetchone()
            cursor.close()
            return row[0] if row else 0
        except Exception as ex:
            logger.error(f"Error counting total messages: {str(ex)}")
            return 0

    async def flushAndClose(self):
        try:
            await asyncio.wait_for(self.dbQueue.join(), timeout=30.0)
        except asyncio.TimeoutError:
            logger.warning(f"DB queue drain timed out; {self.dbQueue.qsize()} actions unprocessed")
        if self.workerTask:
            self.workerTask.cancel()
            try:
                await self.workerTask
            except asyncio.CancelledError:
                pass
        if self.sharedReadConn:
            self.sharedReadConn.close()
        if self.writeConn:
            self.writeConn.close()
