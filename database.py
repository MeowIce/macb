import sqlite3
import asyncio
import logging
import time
import os
import config

logger = logging.getLogger("MacbLogger")

class DatabaseManager:
    def __init__(self, bot, metricsTracker=None):
        self.bot = bot
        self.metricsTracker = metricsTracker
        self.writeConn = None
        self.sharedReadConn = None
        self.dbQueue = asyncio.Queue(maxsize=config.maxDbQueueSize)
        self.workerTask = None
        self.isReady = False

    def getReadConnection(self):
        conn = sqlite3.connect(config.dbPath, check_same_thread=False)
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA busy_timeout=5000;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        return conn

    def initDatabase(self):
        try:
            self.writeConn = sqlite3.connect(config.dbPath, check_same_thread=False)
            self.writeConn.execute("PRAGMA journal_mode=WAL;")
            self.writeConn.execute("PRAGMA busy_timeout=5000;")
            self.writeConn.execute("PRAGMA synchronous=NORMAL;")
            
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
                    contentTypes TEXT
                )
            """)
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_channel_msg ON cachedMessages(channelId, messageId DESC);")
            self.writeConn.commit()
            cursor.close()
            self.isReady = True
            return True
        except Exception as ex:
            logger.critical(f"Database initialization failed: {str(ex)}")
            self.isReady = False
            return False

    def startWorker(self, loop):
        if self.isReady and self.workerTask is None:
            self.workerTask = loop.create_task(self.dbWorker())

    async def enqueueAction(self, actionType, actionData):
        try:
            self.dbQueue.put_nowait((actionType, actionData))
        except Exception as ex:
            logger.error(f"Error enqueuing database action: {str(ex)}")

    async def dbWorker(self):
        while True:
            try:
                if self.bot and hasattr(self.bot, "watchdog"):
                    self.bot.watchdog.feedHeartbeat("DatabaseWorker")
                try:
                    action = await asyncio.wait_for(self.dbQueue.get(), timeout=1.0)
                except asyncio.TimeoutError:
                    continue
                
                actions = [action]
                while not self.dbQueue.empty() and len(actions) < 100:
                    try:
                        nextAction = self.dbQueue.get_nowait()
                        actions.append(nextAction)
                    except asyncio.QueueEmpty:
                        break
                        
                cursor = self.writeConn.cursor()
                for actType, actData in actions:
                    if actType == "save":
                        cursor.execute("""
                            INSERT INTO cachedMessages (
                                messageId, authorId, authorName, authorDisplayName, authorGlobalName, authorAvatar,
                                channelId, channelName, parentChannelName, content, attachments, stickers, embeds,
                                replyReference, messageType, contentTypes
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            ON CONFLICT(messageId) DO UPDATE SET
                                authorName=excluded.authorName,
                                authorDisplayName=excluded.authorDisplayName,
                                authorGlobalName=excluded.authorGlobalName,
                                authorAvatar=excluded.authorAvatar,
                                content=excluded.content,
                                attachments=excluded.attachments,
                                contentTypes=excluded.contentTypes
                        """, actData)
                    elif actType == "bulkSave":
                        cursor.executemany("""
                            INSERT INTO cachedMessages (
                                messageId, authorId, authorName, authorDisplayName, authorGlobalName, authorAvatar,
                                channelId, channelName, parentChannelName, content, attachments, stickers, embeds,
                                replyReference, messageType, contentTypes
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            ON CONFLICT(messageId) DO UPDATE SET
                                authorName=excluded.authorName,
                                authorDisplayName=excluded.authorDisplayName,
                                authorGlobalName=excluded.authorGlobalName,
                                authorAvatar=excluded.authorAvatar,
                                content=excluded.content,
                                attachments=excluded.attachments,
                                contentTypes=excluded.contentTypes
                        """, actData)
                    elif actType == "bulkUpdateOffline":
                        cursor.executemany("UPDATE cachedMessages SET content=?, attachments=?, contentTypes=? WHERE messageId=?", actData)
                    elif actType == "bulkDelete":
                        cursor.executemany("DELETE FROM cachedMessages WHERE messageId = ?", [(mId,) for mId in actData])
                    elif actType == "delete":
                        cursor.execute("DELETE FROM cachedMessages WHERE messageId = ?", (actData,))
                    elif actType == "updateFields":
                        mId, fields = actData
                        setClauses = ", ".join([f"{k} = ?" for k in fields.keys()])
                        values = list(fields.values()) + [mId]
                        cursor.execute(f"UPDATE cachedMessages SET {setClauses} WHERE messageId = ?", values)
                        
                self.writeConn.commit()
                cursor.close()
                
                for _ in range(len(actions)):
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
            sqlPlaceholders = ", ".join(["?"] * len(messageIds))
            sqlQuery = f"SELECT messageId, authorId, authorName, authorDisplayName, authorGlobalName, authorAvatar, channelId, channelName, parentChannelName, content, attachments, stickers, embeds, replyReference, messageType, contentTypes FROM cachedMessages WHERE messageId IN ({sqlPlaceholders})"
            dbCursor.execute(sqlQuery, messageIds)
            dataRows = dbCursor.fetchall()
            dbCursor.close()
            return {row[0]: row for row in dataRows}
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
        await self.dbQueue.join()
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
