# MeowIce's Advanced Chatlogging Bot
# Copyright (c) 2026 MeowIce

# Permission is granted to use, modify, and distribute this software for non-commercial purposes only.
# Selling this software or any derivative works is prohibited without explicit written permission.
# Removing or altering author credits is prohibited.

import discord
from discord import app_commands
from discord.ext import commands
import io
import aiohttp
from datetime import datetime
import sqlite3
import asyncio
import os
import psutil
# ==== CONFIG ====
botToken = "YOUR_BOT_TOKEN"
targetGuildId = 708718758616760339
logChannelId = 879961838043922432
# =================

intents = discord.Intents.default()
intents.message_content = True
intents.guilds = True
intents.messages = True
intents.members = True

bot = commands.Bot(command_prefix="!", intents=intents)

bot.totalScannedMessages = 0
bot.globalOldestDate = None
bot.bootTime = None

def initDatabase():
    try:
        conn = sqlite3.connect("bot_log.db")
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS cached_messages (
                messageId INTEGER PRIMARY KEY,
                authorId INTEGER,
                authorName TEXT,
                authorAvatar TEXT,
                channelId INTEGER,
                content TEXT,
                attachmentUrl TEXT
            )
        """)
        conn.commit()
        conn.close()
        return True
    except Exception:
        return False

def saveToDatabase(messageId, authorId, authorName, authorAvatar, channelId, content, attachmentUrl):
    conn = sqlite3.connect("bot_log.db")
    cursor = conn.cursor()
    cursor.execute("""
        INSERT OR REPLACE INTO cached_messages VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (messageId, authorId, authorName, authorAvatar, channelId, content, attachmentUrl))
    conn.commit()
    conn.close()

def bulkSaveToDatabase(messagesList):
    if not messagesList:
        return
    conn = sqlite3.connect("bot_log.db")
    cursor = conn.cursor()
    cursor.execute("PRAGMA journal_mode=WAL;")
    cursor.execute("PRAGMA synchronous=OFF;")
    cursor.execute("PRAGMA cache_size=-64000;")
    cursor.executemany("""
        INSERT OR REPLACE INTO cached_messages VALUES (?, ?, ?, ?, ?, ?, ?)
    """, messagesList)
    conn.commit()
    conn.close()

def getChannelMessagesFromDatabase(channelId):
    conn = sqlite3.connect("bot_log.db")
    cursor = conn.cursor()
    cursor.execute("""
        SELECT messageId, authorId, authorName, authorAvatar, content, attachmentUrl 
        FROM cached_messages WHERE channelId = ?
    """, (channelId,))
    rows = cursor.fetchall()
    conn.close()
    return {row[0]: row[1:] for row in rows}

def bulkDeleteFromDatabase(messageIds):
    if not messageIds:
        return
    conn = sqlite3.connect("bot_log.db")
    cursor = conn.cursor()
    cursor.executemany("DELETE FROM cached_messages WHERE messageId = ?", [(mId,) for mId in messageIds])
    conn.commit()
    conn.close()

def getFromDatabase(messageId):
    conn = sqlite3.connect("bot_log.db")
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM cached_messages WHERE messageId = ?", (messageId,))
    row = cursor.fetchone()
    conn.close()
    return row

def updateDatabaseContent(messageId, newContent):
    conn = sqlite3.connect("bot_log.db")
    cursor = conn.cursor()
    cursor.execute("UPDATE cached_messages SET content = ? WHERE messageId = ?", (newContent, messageId))
    conn.commit()
    conn.close()

async def mediaDownloader(attachmentUrl):
    maxRetries = 3
    async with aiohttp.ClientSession() as session:
        for attempt in range(maxRetries):
            try:
                async with session.get(attachmentUrl) as response:
                    if response.status == 200:
                        imageData = await response.read()
                        filename = attachmentUrl.split("/")[-1].split("?")[0]
                        if not filename:
                            filename = "file.dat"
                        return discord.File(io.BytesIO(imageData), filename=filename)
                    elif response.status == 429:
                        retryAfter = float(response.headers.get("Retry-After", 2))
                        await asyncio.sleep(retryAfter)
                    elif response.status in [500, 502, 503, 504]:
                        await asyncio.sleep(2 ** attempt)
                    else:
                        break
            except Exception:
                await asyncio.sleep(2 ** attempt)
    return None

async def scanChannel(textChannel, semaphore):
    async with semaphore:
        messageCount = 0
        oldestMessageDate = None
        oldestMessageId = None
        channelMessagesList = []
        offlineEditsList = []
        offlineDeletesList = []
        fetchedIds = set()
        
        dbMessagesDict = await asyncio.to_thread(getChannelMessagesFromDatabase, textChannel.id)
        
        try:
            async for messageItem in textChannel.history(limit=1000):
                if messageItem.author.bot:
                    continue
                url = ""
                if messageItem.attachments:
                    url = messageItem.attachments[0].url
                elif messageItem.stickers:
                    url = messageItem.stickers[0].url
                avatarUrl = messageItem.author.display_avatar.url if messageItem.author.display_avatar else ""
                
                fetchedIds.add(messageItem.id)
                oldestMessageDate = messageItem.created_at
                oldestMessageId = messageItem.id
                
                if messageItem.id in dbMessagesDict:
                    oldAuthorId, oldAuthorName, oldAuthorAvatar, oldContent, oldUrl = dbMessagesDict[messageItem.id]
                    
                    oldText = str(oldContent or "").replace("\r\n", "\n")
                    newText = str(messageItem.content or "").replace("\r\n", "\n")
                    
                    if oldText != newText:
                        offlineEditsList.append({
                            "messageId": messageItem.id,
                            "authorId": oldAuthorId,
                            "authorName": oldAuthorName,
                            "authorAvatar": oldAuthorAvatar,
                            "channelId": textChannel.id,
                            "oldContent": oldContent,
                            "newContent": messageItem.content,
                            "oldUrl": oldUrl,
                            "newUrl": url,
                            "timestamp": messageItem.created_at
                        })
                
                channelMessagesList.append((
                    messageItem.id, 
                    messageItem.author.id, 
                    messageItem.author.name, 
                    avatarUrl, 
                    messageItem.channel.id, 
                    messageItem.content, 
                    url
                ))
                messageCount += 1
                
            if oldestMessageId:
                for dbId, dbData in dbMessagesDict.items():
                    if dbId not in fetchedIds and dbId > oldestMessageId:
                        offlineDeletesList.append({
                            "messageId": dbId,
                            "authorId": dbData[0],
                            "authorName": dbData[1],
                            "authorAvatar": dbData[2],
                            "channelId": textChannel.id,
                            "content": dbData[3],
                            "attachmentUrl": dbData[4]
                        })
                        
        except Exception:
            pass
        return messageCount, oldestMessageDate, channelMessagesList, offlineEditsList, offlineDeletesList

async def backgroundScanTask(targetGuild):
    print("Đang nạp dữ liệu tin nhắn vào database...")
    scanSemaphore = asyncio.Semaphore(10)
    channelTasks = [scanChannel(textChannel, scanSemaphore) for textChannel in targetGuild.text_channels]
    scanResults = await asyncio.gather(*channelTasks)
    print()
    
    totalScannedMessages = 0
    globalOldestDate = None
    allMessagesList = []
    totalOfflineEdits = []
    totalOfflineDeletes = []
    
    for messageCount, oldestMessageDate, channelMessagesList, offlineEditsList, offlineDeletesList in scanResults:
        totalScannedMessages += messageCount
        if channelMessagesList:
            allMessagesList.extend(channelMessagesList)
        if offlineEditsList:
            totalOfflineEdits.extend(offlineEditsList)
        if offlineDeletesList:
            totalOfflineDeletes.extend(offlineDeletesList)
        if oldestMessageDate:
            if globalOldestDate is None or oldestMessageDate < globalOldestDate:
                globalOldestDate = oldestMessageDate
                
    if allMessagesList:
        await asyncio.to_thread(bulkSaveToDatabase, allMessagesList)
        
    logChannel = bot.get_channel(logChannelId)
    if logChannel:
        for editItem in totalOfflineEdits:
            createdUtc = discord.utils.snowflake_time(editItem["messageId"])
            sentTimeStr = createdUtc.astimezone().strftime("%d/%m/%Y %H:%M:%S")
            
            embedEdit = discord.Embed(
                title="Tin nhắn bị chỉnh sửa (Offline)",
                color=discord.Color.dark_orange(),
                description=f"Người gửi: <@{editItem['authorId']}> ({editItem['authorId']})\nKênh: <#{editItem['channelId']}>\nThời gian gửi: {sentTimeStr}\nID Tin nhắn: {editItem['messageId']}"
            )
            embedEdit.set_author(name=editItem["authorName"], icon_url=editItem["authorAvatar"])
            embedEdit.add_field(name="Trước khi sửa", value=f"```\n{editItem['oldContent'] if editItem['oldContent'] else 'Không có nội dung chữ'}\n```", inline=False)
            embedEdit.add_field(name="Sau khi sửa", value=f"```\n{editItem['newContent'] if editItem['newContent'] else 'Không có nội dung chữ'}\n```", inline=False)
            embedEdit.set_footer(text=f"Phát hiện lúc: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
            await logChannel.send(embed=embedEdit)
            await asyncio.sleep(0.2)
            
        deleteIdsToClear = []
        for deleteItem in totalOfflineDeletes:
            deleteIdsToClear.append(deleteItem["messageId"])
            createdUtc = discord.utils.snowflake_time(deleteItem["messageId"])
            sentTimeStr = createdUtc.astimezone().strftime("%d/%m/%Y %H:%M:%S")
            
            embedDelete = discord.Embed(
                title="Tin nhắn bị xoá (Offline)",
                color=discord.Color.dark_red(),
                description=f"Người gửi: <@{deleteItem['authorId']}> ({deleteItem['authorId']})\nKênh: <#{deleteItem['channelId']}>\nThời gian gửi: {sentTimeStr}\nID Tin nhắn: {deleteItem['messageId']}"
            )
            embedDelete.set_author(name=deleteItem["authorName"], icon_url=deleteItem["authorAvatar"])
            embedDelete.add_field(name="Nội dung trước khi xoá", value=f"```\n{deleteItem['content'] if deleteItem['content'] else 'Không có nội dung chữ'}\n```", inline=False)
            embedDelete.set_footer(text=f"Phát hiện lúc: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
            await logChannel.send(embed=embedDelete)
            await asyncio.sleep(0.2)
            
        if deleteIdsToClear:
            await asyncio.to_thread(bulkDeleteFromDatabase, deleteIdsToClear)
                
    bot.totalScannedMessages = totalScannedMessages
    bot.globalOldestDate = globalOldestDate
                
    print(f"Hoàn tất nạp {totalScannedMessages} tin nhắn")
    
    process = psutil.Process(os.getpid())
    ramBytes = process.memory_info().rss
    ramMB = ramBytes / 1048576
    print(f"Mức sử dụng RAM: {ramMB:.2f} MB")
    
    if globalOldestDate:
        formattedDate = globalOldestDate.strftime("%d/%m/%Y %H:%M:%S")
        print(f"Thời gian tin nhắn cũ nhất đã quét: {formattedDate}")
    else:
        print("Thời gian tin nhắn cũ nhất đã quét: Không có dữ liệu")
    print()
    print("==========================")

@bot.tree.command(name="getstats", description="Lấy trạng thái lưu trữ tin nhắn.", guild=discord.Object(id=targetGuildId))
@app_commands.default_permissions(administrator=True)
async def getstats(interaction: discord.Interaction):
    dbSuccess = initDatabase()
    dbStatusStr = "Kết nối thành công" if dbSuccess else "Kết nối thất bại"
    
    fileSizeKB = 0.0
    if os.path.exists("bot_log.db"):
        fileSizeByte = os.path.getsize("bot_log.db")
        fileSizeKB = fileSizeByte / 1024

    totalChannels = 0
    totalMembers = 0
    targetGuild = bot.get_guild(targetGuildId)
    if targetGuild:
        totalChannels = len(targetGuild.text_channels)
        totalMembers = targetGuild.member_count

    process = psutil.Process(os.getpid())
    ramBytes = process.memory_info().rss
    ramMB = ramBytes / 1048576

    formattedDate = "Không có dữ liệu"
    if bot.globalOldestDate:
        formattedDate = bot.globalOldestDate.strftime("%d/%m/%Y %H:%M:%S")

    uptimeDelta = datetime.now() - bot.bootTime
    days = uptimeDelta.days
    hours, remainder = divmod(uptimeDelta.seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    uptimeStr = f"{days} ngày, {hours} giờ, {minutes} phút, {seconds} giây"

    embedStats = discord.Embed(
        title="Báo cáo trạng thái hệ thống",
        color=discord.Color.blue(),
        timestamp=datetime.now()
    )
    
    embedStats.add_field(name="Thống kê Chung", value=f"**Uptime:** {uptimeStr}", inline=False)
    embedStats.add_field(name="Thông tin Bot", value=f"**Username:** {bot.user.name}\n**ID:** {bot.user.id}", inline=False)
    
    dbCombinedValue = (
        f"**Trạng thái:** {dbStatusStr}\n"
        f"**Kích thước tệp:** {fileSizeKB:.2f} KB\n"
        f"**Tin nhắn đã nạp:** {bot.totalScannedMessages}\n"
        f"**Thời gian cũ nhất:** {formattedDate}"
    )
    embedStats.add_field(name="Cơ sở dữ liệu", value=dbCombinedValue, inline=False)
    
    embedStats.add_field(name="Thống kê Máy chủ", value=f"**Kênh giám sát:** {totalChannels}\n**Tổng số người:** {totalMembers}", inline=False)
    embedStats.add_field(name="Tài nguyên", value=f"**Mức sử dụng RAM:** {ramMB:.2f} MB", inline=False)

    await interaction.response.send_message(embed=embedStats)

@bot.event
async def on_ready():
    if bot.bootTime is None:
        bot.bootTime = datetime.now()
        
    print(f"Bot Username: {bot.user.name}")
    print(f"Bot ID: {bot.user.id}")
    print()
    
    dbSuccess = initDatabase()
    if dbSuccess:
        print("Trạng thái Database: Kết nối thành công")
        if os.path.exists("bot_log.db"):
            fileSizeByte = os.path.getsize("bot_log.db")
            fileSizeKB = fileSizeByte / 1024
            print(f"Kích thước tệp Database: {fileSizeKB:.2f} KB")
    else:
        print("Trạng thái Database: Kết nối thất bại")
    print()

    targetGuild = bot.get_guild(targetGuildId)
    if targetGuild:
        totalChannels = len(targetGuild.text_channels)
        totalMembers = targetGuild.member_count
        print(f"Số lượng kênh đang giám sát: {totalChannels}")
        print(f"Tổng số người trong máy chủ: {totalMembers}")
        print()
        
        asyncio.create_task(backgroundScanTask(targetGuild))
        
        try:
            await bot.tree.sync(guild=discord.Object(id=targetGuildId))
            print("Đã đồng bộ Slash Command thành công")
        except Exception as e:
            print(f"Lỗi đồng bộ Slash Command: {e}")
    print()

@bot.event
async def on_message(message):
    if message.guild is None or message.guild.id != targetGuildId:
        return
    if message.author.bot:
        return
    url = ""
    if message.attachments:
        url = message.attachments[0].url
    elif message.stickers:
        url = message.stickers[0].url
    avatarUrl = message.author.display_avatar.url if message.author.display_avatar else ""
    saveToDatabase(message.id, message.author.id, message.author.name, avatarUrl, message.channel.id, message.content, url)
    await bot.process_commands(message)

@bot.event
async def on_raw_message_delete(payload):
    if payload.guild_id != targetGuildId:
        return
        
    messageData = getFromDatabase(payload.message_id)
    if not messageData:
        return
        
    logChannel = bot.get_channel(logChannelId)
    if not logChannel:
        return
        
    authorId = messageData[1]
    authorName = messageData[2]
    authorAvatar = messageData[3]
    channelId = messageData[4]
    content = messageData[5]
    attachmentUrl = messageData[6]
    
    createdUtc = discord.utils.snowflake_time(payload.message_id)
    createdLocal = createdUtc.astimezone()
    sentTimeStr = createdLocal.strftime("%d/%m/%Y %H:%M:%S")
    
    messageType = "Văn bản"
    isSticker = False
    if attachmentUrl:
        if "/stickers/" in attachmentUrl:
            messageType = "Sticker"
            isSticker = True
        else:
            filenameLower = attachmentUrl.split("/")[-1].split("?")[0].lower()
            if any(filenameLower.endswith(ext) for ext in [".png", ".jpg", ".jpeg", ".webp"]):
                messageType = "Media (Hình ảnh)"
            elif filenameLower.endswith(".gif"):
                messageType = "Media (Ảnh động GIF)"
            elif any(filenameLower.endswith(ext) for ext in [".mp4", ".mov", ".webm"]):
                messageType = "Media (Video)"
            else:
                messageType = "Media (Tệp đính kèm)"

    embedMessage = discord.Embed(
        title="Tin nhắn bị xoá",
        color=discord.Color.red(),
        description=f"Người gửi: <@{authorId}> ({authorId})\nKênh: <#{channelId}>\nThời gian gửi: {sentTimeStr}\nID Tin nhắn: {payload.message_id}"
    )
    embedMessage.set_author(name=authorName, icon_url=authorAvatar)
    
    currentTime = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    embedMessage.set_footer(text=f"Thời gian: {currentTime}")
    
    embedMessage.add_field(name="Loại tin nhắn", value=messageType, inline=False)
    
    if content:
        displayContent = content
    else:
        displayContent = "[Tin nhắn Sticker]" if isSticker else "[Tin nhắn không chứa nội dung chữ]"
        
    embedMessage.add_field(name="Nội dung", value=f"```\n{displayContent}\n```", inline=False)
        
    logFile = None
    if attachmentUrl and not isSticker:
        logFile = await mediaDownloader(attachmentUrl)
        if logFile:
            filenameCheck = logFile.filename.lower()
            isImage = any(filenameCheck.endswith(ext) for ext in [".png", ".jpg", ".jpeg", ".gif", ".webp"])
            if isImage:
                embedMessage.set_image(url=f"attachment://{logFile.filename}")
            
    if logFile:
        await logChannel.send(embed=embedMessage, file=logFile)
    else:
        await logChannel.send(embed=embedMessage)

@bot.event
async def on_raw_message_edit(payload):
    if payload.guild_id != targetGuildId:
        return
    if "content" not in payload.data:
        return
        
    messageId = payload.message_id
    oldData = getFromDatabase(messageId)
    if not oldData:
        return
        
    newContent = payload.data["content"]
    oldContent = oldData[5]
    if oldContent == newContent:
        return
        
    authorId = oldData[1]
    authorName = oldData[2]
    authorAvatar = oldData[3]
    channelId = oldData[4]
    attachmentUrl = oldData[6]
    
    createdUtc = discord.utils.snowflake_time(messageId)
    createdLocal = createdUtc.astimezone()
    sentTimeStr = createdLocal.strftime("%d/%m/%Y %H:%M:%S")
    
    messageType = "Văn bản"
    if attachmentUrl:
        if "/stickers/" in attachmentUrl:
            messageType = "Văn bản & Sticker" if (newContent or oldContent) else "Sticker"
        else:
            filenameLower = attachmentUrl.split("/")[-1].split("?")[0].lower()
            if any(filenameLower.endswith(ext) for ext in [".png", ".jpg", ".jpeg", ".webp"]):
                messageType = "Văn bản & Media (Hình ảnh)"
            elif filenameLower.endswith(".gif"):
                messageType = "Văn bản & Media (Ảnh động GIF)"
            elif any(filenameLower.endswith(ext) for ext in [".mp4", ".mov", ".webm"]):
                messageType = "Văn bản & Media (Video)"
            else:
                messageType = "Văn bản & Media (Tệp đính kèm)"
            
    updateDatabaseContent(messageId, newContent)
    
    logChannel = bot.get_channel(logChannelId)
    if not logChannel:
        return
        
    embedMessage = discord.Embed(
        title="Tin nhắn bị chỉnh sửa",
        color=discord.Color.orange(),
        description=f"Người gửi: <@{authorId}> ({authorId})\nKênh: <#{channelId}>\nThời gian gửi: {sentTimeStr}\nID Tin nhắn: {messageId}"
    )
    embedMessage.set_author(name=authorName, icon_url=authorAvatar)
    
    currentTime = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    embedMessage.set_footer(text=f"Thời gian: {currentTime}")
    
    embedMessage.add_field(name="Loại tin nhắn", value=messageType, inline=False)
    
    beforeContent = oldContent if oldContent else "Không có nội dung chữ"
    afterContent = newContent if newContent else "Không có nội dung chữ"
    
    embedMessage.add_field(name="Trước khi sửa", value=f"```\n{beforeContent}\n```", inline=False)
    embedMessage.add_field(name="Sau khi sửa", value=f"```\n{afterContent}\n```", inline=False)
    
    await logChannel.send(embed=embedMessage)

bot.run(botToken, log_handler=None)