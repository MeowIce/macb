import aiohttp
import asyncio
import os
import time
import logging
import discord
import io
import config

logger = logging.getLogger("MacbLogger")

class MediaManager:
    def __init__(self):
        self.session = None

    def initialize(self):
        self.session = aiohttp.ClientSession()

    async def close(self):
        if self.session:
            await self.session.close()

    async def downloadMedia(self, messageId, url):
        if not url or not url.startswith("http"):
            return url
        try:
            filename = url.split("/")[-1].split("?")[0]
            if not filename:
                filename = "file.dat"
            localPath = f"{config.cacheDir}/{messageId}_{filename}"
            
            if os.path.exists(localPath):
                return localPath

            async with self.session.get(url, timeout=30) as response:
                if response.status == 200:
                    with open(localPath, "wb") as localFile:
                        async for chunk in response.content.iter_chunked(65536):
                            localFile.write(chunk)
                    return localPath
                elif response.status == 429:
                    retryAfter = float(response.headers.get("Retry-After", 2))
                    await asyncio.sleep(retryAfter)
                    return await self.downloadMedia(messageId, url)
        except Exception as downloadError:
            logger.error(f"Error downloading media from {url}: {str(downloadError)}")
        return url

    async def convertToDiscordFile(self, pathOrUrl):
        if not pathOrUrl:
            return None
        try:
            if os.path.exists(pathOrUrl):
                filename = pathOrUrl.split("/")[-1]
                return discord.File(pathOrUrl, filename=filename)
            
            if pathOrUrl.startswith("http"):
                maxRetries = 3
                for attempt in range(maxRetries):
                    try:
                        async with self.session.get(pathOrUrl, timeout=15) as response:
                            if response.status == 200:
                                fileData = await response.read()
                                filename = pathOrUrl.split("/")[-1].split("?")[0]
                                if not filename:
                                    filename = "file.dat"
                                return discord.File(io.BytesIO(fileData), filename=filename)
                            elif response.status == 429:
                                await asyncio.sleep(float(response.headers.get("Retry-After", 2)))
                            else:
                                await asyncio.sleep(2 ** attempt)
                    except Exception:
                        await asyncio.sleep(2 ** attempt)
        except Exception as conversionError:
            logger.error(f"Error converting to discord file: {str(conversionError)}")
        return None

    async def cleanCacheTask(self):
        while True:
            try:
                await asyncio.sleep(86400)
                if not os.path.exists(config.cacheDir):
                    continue

                currentTime = time.time()
                maxAgeSecs = config.maxCacheAgeDays * 86400
                totalSize = 0
                filesList = []

                for entry in os.scandir(config.cacheDir):
                    if entry.is_file():
                        fileStat = entry.stat()
                        fileAge = currentTime - fileStat.st_mtime
                        if fileAge > maxAgeSecs:
                            try:
                                os.remove(entry.path)
                            except OSError:
                                pass
                        else:
                            totalSize += fileStat.st_size
                            filesList.append((fileStat.st_mtime, entry.path, fileStat.st_size))

                maxSizeBytes = config.maxCacheSizeMb * 1024 * 1024
                if totalSize > maxSizeBytes:
                    filesList.sort(key=lambda x: x[0])
                    for mtime, path, size in filesList:
                        try:
                            os.remove(path)
                            totalSize -= size
                            if totalSize <= maxSizeBytes:
                                break
                        except OSError:
                            pass
            except Exception as cacheError:
                logger.error(f"Error cleaning cache directory: {str(cacheError)}")