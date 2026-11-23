import asyncio
import logging

import aiohttp

from .parser import HTMLParser

logger = logging.getLogger(__name__)


class AsyncCrawler:
    def __init__(self, max_concurrent: int = 10) -> None:
        self.max_concurrent = max_concurrent
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.session: aiohttp.ClientSession | None = None
        self.parser = HTMLParser()

    def _ensure_session(self) -> aiohttp.ClientSession:
        if self.session is None or self.session.closed:
            timeout = aiohttp.ClientTimeout(total=30, connect=10, sock_read=20)
            connector = aiohttp.TCPConnector(limit=self.max_concurrent, ttl_dns_cache=300)
            self.session = aiohttp.ClientSession(timeout=timeout, connector=connector)
        return self.session

    async def __aenter__(self) -> "AsyncCrawler":
        self._ensure_session()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()

    async def close(self) -> None:
        if self.session is not None and not self.session.closed:
            await self.session.close()
        self.session = None

    async def fetch_url(self, url: str) -> str:
        session = self._ensure_session()
        async with self.semaphore:
            logger.info("start %s", url)
            try:
                async with session.get(url) as response:
                    response.raise_for_status()
                    text = await response.text()
                    logger.info("ok %s (%s bytes)", url, len(text))
                    return text
            except asyncio.TimeoutError:
                logger.warning("timeout %s", url)
                return ""
            except aiohttp.ClientResponseError as e:
                logger.warning("http %s %s %s", url, e.status, e.message)
                return ""
            except aiohttp.ClientError as e:
                logger.warning("network %s %s: %s", url, type(e).__name__, e)
                return ""

    async def fetch_urls(self, urls: list[str]) -> dict[str, str]:
        tasks = []
        for url in urls:
            tasks.append(self.fetch_url(url))

        results = await asyncio.gather(*tasks, return_exceptions=True)

        out: dict[str, str] = {}
        for i, url in enumerate(urls):
            result = results[i]
            if isinstance(result, Exception):
                logger.warning("unexpected %s %s: %s", url, type(result).__name__, result)
                out[url] = ""
            else:
                out[url] = result
        return out

    async def fetch_and_parse(self, url: str) -> dict:
        html = await self.fetch_url(url)
        if not html:
            return {
                "url": url,
                "title": "",
                "text": "",
                "links": [],
                "metadata": {},
                "images": [],
                "headings": [],
                "tables": [],
                "lists": [],
                "errors": ["fetch failed"],
            }
        return await self.parser.parse_html(html, url)
