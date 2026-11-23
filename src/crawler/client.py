import asyncio
import logging
import re
import time
from urllib.parse import urlparse

import aiohttp

from .parser import HTMLParser
from .queue import CrawlerQueue
from .semaphore_manager import SemaphoreManager

logger = logging.getLogger(__name__)


class AsyncCrawler:
    def __init__(
        self,
        max_concurrent: int = 10,
        max_depth: int = 2,
        per_domain_limit: int = 3,
    ) -> None:
        self.max_concurrent = max_concurrent
        self.max_depth = max_depth
        self.sem_manager = SemaphoreManager(max_concurrent, per_domain_limit)
        self.session: aiohttp.ClientSession | None = None
        self.parser = HTMLParser()

        self.visited_urls: set[str] = set()
        self.failed_urls: dict[str, str] = {}
        self.processed_urls: dict[str, dict] = {}
        self.url_depth: dict[str, int] = {}

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
        async with self.sem_manager.limit(url):
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

    def _allowed_url(
        self,
        url: str,
        allowed_hosts: set[str],
        same_domain_only: bool,
        exclude_patterns: list[str] | None,
        include_patterns: list[str] | None,
    ) -> bool:
        parsed = urlparse(url)
        if parsed.scheme not in ("http", "https") or not parsed.netloc:
            return False
        if same_domain_only and parsed.netloc.lower() not in allowed_hosts:
            return False
        if exclude_patterns:
            for pattern in exclude_patterns:
                if re.search(pattern, url):
                    return False
        if include_patterns:
            if not any(re.search(pattern, url) for pattern in include_patterns):
                return False
        return True

    def _print_progress(self, queue: CrawlerQueue, started_at: float) -> None:
        elapsed = max(time.perf_counter() - started_at, 0.001)
        done = len(self.processed_urls)
        speed = done / elapsed
        qstats = queue.get_stats()
        print(
            f"[progress] done={done} queued={qstats['queued']} "
            f"failed={len(self.failed_urls)} active={self.sem_manager.active_tasks} "
            f"speed={speed:.2f} pages/s"
        )

    async def crawl(
        self,
        start_urls: list[str],
        max_pages: int = 100,
        same_domain_only: bool = True,
        exclude_patterns: list[str] | None = None,
        include_patterns: list[str] | None = None,
    ) -> dict[str, dict]:
        self.visited_urls = set()
        self.failed_urls = {}
        self.processed_urls = {}
        self.url_depth = {}

        queue = CrawlerQueue()
        allowed_hosts = {urlparse(u).netloc.lower() for u in start_urls}

        for url in start_urls:
            if queue.add_url_sync(url, priority=10):
                self.url_depth[url] = 0

        started_at = time.perf_counter()
        in_flight: set[asyncio.Task] = set()

        async def process(url: str) -> None:
            if url in self.visited_urls:
                return
            if len(self.processed_urls) >= max_pages:
                return
            self.visited_urls.add(url)
            depth = self.url_depth.get(url, 0)

            try:
                page = await self.fetch_and_parse(url)
                fetch_failed = "fetch failed" in (page.get("errors") or [])
                if fetch_failed:
                    err = "; ".join(page.get("errors") or ["fetch failed"])
                    self.failed_urls[url] = err
                    await queue.mark_failed(url, err)
                    return

                if len(self.processed_urls) >= max_pages:
                    return

                self.processed_urls[url] = page
                await queue.mark_processed(url)

                if depth >= self.max_depth:
                    return

                for link in page.get("links") or []:
                    if link in self.visited_urls:
                        continue
                    if not self._allowed_url(
                        link,
                        allowed_hosts,
                        same_domain_only,
                        exclude_patterns,
                        include_patterns,
                    ):
                        continue
                    next_depth = depth + 1
                    if next_depth > self.max_depth:
                        continue
                    priority = max(0, self.max_depth - next_depth)
                    added = await queue.add_url(link, priority=priority)
                    if added:
                        self.url_depth[link] = next_depth
            except Exception as e:
                logger.warning("crawl error %s: %s", url, e)
                self.failed_urls[url] = str(e)
                await queue.mark_failed(url, str(e))

        while len(self.processed_urls) < max_pages:
            while (
                len(in_flight) < self.max_concurrent
                and len(self.processed_urls) + len(in_flight) < max_pages
            ):
                url = await queue.get_next()
                if url is None:
                    break
                if url in self.visited_urls:
                    continue
                task = asyncio.create_task(process(url))
                in_flight.add(task)
                task.add_done_callback(in_flight.discard)

            self._print_progress(queue, started_at)

            if in_flight:
                await asyncio.wait(in_flight, return_when=asyncio.FIRST_COMPLETED)
                continue

            stats = queue.get_stats()
            if stats["queued"] == 0 and stats["heap_size"] == 0:
                break
            await asyncio.sleep(0.05)

        if in_flight:
            await asyncio.gather(*in_flight, return_exceptions=True)

        self._print_progress(queue, started_at)
        logger.info(
            "crawl finished: processed=%s failed=%s visited=%s",
            len(self.processed_urls),
            len(self.failed_urls),
            len(self.visited_urls),
        )
        return self.processed_urls
