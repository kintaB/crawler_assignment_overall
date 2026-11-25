import asyncio
import logging
import random
import re
import time
from urllib.parse import urlparse

import aiohttp

from .parser import HTMLParser
from .queue import CrawlerQueue
from .rate_limiter import RateLimiter
from .robots_parser import RobotsParser
from .semaphore_manager import SemaphoreManager

logger = logging.getLogger(__name__)

DEFAULT_USER_AGENTS = [
    "AsyncCrawler/1.0 (+https://example.local/bot)",
    "Mozilla/5.0 (compatible; AsyncCrawler/1.0)",
    "AsyncCrawlerEdu/1.0",
]


class AsyncCrawler:
    def __init__(
        self,
        max_concurrent: int = 10,
        max_depth: int = 2,
        per_domain_limit: int = 3,
        requests_per_second: float = 1.0,
        rate_limit_per_domain: bool = True,
        respect_robots: bool = True,
        min_delay: float = 0.0,
        jitter: float = 0.0,
        user_agent: str = "AsyncCrawler/1.0",
        rotate_user_agents: bool = False,
        user_agents: list[str] | None = None,
        max_retries: int = 3,
        backoff_base: float = 0.5,
    ) -> None:
        self.max_concurrent = max_concurrent
        self.max_depth = max_depth
        self.sem_manager = SemaphoreManager(max_concurrent, per_domain_limit)
        self.rate_limiter = RateLimiter(
            requests_per_second=requests_per_second,
            per_domain=rate_limit_per_domain,
        )
        self.robots = RobotsParser()
        self.respect_robots = respect_robots
        self.min_delay = min_delay
        self.jitter = jitter
        self.user_agent = user_agent
        self.rotate_user_agents = rotate_user_agents
        self.user_agents = user_agents or DEFAULT_USER_AGENTS
        self.max_retries = max_retries
        self.backoff_base = backoff_base

        self.session: aiohttp.ClientSession | None = None
        self.parser = HTMLParser()

        self.visited_urls: set[str] = set()
        self.failed_urls: dict[str, str] = {}
        self.processed_urls: dict[str, dict] = {}
        self.url_depth: dict[str, int] = {}
        self.blocked_urls: list[str] = []
        self._last_request_at: float | None = None
        self._observed_gaps: list[float] = []

    def _ensure_session(self) -> aiohttp.ClientSession:
        if self.session is None or self.session.closed:
            timeout = aiohttp.ClientTimeout(total=30, connect=10, sock_read=20)
            connector = aiohttp.TCPConnector(limit=self.max_concurrent, ttl_dns_cache=300)
            self.session = aiohttp.ClientSession(
                timeout=timeout,
                connector=connector,
                headers={"User-Agent": self.user_agent},
            )
            self.robots.set_session(self.session)
        return self.session

    def _pick_user_agent(self) -> str:
        if self.rotate_user_agents and self.user_agents:
            return random.choice(self.user_agents)
        return self.user_agent

    async def __aenter__(self) -> "AsyncCrawler":
        self._ensure_session()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()

    async def close(self) -> None:
        if self.session is not None and not self.session.closed:
            await self.session.close()
        self.session = None
        self.robots.set_session(None)

    async def _polite_wait(self, url: str, user_agent: str) -> bool:
        """
        Robots check + delays + rate limit.
        Returns False if robots.txt forbids the URL.
        """
        domain = urlparse(url).netloc.lower()
        crawl_delay = 0.0
        if self.respect_robots:
            await self.robots.ensure_robots(url)
            if not self.robots.can_fetch(url, user_agent):
                self.blocked_urls.append(url)
                logger.info("blocked by robots.txt: %s", url)
                return False
            crawl_delay = self.robots.get_crawl_delay(user_agent, domain=domain)

        extra = max(self.min_delay, crawl_delay)
        if self.jitter > 0:
            extra += random.uniform(0.0, self.jitter)
        if extra > 0:
            await asyncio.sleep(extra)

        await self.rate_limiter.acquire(domain)

        now = time.monotonic()
        if self._last_request_at is not None:
            self._observed_gaps.append(now - self._last_request_at)
            self._observed_gaps = self._observed_gaps[-200:]
        self._last_request_at = now
        return True

    async def fetch_url(self, url: str) -> str:
        session = self._ensure_session()
        user_agent = self._pick_user_agent()

        if not await self._polite_wait(url, user_agent):
            return ""

        headers = {"User-Agent": user_agent}
        last_error = ""

        async with self.sem_manager.limit(url):
            for attempt in range(self.max_retries):
                logger.info("start %s (attempt %s ua=%s)", url, attempt + 1, user_agent)
                try:
                    async with session.get(url, headers=headers) as response:
                        response.raise_for_status()
                        text = await response.text()
                        logger.info("ok %s (%s bytes)", url, len(text))
                        return text
                except asyncio.TimeoutError:
                    last_error = "timeout"
                    logger.warning("timeout %s", url)
                except aiohttp.ClientResponseError as e:
                    last_error = f"http {e.status}"
                    logger.warning("http %s %s %s", url, e.status, e.message)
                    # no retry on clear client errors except 429/5xx
                    if e.status and e.status < 500 and e.status != 429:
                        break
                except aiohttp.ClientError as e:
                    last_error = f"{type(e).__name__}: {e}"
                    logger.warning("network %s %s: %s", url, type(e).__name__, e)

                if attempt + 1 < self.max_retries:
                    backoff = self.backoff_base * (2**attempt)
                    logger.info("backoff %.2fs after error on %s (%s)", backoff, url, last_error)
                    await asyncio.sleep(backoff)

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
            errors = ["fetch failed"]
            if url in self.blocked_urls:
                errors = ["blocked by robots.txt"]
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
                "errors": errors,
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

    def get_politeness_stats(self) -> dict:
        rl = self.rate_limiter.get_stats()
        avg_gap = (
            sum(self._observed_gaps) / len(self._observed_gaps) if self._observed_gaps else 0.0
        )
        return {
            **rl,
            "avg_observed_gap_sec": round(avg_gap, 3),
            "blocked_by_robots": len(self.blocked_urls),
            "robots_domains_cached": self.robots.get_stats()["domains_cached"],
            "min_delay": self.min_delay,
            "jitter": self.jitter,
            "user_agent": self.user_agent,
        }

    def _print_progress(self, queue: CrawlerQueue, started_at: float) -> None:
        elapsed = max(time.perf_counter() - started_at, 0.001)
        done = len(self.processed_urls)
        speed = done / elapsed
        qstats = queue.get_stats()
        pstats = self.get_politeness_stats()
        print(
            f"[progress] done={done} queued={qstats['queued']} "
            f"failed={len(self.failed_urls)} blocked={pstats['blocked_by_robots']} "
            f"active={self.sem_manager.active_tasks} "
            f"speed={speed:.2f} pages/s "
            f"req/s={pstats['current_req_per_sec']:.2f} "
            f"avg_delay={pstats['avg_delay_sec']:.2f}s"
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
        self.blocked_urls = []
        self._last_request_at = None
        self._observed_gaps = []
        self.robots.blocked_count = 0

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
                errors = page.get("errors") or []
                if "blocked by robots.txt" in errors:
                    self.failed_urls[url] = "blocked by robots.txt"
                    await queue.mark_failed(url, "blocked by robots.txt")
                    return
                fetch_failed = "fetch failed" in errors
                if fetch_failed:
                    err = "; ".join(errors or ["fetch failed"])
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
            "crawl finished: processed=%s failed=%s visited=%s blocked=%s",
            len(self.processed_urls),
            len(self.failed_urls),
            len(self.visited_urls),
            len(self.blocked_urls),
        )
        return self.processed_urls
