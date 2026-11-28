"""Final integration facade: config + sitemap + stats + storage + crawl."""

from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Any

from .client import AsyncCrawler
from .config import load_config
from .logging_setup import setup_logging
from .queue import CrawlerQueue
from .sitemap import SitemapParser
from .stats import CrawlerStats
from .storage import CSVStorage, DataStorage, JSONStorage, MultiStorage, SQLiteStorage

logger = logging.getLogger(__name__)


def build_storage(storage_cfg: dict[str, Any] | None) -> DataStorage | None:
    if not storage_cfg:
        return None
    backends: list[DataStorage] = []
    if storage_cfg.get("json"):
        backends.append(JSONStorage(storage_cfg["json"], buffer_size=5))
    if storage_cfg.get("csv"):
        backends.append(CSVStorage(storage_cfg["csv"], buffer_size=5))
    if storage_cfg.get("sqlite"):
        backends.append(SQLiteStorage(storage_cfg["sqlite"], buffer_size=10))
    if not backends:
        return None
    if len(backends) == 1:
        return backends[0]
    return MultiStorage(backends)


async def ensure_storage_ready(storage: DataStorage | None) -> None:
    if storage is None:
        return
    stores = storage.storages if isinstance(storage, MultiStorage) else [storage]
    for s in stores:
        if isinstance(s, SQLiteStorage):
            await s.init_db()


class AdvancedCrawler(AsyncCrawler):
    """AsyncCrawler + sitemap seeds, stats, config, richer progress."""

    def __init__(
        self,
        config: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        self.config = config or {}
        cfg = self.config

        storage = kwargs.pop("storage", None)
        if storage is None:
            storage = build_storage(cfg.get("storage"))

        super().__init__(
            max_concurrent=int(kwargs.pop("max_concurrent", cfg.get("max_concurrent", 5))),
            max_depth=int(kwargs.pop("max_depth", cfg.get("max_depth", 2))),
            per_domain_limit=int(kwargs.pop("per_domain_limit", cfg.get("per_domain_limit", 3))),
            requests_per_second=float(
                kwargs.pop("requests_per_second", cfg.get("requests_per_second", 2.0))
            ),
            respect_robots=bool(kwargs.pop("respect_robots", cfg.get("respect_robots", True))),
            min_delay=float(kwargs.pop("min_delay", cfg.get("min_delay", 0.0))),
            jitter=float(kwargs.pop("jitter", cfg.get("jitter", 0.0))),
            user_agent=str(kwargs.pop("user_agent", cfg.get("user_agent", "AsyncCrawler/1.0"))),
            storage=storage,
            **kwargs,
        )

        self.stats = CrawlerStats()
        self.sitemap_parser = SitemapParser()
        self.start_urls: list[str] = list(cfg.get("start_urls") or [])
        self.sitemap_urls: list[str] = list(cfg.get("sitemaps") or [])
        self.same_domain_only = bool(cfg.get("same_domain_only", True))
        self.exclude_patterns = list(cfg.get("exclude_patterns") or [])
        self.include_patterns = list(cfg.get("include_patterns") or [])
        self.max_pages_default = int(cfg.get("max_pages", 100))
        self._active_tasks = 0
        self._last_progress_line = ""

    @classmethod
    def from_config(cls, path: str | Path) -> "AdvancedCrawler":
        cfg = load_config(path)
        log_cfg = cfg.get("logging") or {}
        setup_logging(
            level=str(log_cfg.get("level", "INFO")),
            log_file=log_cfg.get("file", "logs/crawler.log"),
        )
        return cls(config=cfg)

    def _ensure_session(self):
        session = super()._ensure_session()
        self.sitemap_parser.set_session(session)
        return session

    async def collect_seed_urls(
        self,
        start_urls: list[str] | None = None,
        sitemap_urls: list[str] | None = None,
    ) -> list[str]:
        seeds: list[str] = []
        seen: set[str] = set()

        for u in start_urls if start_urls is not None else self.start_urls:
            if u and u not in seen:
                seen.add(u)
                seeds.append(u)

        sm_list = sitemap_urls if sitemap_urls is not None else self.sitemap_urls
        # auto-discover sitemap for start hosts if none given
        if not sm_list and seeds:
            sm_list = [SitemapParser.discover_sitemap_url(seeds[0])]

        self._ensure_session()
        for sm in sm_list:
            try:
                urls = await self.sitemap_parser.fetch_sitemap(sm)
                logger.info("sitemap %s contributed %s urls", sm, len(urls))
                for u in urls:
                    if u not in seen:
                        seen.add(u)
                        seeds.append(u)
            except Exception as e:
                logger.warning("sitemap skipped %s: %s", sm, e)

        return seeds

    def _print_progress(self, queue: CrawlerQueue, started_at: float) -> None:
        elapsed = max(time.perf_counter() - started_at, 0.001)
        done = len(self.processed_urls)
        speed = done / elapsed
        qstats = queue.get_stats()
        active = self.sem_manager.active_tasks
        self._active_tasks = active

        # live counters for ETA / progress bar during the run
        self.stats.successful = done
        self.stats.failed = len(self.failed_urls)
        self.stats.blocked = len(self.blocked_urls)
        if self.stats.started_at is None:
            self.stats.started_at = started_at

        pct = self.stats.progress_pct()
        eta = self.stats.eta_sec()
        pct_s = f"{pct:5.1f}%" if pct is not None else "  n/a"
        eta_s = f"{eta:6.1f}s" if eta is not None else "   n/a"
        bar_len = 20
        if pct is not None:
            filled = int(bar_len * pct / 100)
            bar = "#" * filled + "-" * (bar_len - filled)
        else:
            bar = "?" * bar_len

        line = (
            f"[progress] |{bar}| {pct_s} done={done} "
            f"failed={len(self.failed_urls)} queued={qstats['queued']} "
            f"active={active} speed={speed:.2f} p/s eta={eta_s}"
        )
        print(line)
        self._last_progress_line = line

    async def crawl(
        self,
        start_urls: list[str] | None = None,
        max_pages: int | None = None,
        same_domain_only: bool | None = None,
        exclude_patterns: list[str] | None = None,
        include_patterns: list[str] | None = None,
        use_sitemap: bool = True,
    ) -> dict[str, dict]:
        max_pages = self.max_pages_default if max_pages is None else max_pages
        same_domain_only = (
            self.same_domain_only if same_domain_only is None else same_domain_only
        )
        exclude_patterns = (
            self.exclude_patterns if exclude_patterns is None else exclude_patterns
        )
        include_patterns = (
            self.include_patterns if include_patterns is None else include_patterns
        )

        if use_sitemap:
            seeds = await self.collect_seed_urls(start_urls=start_urls)
        else:
            seeds = list(start_urls if start_urls is not None else self.start_urls)

        if not seeds:
            raise ValueError("no start URLs (config start_urls / --urls / sitemap empty)")

        # Cap seed flood from huge sitemaps; crawl will discover more via links.
        seed_cap = max(max_pages * 3, 20)
        if len(seeds) > seed_cap:
            logger.info("truncating seeds %s → %s", len(seeds), seed_cap)
            seeds = seeds[:seed_cap]

        await ensure_storage_ready(self.storage)
        self.stats.start(max_pages=max_pages)
        try:
            results = await super().crawl(
                start_urls=seeds,
                max_pages=max_pages,
                same_domain_only=same_domain_only,
                exclude_patterns=exclude_patterns or None,
                include_patterns=include_patterns or None,
            )
        finally:
            self.stats.finish()

        self._refresh_stats_from_crawl()
        return results

    def _refresh_stats_from_crawl(self) -> None:
        started, finished, target = (
            self.stats.started_at,
            self.stats.finished_at,
            self.stats.max_pages_target,
        )
        self.stats.reset()
        self.stats.started_at = started
        self.stats.finished_at = finished
        self.stats.max_pages_target = target
        for url, page in self.processed_urls.items():
            body_len = len(page.get("text") or "")
            code = int(page.get("status_code") or 200)
            self.stats.record_success(url, status_code=code, body_len=body_len)
        for url in self.blocked_urls:
            self.stats.record_blocked(url)
        for url, err in self.failed_urls.items():
            code = None
            for token, sc in (("404", 404), ("403", 403), ("503", 503), ("429", 429)):
                if token in err:
                    code = sc
                    break
            self.stats.record_failure(url, status_code=code)

    def get_stats(self) -> dict:
        snap = self.stats.snapshot()
        snap["visited"] = len(self.visited_urls)
        snap["storage"] = None if self.storage is None else self.storage.get_stats()
        snap["error_stats"] = self.get_error_stats()
        snap["politeness"] = self.get_politeness_stats()
        return snap

    def export_to_json(self, filename: str) -> None:
        self.stats.export_to_json(filename, extra={"get_stats": self.get_stats()})

    def export_to_html_report(self, filename: str) -> None:
        self.stats.export_to_html_report(filename, extra={"visited": len(self.visited_urls)})

    async def close(self) -> None:
        await super().close()
