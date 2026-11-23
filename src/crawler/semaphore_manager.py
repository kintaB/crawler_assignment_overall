import asyncio
import logging
from contextlib import asynccontextmanager
from urllib.parse import urlparse

logger = logging.getLogger(__name__)


class SemaphoreManager:
    """Global + per-domain concurrency limits."""

    def __init__(self, global_limit: int = 10, per_domain_limit: int = 3) -> None:
        self.global_limit = global_limit
        self.per_domain_limit = per_domain_limit
        self.global_sem = asyncio.Semaphore(global_limit)
        self._domain_sems: dict[str, asyncio.Semaphore] = {}
        self._lock = asyncio.Lock()
        self.active_tasks = 0

    def _domain(self, url: str) -> str:
        return urlparse(url).netloc.lower()

    async def _domain_sem(self, domain: str) -> asyncio.Semaphore:
        async with self._lock:
            if domain not in self._domain_sems:
                self._domain_sems[domain] = asyncio.Semaphore(self.per_domain_limit)
            return self._domain_sems[domain]

    @asynccontextmanager
    async def limit(self, url: str):
        domain = self._domain(url)
        domain_sem = await self._domain_sem(domain)
        await self.global_sem.acquire()
        await domain_sem.acquire()
        self.active_tasks += 1
        try:
            yield
        finally:
            self.active_tasks -= 1
            domain_sem.release()
            self.global_sem.release()

    def get_stats(self) -> dict:
        return {
            "active_tasks": self.active_tasks,
            "global_limit": self.global_limit,
            "per_domain_limit": self.per_domain_limit,
            "domains_tracked": len(self._domain_sems),
        }
