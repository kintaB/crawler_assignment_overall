import asyncio
import heapq
import logging

logger = logging.getLogger(__name__)


class CrawlerQueue:
    """Priority URL queue: higher priority value is taken first."""

    def __init__(self) -> None:
        self._heap: list[tuple[int, int, str]] = []
        self._counter = 0
        self._queued: set[str] = set()
        self._processed: set[str] = set()
        self._failed: dict[str, str] = {}
        self._lock = asyncio.Lock()

    async def add_url(self, url: str, priority: int = 0) -> bool:
        async with self._lock:
            if url in self._queued or url in self._processed or url in self._failed:
                return False
            # heapq is min-heap → store negative priority so bigger priority comes first
            heapq.heappush(self._heap, (-priority, self._counter, url))
            self._counter += 1
            self._queued.add(url)
            return True

    def add_url_sync(self, url: str, priority: int = 0) -> bool:
        """Sync helper for start URLs before the event loop is busy."""
        if url in self._queued or url in self._processed or url in self._failed:
            return False
        heapq.heappush(self._heap, (-priority, self._counter, url))
        self._counter += 1
        self._queued.add(url)
        return True

    async def get_next(self) -> str | None:
        async with self._lock:
            while self._heap:
                _prio, _idx, url = heapq.heappop(self._heap)
                self._queued.discard(url)
                if url in self._processed or url in self._failed:
                    continue
                return url
            return None

    async def mark_processed(self, url: str) -> None:
        async with self._lock:
            self._processed.add(url)
            self._queued.discard(url)

    async def mark_failed(self, url: str, error: str) -> None:
        async with self._lock:
            self._failed[url] = error
            self._queued.discard(url)

    def get_stats(self) -> dict:
        return {
            "queued": len(self._queued),
            "processed": len(self._processed),
            "failed": len(self._failed),
            "heap_size": len(self._heap),
        }
