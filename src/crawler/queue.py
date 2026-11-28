import heapq
import logging

logger = logging.getLogger(__name__)


class CrawlerQueue:
    """Priority URL queue: higher priority value is taken first.

    Interface: add_url / mark_processed / mark_failed are sync;
    only get_next is async (as required by the assignment).
    """

    def __init__(self) -> None:
        self._heap: list[tuple[int, int, str]] = []
        self._counter = 0
        self._queued: set[str] = set()
        self._processed: set[str] = set()
        self._failed: dict[str, str] = {}

    def add_url(self, url: str, priority: int = 0) -> bool:
        if url in self._queued or url in self._processed or url in self._failed:
            return False
        # heapq is min-heap → store negative priority so bigger priority comes first
        heapq.heappush(self._heap, (-priority, self._counter, url))
        self._counter += 1
        self._queued.add(url)
        return True

    async def get_next(self) -> str | None:
        while self._heap:
            _prio, _idx, url = heapq.heappop(self._heap)
            self._queued.discard(url)
            if url in self._processed or url in self._failed:
                continue
            return url
        return None

    def mark_processed(self, url: str) -> None:
        self._processed.add(url)
        self._queued.discard(url)

    def mark_failed(self, url: str, error: str) -> None:
        self._failed[url] = error
        self._queued.discard(url)

    def get_stats(self) -> dict:
        return {
            "queued": len(self._queued),
            "processed": len(self._processed),
            "failed": len(self._failed),
            "heap_size": len(self._heap),
        }
