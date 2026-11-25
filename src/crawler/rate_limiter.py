import asyncio
import logging
import time

logger = logging.getLogger(__name__)


class RateLimiter:
    """Limit request rate globally or per domain."""

    def __init__(self, requests_per_second: float = 1.0, per_domain: bool = True) -> None:
        if requests_per_second <= 0:
            raise ValueError("requests_per_second must be > 0")
        self.requests_per_second = requests_per_second
        self.per_domain = per_domain
        self._min_interval = 1.0 / requests_per_second
        self._last_at: dict[str, float] = {}
        self._locks: dict[str, asyncio.Lock] = {}
        self._meta_lock = asyncio.Lock()
        self._request_times: list[float] = []
        self._delays: list[float] = []
        self._total_waits = 0.0

    async def _lock_for(self, key: str) -> asyncio.Lock:
        async with self._meta_lock:
            if key not in self._locks:
                self._locks[key] = asyncio.Lock()
            return self._locks[key]

    def _key(self, domain: str | None) -> str:
        if self.per_domain and domain:
            return domain.lower()
        return "__global__"

    async def acquire(self, domain: str | None = None) -> None:
        """Wait until a request slot is available for the domain / global budget."""
        key = self._key(domain)
        lock = await self._lock_for(key)
        async with lock:
            now = time.monotonic()
            last = self._last_at.get(key, 0.0)
            wait = self._min_interval - (now - last)
            if wait > 0:
                self._total_waits += wait
                await asyncio.sleep(wait)
                now = time.monotonic()
            delay = now - last if last > 0 else 0.0
            if last > 0:
                self._delays.append(delay)
            self._last_at[key] = now
            self._request_times.append(now)
            # keep a short window for "current" speed
            cutoff = now - 10.0
            self._request_times = [t for t in self._request_times if t >= cutoff]
            self._delays = self._delays[-200:]

    def get_stats(self) -> dict:
        now = time.monotonic()
        recent = [t for t in self._request_times if t >= now - 10.0]
        if len(recent) >= 2:
            span = recent[-1] - recent[0]
            current_rps = (len(recent) - 1) / span if span > 0 else 0.0
        else:
            current_rps = 0.0
        avg_delay = sum(self._delays) / len(self._delays) if self._delays else 0.0
        return {
            "requests_per_second_limit": self.requests_per_second,
            "per_domain": self.per_domain,
            "current_req_per_sec": round(current_rps, 3),
            "avg_delay_sec": round(avg_delay, 3),
            "total_wait_sec": round(self._total_waits, 3),
            "requests_tracked": len(self._request_times),
        }
