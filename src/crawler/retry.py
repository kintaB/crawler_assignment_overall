"""Retry strategy, error stats, and optional circuit breaker."""

from __future__ import annotations

import asyncio
import logging
import time
from collections import defaultdict
from collections.abc import Awaitable, Callable
from typing import Any, TypeVar
from urllib.parse import urlparse

from .errors import (
    BACKOFF_MULTIPLIER,
    DEFAULT_RETRY_ON,
    RETRY_AFTER_DEFAULT,
    NetworkError,
    PermanentError,
    TransientError,
    classify_exception,
    error_type_name,
)

logger = logging.getLogger(__name__)

T = TypeVar("T")


class ErrorStats:
    def __init__(self) -> None:
        self.by_type: dict[str, int] = defaultdict(int)
        self.successful_retries = 0
        self.failed_after_retries = 0
        self.retry_wait_total = 0.0
        self.retry_wait_count = 0
        self.permanent_urls: dict[str, str] = {}
        self.attempts_log: list[dict] = []

    def record_error(self, err: BaseException, url: str | None = None) -> None:
        name = error_type_name(err)
        self.by_type[name] += 1
        if isinstance(err, PermanentError) and url:
            self.permanent_urls[url] = str(err)

    def record_retry_wait(self, seconds: float) -> None:
        self.retry_wait_total += seconds
        self.retry_wait_count += 1

    def record_success_after_retry(self) -> None:
        self.successful_retries += 1

    def record_exhausted(self) -> None:
        self.failed_after_retries += 1

    def get_stats(self) -> dict:
        avg = self.retry_wait_total / self.retry_wait_count if self.retry_wait_count else 0.0
        return {
            "errors_by_type": dict(self.by_type),
            "successful_retries": self.successful_retries,
            "failed_after_retries": self.failed_after_retries,
            "avg_retry_wait_sec": round(avg, 3),
            "permanent_urls": dict(self.permanent_urls),
            "attempts_logged": len(self.attempts_log),
        }


class CircuitBreaker:
    """Simple per-domain circuit breaker."""

    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout: float = 30.0,
        window_sec: float = 60.0,
    ) -> None:
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.window_sec = window_sec
        self._failures: dict[str, list[float]] = defaultdict(list)
        self._opened_at: dict[str, float] = {}

    def _domain(self, url: str) -> str:
        return urlparse(url).netloc.lower() or url

    def _prune(self, domain: str, now: float) -> None:
        cutoff = now - self.window_sec
        self._failures[domain] = [t for t in self._failures[domain] if t >= cutoff]

    def allow(self, url: str) -> bool:
        domain = self._domain(url)
        now = time.monotonic()
        opened = self._opened_at.get(domain)
        if opened is None:
            return True
        if now - opened >= self.recovery_timeout:
            # half-open: allow one try
            del self._opened_at[domain]
            self._failures[domain].clear()
            logger.info("circuit half-open for %s", domain)
            return True
        return False

    def record_success(self, url: str) -> None:
        domain = self._domain(url)
        self._failures[domain].clear()
        self._opened_at.pop(domain, None)

    def record_failure(self, url: str) -> None:
        domain = self._domain(url)
        now = time.monotonic()
        self._prune(domain, now)
        self._failures[domain].append(now)
        if len(self._failures[domain]) >= self.failure_threshold:
            self._opened_at[domain] = now
            logger.warning(
                "circuit OPEN for %s after %s failures",
                domain,
                len(self._failures[domain]),
            )

    def get_stats(self) -> dict:
        now = time.monotonic()
        open_domains = {
            d: round(self.recovery_timeout - (now - t), 1)
            for d, t in self._opened_at.items()
        }
        return {
            "open_domains": open_domains,
            "failure_threshold": self.failure_threshold,
            "recovery_timeout": self.recovery_timeout,
        }


class RetryStrategy:
    def __init__(
        self,
        max_retries: int = 3,
        backoff_factor: float = 2.0,
        retry_on: list | None = None,
        base_delay: float = 0.5,
        max_retries_by_type: dict[type[BaseException], int] | None = None,
    ) -> None:
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        self.base_delay = base_delay
        self.retry_on: tuple[type[BaseException], ...] = tuple(retry_on) if retry_on else DEFAULT_RETRY_ON
        self.max_retries_by_type = max_retries_by_type or {
            TransientError: max_retries,
            NetworkError: max_retries,
        }
        self.stats = ErrorStats()

    def _should_retry(self, err: BaseException, attempt: int, type_attempts: dict[str, int]) -> bool:
        if not isinstance(err, self.retry_on):
            return False
        name = error_type_name(err)
        limit = self.max_retries
        for cls, lim in self.max_retries_by_type.items():
            if isinstance(err, cls):
                limit = lim
                break
        # attempt is 0-based attempt index that just failed; retries left if attempt+1 < max_retries
        # also respect per-type count of failures of this class
        if type_attempts.get(name, 0) >= limit:
            return False
        return attempt + 1 < self.max_retries

    def _backoff_seconds(self, err: BaseException, attempt: int) -> float:
        mult = BACKOFF_MULTIPLIER.get(type(err), 1.0)
        for cls, m in BACKOFF_MULTIPLIER.items():
            if isinstance(err, cls):
                mult = m
                break
        delay = self.base_delay * (self.backoff_factor**attempt) * mult
        if isinstance(err, TransientError) and err.status == 429:
            delay = max(delay, RETRY_AFTER_DEFAULT * (self.backoff_factor**attempt))
        return delay

    async def execute_with_retry(
        self,
        fn: Callable[..., Awaitable[T]],
        *args: Any,
        **kwargs: Any,
    ) -> T:
        """
        Call async callable `fn(*args, **kwargs)` with retries.
        Raises the last classified error if all attempts fail.
        """
        url = kwargs.get("url")
        if url is None and args:
            url = args[0] if isinstance(args[0], str) else None

        last_err: BaseException | None = None
        type_attempts: dict[str, int] = defaultdict(int)

        for attempt in range(self.max_retries):
            try:
                result = await fn(*args, **kwargs)
                if attempt > 0:
                    self.stats.record_success_after_retry()
                    logger.info(
                        "retry success url=%s after %s attempts",
                        url,
                        attempt + 1,
                    )
                return result
            except Exception as raw:
                err = classify_exception(raw, url=url if isinstance(url, str) else None)
                last_err = err
                name = error_type_name(err)
                type_attempts[name] += 1
                err_url = url if isinstance(url, str) else getattr(err, "url", None)
                self.stats.record_error(err, err_url if isinstance(err_url, str) else None)

                entry = {
                    "url": err_url,
                    "attempt": attempt + 1,
                    "error_type": name,
                    "message": str(err),
                    "status": getattr(err, "status", None),
                }
                self.stats.attempts_log.append(entry)

                will_retry = self._should_retry(err, attempt, type_attempts)
                if not will_retry:
                    logger.warning(
                        "give up url=%s type=%s attempt=%s/%s msg=%s",
                        url,
                        name,
                        attempt + 1,
                        self.max_retries,
                        err,
                    )
                    break

                wait = self._backoff_seconds(err, attempt)
                self.stats.record_retry_wait(wait)
                entry["next_wait_sec"] = round(wait, 3)
                logger.warning(
                    "retry url=%s type=%s attempt=%s/%s wait=%.2fs msg=%s",
                    url,
                    name,
                    attempt + 1,
                    self.max_retries,
                    wait,
                    err,
                )
                await asyncio.sleep(wait)

        self.stats.record_exhausted()
        assert last_err is not None
        raise last_err
