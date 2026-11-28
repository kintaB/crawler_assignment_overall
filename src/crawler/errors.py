"""Crawler error taxonomy for retries and reporting."""

from __future__ import annotations

import asyncio

import aiohttp


class CrawlerError(Exception):
    """Base crawler error."""

    def __init__(self, message: str = "", *, url: str | None = None, status: int | None = None) -> None:
        super().__init__(message)
        self.url = url
        self.status = status


class TransientError(CrawlerError):
    """Temporary failures worth retrying (timeouts, 429, 503, 500…)."""


class PermanentError(CrawlerError):
    """Non-retryable failures (404, 403, 401…)."""


class NetworkError(CrawlerError):
    """Transport / DNS / connection failures."""


class ParseError(CrawlerError):
    """HTML / content parse failures."""


# Default retryable classes (assignment: Transient + Network)
DEFAULT_RETRY_ON: tuple[type[BaseException], ...] = (TransientError, NetworkError)

# Per-type backoff multipliers on top of base * factor**attempt
BACKOFF_MULTIPLIER: dict[type[BaseException], float] = {
    TransientError: 1.0,
    NetworkError: 1.5,
    PermanentError: 0.0,
    ParseError: 0.0,
}

# Extra wait specifically for 429
RETRY_AFTER_DEFAULT = 2.0


def classify_http_status(status: int, message: str = "", *, url: str | None = None) -> CrawlerError:
    if status == 429:
        return TransientError(message or "too many requests", url=url, status=status)
    if status == 503:
        return TransientError(message or "service unavailable", url=url, status=status)
    if status >= 500:
        return TransientError(message or f"server error {status}", url=url, status=status)
    if status in (401, 403, 404, 410):
        return PermanentError(message or f"http {status}", url=url, status=status)
    if 400 <= status < 500:
        return PermanentError(message or f"client error {status}", url=url, status=status)
    return TransientError(message or f"http {status}", url=url, status=status)


def classify_exception(exc: BaseException, *, url: str | None = None) -> CrawlerError:
    """Map low-level exceptions onto our taxonomy."""
    if isinstance(exc, CrawlerError):
        if exc.url is None and url:
            exc.url = url
        return exc

    if isinstance(exc, (asyncio.TimeoutError, TimeoutError)):
        return TransientError("timeout", url=url)

    if isinstance(exc, aiohttp.ClientResponseError):
        return classify_http_status(exc.status, exc.message, url=url or str(exc.request_info.url))

    if isinstance(
        exc,
        (
            aiohttp.ClientConnectorError,
            aiohttp.ClientConnectorDNSError,
            aiohttp.ServerConnectionError,
            aiohttp.ClientConnectionError,
        ),
    ):
        return NetworkError(f"{type(exc).__name__}: {exc}", url=url)

    if isinstance(exc, (aiohttp.ConnectionTimeoutError, aiohttp.ServerTimeoutError)):
        return TransientError(f"{type(exc).__name__}: {exc}", url=url)

    if isinstance(exc, aiohttp.ClientError):
        return NetworkError(f"{type(exc).__name__}: {exc}", url=url)

    # Unknown → treat as network-ish retryable to be safe in crawl context? Better permanent.
    return PermanentError(f"{type(exc).__name__}: {exc}", url=url)


def error_type_name(exc: BaseException) -> str:
    if isinstance(exc, TransientError):
        return "TransientError"
    if isinstance(exc, PermanentError):
        return "PermanentError"
    if isinstance(exc, NetworkError):
        return "NetworkError"
    if isinstance(exc, ParseError):
        return "ParseError"
    return type(exc).__name__
