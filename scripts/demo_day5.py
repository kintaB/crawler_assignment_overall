import asyncio
import json
import logging
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from crawler.client import AsyncCrawler
from crawler.errors import NetworkError, PermanentError, TransientError
from crawler.retry import RetryStrategy

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)


async def test_retry_on_timeout() -> None:
    print("\n=== test: retries on timeout / TransientError ===")
    strategy = RetryStrategy(max_retries=3, backoff_factor=2.0, base_delay=0.05)
    calls = {"n": 0}

    async def flaky() -> str:
        calls["n"] += 1
        if calls["n"] < 3:
            raise TransientError("timeout", url="https://t.example/")
        return "ok"

    result = await strategy.execute_with_retry(flaky)
    assert result == "ok"
    assert calls["n"] == 3
    assert strategy.stats.successful_retries == 1
    print("ok after", calls["n"], "attempts; stats", strategy.stats.get_stats())


async def test_retry_on_503() -> None:
    print("\n=== test: retries on 503 ===")
    strategy = RetryStrategy(max_retries=3, backoff_factor=2.0, base_delay=0.05)
    calls = {"n": 0}

    async def flaky() -> str:
        calls["n"] += 1
        if calls["n"] < 2:
            raise TransientError("service unavailable", url="https://s.example/", status=503)
        return "recovered"

    assert await strategy.execute_with_retry(flaky) == "recovered"
    print("503 recovered after", calls["n"], "attempts")


async def test_no_retry_on_404() -> None:
    print("\n=== test: no retry on 404 PermanentError ===")
    strategy = RetryStrategy(max_retries=5, backoff_factor=2.0, base_delay=0.05)
    calls = {"n": 0}

    async def always_404() -> str:
        calls["n"] += 1
        raise PermanentError("not found", url="https://x.example/missing", status=404)

    try:
        await strategy.execute_with_retry(always_404)
        raise AssertionError("should have raised")
    except PermanentError as e:
        assert e.status == 404
    assert calls["n"] == 1, calls
    assert "https://x.example/missing" in strategy.stats.permanent_urls
    print("called once only; permanent_urls=", strategy.stats.permanent_urls)


async def test_exponential_backoff() -> None:
    print("\n=== test: exponential backoff timing ===")
    strategy = RetryStrategy(max_retries=3, backoff_factor=2.0, base_delay=0.1)
    calls = {"n": 0}

    async def always_fail() -> str:
        calls["n"] += 1
        raise NetworkError("connection refused", url="https://n.example/")

    t0 = time.perf_counter()
    try:
        await strategy.execute_with_retry(always_fail)
    except NetworkError:
        pass
    elapsed = time.perf_counter() - t0
    # waits: 0.1*1.5 + 0.1*2*1.5 = 0.15 + 0.30 = 0.45 (NetworkError mult 1.5)
    print(f"elapsed={elapsed:.3f}s calls={calls['n']} avg_wait={strategy.stats.get_stats()['avg_retry_wait_sec']}")
    assert calls["n"] == 3
    assert elapsed >= 0.4


async def test_httpbin_statuses() -> None:
    print("\n=== test: live httpbin status codes ===")
    crawler = AsyncCrawler(
        max_concurrent=2,
        requests_per_second=5.0,
        respect_robots=False,
        min_delay=0.0,
        max_retries=3,
        backoff_factor=2.0,
        backoff_base=0.2,
        enable_circuit_breaker=False,
    )
    try:
        # 404 → no useful body, permanent, empty string
        html_404 = await crawler.fetch_url("https://httpbin.org/status/404")
        assert html_404 == ""
        assert any("PermanentError" in v or "404" in v for v in crawler.fetch_errors.values())

        # reset attempt counters somewhat by new strategy stats still accumulate
        html_200 = await crawler.fetch_url("https://httpbin.org/html")
        assert len(html_200) > 0

        # 503 may retry then fail or succeed if httpbin flaky — just exercise path
        html_503 = await crawler.fetch_url("https://httpbin.org/status/503")
        assert html_503 == ""
        stats = crawler.get_error_stats()
        print("error stats:", json.dumps(stats, ensure_ascii=False, indent=2)[:800])
        assert stats["errors_by_type"].get("PermanentError", 0) >= 1
        assert stats["errors_by_type"].get("TransientError", 0) >= 1
    finally:
        await crawler.close()


async def demo_crawl_mixed() -> None:
    print("\n=== demo: crawl + error report ===")
    crawler = AsyncCrawler(
        max_concurrent=3,
        max_depth=0,
        requests_per_second=2.0,
        respect_robots=False,
        min_delay=0.1,
        max_retries=3,
        backoff_base=0.3,
        enable_circuit_breaker=True,
        circuit_failure_threshold=8,
    )
    try:
        urls = [
            "https://httpbin.org/html",
            "https://httpbin.org/status/404",
            "https://httpbin.org/status/503",
            "https://httpbin.org/delay/1",
        ]
        # fetch_urls exercises parallel retries
        pages = await crawler.fetch_urls(urls)
        print("fetched non-empty:", {u: bool(body) for u, body in pages.items()})

        report = {
            "results": {u: {"ok": bool(body), "bytes": len(body)} for u, body in pages.items()},
            "fetch_errors": crawler.fetch_errors,
            "error_stats": crawler.get_error_stats(),
            "politeness": crawler.get_politeness_stats(),
        }
        out = ROOT / "data" / "day5_error_report.json"
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"saved: {out}")
        print(
            "summary:",
            "ok=",
            sum(1 for b in pages.values() if b),
            "errors_by_type=",
            report["error_stats"]["errors_by_type"],
            "successful_retries=",
            report["error_stats"]["successful_retries"],
        )
    finally:
        await crawler.close()


async def main() -> None:
    await test_retry_on_timeout()
    await test_retry_on_503()
    await test_no_retry_on_404()
    await test_exponential_backoff()
    await test_httpbin_statuses()
    await demo_crawl_mixed()
    print("\nDay 5 checks passed.")


if __name__ == "__main__":
    asyncio.run(main())
