import asyncio
import logging
import sys
import time
from pathlib import Path

import aiohttp

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from crawler.client import AsyncCrawler

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)

# 8 URLs: valid + delay + 404 + bad host
DEMO_URLS = [
    "https://httpbin.org/get",
    "https://httpbin.org/html",
    "https://httpbin.org/uuid",
    "https://httpbin.org/delay/1",
    "https://httpbin.org/delay/2",
    "https://httpbin.org/headers",
    "https://httpbin.org/status/404",
    "https://this-host-does-not-exist-xyz.invalid/",
]

TIMING_URLS = [
    "https://httpbin.org/get",
    "https://httpbin.org/html",
    "https://httpbin.org/delay/1",
    "https://httpbin.org/delay/2",
    "https://httpbin.org/uuid",
]


def summarize(label: str, results: dict[str, str], elapsed: float) -> None:
    ok = sum(1 for value in results.values() if value)
    print(f"\n=== {label} ===")
    print(f"time: {elapsed:.2f}s | ok: {ok}/{len(results)}")
    for url, body in results.items():
        status = f"OK {len(body)} bytes" if body else "FAIL"
        print(f"  [{status}] {url}")


async def run_parallel(urls: list[str], max_concurrent: int = 5) -> dict[str, str]:
    crawler = AsyncCrawler(max_concurrent=max_concurrent)
    try:
        return await crawler.fetch_urls(urls)
    finally:
        await crawler.close()


async def run_sequential(urls: list[str]) -> dict[str, str]:
    crawler = AsyncCrawler(max_concurrent=1)
    try:
        out: dict[str, str] = {}
        for url in urls:
            out[url] = await crawler.fetch_url(url)
        return out
    finally:
        await crawler.close()


async def demo_timeout() -> None:
    """Force a timeout: delay/5 with total timeout 2s."""
    print("\n=== timeout test (delay/5, timeout total=2s) ===")
    crawler = AsyncCrawler(max_concurrent=1)
    crawler.session = aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(total=2, connect=1, sock_read=2),
        connector=aiohttp.TCPConnector(limit=1),
    )
    try:
        url = "https://httpbin.org/delay/5"
        body = await crawler.fetch_url(url)
        print(f"  result: {'OK' if body else 'TIMEOUT/FAIL'} for {url}")
    finally:
        await crawler.close()


async def main() -> None:
    # Style from homework example: create -> fetch -> close
    t0 = time.perf_counter()
    sequential = await run_sequential(TIMING_URLS)
    t_seq = time.perf_counter() - t0

    t0 = time.perf_counter()
    parallel = await run_parallel(TIMING_URLS, max_concurrent=5)
    t_par = time.perf_counter() - t0

    summarize("sequential (max_concurrent=1)", sequential, t_seq)
    summarize("parallel (max_concurrent=5)", parallel, t_par)
    if t_par > 0:
        print(f"\nparallel is ~{t_seq / t_par:.1f}x faster")

    t0 = time.perf_counter()
    mixed = await run_parallel(DEMO_URLS, max_concurrent=5)
    summarize("demo 8 URLs (valid + 404 + bad host)", mixed, time.perf_counter() - t0)

    await demo_timeout()


if __name__ == "__main__":
    asyncio.run(main())
