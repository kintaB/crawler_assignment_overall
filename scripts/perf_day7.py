"""Lightweight performance comparison: async crawler vs naive sync requests."""

from __future__ import annotations

import asyncio
import json
import sys
import time
import tracemalloc
from pathlib import Path
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from crawler.advanced import AdvancedCrawler

URLS = [
    "https://httpbin.org/html",
    "https://httpbin.org/links/5/0",
]


def sync_fetch(urls: list[str], rounds: int) -> dict:
    t0 = time.perf_counter()
    ok = 0
    for _ in range(rounds):
        for u in urls:
            req = Request(u, headers={"User-Agent": "SyncBench/1.0"})
            with urlopen(req, timeout=30) as resp:
                body = resp.read()
                if body:
                    ok += 1
    elapsed = time.perf_counter() - t0
    return {"ok": ok, "elapsed": elapsed, "speed": ok / max(elapsed, 0.001)}


async def async_fetch(urls: list[str], rounds: int) -> dict:
    crawler = AdvancedCrawler(
        config={
            "start_urls": urls,
            "max_pages": len(urls) * rounds,
            "max_depth": 0,
            "max_concurrent": 5,
            "requests_per_second": 20.0,
            "respect_robots": False,
            "min_delay": 0.0,
            "same_domain_only": False,
            "storage": {},
        }
    )
    # depth 0 + multiple seeds: expand seeds by repeating for rounds
    seeds = urls * rounds
    tracemalloc.start()
    t0 = time.perf_counter()
    try:
        await crawler.crawl(start_urls=seeds, max_pages=len(seeds), use_sitemap=False)
        current, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
        await crawler.close()
    elapsed = time.perf_counter() - t0
    ok = len(crawler.processed_urls)
    return {
        "ok": ok,
        "elapsed": elapsed,
        "speed": ok / max(elapsed, 0.001),
        "peak_memory_kib": peak / 1024,
        "current_memory_kib": current / 1024,
    }


async def scale_test(n: int) -> dict:
    """Crawl up to n pages on books.toscrape (polite, capped)."""
    crawler = AdvancedCrawler(
        config={
            "start_urls": ["https://books.toscrape.com/"],
            "max_pages": n,
            "max_depth": 2,
            "max_concurrent": 8,
            "requests_per_second": 3.0,
            "respect_robots": True,
            "min_delay": 0.2,
            "same_domain_only": True,
            "storage": {"json": str(ROOT / "data" / "day7" / f"perf_{n}.json")},
        }
    )
    tracemalloc.start()
    t0 = time.perf_counter()
    try:
        await crawler.crawl(use_sitemap=False)
        current, peak = tracemalloc.get_traced_memory()
        stats = crawler.get_stats()
    finally:
        tracemalloc.stop()
        await crawler.close()
    elapsed = time.perf_counter() - t0
    return {
        "target_pages": n,
        "successful": stats["successful"],
        "elapsed": round(elapsed, 3),
        "speed": stats["avg_speed_pages_per_sec"],
        "peak_memory_mib": round(peak / (1024 * 1024), 3),
    }


async def main() -> None:
    print("=== sync vs async (httpbin, small) ===")
    rounds = 2
    sync = sync_fetch(URLS, rounds)
    async_res = await async_fetch(URLS, rounds)
    print("sync:", sync)
    print("async:", async_res)

    print("\n=== scalability (books.toscrape) ===")
    # keep modest for CI/homework machines; comment notes 500/1000
    results = []
    for n in (20, 40):
        row = await scale_test(n)
        results.append(row)
        print(row)

    out = ROOT / "data" / "day7" / "perf_report.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "sync_vs_async": {"sync": sync, "async": async_res},
        "scale": results,
        "note": "For 100/500/1000 raise scale_test targets and rate limits carefully.",
    }
    out.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print("saved", out)


if __name__ == "__main__":
    asyncio.run(main())
