import asyncio
import json
import logging
import sys
import time
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from crawler.client import AsyncCrawler
from crawler.rate_limiter import RateLimiter
from crawler.robots_parser import RobotsParser

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)

SAMPLE_ROBOTS = """
User-agent: *
Disallow: /private
Disallow: /admin
Crawl-delay: 1

User-agent: MyBot/1.0
Disallow: /secret
Allow: /
Crawl-delay: 0.5
"""


async def test_rate_limiter_one_domain() -> None:
    print("\n=== test: rate limit one domain ===")
    limiter = RateLimiter(requests_per_second=5.0, per_domain=True)
    t0 = time.perf_counter()
    for _ in range(5):
        await limiter.acquire("example.com")
    elapsed = time.perf_counter() - t0
    # 5 requests at 5 rps → ~4 intervals → ~0.8s
    print(f"5 acquires @5rps took {elapsed:.3f}s")
    assert elapsed >= 0.7, elapsed
    stats = limiter.get_stats()
    print("stats:", stats)
    assert stats["avg_delay_sec"] >= 0.15


async def test_rate_limiter_multi_domain() -> None:
    print("\n=== test: rate limit multi domain (parallel) ===")
    limiter = RateLimiter(requests_per_second=2.0, per_domain=True)

    async def hit(domain: str, n: int = 2) -> float:
        t0 = time.perf_counter()
        for _ in range(n):
            await limiter.acquire(domain)
        return time.perf_counter() - t0

    t0 = time.perf_counter()
    times = await asyncio.gather(hit("a.test"), hit("b.test"))
    wall = time.perf_counter() - t0
    print(f"per-domain times={times}, wall={wall:.3f}s")
    # parallel domains should finish near one domain's cost, not sum
    assert wall < sum(times) * 0.9


async def test_robots_parser() -> None:
    print("\n=== test: robots.txt parse + block ===")
    robots = RobotsParser()

    mock_resp = AsyncMock()
    mock_resp.status = 200
    mock_resp.text = AsyncMock(return_value=SAMPLE_ROBOTS)
    mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
    mock_resp.__aexit__ = AsyncMock(return_value=None)

    session = MagicMock()
    session.closed = False
    session.get = MagicMock(return_value=mock_resp)
    robots.set_session(session)

    rules = await robots.fetch_robots("https://books.toscrape.com/")
    print("rules agents:", list(rules.keys()))
    assert "*" in rules
    assert "/private" in rules["*"]["disallow"]

    assert robots.can_fetch("https://books.toscrape.com/catalogue/a", "MyBot/1.0") is True
    assert robots.can_fetch("https://books.toscrape.com/secret/x", "MyBot/1.0") is False
    assert robots.can_fetch("https://books.toscrape.com/private/x", "*") is False
    delay = robots.get_crawl_delay("MyBot/1.0", domain="books.toscrape.com")
    print(f"crawl-delay MyBot={delay}")
    assert delay == 0.5
    print("blocked_count:", robots.blocked_count)
    assert robots.blocked_count >= 1


async def test_crawler_blocks_disallowed() -> None:
    print("\n=== test: crawler blocks disallowed URL ===")
    crawler = AsyncCrawler(
        max_concurrent=2,
        requests_per_second=10.0,
        respect_robots=True,
        min_delay=0.0,
        user_agent="MyBot/1.0",
    )
    try:
        with patch.object(
            RobotsParser,
            "fetch_robots",
            new=AsyncMock(
                return_value={
                    "MyBot/1.0": {
                        "allow": ["/"],
                        "disallow": ["/secret"],
                        "crawl_delay": 0.0,
                    },
                    "*": {"allow": [], "disallow": ["/secret"], "crawl_delay": 0.0},
                }
            ),
        ):
            # inject parser state as if robots already loaded
            from urllib.robotparser import RobotFileParser

            rp = RobotFileParser()
            rp.parse(SAMPLE_ROBOTS.splitlines())
            crawler.robots._parsers["books.toscrape.com"] = rp
            crawler.robots._rules["books.toscrape.com"] = {
                "MyBot/1.0": {
                    "allow": ["/"],
                    "disallow": ["/secret"],
                    "crawl_delay": 0.5,
                },
                "*": {
                    "allow": [],
                    "disallow": ["/private", "/admin"],
                    "crawl_delay": 1.0,
                },
            }
            html = await crawler.fetch_url("https://books.toscrape.com/secret/page")
            assert html == ""
            assert "https://books.toscrape.com/secret/page" in crawler.blocked_urls
            print("blocked OK:", crawler.blocked_urls)
    finally:
        await crawler.close()


async def demo_crawl() -> None:
    print("\n=== demo: polite crawl books.toscrape.com ===")
    crawler = AsyncCrawler(
        max_concurrent=5,
        max_depth=1,
        per_domain_limit=2,
        requests_per_second=2.0,
        respect_robots=True,
        min_delay=0.5,
        jitter=0.1,
        user_agent="MyBot/1.0",
    )
    try:
        results = await crawler.crawl(
            start_urls=["https://books.toscrape.com/"],
            max_pages=5,
            same_domain_only=True,
        )
        stats = crawler.get_politeness_stats()
        print(f"\nОбработано: {len(results)} страниц")
        print(f"failed={len(crawler.failed_urls)} blocked={len(crawler.blocked_urls)}")
        print("politeness stats:", json.dumps(stats, ensure_ascii=False, indent=2))

        # also try an obviously private path if linked somehow — force check
        await crawler.robots.ensure_robots("https://books.toscrape.com/")
        fake = "https://books.toscrape.com/private/demo"
        # site may not disallow /private; still show can_fetch against live robots
        allowed = crawler.robots.can_fetch(fake, crawler.user_agent)
        print(f"live robots can_fetch({fake}) = {allowed}")

        out_path = ROOT / "data" / "day4_results.json"
        out_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "pages": {
                url: {
                    "title": page.get("title"),
                    "links_count": len(page.get("links") or []),
                    "depth": crawler.url_depth.get(url),
                }
                for url, page in results.items()
            },
            "blocked_urls": crawler.blocked_urls,
            "stats": stats,
        }
        out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"saved: {out_path}")
    finally:
        await crawler.close()


async def main() -> None:
    await test_rate_limiter_one_domain()
    await test_rate_limiter_multi_domain()
    await test_robots_parser()
    await test_crawler_blocks_disallowed()
    await demo_crawl()
    print("\nDay 4 checks passed.")


if __name__ == "__main__":
    asyncio.run(main())
