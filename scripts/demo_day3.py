import asyncio
import json
import logging
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from crawler.client import AsyncCrawler
from crawler.queue import CrawlerQueue

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)


async def test_queue_priority() -> None:
    q = CrawlerQueue()
    q.add_url("https://low.example", priority=1)
    q.add_url("https://high.example", priority=10)
    q.add_url("https://mid.example", priority=5)
    order = []
    for _ in range(3):
        order.append(await q.get_next())
    print("priority order:", order)
    assert order[0] == "https://high.example"


async def main() -> None:
    await test_queue_priority()

    crawler = AsyncCrawler(max_concurrent=5, max_depth=1, per_domain_limit=3)
    try:
        results = await crawler.crawl(
            start_urls=["https://books.toscrape.com/"],
            max_pages=8,
            same_domain_only=True,
            exclude_patterns=[r"\?.*"],  # skip query-ish noise if any
        )
        print(f"\nОбработано: {len(results)} страниц")
        print(f"visited={len(crawler.visited_urls)} failed={len(crawler.failed_urls)}")
        print(f"depths sample: {list(crawler.url_depth.items())[:5]}")

        out_path = ROOT / "data" / "day3_results.json"
        out_path.parent.mkdir(parents=True, exist_ok=True)
        summary = {
            url: {
                "title": page.get("title"),
                "text_length": len(page.get("text") or ""),
                "links_count": len(page.get("links") or []),
                "depth": crawler.url_depth.get(url),
            }
            for url, page in results.items()
        }
        out_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"saved: {out_path}")
        for url, meta in list(summary.items())[:3]:
            print(meta, url)
    finally:
        await crawler.close()


if __name__ == "__main__":
    asyncio.run(main())
