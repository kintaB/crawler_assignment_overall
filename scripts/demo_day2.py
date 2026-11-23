import asyncio
import logging
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from crawler.client import AsyncCrawler
from crawler.parser import HTMLParser

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)

URLS = [
    "https://httpbin.org/html",
    "https://books.toscrape.com/",
]

BROKEN_HTML = "<html><title>Broken</title><body><h1>Hi<a href='/x'>link</body>"


def summarize(page: dict) -> dict:
    return {
        "url": page.get("url"),
        "title": page.get("title"),
        "text_length": len(page.get("text") or ""),
        "links_count": len(page.get("links") or []),
        "links": (page.get("links") or [])[:5],
        "images_count": len(page.get("images") or []),
        "headings_count": len(page.get("headings") or []),
        "errors": page.get("errors") or [],
    }


async def main() -> None:
    crawler = AsyncCrawler(max_concurrent=5)
    try:
        for url in URLS:
            page = await crawler.fetch_and_parse(url)
            print("\n=== fetched+parsed ===")
            print(summarize(page))
            if page.get("headings"):
                print("headings sample:", page["headings"][:3])
    finally:
        await crawler.close()

    parser = HTMLParser()
    broken = await parser.parse_html(BROKEN_HTML, "https://example.com/page")
    print("\n=== broken HTML ===")
    print(summarize(broken))
    print("absolute links:", broken.get("links"))


if __name__ == "__main__":
    asyncio.run(main())
