"""Day 7 demo: AdvancedCrawler from config + sitemap unit check + exports."""

from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from crawler.advanced import AdvancedCrawler
from crawler.sitemap import SitemapParser
from crawler.stats import CrawlerStats


SAMPLE_URLSET = """<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <url><loc>https://example.com/a</loc></url>
  <url><loc>https://example.com/b</loc></url>
</urlset>
"""

SAMPLE_INDEX = """<?xml version="1.0" encoding="UTF-8"?>
<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <sitemap><loc>https://example.com/sitemap-pages.xml</loc></sitemap>
</sitemapindex>
"""


async def test_sitemap_parser() -> None:
    print("\n=== test: SitemapParser (mock HTTP) ===")
    parser = SitemapParser()

    class FakeResp:
        def __init__(self, text: str, status: int = 200):
            self.status = status
            self._text = text

        async def text(self, errors: str = "replace") -> str:
            return self._text

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return None

    class FakeSession:
        closed = False

        def get(self, url: str):
            if "sitemap-pages" in url:
                return FakeResp(SAMPLE_URLSET)
            return FakeResp(SAMPLE_INDEX)

    parser.set_session(FakeSession())  # type: ignore[arg-type]
    urls = await parser.fetch_sitemap("https://example.com/sitemap.xml")
    assert urls == ["https://example.com/a", "https://example.com/b"], urls
    print("index → child urlset OK:", urls)


def test_stats_export(tmp: Path) -> None:
    print("\n=== test: CrawlerStats export ===")
    st = CrawlerStats()
    st.start(max_pages=10)
    st.record_success("https://a.test/1", status_code=200, body_len=100)
    st.record_success("https://a.test/2", status_code=200, body_len=50)
    st.record_failure("https://b.test/x", status_code=404)
    st.finish()
    js = tmp / "stats.json"
    html = tmp / "report.html"
    st.export_to_json(str(js))
    st.export_to_html_report(str(html))
    assert js.exists() and html.exists()
    data = json.loads(js.read_text(encoding="utf-8"))
    assert data["successful"] == 2
    assert data["failed"] == 1
    print("exported", js.name, html.name)


async def demo_from_config() -> None:
    print("\n=== demo: AdvancedCrawler.from_config ===")
    cfg_src = ROOT / "config.example.json"
    cfg_run = ROOT / "data" / "day7" / "config.run.json"
    cfg_run.parent.mkdir(parents=True, exist_ok=True)
    data = json.loads(cfg_src.read_text(encoding="utf-8"))
    data["max_pages"] = 5
    data["max_depth"] = 1
    data["storage"] = {
        "json": str(ROOT / "data" / "day7" / "results.json"),
        "csv": str(ROOT / "data" / "day7" / "results.csv"),
        "sqlite": str(ROOT / "data" / "day7" / "crawler.db"),
    }
    data["output"] = {
        "stats_json": str(ROOT / "data" / "day7" / "stats.json"),
        "html_report": str(ROOT / "data" / "day7" / "report.html"),
    }
    data["logging"] = {"level": "INFO", "file": str(ROOT / "logs" / "crawler.log")}
    cfg_run.write_text(json.dumps(data, indent=2), encoding="utf-8")

    crawler = AdvancedCrawler.from_config(cfg_run)
    try:
        # books.toscrape often has no sitemap — still fine
        await crawler.crawl(use_sitemap=True)
        stats = crawler.get_stats()
        print(f"Обработано: {stats['total_pages']} страниц")
        print(f"Успешно: {stats['successful']}")
        print(f"Ошибок: {stats['failed']}")
        crawler.export_to_json(data["output"]["stats_json"])
        crawler.export_to_html_report(data["output"]["html_report"])
        print("report:", data["output"]["html_report"])
        assert stats["successful"] >= 1
    finally:
        await crawler.close()


async def main() -> None:
    await test_sitemap_parser()
    test_stats_export(ROOT / "data" / "day7")
    await demo_from_config()
    print("\nDay 7 checks passed.")


if __name__ == "__main__":
    asyncio.run(main())
