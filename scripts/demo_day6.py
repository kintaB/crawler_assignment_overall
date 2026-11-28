import asyncio
import json
import logging
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from crawler.client import AsyncCrawler
from crawler.storage import CSVStorage, JSONStorage, MultiStorage, SQLiteStorage, normalize_record

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)

SAMPLE = {
    "url": "https://example.com/page",
    "title": "Hello, \"world\"",
    "text": "line1\nline2; comma, ok",
    "links": ["https://example.com/a", "https://example.com/b"],
    "metadata": {"lang": "en"},
    "status_code": 200,
    "content_type": "text/html",
}


async def test_json_storage(tmp: Path) -> None:
    print("\n=== test: JSONStorage ===")
    path = tmp / "pages.json"
    store = JSONStorage(path, pretty=True, buffer_size=2)
    await store.save_safe(SAMPLE)
    await store.save_safe({**SAMPLE, "url": "https://example.com/2", "title": "Second"})
    await store.close()
    rows = await JSONStorage(path).read_all()
    assert len(rows) == 2
    assert rows[0]["url"] == SAMPLE["url"]
    assert "crawled_at" in rows[0]
    print("json rows:", len(rows), "title0=", rows[0]["title"])


async def test_jsonl_and_csv(tmp: Path) -> None:
    print("\n=== test: JSONL + CSVStorage ===")
    jsonl = JSONStorage(tmp / "pages.jsonl", buffer_size=1)
    csv_store = CSVStorage(tmp / "pages.csv", buffer_size=1, encoding="utf-8-sig")
    for i in range(3):
        await jsonl.save_safe({**SAMPLE, "url": f"https://example.com/{i}"})
        await csv_store.save_safe({**SAMPLE, "url": f"https://example.com/{i}"})
    await jsonl.close()
    await csv_store.close()
    assert len(await jsonl.read_all()) == 3
    csv_rows = await csv_store.read_all()
    assert len(csv_rows) == 3
    assert csv_rows[0]["title"] == SAMPLE["title"]
    print("jsonl=3 csv=3 special chars ok")


async def test_sqlite(tmp: Path) -> None:
    print("\n=== test: SQLiteStorage batch ===")
    db = SQLiteStorage(tmp / "crawler.db", buffer_size=5)
    await db.init_db()
    for i in range(7):
        await db.save_safe({**SAMPLE, "url": f"https://example.com/db/{i}"})
    await db.close()
    db2 = SQLiteStorage(tmp / "crawler.db")
    await db2.init_db()
    assert await db2.count() == 7
    rows = await db2.read_all()
    assert rows[0]["links"][0].startswith("https://")
    await db2.close()
    print("sqlite count=7")


async def test_save_error_handling(tmp: Path) -> None:
    print("\n=== test: save errors do not crash ===")
    store = JSONStorage(tmp / "readonly_dir_as_file.json", buffer_size=1, max_save_retries=2)
    # point to impossible path under a file-as-directory trick
    bad = JSONStorage(tmp / "nope" / "x" / "out.json", buffer_size=1, max_save_retries=2)
    # make parent a file so mkdir/write fails
    blocker = tmp / "nope"
    blocker.write_text("not a dir", encoding="utf-8")
    ok = await bad.save_safe(SAMPLE)
    assert ok is False
    assert bad.save_errors >= 1
    print("save_safe returned False, errors=", bad.save_errors)


async def test_normalize() -> None:
    rec = normalize_record({"url": "u", "title": "t"})
    assert set(rec.keys()) >= {
        "url",
        "title",
        "text",
        "links",
        "metadata",
        "crawled_at",
        "status_code",
        "content_type",
    }


async def demo_crawl() -> None:
    print("\n=== demo: crawl → JSON + CSV + SQLite ===")
    out = ROOT / "data" / "day6"
    out.mkdir(parents=True, exist_ok=True)
    json_store = JSONStorage(out / "results.json", pretty=True, buffer_size=2)
    csv_store = CSVStorage(out / "results.csv", buffer_size=2)
    sqlite_store = SQLiteStorage(out / "crawler.db", buffer_size=3)
    await sqlite_store.init_db()
    multi = MultiStorage([json_store, csv_store, sqlite_store])

    crawler = AsyncCrawler(
        max_concurrent=3,
        max_depth=1,
        requests_per_second=2.0,
        respect_robots=True,
        min_delay=0.3,
        storage=multi,
        storage_batch_size=1,
        enable_circuit_breaker=False,
    )
    try:
        results = await crawler.crawl(
            start_urls=["https://books.toscrape.com/"],
            max_pages=5,
            same_domain_only=True,
        )
        print(f"crawled pages in memory: {len(results)}")
        print("storage stats:", json.dumps(multi.get_stats(), ensure_ascii=False, indent=2))
    finally:
        await crawler.close()

    # read back
    json_rows = await JSONStorage(out / "results.json").read_all()
    csv_rows = await CSVStorage(out / "results.csv").read_all()
    db = SQLiteStorage(out / "crawler.db")
    await db.init_db()
    db_rows = await db.read_all()
    db_count = await db.count()
    await db.close()

    summary = {
        "json_count": len(json_rows),
        "csv_count": len(csv_rows),
        "sqlite_count": db_count,
        "sample_titles": [r.get("title") for r in json_rows[:3]],
        "sample_urls": [r.get("url") for r in db_rows[:3]],
    }
    summary_path = out / "day6_summary.json"
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print("read-back:", summary)
    print("saved summary:", summary_path)
    assert len(json_rows) >= 1
    assert len(csv_rows) == len(json_rows)
    assert db_count == len(json_rows)


async def main() -> None:
    await test_normalize()
    with tempfile.TemporaryDirectory() as td:
        tmp = Path(td)
        await test_json_storage(tmp)
        await test_jsonl_and_csv(tmp)
        await test_sqlite(tmp)
        await test_save_error_handling(tmp)
    await demo_crawl()
    print("\nDay 6 checks passed.")


if __name__ == "__main__":
    asyncio.run(main())
