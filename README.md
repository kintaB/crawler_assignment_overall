# Async Web Crawler (Days 1–7)

High-performance **asynchronous** web crawler built with `asyncio` and `aiohttp`.  
Homework project: parallel fetch, HTML parse, polite crawling, retries, storage, CLI.

## Features

| Area | Components |
|------|------------|
| Fetch | `AsyncCrawler`, connection pool, timeouts |
| Parse | `HTMLParser` (BeautifulSoup / lxml) |
| Crawl | priority `CrawlerQueue`, depth, concurrency |
| Politeness | `RateLimiter`, `RobotsParser`, delays / jitter |
| Resilience | typed errors, `RetryStrategy`, circuit breaker |
| Storage | JSON / CSV / SQLite via `aiofiles` / `aiosqlite` |
| Day 7 | sitemap, stats, HTML report, YAML/JSON config, CLI |

## Install

```bash
pip install -r requirements.txt
```

## Quick start (CLI)

```bash
python crawler.py --urls https://books.toscrape.com/ --max-pages 10 --output data/day7/results.json
```

With config file:

```bash
python crawler.py --config config.example.json
# or
python crawler.py --config config.example.yaml
```

Useful flags: `--max-depth`, `--rate-limit`, `--respect-robots` / `--no-respect-robots`, `--sitemap`, `--no-sitemap`, `--report`, `--stats-json`, `--log-level`.

## Python API

```python
import asyncio
from crawler import AdvancedCrawler

async def main():
    crawler = AdvancedCrawler.from_config("config.example.json")
    await crawler.crawl()
    stats = crawler.get_stats()
    print(f"Обработано: {stats['total_pages']} страниц")
    print(f"Успешно: {stats['successful']}")
    print(f"Ошибок: {stats['failed']}")
    crawler.export_to_html_report("data/day7/report.html")
    crawler.export_to_json("data/day7/stats.json")
    await crawler.close()

asyncio.run(main())
```

## Configuration

See `config.example.json` / `config.example.yaml`.

| Key | Meaning |
|-----|---------|
| `start_urls` | Seed URLs |
| `sitemaps` | Sitemap URL list (empty → try `/sitemap.xml`) |
| `max_pages` / `max_depth` / `max_concurrent` | Crawl limits |
| `requests_per_second` | Rate limit |
| `respect_robots` / `min_delay` / `jitter` | Politeness |
| `exclude_patterns` / `include_patterns` | Regex filters |
| `storage.json` / `csv` / `sqlite` | Output backends |
| `logging.level` / `logging.file` | Console + rotating file |
| `output.stats_json` / `html_report` | Reports |

## Project layout

```
crawler.py                 # CLI
config.example.*           # sample configs
src/crawler/
  client.py                # AsyncCrawler
  advanced.py              # AdvancedCrawler (Day 7)
  queue.py / parser.py
  rate_limiter.py / robots_parser.py
  errors.py / retry.py
  storage.py
  sitemap.py / stats.py / config.py / logging_setup.py
scripts/demo_day1.py … demo_day7.py
scripts/perf_day7.py
```

## Day demos

```bash
python scripts/demo_day1.py
python scripts/demo_day2.py
python scripts/demo_day3.py
python scripts/demo_day4.py
python scripts/demo_day5.py
python scripts/demo_day6.py
python scripts/demo_day7.py
python scripts/perf_day7.py
```

## API overview

- **`AsyncCrawler`** — core crawl / fetch / parse / storage hook  
- **`AdvancedCrawler`** — config, sitemap seeds, live progress, stats export  
- **`SitemapParser.fetch_sitemap`** — urlset + recursive sitemap index  
- **`CrawlerStats`** — counters, speed, status codes, top domains, HTML/JSON export  
- **`RetryStrategy` / error types** — transient vs permanent retries  
- **`DataStorage` backends** — async JSON / CSV / SQLite  

## Logging

`setup_logging` writes to console and a **rotating** file (`logs/crawler.log` by default).

## Notes

- Be polite: keep `requests_per_second` and `min_delay` reasonable.  
- Optional extras (proxy, Playwright, distributed workers) are out of scope for the base homework.  
- Performance script uses modest page counts; raise targets carefully for 100/500/1000-page runs.
