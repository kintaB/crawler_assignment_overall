#!/usr/bin/env python3
"""CLI entrypoint for the async crawler (Day 7)."""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / "src"))

from crawler.advanced import AdvancedCrawler
from crawler.config import load_config
from crawler.logging_setup import setup_logging


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Async web crawler (days 1–7)")
    p.add_argument("--urls", nargs="+", help="Start URL(s)")
    p.add_argument("--sitemap", nargs="+", dest="sitemaps", help="Sitemap URL(s)")
    p.add_argument("--max-pages", type=int, help="Max pages to process")
    p.add_argument("--max-depth", type=int, help="Max crawl depth")
    p.add_argument("--max-concurrent", type=int, help="Max concurrent tasks")
    p.add_argument("--rate-limit", type=float, help="Requests per second")
    p.add_argument("--output", help="JSON results path (storage.json)")
    p.add_argument("--config", help="YAML/JSON config file")
    p.add_argument(
        "--respect-robots",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Respect robots.txt (default from config / true)",
    )
    p.add_argument("--log-level", default=None, help="DEBUG/INFO/WARNING/ERROR")
    p.add_argument("--no-sitemap", action="store_true", help="Do not fetch /sitemap.xml")
    p.add_argument("--report", help="HTML report path")
    p.add_argument("--stats-json", help="Stats JSON path")
    return p.parse_args(argv)


async def async_main(args: argparse.Namespace) -> int:
    cfg = load_config(args.config) if args.config else load_config(None)

    if args.urls:
        cfg["start_urls"] = args.urls
    if args.sitemaps:
        cfg["sitemaps"] = args.sitemaps
    if args.max_pages is not None:
        cfg["max_pages"] = args.max_pages
    if args.max_depth is not None:
        cfg["max_depth"] = args.max_depth
    if args.max_concurrent is not None:
        cfg["max_concurrent"] = args.max_concurrent
    if args.rate_limit is not None:
        cfg["requests_per_second"] = args.rate_limit
    if args.respect_robots is not None:
        cfg["respect_robots"] = args.respect_robots
    if args.output:
        cfg.setdefault("storage", {})["json"] = args.output
    if args.report:
        cfg.setdefault("output", {})["html_report"] = args.report
    if args.stats_json:
        cfg.setdefault("output", {})["stats_json"] = args.stats_json

    log_cfg = cfg.get("logging") or {}
    setup_logging(
        level=args.log_level or str(log_cfg.get("level", "INFO")),
        log_file=log_cfg.get("file", "logs/crawler.log"),
    )

    if not cfg.get("start_urls") and not cfg.get("sitemaps"):
        print("Provide --urls and/or --config with start_urls", file=sys.stderr)
        return 2

    crawler = AdvancedCrawler(config=cfg)
    try:
        await crawler.crawl(use_sitemap=not args.no_sitemap)
        stats = crawler.get_stats()
        print(
            f"Обработано: {stats['total_pages']} | "
            f"успешно: {stats['successful']} | "
            f"ошибок: {stats['failed']} | "
            f"скорость: {stats['avg_speed_pages_per_sec']} p/s"
        )

        out = cfg.get("output") or {}
        stats_path = out.get("stats_json") or "data/day7/stats.json"
        html_path = out.get("html_report") or "data/day7/report.html"
        crawler.export_to_json(stats_path)
        crawler.export_to_html_report(html_path)
        print(f"stats → {stats_path}")
        print(f"report → {html_path}")
        print(json.dumps({k: stats[k] for k in ("successful", "failed", "top_domains")}, ensure_ascii=False))
    finally:
        await crawler.close()
    return 0


def main() -> None:
    args = parse_args()
    raise SystemExit(asyncio.run(async_main(args)))


if __name__ == "__main__":
    main()
