"""Crawl run statistics and report export."""

from __future__ import annotations

import json
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse


class CrawlerStats:
    def __init__(self) -> None:
        self.reset()

    def reset(self) -> None:
        self.started_at: float | None = None
        self.finished_at: float | None = None
        self.successful = 0
        self.failed = 0
        self.blocked = 0
        self.status_codes: Counter[int] = Counter()
        self.domains: Counter[str] = Counter()
        self.bytes_downloaded = 0
        self.max_pages_target: int | None = None

    def start(self, max_pages: int | None = None) -> None:
        self.reset()
        self.started_at = time.perf_counter()
        self.max_pages_target = max_pages

    def finish(self) -> None:
        self.finished_at = time.perf_counter()

    def record_success(self, url: str, *, status_code: int = 200, body_len: int = 0) -> None:
        self.successful += 1
        self.status_codes[status_code] += 1
        domain = urlparse(url).netloc.lower() or "unknown"
        self.domains[domain] += 1
        self.bytes_downloaded += body_len

    def record_failure(self, url: str, *, status_code: int | None = None) -> None:
        self.failed += 1
        if status_code is not None:
            self.status_codes[status_code] += 1
        domain = urlparse(url).netloc.lower() or "unknown"
        self.domains[domain] += 1

    def record_blocked(self, url: str) -> None:
        self.blocked += 1
        domain = urlparse(url).netloc.lower() or "unknown"
        self.domains[domain] += 1

    @property
    def total_pages(self) -> int:
        return self.successful + self.failed + self.blocked

    @property
    def elapsed_sec(self) -> float:
        if self.started_at is None:
            return 0.0
        end = self.finished_at if self.finished_at is not None else time.perf_counter()
        return max(end - self.started_at, 0.001)

    @property
    def pages_per_sec(self) -> float:
        return self.successful / self.elapsed_sec

    def eta_sec(self) -> float | None:
        if not self.max_pages_target or self.pages_per_sec <= 0:
            return None
        remaining = max(self.max_pages_target - self.successful, 0)
        return remaining / self.pages_per_sec

    def progress_pct(self) -> float | None:
        if not self.max_pages_target:
            return None
        return min(100.0, 100.0 * self.successful / max(self.max_pages_target, 1))

    def top_domains(self, n: int = 10) -> list[tuple[str, int]]:
        return self.domains.most_common(n)

    def snapshot(self) -> dict:
        return {
            "total_pages": self.total_pages,
            "successful": self.successful,
            "failed": self.failed,
            "blocked": self.blocked,
            "avg_speed_pages_per_sec": round(self.pages_per_sec, 3),
            "elapsed_sec": round(self.elapsed_sec, 3),
            "status_codes": {str(k): v for k, v in sorted(self.status_codes.items())},
            "top_domains": [{"domain": d, "pages": c} for d, c in self.top_domains()],
            "bytes_downloaded": self.bytes_downloaded,
            "progress_pct": None if self.progress_pct() is None else round(self.progress_pct(), 1),
            "eta_sec": None if self.eta_sec() is None else round(self.eta_sec(), 1),
            "generated_at": datetime.now(timezone.utc).isoformat(),
        }

    def export_to_json(self, filename: str, extra: dict | None = None) -> None:
        path = Path(filename)
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = self.snapshot()
        if extra:
            payload["extra"] = extra
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    def export_to_html_report(self, filename: str, extra: dict | None = None) -> None:
        path = Path(filename)
        path.parent.mkdir(parents=True, exist_ok=True)
        snap = self.snapshot()
        if extra:
            snap["extra"] = extra

        status_rows = "".join(
            f"<tr><td>{code}</td><td>{count}</td></tr>"
            for code, count in snap["status_codes"].items()
        ) or "<tr><td colspan='2'>n/a</td></tr>"

        domain_rows = "".join(
            f"<tr><td>{d['domain']}</td><td>{d['pages']}</td></tr>"
            for d in snap["top_domains"]
        ) or "<tr><td colspan='2'>n/a</td></tr>"

        # simple CSS bar chart for status codes
        max_status = max(self.status_codes.values(), default=1)
        bars = "".join(
            f'<div class="bar-row"><span class="label">{code}</span>'
            f'<div class="bar" style="width:{max(4, int(100 * count / max_status))}%"></div>'
            f"<span class='val'>{count}</span></div>"
            for code, count in sorted(self.status_codes.items())
        )

        max_dom = max((c for _, c in self.top_domains()), default=1)
        domain_bars = "".join(
            f'<div class="bar-row"><span class="label">{d}</span>'
            f'<div class="bar domain" style="width:{max(4, int(100 * c / max_dom))}%"></div>'
            f"<span class='val'>{c}</span></div>"
            for d, c in self.top_domains()
        )

        html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <title>Crawler report</title>
  <style>
    body {{ font-family: Georgia, serif; margin: 2rem; background: #f7f3ea; color: #1a1a1a; }}
    h1 {{ font-size: 1.8rem; }}
    .cards {{ display: flex; flex-wrap: wrap; gap: 1rem; margin: 1.5rem 0; }}
    .card {{ background: #fff; border: 1px solid #d9d0c0; padding: 1rem 1.2rem; min-width: 140px; }}
    .card strong {{ display: block; font-size: 1.4rem; }}
    table {{ border-collapse: collapse; width: 100%; max-width: 640px; background: #fff; }}
    th, td {{ border: 1px solid #d9d0c0; padding: 0.4rem 0.6rem; text-align: left; }}
    .bar-row {{ display: flex; align-items: center; gap: 0.5rem; margin: 0.35rem 0; }}
    .bar-row .label {{ width: 160px; font-size: 0.85rem; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }}
    .bar {{ height: 14px; background: #2f5d50; }}
    .bar.domain {{ background: #b85c38; }}
    .val {{ font-size: 0.85rem; }}
    pre {{ background: #fff; border: 1px solid #d9d0c0; padding: 1rem; overflow: auto; }}
  </style>
</head>
<body>
  <h1>Crawler report</h1>
  <p>Generated at {snap["generated_at"]}</p>
  <div class="cards">
    <div class="card"><span>Total</span><strong>{snap["total_pages"]}</strong></div>
    <div class="card"><span>Successful</span><strong>{snap["successful"]}</strong></div>
    <div class="card"><span>Failed</span><strong>{snap["failed"]}</strong></div>
    <div class="card"><span>Blocked</span><strong>{snap["blocked"]}</strong></div>
    <div class="card"><span>Speed</span><strong>{snap["avg_speed_pages_per_sec"]} p/s</strong></div>
    <div class="card"><span>Elapsed</span><strong>{snap["elapsed_sec"]} s</strong></div>
  </div>

  <h2>Status codes</h2>
  <div>{bars or "<p>n/a</p>"}</div>
  <table>
    <thead><tr><th>Code</th><th>Count</th></tr></thead>
    <tbody>{status_rows}</tbody>
  </table>

  <h2>Top domains</h2>
  <div>{domain_bars or "<p>n/a</p>"}</div>
  <table>
    <thead><tr><th>Domain</th><th>Pages</th></tr></thead>
    <tbody>{domain_rows}</tbody>
  </table>

  <h2>Raw JSON</h2>
  <pre>{json.dumps(snap, ensure_ascii=False, indent=2)}</pre>
</body>
</html>
"""
        path.write_text(html, encoding="utf-8")
