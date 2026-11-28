"""Load crawler settings from YAML or JSON."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


DEFAULT_CONFIG: dict[str, Any] = {
    "start_urls": [],
    "sitemaps": [],
    "max_pages": 100,
    "max_depth": 2,
    "max_concurrent": 5,
    "per_domain_limit": 3,
    "requests_per_second": 2.0,
    "respect_robots": True,
    "min_delay": 0.0,
    "jitter": 0.0,
    "user_agent": "AsyncCrawler/1.0",
    "same_domain_only": True,
    "exclude_patterns": [],
    "include_patterns": [],
    "storage": {
        "json": None,
        "csv": None,
        "sqlite": None,
    },
    "logging": {
        "level": "INFO",
        "file": "logs/crawler.log",
    },
    "output": {
        "stats_json": "data/day7/stats.json",
        "html_report": "data/day7/report.html",
    },
}


def _deep_merge(base: dict, override: dict) -> dict:
    out = dict(base)
    for k, v in override.items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = _deep_merge(out[k], v)
        else:
            out[k] = v
    return out


def load_config(path: str | Path | None) -> dict[str, Any]:
    cfg = json.loads(json.dumps(DEFAULT_CONFIG))  # deep copy via json
    if path is None:
        return cfg
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"config not found: {p}")
    text = p.read_text(encoding="utf-8")
    suffix = p.suffix.lower()
    if suffix in (".yaml", ".yml"):
        try:
            import yaml
        except ImportError as e:
            raise ImportError("PyYAML required for YAML configs: pip install pyyaml") from e
        data = yaml.safe_load(text) or {}
    elif suffix == ".json":
        data = json.loads(text)
    else:
        # try JSON then YAML
        try:
            data = json.loads(text)
        except json.JSONDecodeError:
            import yaml

            data = yaml.safe_load(text) or {}
    if not isinstance(data, dict):
        raise ValueError("config root must be a mapping/object")
    return _deep_merge(cfg, data)
