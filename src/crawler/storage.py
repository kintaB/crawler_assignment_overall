"""Async storage backends for crawled pages."""

from __future__ import annotations

import asyncio
import csv
import io
import json
import logging
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import aiofiles
import aiosqlite

logger = logging.getLogger(__name__)

STANDARD_FIELDS = (
    "url",
    "title",
    "text",
    "links",
    "metadata",
    "crawled_at",
    "status_code",
    "content_type",
)


def normalize_record(data: dict[str, Any]) -> dict[str, Any]:
    """Map parser/crawler page dict into the Day-6 standard shape."""
    meta = data.get("metadata") or {}
    if not isinstance(meta, dict):
        meta = {"value": meta}
    crawled_at = data.get("crawled_at")
    if crawled_at is None:
        crawled_at = datetime.now(timezone.utc).isoformat()
    elif isinstance(crawled_at, datetime):
        crawled_at = crawled_at.isoformat()

    links = data.get("links") or []
    if not isinstance(links, list):
        links = list(links)

    return {
        "url": str(data.get("url") or ""),
        "title": str(data.get("title") or ""),
        "text": str(data.get("text") or ""),
        "links": [str(x) for x in links],
        "metadata": meta,
        "crawled_at": crawled_at,
        "status_code": int(data.get("status_code") or meta.get("status_code") or 200),
        "content_type": str(
            data.get("content_type") or meta.get("content_type") or "text/html"
        ),
    }


class DataStorage(ABC):
    """Abstract async storage."""

    def __init__(self, max_save_retries: int = 3, save_retry_delay: float = 0.2) -> None:
        self.max_save_retries = max_save_retries
        self.save_retry_delay = save_retry_delay
        self.saved_count = 0
        self.save_errors = 0

    @abstractmethod
    async def save(self, data: dict) -> None:
        ...

    @abstractmethod
    async def close(self) -> None:
        ...

    async def save_safe(self, data: dict) -> bool:
        """Save with retries; never raise to caller (crawl continues)."""
        record = normalize_record(data)
        last: Exception | None = None
        for attempt in range(self.max_save_retries):
            try:
                await self.save(record)
                self.saved_count += 1
                return True
            except Exception as e:
                last = e
                self.save_errors += 1
                logger.warning(
                    "storage save failed attempt=%s/%s url=%s err=%s",
                    attempt + 1,
                    self.max_save_retries,
                    record.get("url"),
                    e,
                )
                if attempt + 1 < self.max_save_retries:
                    await asyncio.sleep(self.save_retry_delay * (2**attempt))
        logger.error("storage give up url=%s last=%s", record.get("url"), last)
        return False

    async def flush(self) -> None:
        """Flush buffered rows if any."""
        return None

    def get_stats(self) -> dict:
        return {
            "backend": type(self).__name__,
            "saved_count": self.saved_count,
            "save_errors": self.save_errors,
        }


class JSONStorage(DataStorage):
    """
    Append-friendly JSON storage.
    Uses JSON Lines (.jsonl) when path ends with .jsonl, otherwise a JSON array file.
    """

    def __init__(
        self,
        path: str | Path,
        *,
        pretty: bool = True,
        buffer_size: int = 1,
        max_save_retries: int = 3,
    ) -> None:
        super().__init__(max_save_retries=max_save_retries)
        self.path = Path(path)
        self.pretty = pretty
        self.buffer_size = max(1, buffer_size)
        self._buffer: list[dict] = []
        self._lock = asyncio.Lock()
        self._jsonl = self.path.suffix.lower() == ".jsonl"

    async def _ensure_parent(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)

    async def save(self, data: dict) -> None:
        record = normalize_record(data)
        async with self._lock:
            self._buffer.append(record)
            if len(self._buffer) >= self.buffer_size:
                await self._flush_unlocked()

    async def flush(self) -> None:
        async with self._lock:
            await self._flush_unlocked()

    async def _flush_unlocked(self) -> None:
        if not self._buffer:
            return
        batch = self._buffer
        self._buffer = []
        await self._ensure_parent()

        if self._jsonl:
            # line-by-line append — good for large volumes
            async with aiofiles.open(self.path, "a", encoding="utf-8") as f:
                for row in batch:
                    await f.write(json.dumps(row, ensure_ascii=False) + "\n")
            return

        existing: list[dict] = []
        if self.path.exists() and self.path.stat().st_size > 0:
            async with aiofiles.open(self.path, "r", encoding="utf-8") as f:
                raw = await f.read()
            if raw.strip():
                loaded = json.loads(raw)
                if isinstance(loaded, list):
                    existing = loaded
                elif isinstance(loaded, dict):
                    existing = [loaded]

        existing.extend(batch)
        text = json.dumps(
            existing,
            ensure_ascii=False,
            indent=2 if self.pretty else None,
        )
        async with aiofiles.open(self.path, "w", encoding="utf-8") as f:
            await f.write(text)

    async def close(self) -> None:
        await self.flush()

    async def read_all(self) -> list[dict]:
        await self.flush()
        if not self.path.exists():
            return []
        async with aiofiles.open(self.path, "r", encoding="utf-8") as f:
            raw = await f.read()
        if not raw.strip():
            return []
        if self._jsonl:
            return [json.loads(line) for line in raw.splitlines() if line.strip()]
        data = json.loads(raw)
        return data if isinstance(data, list) else [data]


class CSVStorage(DataStorage):
    def __init__(
        self,
        path: str | Path,
        *,
        encoding: str = "utf-8-sig",
        buffer_size: int = 10,
        max_save_retries: int = 3,
    ) -> None:
        super().__init__(max_save_retries=max_save_retries)
        self.path = Path(path)
        self.encoding = encoding
        self.buffer_size = max(1, buffer_size)
        self._buffer: list[dict] = []
        self._lock = asyncio.Lock()
        self._header_written = False

    async def save(self, data: dict) -> None:
        record = normalize_record(data)
        async with self._lock:
            self._buffer.append(record)
            if len(self._buffer) >= self.buffer_size:
                await self._flush_unlocked()

    async def flush(self) -> None:
        async with self._lock:
            await self._flush_unlocked()

    def _row_to_csv_dict(self, record: dict) -> dict[str, str]:
        return {
            "url": record["url"],
            "title": record["title"],
            "text": record["text"],
            "links": json.dumps(record["links"], ensure_ascii=False),
            "metadata": json.dumps(record["metadata"], ensure_ascii=False),
            "crawled_at": str(record["crawled_at"]),
            "status_code": str(record["status_code"]),
            "content_type": record["content_type"],
        }

    async def _flush_unlocked(self) -> None:
        if not self._buffer:
            return
        batch = self._buffer
        self._buffer = []
        self.path.parent.mkdir(parents=True, exist_ok=True)

        write_header = not self._header_written and (
            not self.path.exists() or self.path.stat().st_size == 0
        )
        buf = io.StringIO()
        writer = csv.DictWriter(
            buf,
            fieldnames=list(STANDARD_FIELDS),
            extrasaction="ignore",
            quoting=csv.QUOTE_MINIMAL,
        )
        if write_header:
            writer.writeheader()
            self._header_written = True
        else:
            self._header_written = True
        for record in batch:
            writer.writerow(self._row_to_csv_dict(record))

        async with aiofiles.open(self.path, "a", encoding=self.encoding, newline="") as f:
            await f.write(buf.getvalue())

    async def close(self) -> None:
        await self.flush()

    async def read_all(self) -> list[dict]:
        await self.flush()
        if not self.path.exists():
            return []
        async with aiofiles.open(self.path, "r", encoding=self.encoding, newline="") as f:
            raw = await f.read()
        reader = csv.DictReader(io.StringIO(raw))
        rows = []
        for row in reader:
            try:
                links = json.loads(row.get("links") or "[]")
            except json.JSONDecodeError:
                links = []
            try:
                metadata = json.loads(row.get("metadata") or "{}")
            except json.JSONDecodeError:
                metadata = {}
            rows.append(
                {
                    "url": row.get("url", ""),
                    "title": row.get("title", ""),
                    "text": row.get("text", ""),
                    "links": links,
                    "metadata": metadata,
                    "crawled_at": row.get("crawled_at", ""),
                    "status_code": int(row.get("status_code") or 0),
                    "content_type": row.get("content_type", ""),
                }
            )
        return rows


class SQLiteStorage(DataStorage):
    def __init__(
        self,
        path: str | Path,
        *,
        buffer_size: int = 20,
        max_save_retries: int = 3,
    ) -> None:
        super().__init__(max_save_retries=max_save_retries)
        self.path = Path(path)
        self.buffer_size = max(1, buffer_size)
        self._buffer: list[dict] = []
        self._lock = asyncio.Lock()
        self._db: aiosqlite.Connection | None = None

    async def init_db(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._db = await aiosqlite.connect(self.path)
        await self._db.execute(
            """
            CREATE TABLE IF NOT EXISTS pages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                url TEXT NOT NULL UNIQUE,
                title TEXT,
                text TEXT,
                links TEXT,
                metadata TEXT,
                crawled_at TEXT,
                status_code INTEGER,
                content_type TEXT
            )
            """
        )
        await self._db.execute(
            "CREATE INDEX IF NOT EXISTS idx_pages_crawled_at ON pages(crawled_at)"
        )
        await self._db.execute(
            "CREATE INDEX IF NOT EXISTS idx_pages_status ON pages(status_code)"
        )
        await self._db.commit()

    async def _ensure_db(self) -> aiosqlite.Connection:
        if self._db is None:
            await self.init_db()
        assert self._db is not None
        return self._db

    async def save(self, data: dict) -> None:
        record = normalize_record(data)
        async with self._lock:
            self._buffer.append(record)
            if len(self._buffer) >= self.buffer_size:
                await self._flush_unlocked()

    async def flush(self) -> None:
        async with self._lock:
            await self._flush_unlocked()

    async def _flush_unlocked(self) -> None:
        if not self._buffer:
            return
        batch = self._buffer
        self._buffer = []
        db = await self._ensure_db()
        rows = [
            (
                r["url"],
                r["title"],
                r["text"],
                json.dumps(r["links"], ensure_ascii=False),
                json.dumps(r["metadata"], ensure_ascii=False),
                r["crawled_at"],
                r["status_code"],
                r["content_type"],
            )
            for r in batch
        ]
        await db.executemany(
            """
            INSERT INTO pages (url, title, text, links, metadata, crawled_at, status_code, content_type)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(url) DO UPDATE SET
                title=excluded.title,
                text=excluded.text,
                links=excluded.links,
                metadata=excluded.metadata,
                crawled_at=excluded.crawled_at,
                status_code=excluded.status_code,
                content_type=excluded.content_type
            """,
            rows,
        )
        await db.commit()

    async def close(self) -> None:
        await self.flush()
        if self._db is not None:
            await self._db.close()
            self._db = None

    async def read_all(self) -> list[dict]:
        await self.flush()
        db = await self._ensure_db()
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT url, title, text, links, metadata, crawled_at, status_code, content_type FROM pages"
        ) as cur:
            out = []
            async for row in cur:
                out.append(
                    {
                        "url": row["url"],
                        "title": row["title"],
                        "text": row["text"],
                        "links": json.loads(row["links"] or "[]"),
                        "metadata": json.loads(row["metadata"] or "{}"),
                        "crawled_at": row["crawled_at"],
                        "status_code": row["status_code"],
                        "content_type": row["content_type"],
                    }
                )
            return out

    async def count(self) -> int:
        await self.flush()
        db = await self._ensure_db()
        async with db.execute("SELECT COUNT(*) FROM pages") as cur:
            row = await cur.fetchone()
            return int(row[0]) if row else 0


class MultiStorage(DataStorage):
    """Fan-out save to several backends (JSON + CSV + SQLite in demos)."""

    def __init__(self, storages: list[DataStorage]) -> None:
        super().__init__()
        self.storages = storages

    async def save(self, data: dict) -> None:
        record = normalize_record(data)

        async def _one(store: DataStorage) -> None:
            try:
                await store.save(record)
                store.saved_count += 1
            except Exception as e:
                store.save_errors += 1
                logger.warning("multi-storage child %s failed: %s", type(store).__name__, e)

        await asyncio.gather(*(_one(s) for s in self.storages))

    async def flush(self) -> None:
        await asyncio.gather(*(s.flush() for s in self.storages), return_exceptions=True)

    async def close(self) -> None:
        await self.flush()
        await asyncio.gather(*(s.close() for s in self.storages), return_exceptions=True)

    def get_stats(self) -> dict:
        return {
            "backend": "MultiStorage",
            "saved_count": self.saved_count,
            "save_errors": self.save_errors,
            "children": [s.get_stats() for s in self.storages],
        }
