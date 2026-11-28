"""Async sitemap.xml / sitemap index parser."""

from __future__ import annotations

import logging
from urllib.parse import urljoin, urlparse
from xml.etree import ElementTree as ET

import aiohttp

logger = logging.getLogger(__name__)

# sitemap XML often uses this namespace
_NS = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}


class SitemapParser:
    def __init__(
        self,
        session: aiohttp.ClientSession | None = None,
        *,
        max_sitemaps: int = 50,
        timeout: float = 30.0,
    ) -> None:
        self._session = session
        self.max_sitemaps = max_sitemaps
        self.timeout = timeout
        self._visited_sitemaps: set[str] = set()

    def set_session(self, session: aiohttp.ClientSession | None) -> None:
        self._session = session

    async def _get_text(self, url: str) -> str:
        owns = False
        session = self._session
        if session is None or session.closed:
            session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=self.timeout)
            )
            owns = True
        try:
            async with session.get(url) as resp:
                if resp.status != 200:
                    logger.warning("sitemap HTTP %s for %s", resp.status, url)
                    return ""
                return await resp.text(errors="replace")
        except Exception as e:
            logger.warning("sitemap fetch failed %s: %s", url, e)
            return ""
        finally:
            if owns:
                await session.close()

    def _local(self, tag: str) -> str:
        if "}" in tag:
            return tag.rsplit("}", 1)[-1]
        return tag

    def _parse_urls_structured(self, root: ET.Element) -> tuple[list[str], list[str]]:
        root_name = self._local(root.tag).lower()
        pages: list[str] = []
        children: list[str] = []

        if root_name == "sitemapindex":
            for sm in root:
                if self._local(sm.tag).lower() != "sitemap":
                    continue
                for child in sm:
                    if self._local(child.tag).lower() == "loc" and child.text:
                        children.append(child.text.strip())
        else:
            # urlset or unknown — collect url/loc
            for url_el in root:
                if self._local(url_el.tag).lower() != "url":
                    # also accept flat loc under root
                    if self._local(url_el.tag).lower() == "loc" and url_el.text:
                        pages.append(url_el.text.strip())
                    continue
                for child in url_el:
                    if self._local(child.tag).lower() == "loc" and child.text:
                        pages.append(child.text.strip())
        return pages, children

    async def fetch_sitemap(self, sitemap_url: str) -> list[str]:
        """Fetch sitemap or sitemap index; return flat list of page URLs."""
        self._visited_sitemaps.clear()
        return await self._fetch_recursive(sitemap_url)

    async def _fetch_recursive(self, sitemap_url: str) -> list[str]:
        if sitemap_url in self._visited_sitemaps:
            return []
        if len(self._visited_sitemaps) >= self.max_sitemaps:
            logger.warning("sitemap recursion limit (%s) reached", self.max_sitemaps)
            return []
        self._visited_sitemaps.add(sitemap_url)

        text = await self._get_text(sitemap_url)
        if not text.strip():
            return []

        try:
            root = ET.fromstring(text)
        except ET.ParseError as e:
            logger.warning("sitemap XML parse error %s: %s", sitemap_url, e)
            return []

        pages, children = self._parse_urls_structured(root)
        logger.info(
            "sitemap %s → %s urls, %s child sitemaps",
            sitemap_url,
            len(pages),
            len(children),
        )

        out = list(pages)
        for child in children:
            child_url = urljoin(sitemap_url, child)
            out.extend(await self._fetch_recursive(child_url))

        # de-dupe preserve order
        seen: set[str] = set()
        unique: list[str] = []
        for u in out:
            if u not in seen:
                seen.add(u)
                unique.append(u)
        return unique

    @staticmethod
    def discover_sitemap_url(base_url: str) -> str:
        parsed = urlparse(base_url)
        root = f"{parsed.scheme}://{parsed.netloc}"
        return urljoin(root + "/", "sitemap.xml")
