import logging
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)


class HTMLParser:
    def _make_soup(self, html: str) -> BeautifulSoup:
        try:
            return BeautifulSoup(html, "lxml")
        except Exception as e:
            logger.warning("lxml failed (%s), fallback to html.parser", type(e).__name__)
            return BeautifulSoup(html, "html.parser")

    def _is_valid_url(self, url: str) -> bool:
        try:
            parsed = urlparse(url)
            return parsed.scheme in ("http", "https") and bool(parsed.netloc)
        except Exception:
            return False

    async def parse_html(self, html: str, url: str) -> dict:
        result = {
            "url": url,
            "title": "",
            "text": "",
            "links": [],
            "metadata": {},
            "images": [],
            "headings": [],
            "tables": [],
            "lists": [],
            "errors": [],
        }
        if not html:
            result["errors"].append("empty html")
            return result

        try:
            soup = self._make_soup(html)
        except Exception as e:
            logger.warning("parse failed for %s: %s", url, e)
            result["errors"].append(f"parse failed: {type(e).__name__}")
            return result

        try:
            result["metadata"] = self.extract_metadata(soup)
            result["title"] = result["metadata"].get("title", "")
        except Exception as e:
            logger.warning("metadata failed for %s: %s", url, e)
            result["errors"].append("metadata failed")

        try:
            result["text"] = self.extract_text(soup)
        except Exception as e:
            logger.warning("text failed for %s: %s", url, e)
            result["errors"].append("text failed")

        try:
            result["links"] = self.extract_links(soup, url)
        except Exception as e:
            logger.warning("links failed for %s: %s", url, e)
            result["errors"].append("links failed")

        try:
            result["images"] = self.extract_images(soup, url)
        except Exception as e:
            logger.warning("images failed for %s: %s", url, e)
            result["errors"].append("images failed")

        try:
            result["headings"] = self.extract_headings(soup)
        except Exception as e:
            logger.warning("headings failed for %s: %s", url, e)
            result["errors"].append("headings failed")

        try:
            result["tables"] = self.extract_tables(soup)
        except Exception as e:
            logger.warning("tables failed for %s: %s", url, e)
            result["errors"].append("tables failed")

        try:
            result["lists"] = self.extract_lists(soup)
        except Exception as e:
            logger.warning("lists failed for %s: %s", url, e)
            result["errors"].append("lists failed")

        return result

    def extract_links(
        self,
        soup: BeautifulSoup,
        base_url: str,
        same_host_only: bool = False,
    ) -> list[str]:
        base_host = urlparse(base_url).netloc
        links: list[str] = []
        seen: set[str] = set()

        for tag in soup.find_all("a", href=True):
            href = tag.get("href", "").strip()
            if not href or href.startswith(("#", "mailto:", "javascript:", "tel:")):
                continue
            absolute = urljoin(base_url, href)
            if not self._is_valid_url(absolute):
                continue
            if same_host_only and urlparse(absolute).netloc != base_host:
                continue
            if absolute not in seen:
                seen.add(absolute)
                links.append(absolute)
        return links

    def extract_text(self, soup: BeautifulSoup, selector: str | None = None) -> str:
        root = soup.select_one(selector) if selector else soup.body or soup
        if root is None:
            return ""
        for tag in root(["script", "style", "noscript"]):
            tag.decompose()
        text = root.get_text(separator=" ", strip=True)
        return " ".join(text.split())

    def extract_metadata(self, soup: BeautifulSoup) -> dict:
        title = ""
        if soup.title and soup.title.string:
            title = soup.title.string.strip()

        def meta_content(*names: str) -> str:
            for name in names:
                tag = soup.find("meta", attrs={"name": name}) or soup.find(
                    "meta", attrs={"property": name}
                )
                if tag and tag.get("content"):
                    return tag["content"].strip()
            return ""

        return {
            "title": title,
            "description": meta_content("description", "og:description"),
            "keywords": meta_content("keywords"),
        }

    def extract_images(self, soup: BeautifulSoup, base_url: str) -> list[dict]:
        images: list[dict] = []
        for img in soup.find_all("img"):
            src = (img.get("src") or "").strip()
            if not src:
                continue
            absolute = urljoin(base_url, src)
            images.append({"src": absolute, "alt": (img.get("alt") or "").strip()})
        return images

    def extract_headings(self, soup: BeautifulSoup) -> list[dict]:
        headings: list[dict] = []
        for level in ("h1", "h2", "h3"):
            for tag in soup.find_all(level):
                text = tag.get_text(" ", strip=True)
                if text:
                    headings.append({"level": level, "text": text})
        return headings

    def extract_tables(self, soup: BeautifulSoup) -> list[list[list[str]]]:
        tables: list[list[list[str]]] = []
        for table in soup.find_all("table"):
            rows: list[list[str]] = []
            for tr in table.find_all("tr"):
                cells = [
                    cell.get_text(" ", strip=True)
                    for cell in tr.find_all(["th", "td"])
                ]
                if cells:
                    rows.append(cells)
            if rows:
                tables.append(rows)
        return tables

    def extract_lists(self, soup: BeautifulSoup) -> list[dict]:
        lists: list[dict] = []
        for tag in soup.find_all(["ul", "ol"]):
            items = [li.get_text(" ", strip=True) for li in tag.find_all("li", recursive=False)]
            items = [item for item in items if item]
            if items:
                lists.append({"type": tag.name, "items": items})
        return lists
