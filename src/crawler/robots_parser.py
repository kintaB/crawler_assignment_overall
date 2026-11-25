import asyncio
import logging
from urllib.parse import urljoin, urlparse
from urllib.robotparser import RobotFileParser

import aiohttp

logger = logging.getLogger(__name__)


class RobotsParser:
    """Fetch and cache robots.txt rules per domain."""

    def __init__(self) -> None:
        self._parsers: dict[str, RobotFileParser] = {}
        self._rules: dict[str, dict] = {}
        self._lock = asyncio.Lock()
        self._session: aiohttp.ClientSession | None = None
        self.blocked_count = 0

    def set_session(self, session: aiohttp.ClientSession | None) -> None:
        self._session = session

    def _domain(self, url: str) -> str:
        return urlparse(url).netloc.lower()

    def _robots_url(self, base_url: str) -> str:
        parsed = urlparse(base_url)
        root = f"{parsed.scheme}://{parsed.netloc}"
        return urljoin(root + "/", "robots.txt")

    def _parse_rules_dict(self, text: str) -> dict:
        """Lightweight summary for demos / inspection."""
        agents: dict[str, dict] = {}
        current: list[str] = []
        for raw in text.splitlines():
            line = raw.split("#", 1)[0].strip()
            if not line:
                continue
            if ":" not in line:
                continue
            key, value = line.split(":", 1)
            key = key.strip().lower()
            value = value.strip()
            if key == "user-agent":
                current = [value]
                for ua in current:
                    agents.setdefault(ua, {"allow": [], "disallow": [], "crawl_delay": None})
            elif key == "allow" and current:
                for ua in current:
                    agents.setdefault(ua, {"allow": [], "disallow": [], "crawl_delay": None})
                    agents[ua]["allow"].append(value)
            elif key == "disallow" and current:
                for ua in current:
                    agents.setdefault(ua, {"allow": [], "disallow": [], "crawl_delay": None})
                    agents[ua]["disallow"].append(value)
            elif key == "crawl-delay" and current:
                try:
                    delay = float(value)
                except ValueError:
                    delay = None
                for ua in current:
                    agents.setdefault(ua, {"allow": [], "disallow": [], "crawl_delay": None})
                    agents[ua]["crawl_delay"] = delay
        return agents

    async def fetch_robots(self, base_url: str) -> dict:
        domain = self._domain(base_url)
        async with self._lock:
            if domain in self._rules:
                return self._rules[domain]

        robots_url = self._robots_url(base_url)
        text = ""
        owns_session = False
        session = self._session
        if session is None or session.closed:
            session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=15, connect=5)
            )
            owns_session = True

        try:
            async with session.get(robots_url) as response:
                if response.status == 200:
                    text = await response.text(errors="replace")
                else:
                    logger.info("robots.txt missing/unavailable %s status=%s", robots_url, response.status)
        except Exception as e:
            logger.warning("robots.txt fetch failed %s: %s", robots_url, e)
            text = ""
        finally:
            if owns_session:
                await session.close()

        rp = RobotFileParser()
        rp.set_url(robots_url)
        if text:
            rp.parse(text.splitlines())
        else:
            # empty / missing → allow all (common crawler default)
            rp.parse([])

        rules = self._parse_rules_dict(text) if text else {}
        async with self._lock:
            self._parsers[domain] = rp
            self._rules[domain] = rules
        logger.info("robots cached for %s (%s agents)", domain, len(rules))
        return rules

    async def ensure_robots(self, url: str) -> dict:
        domain = self._domain(url)
        if domain in self._rules:
            return self._rules[domain]
        parsed = urlparse(url)
        base = f"{parsed.scheme}://{parsed.netloc}"
        return await self.fetch_robots(base)

    def can_fetch(self, url: str, user_agent: str = "*") -> bool:
        domain = self._domain(url)
        rp = self._parsers.get(domain)
        if rp is None:
            # not loaded yet → allow; caller should ensure_robots first
            return True
        try:
            allowed = rp.can_fetch(user_agent, url)
        except Exception:
            allowed = True
        if not allowed:
            self.blocked_count += 1
            logger.info("robots disallow %s (ua=%s)", url, user_agent)
        return allowed

    def get_crawl_delay(self, user_agent: str = "*", domain: str | None = None) -> float:
        """Return Crawl-delay seconds for UA (falls back to *). 0 if none."""
        domains = [domain] if domain else list(self._rules.keys())
        for d in domains:
            rules = self._rules.get(d) or {}
            for ua in (user_agent, "*"):
                entry = rules.get(ua)
                if entry and entry.get("crawl_delay") is not None:
                    return float(entry["crawl_delay"])
            # RobotFileParser crawl_delay if available
            rp = self._parsers.get(d)
            if rp is not None:
                try:
                    delay = rp.crawl_delay(user_agent)
                    if delay is not None:
                        return float(delay)
                    delay = rp.crawl_delay("*")
                    if delay is not None:
                        return float(delay)
                except Exception:
                    pass
        return 0.0

    def get_stats(self) -> dict:
        return {
            "domains_cached": len(self._parsers),
            "blocked_urls": self.blocked_count,
        }
