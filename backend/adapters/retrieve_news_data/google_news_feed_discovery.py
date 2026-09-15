"""Discover publisher RSS/Atom feeds from a Google News keyword query.

Google News is used only as a bounded source of relevant publishers.  The
returned candidates are the publishers' own feeds, discovered from HTML feed
metadata (with a small conventional-path fallback) and validated before they
are exposed to an operator.
"""

from __future__ import annotations

import asyncio
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime
from html.parser import HTMLParser
from urllib.parse import urlencode, urljoin, urlsplit, urlunsplit

import feedparser
import httpx

from adapters.retrieve_news_data.feed_http import (
    UnsafeFeedUrl,
    fetch_discovery_url,
    validate_public_url,
)
from adapters.retrieve_news_data.google_news_rss_searcher import (
    RssFeedSearcher,
    decode_google_news_url,
)
from core.config import DEFAULT_USER_AGENT
from core.feed_urls import infer_feed_source_label
from core.logger import get_logger
from models.discovery_models import (
    DiscoveredEntry,
    FeedDiscoveryCandidate,
    FeedDiscoveryResult,
    FeedDiscoveryStats,
)
from models.storage_models import FeedPollTarget


logger = get_logger(__name__)

_GOOGLE_NEWS_SEARCH_URL = "https://news.google.com/rss/search"
_FEED_MIME_TYPES = frozenset(
    {
        "application/atom+xml",
        "application/rss+xml",
        "application/rdf+xml",
        "text/rss+xml",
    }
)
_COMMON_FEED_PATHS = ("feed", "rss", "rss.xml", "feed.xml", "atom.xml")


class FeedDiscoveryError(RuntimeError):
    """Raised when the Google News seed search itself cannot be completed."""


class _FeedLinkParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.links: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.casefold() != "link":
            return
        values = {name.casefold(): value or "" for name, value in attrs}
        rel = {part.casefold() for part in values.get("rel", "").split()}
        mime = values.get("type", "").split(";", 1)[0].strip().casefold()
        href = values.get("href", "").strip()
        if href and "alternate" in rel and mime in _FEED_MIME_TYPES:
            self.links.append(href)


@dataclass
class _Publisher:
    source_label: str
    website_url: str
    discovery_url: str
    article_url: str
    matched_articles: int
    sample_titles: list[str]
    first_seen: int
    last_published_at: datetime | None


class GoogleNewsFeedDiscovery:
    """Find and validate publisher feeds relevant to a free-text query.

    The class performs no persistence and has no TSV/OPML dependency.  Its
    output maps directly to ``FeedSeed`` once an operator selects candidates.
    Network work is bounded by the article, publisher and worker limits.
    """

    def __init__(
        self,
        http_client: httpx.AsyncClient | None = None,
        *,
        max_articles: int = 30,
        max_publishers: int = 12,
        max_concurrent_publishers: int = 4,
        max_feeds_per_publisher: int = 1,
        request_timeout_seconds: float = 8.0,
        user_agent: str = DEFAULT_USER_AGENT,
    ) -> None:
        if max_articles < 1:
            raise ValueError("max_articles must be at least 1")
        if max_publishers < 1:
            raise ValueError("max_publishers must be at least 1")
        if max_concurrent_publishers < 1:
            raise ValueError("max_concurrent_publishers must be at least 1")
        if max_feeds_per_publisher < 1:
            raise ValueError("max_feeds_per_publisher must be at least 1")
        self._http_client = http_client
        self._max_articles = max_articles
        self._max_publishers = max_publishers
        self._max_concurrent_publishers = max_concurrent_publishers
        self._max_feeds_per_publisher = max_feeds_per_publisher
        self._request_timeout_seconds = request_timeout_seconds
        self._user_agent = user_agent or DEFAULT_USER_AGENT
        self._resolve_dns = http_client is None

    @staticmethod
    def build_search_feed_url(
        query: str,
        *,
        language: str = "en",
        country: str = "US",
    ) -> str:
        cleaned_query = query.strip()
        if not cleaned_query:
            raise ValueError("query must not be blank")
        cleaned_language = language.strip().lower()
        cleaned_country = country.strip().upper()
        if not cleaned_language or not cleaned_country:
            raise ValueError("language and country must not be blank")
        params = urlencode(
            {
                "q": cleaned_query,
                "hl": f"{cleaned_language}-{cleaned_country}",
                "gl": cleaned_country,
                "ceid": f"{cleaned_country}:{cleaned_language}",
            }
        )
        return f"{_GOOGLE_NEWS_SEARCH_URL}?{params}"

    async def discover(
        self,
        query: str,
        *,
        language: str = "en",
        country: str = "US",
        limit: int | None = None,
    ) -> FeedDiscoveryResult:
        """Return ranked, validated feeds for publishers matching ``query``."""
        search_url = self.build_search_feed_url(
            query, language=language, country=country
        )
        result_limit = self._max_publishers if limit is None else limit
        if result_limit < 1:
            raise ValueError("limit must be at least 1")

        own_client = self._http_client is None
        client = self._http_client or httpx.AsyncClient(
            follow_redirects=True,
            headers={"User-Agent": self._user_agent},
        )
        try:
            entries = await self._search_entries(search_url, client)
            entries = entries[: self._max_articles]
            publishers = await self._publishers(entries)
            selected_publishers = publishers[: self._max_publishers]
            candidates = await self._discover_publishers(
                selected_publishers, client
            )
            candidates.sort(
                key=lambda item: (
                    -item[2],
                    item[1],
                    item[0].source_label.casefold(),
                    item[0].feed_url,
                )
            )
            unique: list[FeedDiscoveryCandidate] = []
            seen_urls: set[str] = set()
            for candidate, _, _ in candidates:
                key = candidate.feed_url.casefold()
                if key in seen_urls:
                    continue
                seen_urls.add(key)
                unique.append(candidate)
                if len(unique) >= result_limit:
                    break
            return FeedDiscoveryResult(
                query=query.strip(),
                candidates=tuple(unique),
                stats=FeedDiscoveryStats(
                    search_results=len(entries),
                    publishers_inspected=len(selected_publishers),
                    pages_inspected=len(selected_publishers),
                    feeds_validated=len(unique),
                    google_fallback_used=True,
                ),
            )
        finally:
            if own_client:
                await client.aclose()

    async def _search_entries(
        self, search_url: str, client: httpx.AsyncClient
    ) -> list[DiscoveredEntry]:
        searcher = RssFeedSearcher(
            http_client=client,
            max_concurrent_feeds=1,
            feed_fetch_timeout_seconds=self._request_timeout_seconds,
        )
        target = FeedPollTarget(
            feed_url=search_url,
            source_label="Google News keyword search",
            etag=None,
            last_modified=None,
        )
        polls = [poll async for poll in searcher.poll_feeds([target])]
        poll = polls[0]
        if poll.status == "failed":
            assert poll.error is not None
            raise FeedDiscoveryError(poll.error.error_message)
        return poll.entries

    async def _publishers(
        self, entries: list[DiscoveredEntry]
    ) -> list[_Publisher]:
        missing_source = [entry for entry in entries if not entry.source_url]
        decoded_by_entry: dict[str, str] = {}
        if missing_source:
            entry_iterator = iter(missing_source)

            async def decode_worker() -> None:
                for entry in entry_iterator:
                    decoded_by_entry[entry.entry_url] = (
                        await decode_google_news_url(entry.entry_url)
                    )

            workers = [
                asyncio.create_task(decode_worker())
                for _ in range(min(2, len(missing_source)))
            ]
            await asyncio.gather(*workers)

        grouped: dict[str, _Publisher] = {}
        for index, entry in enumerate(entries):
            article_url = decoded_by_entry.get(entry.entry_url, entry.entry_url)
            source_url = self._external_url(entry.source_url)
            website_url = self._website_url(source_url or article_url)
            if not website_url:
                continue
            key = self._publisher_key(website_url)
            if not key:
                continue
            current = grouped.get(key)
            if current is None:
                grouped[key] = _Publisher(
                    source_label=(
                        entry.source.strip()
                        or infer_feed_source_label(website_url)
                    ),
                    website_url=website_url,
                    discovery_url=source_url or website_url,
                    article_url=article_url,
                    matched_articles=1,
                    sample_titles=[entry.title] if entry.title else [],
                    first_seen=index,
                    last_published_at=entry.published_at,
                )
                continue
            current.matched_articles += 1
            if entry.title and len(current.sample_titles) < 3:
                current.sample_titles.append(entry.title)
            if entry.published_at is not None and (
                current.last_published_at is None
                or entry.published_at > current.last_published_at
            ):
                current.last_published_at = entry.published_at
        return sorted(grouped.values(), key=lambda item: item.first_seen)

    async def _discover_publishers(
        self,
        publishers: list[_Publisher],
        client: httpx.AsyncClient,
    ) -> list[tuple[FeedDiscoveryCandidate, int, int]]:
        publisher_iterator = iter(publishers)
        results: list[tuple[FeedDiscoveryCandidate, int, int]] = []

        async def worker() -> None:
            for publisher in publisher_iterator:
                found = await self._discover_publisher(publisher, client)
                results.extend(
                    (candidate, publisher.first_seen, publisher.matched_articles)
                    for candidate in found
                )

        workers = [
            asyncio.create_task(worker())
            for _ in range(min(self._max_concurrent_publishers, len(publishers)))
        ]
        if workers:
            await asyncio.gather(*workers)
        return results

    async def _discover_publisher(
        self, publisher: _Publisher, client: httpx.AsyncClient
    ) -> list[FeedDiscoveryCandidate]:
        candidate_urls: list[str] = []
        page_urls = self._dedupe(
            (
                publisher.discovery_url,
                self._external_url(publisher.article_url),
            )
        )
        for page_url in page_urls:
            try:
                response = await fetch_discovery_url(
                    client,
                    page_url,
                    timeout_seconds=self._request_timeout_seconds,
                    user_agent=self._user_agent,
                    resolve_dns=self._resolve_dns,
                )
            except (httpx.HTTPError, ValueError, UnsafeFeedUrl) as exc:
                logger.debug(
                    "Feed discovery page fetch failed for %s: %s",
                    page_url,
                    exc,
                )
                continue
            if self._feed_version(self._parsed_feed(response.body)):
                candidate_urls.append(response.url)
                break
            parser = _FeedLinkParser()
            try:
                parser.feed(response.body.decode("utf-8", errors="replace"))
            except ValueError:
                continue
            candidate_urls.extend(
                urljoin(response.url, href) for href in parser.links
            )
            if parser.links:
                break

        homepage = self._website_url(publisher.website_url)
        candidate_urls.extend(urljoin(homepage, path) for path in _COMMON_FEED_PATHS)

        found: list[FeedDiscoveryCandidate] = []
        for candidate_url in self._dedupe(candidate_urls):
            validated = await self._validate_feed(candidate_url, client)
            if validated is None:
                continue
            feed_url, feed_title = validated
            found.append(
                FeedDiscoveryCandidate(
                    feed_url=feed_url,
                    source_label=feed_title or publisher.source_label,
                    website_url=publisher.website_url,
                    feed_scope="sitewide",
                    discovery_source="google_fallback",
                    relevance_reasons=(
                        f"Publisher appeared in {publisher.matched_articles} matching Google News article(s)",
                    ),
                    sample_titles=tuple(publisher.sample_titles),
                    last_published_at=publisher.last_published_at,
                )
            )
            if len(found) >= self._max_feeds_per_publisher:
                break
        return found

    async def _validate_feed(
        self, url: str, client: httpx.AsyncClient
    ) -> tuple[str, str] | None:
        try:
            validated_url = validate_public_url(url)
            response = await fetch_discovery_url(
                client,
                validated_url,
                timeout_seconds=self._request_timeout_seconds,
                user_agent=self._user_agent,
                resolve_dns=self._resolve_dns,
            )
            parsed = self._parsed_feed(response.body)
            if not self._feed_version(parsed) or not parsed.entries:
                return None
            final_url = validate_public_url(response.url)
        except (httpx.HTTPError, ValueError, UnsafeFeedUrl):
            return None
        title = (parsed.feed.get("title") or "").strip() if parsed.feed else ""
        return final_url, title

    @staticmethod
    def _parsed_feed(body: str):
        return feedparser.parse(body)

    @staticmethod
    def _feed_version(parsed) -> str:
        """Return the detected feed version, or ``""`` when there is none.

        ``feedparser`` omits the ``version`` key entirely for a body it cannot
        sniff at all (an empty response, most notably), and attribute access on
        a missing key raises ``AttributeError``.  Discovery probes arbitrary
        publisher URLs, so a blank ``200`` at ``/feed`` is routine input, not an
        error.
        """
        return parsed.get("version") or ""

    @staticmethod
    def _website_url(url: str) -> str:
        validated = GoogleNewsFeedDiscovery._external_url(url)
        if not validated:
            return ""
        parts = urlsplit(validated)
        return urlunsplit((parts.scheme, parts.netloc, "/", "", ""))

    @staticmethod
    def _external_url(url: str) -> str:
        try:
            validated = validate_public_url(url)
        except (ValueError, UnsafeFeedUrl):
            return ""
        parts = urlsplit(validated)
        if parts.hostname in {"news.google.com", "google.com", "www.google.com"}:
            return ""
        return urlunsplit((parts.scheme, parts.netloc, parts.path or "/", "", ""))

    @staticmethod
    def _publisher_key(url: str) -> str:
        hostname = (urlsplit(url).hostname or "").casefold().rstrip(".")
        return hostname.removeprefix("www.")

    @staticmethod
    def _dedupe(urls: Iterable[str]) -> list[str]:
        result: list[str] = []
        seen: set[str] = set()
        for url in urls:
            if not url:
                continue
            key = url.casefold()
            if key in seen:
                continue
            seen.add(key)
            result.append(url)
        return result
