"""Feed-first discovery for publications focused on a requested topic."""

from __future__ import annotations

import asyncio
import calendar
from collections import Counter
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from html.parser import HTMLParser
import re
from typing import Literal, Protocol
from urllib.parse import parse_qs, urljoin, urlsplit, urlunsplit

import feedparser
import httpx
import tldextract

from adapters.retrieve_news_data.feed_http import (
    UnsafeFeedUrl,
    fetch_discovery_url,
    validate_public_url,
)
from adapters.retrieve_news_data.google_news_feed_discovery import (
    FeedDiscoveryError,
    GoogleNewsFeedDiscovery,
)
from core.config import DEFAULT_USER_AGENT
from core.feed_urls import infer_feed_source_label
from core.logger import get_logger
from models.discovery_models import (
    FeedDiscoveryCandidate,
    FeedDiscoveryResult,
    FeedDiscoveryStats,
)
from models.search_and_crawl_models import SearchResult


logger = get_logger(__name__)

_extract_domain = tldextract.TLDExtract(suffix_list_urls=(), cache_dir=None)
_FEED_MIME_TYPES = frozenset(
    {
        "application/atom+xml",
        "application/rss+xml",
        "application/rdf+xml",
        "application/feed+json",
        "text/rss+xml",
        "text/xml",
    }
)
_FEED_HINT_RE = re.compile(r"(?:^|[\W_])(rss|atom|feed|syndication)(?:$|[\W_])", re.I)
_FEED_INDEX_HINT_RE = re.compile(
    r"(?:rss|atom|feed|syndication).*(?:list|directory|all|available)|"
    r"(?:list|directory|all|available).*(?:rss|atom|feed|syndication)",
    re.I,
)
_TOKEN_RE = re.compile(r"[\w]+", re.UNICODE)
_STOP_WORDS = frozenset(
    {
        "a", "an", "and", "about", "for", "from", "in", "of", "on", "or",
        "the", "to", "with", "news", "feed", "feeds", "rss", "atom",
    }
)
_COMMON_FEED_PATHS = ("feed", "rss", "rss.xml", "feed.xml", "atom.xml")
_COUNTRY_NAMES = {
    "AU": "australia",
    "CA": "canada",
    "DE": "germany",
    "FR": "france",
    "GB": "united kingdom",
    "IN": "india",
    "JP": "japan",
    "KR": "south korea",
    "SG": "singapore",
    "US": "united states",
    "VN": "vietnam",
}


class FeedSearchProvider(Protocol):
    async def search(
        self,
        query: str,
        max_results: int = 10,
        *,
        search_depth: str = "basic",
        topic: str = "general",
        country: str | None = None,
    ) -> list[SearchResult]: ...


@dataclass(frozen=True)
class _SearchSeed:
    url: str
    title: str
    snippet: str
    score: float
    intent: Literal["direct", "publisher", "both"]


@dataclass(frozen=True)
class _Link:
    url: str
    label: str


@dataclass(frozen=True)
class _Page:
    url: str
    title: str
    description: str
    feed_links: tuple[_Link, ...]
    index_links: tuple[str, ...]


@dataclass(frozen=True)
class _ValidatedFeed:
    url: str
    title: str
    website_url: str
    subtitle: str
    categories: tuple[str, ...]
    sample_titles: tuple[str, ...]
    last_published_at: datetime | None


@dataclass(frozen=True)
class _RankedCandidate:
    candidate: FeedDiscoveryCandidate
    provider_score: float
    evidence_count: int


@dataclass(frozen=True)
class _Inspection:
    candidates: tuple[_RankedCandidate, ...]
    pages_inspected: int
    feeds_validated: int


class _DiscoveryHtmlParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.title = ""
        self.description = ""
        self.alternates: list[tuple[str, str]] = []
        self.anchors: list[tuple[str, str]] = []
        self._in_title = False
        self._anchor_href = ""
        self._anchor_label: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        values = {name.casefold(): value or "" for name, value in attrs}
        normalized_tag = tag.casefold()
        if normalized_tag == "title":
            self._in_title = True
            return
        if normalized_tag == "meta":
            name = values.get("name", "").casefold()
            prop = values.get("property", "").casefold()
            if name == "description" or prop == "og:description":
                self.description = self.description or values.get("content", "").strip()
            return
        if normalized_tag == "link":
            rel = {part.casefold() for part in values.get("rel", "").split()}
            mime = values.get("type", "").split(";", 1)[0].strip().casefold()
            href = values.get("href", "").strip()
            if href and "alternate" in rel and mime in _FEED_MIME_TYPES:
                self.alternates.append((href, values.get("title", "").strip()))
            return
        if normalized_tag == "a":
            self._anchor_href = values.get("href", "").strip()
            self._anchor_label = [values.get("title", "").strip()]

    def handle_data(self, data: str) -> None:
        if self._in_title:
            self.title += data
        if self._anchor_href:
            self._anchor_label.append(data)

    def handle_endtag(self, tag: str) -> None:
        normalized_tag = tag.casefold()
        if normalized_tag == "title":
            self._in_title = False
        if normalized_tag == "a" and self._anchor_href:
            self.anchors.append(
                (self._anchor_href, " ".join(self._anchor_label).strip())
            )
            self._anchor_href = ""
            self._anchor_label = []


class TopicFeedDiscovery:
    """Discover feeds directly from topic-relevant web and publisher pages."""

    def __init__(
        self,
        search_provider: FeedSearchProvider,
        *,
        google_fallback: GoogleNewsFeedDiscovery | None = None,
        http_client: httpx.AsyncClient | None = None,
        max_search_results_per_intent: int = 10,
        max_search_results: int = 20,
        max_publishers: int = 15,
        max_seed_pages_per_host: int = 2,
        max_concurrent_pages: int = 4,
        max_feeds_per_publisher: int = 3,
        max_results_per_publisher: int = 2,
        fallback_min_candidates: int = 6,
        request_timeout_seconds: float = 8.0,
        user_agent: str = DEFAULT_USER_AGENT,
        resolve_dns: bool | None = None,
    ) -> None:
        self._search_provider = search_provider
        self._google_fallback = google_fallback
        self._http_client = http_client
        self._max_search_results_per_intent = max_search_results_per_intent
        self._max_search_results = max_search_results
        self._max_publishers = max_publishers
        self._max_seed_pages_per_host = max_seed_pages_per_host
        self._max_concurrent_pages = max_concurrent_pages
        self._max_feeds_per_publisher = max_feeds_per_publisher
        self._max_results_per_publisher = max_results_per_publisher
        self._fallback_min_candidates = fallback_min_candidates
        self._request_timeout_seconds = request_timeout_seconds
        self._user_agent = user_agent or DEFAULT_USER_AGENT
        self._resolve_dns = http_client is None if resolve_dns is None else resolve_dns

    async def discover(
        self,
        query: str,
        *,
        language: str = "en",
        country: str = "US",
        limit: int | None = None,
    ) -> FeedDiscoveryResult:
        cleaned_query = query.strip()
        if not cleaned_query:
            raise ValueError("query must not be blank")
        result_limit = 12 if limit is None else limit
        if result_limit < 1:
            raise ValueError("limit must be at least 1")

        own_client = self._http_client is None
        client = self._http_client or httpx.AsyncClient()
        warnings: list[str] = []
        try:
            seeds, search_errors = await self._search_seeds(
                cleaned_query, language=language, country=country
            )
            warnings.extend(search_errors)
            inspection = await self._inspect_seeds(seeds, cleaned_query, client)
            ranked = list(inspection.candidates)
            feed_first = self._select_diverse(ranked, result_limit)

            fallback_used = False
            fallback_stats = FeedDiscoveryStats()
            fallback_threshold = min(self._fallback_min_candidates, result_limit)
            if len(feed_first) < fallback_threshold and self._google_fallback is not None:
                fallback_used = True
                warnings.append(
                    "Feed-first discovery returned fewer than "
                    f"{fallback_threshold} candidates; Google News fallback was used."
                )
                try:
                    fallback = await self._google_fallback.discover(
                        cleaned_query,
                        language=language,
                        country=country,
                        limit=result_limit,
                    )
                    fallback_stats = fallback.stats
                    warnings.extend(fallback.warnings)
                    ranked.extend(
                        _RankedCandidate(item, 0.0, len(item.relevance_reasons))
                        for item in fallback.candidates
                    )
                except FeedDiscoveryError as exc:
                    warnings.append(f"Google News fallback failed: {exc}")

            selected = self._select_diverse(ranked, result_limit)
            if not seeds and search_errors and not selected:
                raise FeedDiscoveryError("; ".join(search_errors))

            return FeedDiscoveryResult(
                query=cleaned_query,
                candidates=tuple(selected),
                stats=FeedDiscoveryStats(
                    search_results=len(seeds),
                    publishers_inspected=len({self._host(seed.url) for seed in seeds}),
                    pages_inspected=(
                        inspection.pages_inspected + fallback_stats.pages_inspected
                    ),
                    feeds_validated=(
                        inspection.feeds_validated + fallback_stats.feeds_validated
                    ),
                    google_fallback_used=fallback_used,
                ),
                warnings=tuple(self._dedupe(warnings)),
            )
        finally:
            if own_client:
                await client.aclose()

    async def _search_seeds(
        self, query: str, *, language: str, country: str
    ) -> tuple[list[_SearchSeed], list[str]]:
        direct_query = (
            f"RSS and Atom feeds focused on {query}, including category and topic feeds"
            # f"prefer sources publishing in language {language}"
        )
        publisher_query = (
            f"specialist news publications and blogs focused primarily on {query}"
            # f"prefer sources publishing in language {language}"
        )
        localized_country = _COUNTRY_NAMES.get(country.strip().upper())

        async def run(intent: Literal["direct", "publisher"], search_query: str):
            try:
                results = await self._search_provider.search(
                    search_query,
                    max_results=self._max_search_results_per_intent,
                    search_depth="basic",
                    topic="general",
                    country=localized_country,
                )
                return intent, results, None
            except Exception as exc:
                logger.warning("Tavily %s feed search failed: %s", intent, exc)
                return intent, [], f"Tavily {intent} search failed: {exc}"

        outcomes = await asyncio.gather(
            run("direct", direct_query), run("publisher", publisher_query)
        )
        seeds: list[_SearchSeed] = []
        errors: list[str] = []
        for intent, results, error in outcomes:
            if error:
                errors.append(error)
            for result in results:
                try:
                    url = validate_public_url(result.url)
                except (ValueError, UnsafeFeedUrl):
                    continue
                seeds.append(
                    _SearchSeed(
                        url=url,
                        title=result.title,
                        snippet=result.content_snippet,
                        score=float(result.score or 0.0),
                        intent=intent,
                    )
                )

        merged_by_url: dict[str, _SearchSeed] = {}
        for seed in seeds:
            key = seed.url.casefold()
            current = merged_by_url.get(key)
            if current is None:
                merged_by_url[key] = seed
                continue
            merged_by_url[key] = _SearchSeed(
                url=current.url,
                title=" · ".join(self._dedupe((current.title, seed.title))),
                snippet=" ".join(self._dedupe((current.snippet, seed.snippet))),
                score=max(current.score, seed.score),
                intent=current.intent if current.intent == seed.intent else "both",
            )
        seeds = list(merged_by_url.values())
        intent_order = {"direct": 0, "both": 1, "publisher": 2}
        seeds.sort(key=lambda item: (-item.score, intent_order[item.intent], item.url))
        selected: list[_SearchSeed] = []
        seen_urls: set[str] = set()
        host_counts: Counter[str] = Counter()
        accepted_hosts: set[str] = set()
        for seed in seeds:
            key = seed.url.casefold()
            host = self._host(seed.url)
            if key in seen_urls or not host:
                continue
            if host not in accepted_hosts and len(accepted_hosts) >= self._max_publishers:
                continue
            if host_counts[host] >= self._max_seed_pages_per_host:
                continue
            seen_urls.add(key)
            accepted_hosts.add(host)
            host_counts[host] += 1
            selected.append(seed)
            if len(selected) >= self._max_search_results:
                break
        return selected, errors

    async def _inspect_seeds(
        self,
        seeds: list[_SearchSeed],
        query: str,
        client: httpx.AsyncClient,
    ) -> _Inspection:
        iterator = iter(seeds)
        inspections: list[_Inspection] = []

        async def worker() -> None:
            for seed in iterator:
                inspections.append(await self._inspect_seed(seed, query, client))

        workers = [
            asyncio.create_task(worker())
            for _ in range(min(self._max_concurrent_pages, len(seeds)))
        ]
        if workers:
            await asyncio.gather(*workers)
        return _Inspection(
            candidates=tuple(
                candidate
                for inspection in inspections
                for candidate in inspection.candidates
            ),
            pages_inspected=sum(item.pages_inspected for item in inspections),
            feeds_validated=sum(item.feeds_validated for item in inspections),
        )

    async def _inspect_seed(
        self, seed: _SearchSeed, query: str, client: httpx.AsyncClient
    ) -> _Inspection:
        pages_inspected = 0
        candidate_links: list[_Link] = []
        page: _Page | None = None
        try:
            response = await self._fetch(client, seed.url)
            pages_inspected += 1
            parsed = feedparser.parse(response.body)
            if parsed.get("version") or "":
                candidate_links.append(_Link(seed.url, seed.title))
            else:
                page = self._parse_page(response.url, response.body)
                candidate_links.extend(page.feed_links)
                candidate_links.extend(self._cms_candidates(page.url))
                if seed.intent in {"publisher", "both"}:
                    root = self._website_url(page.url)
                    candidate_links.extend(
                        _Link(urljoin(root, path), path) for path in _COMMON_FEED_PATHS
                    )
                if page.index_links:
                    index_url = page.index_links[0]
                    try:
                        index_response = await self._fetch(client, index_url)
                        pages_inspected += 1
                        index_page = self._parse_page(
                            index_response.url, index_response.body
                        )
                        candidate_links.extend(index_page.feed_links)
                    except (httpx.HTTPError, ValueError, UnsafeFeedUrl):
                        pass
        except (httpx.HTTPError, ValueError, UnsafeFeedUrl) as exc:
            logger.debug("Feed seed fetch failed for %s: %s", seed.url, exc)
            return _Inspection((), pages_inspected, 0)

        found: list[_RankedCandidate] = []
        validated_urls: set[str] = set()
        for link in self._dedupe_links(candidate_links):
            validated = await self._validate_feed(link.url, client)
            if validated is None:
                continue
            key = validated.url.casefold()
            if key in validated_urls:
                continue
            validated_urls.add(key)
            scope = self._feed_scope(validated.url, link.label)
            reasons = self._relevance_reasons(
                query=query,
                seed=seed,
                page=page,
                link=link,
                feed=validated,
                scope=scope,
            )
            if not self._is_relevant(seed, scope, reasons):
                continue
            website_url = validated.website_url or self._website_url(validated.url)
            found.append(
                _RankedCandidate(
                    candidate=FeedDiscoveryCandidate(
                        feed_url=validated.url,
                        source_label=(
                            validated.title or infer_feed_source_label(validated.url)
                        ),
                        website_url=website_url,
                        feed_scope=scope,
                        discovery_source=self._candidate_source(seed.intent, scope),
                        relevance_reasons=tuple(reasons),
                        sample_titles=validated.sample_titles,
                        last_published_at=validated.last_published_at,
                    ),
                    provider_score=seed.score,
                    evidence_count=len(reasons),
                )
            )
            if len(found) >= self._max_feeds_per_publisher:
                break
        return _Inspection(tuple(found), pages_inspected, len(validated_urls))

    async def _validate_feed(
        self, url: str, client: httpx.AsyncClient
    ) -> _ValidatedFeed | None:
        try:
            response = await self._fetch(client, url)
            parsed = feedparser.parse(response.body)
            if not (parsed.get("version") or "") or not parsed.entries:
                return None
            final_url = validate_public_url(response.url)
        except (httpx.HTTPError, ValueError, UnsafeFeedUrl):
            return None

        feed = parsed.feed or {}
        website_url = ""
        if feed.get("link"):
            try:
                website_url = validate_public_url(
                    urljoin(final_url, str(feed.get("link")))
                )
            except (ValueError, UnsafeFeedUrl):
                pass
        entries = list(parsed.entries[:12])
        categories = tuple(
            str(tag.get("term") or "").strip()
            for tag in [
                *(feed.get("tags") or []),
                *(
                    tag
                    for entry in entries
                    for tag in (entry.get("tags") or [])
                ),
            ]
            if isinstance(tag, dict) and str(tag.get("term") or "").strip()
        )
        sample_titles = tuple(
            str(entry.get("title") or "").strip()
            for entry in entries[:3]
            if str(entry.get("title") or "").strip()
        )
        dates = [
            date
            for entry in entries
            if (date := self._entry_date(entry)) is not None
        ]
        last_published_at = max(dates, default=None)
        if (
            last_published_at is not None
            and datetime.now(timezone.utc) - last_published_at > timedelta(days=365)
        ):
            return None
        return _ValidatedFeed(
            url=final_url,
            title=str(feed.get("title") or "").strip(),
            website_url=website_url,
            subtitle=str(feed.get("subtitle") or feed.get("description") or "").strip(),
            categories=categories,
            sample_titles=sample_titles,
            last_published_at=last_published_at,
        )

    async def _fetch(self, client: httpx.AsyncClient, url: str):
        return await fetch_discovery_url(
            client,
            url,
            timeout_seconds=self._request_timeout_seconds,
            user_agent=self._user_agent,
            resolve_dns=self._resolve_dns,
        )

    def _parse_page(self, page_url: str, body: bytes) -> _Page:
        parser = _DiscoveryHtmlParser()
        parser.feed(body.decode("utf-8", errors="replace"))
        feed_links = [
            _Link(urljoin(page_url, href), label)
            for href, label in parser.alternates
        ]
        index_links: list[str] = []
        for href, label in parser.anchors:
            absolute = urljoin(page_url, href)
            hint = f"{href} {label}"
            if self._looks_like_feed_url(absolute) or _FEED_HINT_RE.search(hint):
                feed_links.append(_Link(absolute, label))
            if (
                _FEED_INDEX_HINT_RE.search(hint)
                and self._host(absolute) == self._host(page_url)
            ):
                index_links.append(absolute)
        return _Page(
            url=page_url,
            title=parser.title.strip(),
            description=parser.description.strip(),
            feed_links=tuple(self._dedupe_links(feed_links)),
            index_links=tuple(self._dedupe(index_links)),
        )

    @staticmethod
    def _cms_candidates(page_url: str) -> list[_Link]:
        parts = urlsplit(page_url)
        path = parts.path.rstrip("/")
        lower = path.casefold()
        candidates: list[_Link] = []
        if any(marker in lower for marker in ("/category/", "/tag/", "/topic/")):
            base = urlunsplit((parts.scheme, parts.netloc, path + "/", "", ""))
            candidates.extend(
                (_Link(urljoin(base, "feed/"), "category feed"),
                 _Link(urljoin(base, "rss/"), "topic RSS"))
            )
            query = parse_qs(parts.query)
            query["feed"] = ["rss2"]
            query_string = "&".join(
                f"{key}={value}" for key, values in query.items() for value in values
            )
            candidates.append(
                _Link(urlunsplit((parts.scheme, parts.netloc, parts.path, query_string, "")), "category feed")
            )
        if parts.hostname in {"medium.com", "www.medium.com"} and "/tag/" in lower:
            slug = path.split("/tag/", 1)[1].split("/", 1)[0]
            if slug:
                candidates.append(
                    _Link(f"https://medium.com/feed/tag/{slug}", "Medium topic feed")
                )
        if parts.hostname and parts.hostname.endswith("substack.com"):
            candidates.append(
                _Link(urlunsplit((parts.scheme, parts.netloc, "/feed", "", "")), "Substack feed")
            )
        return candidates

    def _relevance_reasons(
        self,
        *,
        query: str,
        seed: _SearchSeed,
        page: _Page | None,
        link: _Link,
        feed: _ValidatedFeed,
        scope: Literal["category", "tag", "sitewide"],
    ) -> list[str]:
        reasons: list[str] = []
        feed_text = " ".join(
            (feed.title, feed.subtitle, " ".join(feed.categories), link.label, feed.url)
        )
        page_text = " ".join(
            (seed.title, seed.snippet, page.title if page else "", page.description if page else "")
        )
        if self._has_query_overlap(query, feed_text):
            reasons.append(f'Feed metadata matches “{query}”')
        if scope in {"category", "tag"}:
            reasons.append("Publisher exposes a dedicated category or topic feed")
        if self._has_query_overlap(query, page_text):
            reasons.append(
                f'Containing page or publisher profile focuses on “{query}”'
            )
        if seed.intent in {"direct", "both"} and feed.url.casefold() == seed.url.casefold():
            reasons.append(f'Tavily ranked this feed URL for “{query}”')
        elif seed.intent in {"direct", "both"}:
            reasons.append(f'Tavily ranked the containing feed page for “{query}”')
        if seed.intent in {"publisher", "both"} and self._has_query_overlap(query, page_text):
            reasons.append(f'Tavily identified this as a specialist publisher for “{query}”')
        if feed.last_published_at is not None:
            reasons.append("Feed has recently dated entries")
        return self._dedupe(reasons)

    @staticmethod
    def _is_relevant(
        seed: _SearchSeed,
        scope: Literal["category", "tag", "sitewide"],
        reasons: list[str],
    ) -> bool:
        if scope in {"category", "tag"}:
            return any(
                "Feed metadata matches" in reason or "this feed URL" in reason
                for reason in reasons
            )
        if seed.intent not in {"publisher", "both"}:
            return any("matches" in reason for reason in reasons)
        return any("specialist publisher" in reason for reason in reasons)

    @staticmethod
    def _candidate_source(
        intent: Literal["direct", "publisher", "both"],
        scope: Literal["category", "tag", "sitewide"],
    ) -> Literal["tavily_direct", "tavily_publisher"]:
        if intent == "direct" or (intent == "both" and scope != "sitewide"):
            return "tavily_direct"
        return "tavily_publisher"

    def _select_diverse(
        self, ranked: list[_RankedCandidate], limit: int
    ) -> list[FeedDiscoveryCandidate]:
        source_order = {"tavily_direct": 0, "tavily_publisher": 1, "google_fallback": 2}
        scope_order = {"category": 0, "tag": 0, "sitewide": 1}
        ranked.sort(
            key=lambda item: (
                source_order[item.candidate.discovery_source],
                scope_order[item.candidate.feed_scope],
                -item.provider_score,
                -item.evidence_count,
                item.candidate.source_label.casefold(),
                item.candidate.feed_url,
            )
        )
        selected: list[FeedDiscoveryCandidate] = []
        seen_urls: set[str] = set()
        seen_feed_identities: set[tuple[str, str]] = set()
        host_counts: Counter[str] = Counter()
        for item in ranked:
            candidate = item.candidate
            key = candidate.feed_url.casefold()
            host = self._publisher_host(candidate.website_url or candidate.feed_url)
            identity = (host, candidate.source_label.casefold())
            if (
                key in seen_urls
                or identity in seen_feed_identities
                or host_counts[host] >= self._max_results_per_publisher
            ):
                continue
            seen_urls.add(key)
            seen_feed_identities.add(identity)
            host_counts[host] += 1
            selected.append(candidate)
            if len(selected) >= limit:
                break
        return selected

    @staticmethod
    def _feed_scope(url: str, label: str) -> Literal["category", "tag", "sitewide"]:
        text = f"{urlsplit(url).path} {label}".casefold()
        if "/tag/" in text or " tag " in f" {text} ":
            return "tag"
        if any(marker in text for marker in ("/category/", "/topic/", "category", "topic")):
            return "category"
        return "sitewide"

    @staticmethod
    def _entry_date(entry: dict) -> datetime | None:
        value = entry.get("published_parsed") or entry.get("updated_parsed")
        if not value:
            return None
        try:
            return datetime.fromtimestamp(calendar.timegm(value), tz=timezone.utc)
        except (OverflowError, TypeError, ValueError):
            return None

    @staticmethod
    def _looks_like_feed_url(url: str) -> bool:
        parts = urlsplit(url)
        path = parts.path.casefold()
        query = parts.query.casefold()
        return (
            path.endswith((".rss", ".xml", ".atom", ".json"))
            or any(token in path.split("/") for token in ("feed", "feeds", "rss", "atom"))
            or "feed=rss" in query
            or "format=rss" in query
        )

    @staticmethod
    def _has_query_overlap(query: str, text: str) -> bool:
        query_tokens = {
            token.casefold()
            for token in _TOKEN_RE.findall(query)
            if token.casefold() not in _STOP_WORDS and len(token) > 1
        }
        if not query_tokens:
            query_tokens = {query.casefold()}
        text_tokens = {token.casefold() for token in _TOKEN_RE.findall(text)}
        return bool(query_tokens & text_tokens)

    @staticmethod
    def _website_url(url: str) -> str:
        try:
            validated = validate_public_url(url)
        except (ValueError, UnsafeFeedUrl):
            return ""
        parts = urlsplit(validated)
        return urlunsplit((parts.scheme, parts.netloc, "/", "", ""))

    @staticmethod
    def _host(url: str) -> str:
        return (urlsplit(url).hostname or "").casefold().removeprefix("www.")

    @staticmethod
    def _publisher_host(url: str) -> str:
        host = TopicFeedDiscovery._host(url)
        extracted = _extract_domain(host)
        return ".".join(part for part in (extracted.domain, extracted.suffix) if part) or host

    @staticmethod
    def _dedupe(values: Iterable[str]) -> list[str]:
        result: list[str] = []
        seen: set[str] = set()
        for value in values:
            if not value:
                continue
            key = value.casefold()
            if key in seen:
                continue
            seen.add(key)
            result.append(value)
        return result

    @staticmethod
    def _dedupe_links(values: Iterable[_Link]) -> list[_Link]:
        result: list[_Link] = []
        seen: set[str] = set()
        for value in values:
            if not value.url:
                continue
            key = value.url.casefold()
            if key in seen:
                continue
            seen.add(key)
            result.append(value)
        return result
