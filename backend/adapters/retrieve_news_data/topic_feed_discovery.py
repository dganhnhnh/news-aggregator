"""Feed-first discovery for publications focused on a requested topic."""

from __future__ import annotations

import asyncio
import calendar
from collections import Counter, defaultdict, deque
from collections.abc import Iterable
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from html.parser import HTMLParser
import re
from typing import Literal, Protocol
from urllib.parse import parse_qs, urldefrag, urljoin, urlsplit, urlunsplit

import feedparser
import httpx
import tldextract

from adapters.retrieve_news_data.feed_topics import matches_topic
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
    declared: bool = True


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
    entry_texts: tuple[str, ...] = ()


@dataclass(frozen=True)
class _RankedCandidate:
    candidate: FeedDiscoveryCandidate
    provider_score: float
    evidence_count: int
    article_coverage: float = 0.0
    matching_articles: int = 0
    sample_size: int = 0


@dataclass(frozen=True)
class _Resource:
    page: _Page | None = None
    feed: _ValidatedFeed | None = None


@dataclass(frozen=True)
class _PreparedSeed:
    seed: _SearchSeed
    page: _Page | None
    links: tuple[_Link, ...]


class _DiscoverySession:
    """One request's feed budget and parsed-resource cache, including fallback.

    Page reads temporarily reserve a slot because an arbitrary search URL may
    return RSS. HTML releases that slot unless the URL was also a feed probe.
    Reservations happen before scheduling I/O, so concurrent workers cannot
    overshoot the limit. Redirect hops belong to the original URL attempt.
    """

    def __init__(
        self, discovery: TopicFeedDiscovery, client: httpx.AsyncClient, limit: int
    ) -> None:
        self.discovery = discovery
        self.client = client
        self.limit = limit
        self.attempts: set[str] = set()
        self.pending_pages: set[str] = set()
        self.tasks: dict[str, asyncio.Task[_Resource | None]] = {}
        self.aliases: dict[str, str] = {}
        self.pages_inspected = 0

    @staticmethod
    def key(url: str) -> str:
        return urldefrag(validate_public_url(url))[0]

    @property
    def remaining(self) -> int:
        return self.limit - len(self.attempts | self.pending_pages)

    @property
    def validated_count(self) -> int:
        return len({
            resource.feed.url
            for task in set(self.tasks.values())
            if task.done() and not task.cancelled() and task.exception() is None
            if (resource := task.result()) is not None and resource.feed is not None
        })

    async def read(self, url: str, *, as_feed: bool) -> _Resource | None:
        try:
            key = self.key(url)
            key = self.aliases.get(key, key)
        except (ValueError, UnsafeFeedUrl):
            return None
        task = self.tasks.get(key)
        if as_feed and key not in self.attempts:
            if key not in self.pending_pages and self.remaining <= 0:
                return None
            self.pending_pages.discard(key)
            self.attempts.add(key)
        if task is None:
            if key not in self.attempts:
                if self.remaining <= 0:
                    return None
                self.pending_pages.add(key)
            task = asyncio.create_task(self._load(key))
            self.tasks[key] = task
        return await task

    async def _load(self, key: str) -> _Resource | None:
        try:
            response = await self.discovery._fetch(self.client, key)
            final_key = self.key(response.url)
            # Reuse parsed results when a later seed names the redirect target.
            if final_key != key and final_key not in self.tasks:
                self.aliases.setdefault(final_key, key)
            parsed = feedparser.parse(response.body)
            if parsed.get("version"):
                self.attempts.add(key)
                return _Resource(feed=await self.discovery._validate_feed(
                    key, self.client, response=response
                ))
            self.pages_inspected += 1
            return _Resource(page=self.discovery._parse_page(response.url, response.body))
        except (httpx.HTTPError, ValueError, UnsafeFeedUrl) as exc:
            logger.debug("Feed discovery fetch failed for %s: %s", key, exc)
            return None
        finally:
            self.pending_pages.discard(key)

    async def close(self) -> None:
        tasks = set(self.tasks.values())
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


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
        max_search_results: int = 30,
        max_publishers: int = 24,
        max_seed_pages_per_host: int = 2,
        max_concurrent_pages: int = 4,
        max_feed_attempts_per_page: int = 24,
        max_feed_urls_checked: int = 50,
        max_results_per_publisher: int = 2,
        fallback_min_candidates: int = 6,
        request_timeout_seconds: float = 8.0,
        user_agent: str = DEFAULT_USER_AGENT,
        resolve_dns: bool | None = None,
    ) -> None:
        if not 1 <= max_feed_urls_checked <= 50:
            raise ValueError("max_feed_urls_checked must be between 1 and 50")
        if max_concurrent_pages < 1 or max_feed_attempts_per_page < 1:
            raise ValueError("discovery concurrency and per-page attempts must be positive")
        self._max_feed_urls_checked = max_feed_urls_checked
        self._search_provider = search_provider
        self._google_fallback = google_fallback
        self._http_client = http_client
        self._max_search_results_per_intent = max_search_results_per_intent
        self._max_search_results = max_search_results
        self._max_publishers = max_publishers
        self._max_seed_pages_per_host = max_seed_pages_per_host
        self._max_concurrent_pages = max_concurrent_pages
        self._max_feed_attempts_per_page = max_feed_attempts_per_page
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
        result_limit = 25 if limit is None else limit
        if result_limit < 1:
            raise ValueError("limit must be at least 1")

        own_client = self._http_client is None
        client = self._http_client or httpx.AsyncClient()
        warnings: list[str] = []
        session = _DiscoverySession(self, client, self._max_feed_urls_checked)
        try:
            seeds, search_errors = await self._search_seeds(
                cleaned_query, language=language, country=country
            )
            warnings.extend(search_errors)
            ranked = await self._inspect_seeds(seeds, cleaned_query, session)
            feed_first = self._select_diverse(ranked, result_limit)

            fallback_used = False
            fallback_threshold = min(self._fallback_min_candidates, result_limit)
            if (
                len(feed_first) < fallback_threshold
                and self._google_fallback is not None
                and session.remaining > 0
            ):
                fallback_used = True
                warnings.append(
                    "Feed-first discovery returned fewer than "
                    f"{fallback_threshold} candidates; Google News fallback was used."
                )
                try:
                    publishers = await self._google_fallback.search_publishers(
                        cleaned_query, language=language, country=country
                    )
                    fallback_ranked = await self._inspect_seeds(
                        [
                            _SearchSeed(item.url, item.title, "", 0.0, "publisher")
                            for item in publishers
                        ],
                        cleaned_query,
                        session,
                    )
                    ranked.extend(
                        replace(item, candidate=replace(
                            item.candidate, discovery_source="google_fallback"
                        ))
                        for item in fallback_ranked
                    )
                except FeedDiscoveryError as exc:
                    warnings.append(f"Google News fallback failed: {exc}")

            if len(session.attempts) >= session.limit:
                warnings.append(
                    f"Reached the limit of {session.limit} feed URLs checked; "
                    "returning candidates found within this budget."
                )
            selected = self._select_diverse(ranked, result_limit)
            if not seeds and search_errors and not selected:
                raise FeedDiscoveryError("; ".join(search_errors))

            return FeedDiscoveryResult(
                query=cleaned_query,
                candidates=tuple(selected),
                stats=FeedDiscoveryStats(
                    search_results=len(seeds),
                    publishers_inspected=len({self._host(seed.url) for seed in seeds}),
                    pages_inspected=session.pages_inspected,
                    feeds_validated=session.validated_count,
                    google_fallback_used=fallback_used,
                    feeds_attempted=len(session.attempts),
                    feeds_relevant=len({item.candidate.feed_url for item in ranked}),
                ),
                warnings=tuple(self._dedupe(warnings)),
            )
        finally:
            await session.close()
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
            run("direct", direct_query), run("publisher", publisher_query),
            run("direct", f'{query} RSS feed'),
            run("publisher", f'{query} news analysis blog'),
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
            key = _DiscoverySession.key(seed.url)
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
            key = _DiscoverySession.key(seed.url)
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

    async def _run_workers(self, items, operation) -> None:
        iterator = iter(items)

        async def worker() -> None:
            for item in iterator:
                await operation(item)

        workers = [
            asyncio.create_task(worker())
            for _ in range(min(self._max_concurrent_pages, len(items)))
        ]
        try:
            await asyncio.gather(*workers)
        finally:
            for task in workers:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*workers, return_exceptions=True)

    async def _inspect_seeds(
        self, seeds: list[_SearchSeed], query: str, session: _DiscoverySession
    ) -> list[_RankedCandidate]:
        # Discover links before spending the validation budget. Network timing
        # cannot let the first directory monopolize all 50 feed checks.
        prepared: list[_PreparedSeed | None] = [None] * len(seeds)

        async def prepare(index):
            prepared[index] = await self._prepare_seed(seeds[index], session)

        await self._run_workers(list(range(len(seeds))), prepare)
        pool: dict[str, tuple[_PreparedSeed, _Link]] = {}

        def priority(item):
            source, link = item
            return (
                not self._has_query_overlap(query, f"{link.label} {link.url}"),
                not link.declared,
                not self._looks_like_feed_url(link.url),
                -source.seed.score,
                link.url,
            )

        for source in prepared:
            if source is None:
                continue
            links = sorted(source.links, key=lambda link: priority((source, link)))
            for link in links[:self._max_feed_attempts_per_page]:
                if self._is_comment_feed(link.url, link.label):
                    continue
                try:
                    key = session.key(link.url)
                except (ValueError, UnsafeFeedUrl):
                    continue
                item = (source, link)
                if key not in pool or priority(item) < priority(pool[key]):
                    pool[key] = item

        # Within each evidence tier, try one URL per publisher per round.
        tiers = defaultdict(lambda: defaultdict(deque))
        for item in sorted(pool.values(), key=priority):
            tiers[priority(item)[:3]][self._publisher_host(item[1].url)].append(item)
        queue = []
        for tier in sorted(tiers):
            publishers = tiers[tier]
            while publishers:
                for host in list(publishers):
                    queue.append(publishers[host].popleft())
                    if not publishers[host]:
                        del publishers[host]

        found: list[_RankedCandidate] = []

        async def inspect(item):
            source, link = item
            resource = await session.read(link.url, as_feed=True)
            if resource is None or resource.feed is None:
                return
            feed = resource.feed
            scope = self._feed_scope(feed.url, link.label)
            if not self._is_relevant(query, feed, scope):
                return
            reasons = self._relevance_reasons(
                query=query, seed=source.seed, page=source.page,
                link=link, feed=feed, scope=scope,
            )
            matches, total = self._article_matches(query, feed)
            found.append(_RankedCandidate(
                candidate=FeedDiscoveryCandidate(
                    feed_url=feed.url,
                    source_label=feed.title or infer_feed_source_label(feed.url),
                    website_url=feed.website_url or self._website_url(feed.url),
                    feed_scope=scope,
                    discovery_source=self._candidate_source(source.seed.intent, scope),
                    relevance_reasons=tuple(reasons),
                    sample_titles=feed.sample_titles,
                    last_published_at=feed.last_published_at,
                ),
                provider_score=source.seed.score,
                evidence_count=len(reasons),
                article_coverage=matches / total if total else 0.0,
                matching_articles=matches,
                sample_size=total,
            ))

        await self._run_workers(queue, inspect)
        return found

    async def _prepare_seed(
        self, seed: _SearchSeed, session: _DiscoverySession
    ) -> _PreparedSeed | None:
        resource = await session.read(
            seed.url, as_feed=self._looks_like_feed_url(seed.url)
        )
        if resource is None:
            return None
        if resource.feed is not None:
            return _PreparedSeed(seed, None, (_Link(resource.feed.url, seed.title),))
        page = resource.page
        if page is None:
            return None
        links = list(page.feed_links)
        links.extend(self._cms_candidates(page.url))
        if seed.intent in {"publisher", "both"}:
            root = self._website_url(page.url)
            links.extend(
                _Link(urljoin(root, path), path, declared=False)
                for path in _COMMON_FEED_PATHS
            )
        for index_url in page.index_links[:2]:
            # This is a page-discovery read. HTML does not spend the feed
            # budget, but an RSS response still does and is never fetched twice.
            index = await session.read(index_url, as_feed=False)
            if index is not None and index.page is not None:
                links = [link for link in links if urldefrag(link.url)[0] != index_url]
                links.extend(index.page.feed_links)
            elif index is not None and index.feed is not None:
                links.append(_Link(index.feed.url, "RSS feed"))
        return _PreparedSeed(seed, page, tuple(self._dedupe_links(links)))

    async def _validate_feed(
        self, url: str, client: httpx.AsyncClient, *, response=None
    ) -> _ValidatedFeed | None:
        try:
            response = response or await self._fetch(client, url)
            parsed = feedparser.parse(response.body)
            if not (parsed.get("version") or "") or not parsed.entries:
                return None
            final_url = validate_public_url(response.url)
        except (httpx.HTTPError, ValueError, UnsafeFeedUrl):
            return None

        feed = parsed.feed or {}
        if self._is_comment_feed(final_url, str(feed.get("title") or "")):
            return None
        website_url = ""
        if feed.get("link"):
            try:
                website_url = validate_public_url(
                    urljoin(final_url, str(feed.get("link")))
                )
            except (ValueError, UnsafeFeedUrl):
                pass
        entries = list(parsed.entries[:12])
        # News Radar ingests articles, not a directory's comments or podcasts.
        media_entries = sum(
            any(str(item.get("type", "")).startswith(("audio/", "video/"))
                for item in entry.get("enclosures", []))
            for entry in entries
        )
        directory_entries = sum(
            bool(re.search(r"\b(?:rss|podcast) feeds?\b", str(entry.get("title", "")), re.I))
            for entry in entries
        )
        if media_entries > len(entries) / 2 or (
            len(entries) >= 3 and directory_entries / len(entries) >= 0.6
        ):
            return None
        categories = tuple(
            str(tag.get("term") or "").strip()
            for tag in (feed.get("tags") or [])
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
            entry_texts=tuple(
                " ".join((str(entry.get("title") or ""),
                          re.sub(r"<[^>]+>", " ", str(entry.get("summary") or ""))[:1200]))
                for entry in entries
            ),
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
            if not href or href.startswith("#"):
                continue
            absolute = urldefrag(urljoin(page_url, href))[0]
            if urlsplit(absolute).scheme not in {"http", "https"}:
                continue
            if absolute == urldefrag(page_url)[0]:
                continue
            hint = f"{href} {label}"
            if self._looks_like_feed_url(absolute) or _FEED_HINT_RE.search(hint):
                feed_links.append(_Link(absolute, label))
            if (
                _FEED_INDEX_HINT_RE.search(hint)
                and self._host(absolute) == self._host(page_url)
            ):
                index_links.append(absolute)
            elif re.fullmatch(r"/?(?:rss|feeds?)/?", urlsplit(absolute).path, re.I):
                if self._host(absolute) == self._host(page_url):
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
                (_Link(urljoin(base, "feed/"), "category feed", declared=False),
                 _Link(urljoin(base, "rss/"), "topic RSS", declared=False))
            )
            query = parse_qs(parts.query)
            query["feed"] = ["rss2"]
            query_string = "&".join(
                f"{key}={value}" for key, values in query.items() for value in values
            )
            candidates.append(
                _Link(urlunsplit((parts.scheme, parts.netloc, parts.path, query_string, "")), "category feed", declared=False)
            )
        if parts.hostname in {"medium.com", "www.medium.com"} and "/tag/" in lower:
            slug = path.split("/tag/", 1)[1].split("/", 1)[0]
            if slug:
                candidates.append(
                    _Link(f"https://medium.com/feed/tag/{slug}", "Medium topic feed", declared=False)
                )
        if parts.hostname and parts.hostname.endswith("substack.com"):
            candidates.append(
                _Link(urlunsplit((parts.scheme, parts.netloc, "/feed", "", "")), "Substack feed", declared=False)
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
        # A listicle/search snippet can describe the topic without its own feed
        # covering it. Only feed-owned evidence determines acceptance.
        feed_text = " ".join((feed.title, feed.subtitle, " ".join(feed.categories)))
        if self._has_query_overlap(query, feed_text):
            reasons.append(f'Feed metadata matches “{query}”')
        path_matches = self._has_query_overlap(query, feed.url)
        if path_matches and scope in {"category", "tag"}:
            reasons.append(f'Dedicated feed path matches “{query}”')
        matches, total = self._article_matches(query, feed)
        if total:
            reasons.append(f'Recent articles match “{query}” ({matches}/{total})')
        if total < 3:
            reasons.append("Limited article sample; topic focus is less certain")
        if scope in {"category", "tag"}:
            reasons.append("Publisher exposes a dedicated category or topic feed")
        if feed.last_published_at is not None:
            reasons.append("Feed has recently dated entries")
        return self._dedupe(reasons)

    def _article_matches(self, query: str, feed: _ValidatedFeed) -> tuple[int, int]:
        texts = feed.entry_texts or feed.sample_titles
        return sum(self._has_query_overlap(query, text) for text in texts), len(texts)

    def _is_relevant(
        self,
        query: str,
        feed: _ValidatedFeed,
        scope: Literal["category", "tag", "sitewide"],
    ) -> bool:
        matches, total = self._article_matches(query, feed)
        if total >= 3:
            # Metadata/path cannot override contradictory article evidence.
            return matches >= 3 and matches / total >= 0.6
        metadata = " ".join((feed.title, feed.subtitle, " ".join(feed.categories)))
        return total > 0 and matches == total and (
            self._has_query_overlap(query, metadata)
            or (scope in {"category", "tag"} and self._has_query_overlap(query, feed.url))
        )

    @staticmethod
    def _candidate_source(
        intent: Literal["direct", "publisher", "both"],
        scope: Literal["category", "tag", "sitewide"],
    ) -> Literal["tavily_direct", "tavily_publisher"]:
        if intent == "direct" or (intent == "both" and scope != "sitewide"):
            return "tavily_direct"
        return "tavily_publisher"

    @staticmethod
    def _rank_key(item: _RankedCandidate) -> tuple:
        source_order = {"tavily_direct": 0, "tavily_publisher": 1, "google_fallback": 2}
        scope_order = {"category": 0, "tag": 0, "sitewide": 1}
        return (
            item.sample_size < 3,
            -item.article_coverage,
            -item.matching_articles,
            scope_order[item.candidate.feed_scope],
            -item.evidence_count,
            source_order[item.candidate.discovery_source],
            -item.provider_score,
            item.candidate.source_label.casefold(),
            item.candidate.feed_url,
        )

    def _select_diverse(
        self, ranked: list[_RankedCandidate], limit: int
    ) -> list[FeedDiscoveryCandidate]:
        ranked.sort(key=self._rank_key)
        selected: list[FeedDiscoveryCandidate] = []
        seen_urls: set[str] = set()
        seen_feed_identities: set[tuple[str, str]] = set()
        host_counts: Counter[str] = Counter()
        for item in ranked:
            candidate = item.candidate
            key = _DiscoverySession.key(candidate.feed_url)
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
        return matches_topic(query, text)

    @staticmethod
    def _is_comment_feed(url: str, title: str) -> bool:
        return bool(
            re.search(r"/(?:comments?)(?:/|$)|[?&]feed=comments", url, re.I)
            or re.match(r"comments? (?:on|for|feed)\b", title, re.I)
        )

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
        # Each hosted publication is independent, rather than one publisher
        # for every newsletter/blog sharing the platform's registrable domain.
        if host.endswith((".substack.com", ".wordpress.com", ".blogspot.com")):
            return host
        if host in {"medium.com", "www.medium.com"}:
            parts = urlsplit(url).path.strip("/").split("/")
            if parts[0] == "feed":
                parts = parts[1:]
            if parts and parts[0] and parts[0] not in {"tag", "topic"}:
                return f"{host}/{parts[0]}"
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
        result: dict[str, _Link] = {}
        for value in values:
            if not value.url:
                continue
            try:
                key = _DiscoverySession.key(value.url)
            except (ValueError, UnsafeFeedUrl):
                continue
            current = result.get(key)
            result[key] = _Link(
                key,
                " · ".join(TopicFeedDiscovery._dedupe((current.label, value.label)))
                if current else value.label,
                declared=value.declared or (current.declared if current else False),
            )
        return list(result.values())
