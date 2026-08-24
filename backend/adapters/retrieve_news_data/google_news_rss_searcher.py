"""
Google News RSS searcher.

Fetches RSS feeds from a TSV config file (list_queries_ggnews.tsv), decodes
Google News redirect URLs, and crawls the real article URLs via crawl4ai.

Pipeline per article:
  1. Parse RSS XML  (feedparser)
  2. Extract: title, link (Google redirect URL), pubDate, source name
  3. Decode Google redirect → real article URL  (googlenewsdecoder.new_decoderv1)
  4. Crawl real URL  (crawl4ai)

Domain models (CrawlResult, CrawlerProvider, CrawlError, CrawlMetadata) live
in backend/models/search_and_crawl_models.py so they can be imported without
pulling in any I/O dependencies.
"""
import asyncio
from collections.abc import AsyncIterator, Iterator, Sequence
import csv
from datetime import datetime, timezone
import xml.etree.ElementTree as ET
from dataclasses import replace
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import NamedTuple, Optional

import feedparser
import httpx
from googlenewsdecoder import new_decoderv1

from core.html_content import (
    FEED_CONTENT_KEY,
    MIN_FULL_CONTENT_CHARS,
    html_to_markdown,
    is_full_article,
)
from core.config import DEFAULT_USER_AGENT
from core.logger import get_logger
from core.urls import UnusableUrl, is_probably_article, resolve_crawl_url
from models.discovery_models import DiscoveredEntry, FeedPoll
from models.search_and_crawl_models import (
    CrawlMetadata,
    CrawlResult,
    CrawlerProvider,
)
from models.storage_models import (
    CrawlBatch,
    CrawlFailureOccurrence,
    FailureTarget,
    FeedPollTarget,
)

logger = get_logger(__name__)


class RssFeedSource(NamedTuple):
    """Normalized feed configuration shared by TSV and OPML inputs."""

    query_label: str
    rss_url: str


class _ArticleJob(NamedTuple):
    entry: dict
    query_label: str
    feed_title: str


# ─── Shared decode cache + rate-limiter ───────────────────────────────────────
#
# Cache: Google News URL → decoded real URL.
# Persists for the lifetime of the process (survives across feed runs).
# Key = full Google News URL so each unique URL is decoded at most once.
_DECODE_CACHE: dict[str, str] = {}

# Rate-limit: max N concurrent decode calls, with a pause between each.
# Reduces 429 Too Many Requests from Google.
_DECODE_SEMAPHORE: asyncio.Semaphore = asyncio.Semaphore(2)
_DECODE_DELAY: float = 0.5  # seconds between consecutive decode attempts


async def _decode_google_news_url_async(goog_url: str) -> str:
    """
    Decode a Google News redirect URL to the real article URL.

    Uses ``googlenewsdecoder.new_decoderv1`` (synchronous HTTP call).
    Returns the original URL on failure so the caller still gets something to crawl.

    Thread-safe cache + rate-limiting to avoid Google 429 errors.
    """
    if not goog_url:
        return ""

    # Already a clean URL — skip decoding
    if "news.google.com" not in goog_url:
        return goog_url

    # Cache hit — avoid re-decode entirely
    if goog_url in _DECODE_CACHE:
        cached = _DECODE_CACHE[goog_url]
        logger.debug("[GoogleNews] cache hit  %s → %s", goog_url[:60], cached[:60])
        return cached

    # Rate-limit: serialize decode calls
    async with _DECODE_SEMAPHORE:
        # Double-check cache after acquiring semaphore (another task may have filled it)
        if goog_url in _DECODE_CACHE:
            return _DECODE_CACHE[goog_url]

        try:
            result = await asyncio.to_thread(new_decoderv1, goog_url)
            if result.get("status"):
                decoded = result.get("decoded_url", "").strip()
                if decoded and decoded != goog_url:
                    logger.debug(
                        "[GoogleNews] decoded  %s → %s", goog_url[:60], decoded[:60]
                    )
                    _DECODE_CACHE[goog_url] = decoded
                    return decoded
            else:
                # 429 / rate-limit from Google — back off and retry once
                if result.get("message", "").__contains__("429"):
                    logger.warning(
                        "[GoogleNews] Rate-limited (429) for %s — backing off 5s…",
                        goog_url[:60],
                    )
                    await asyncio.sleep(5)
                    result = await asyncio.to_thread(new_decoderv1, goog_url)
                    if result.get("status"):
                        decoded = result.get("decoded_url", "").strip()
                        if decoded and decoded != goog_url:
                            _DECODE_CACHE[goog_url] = decoded
                            return decoded
        except Exception as exc:
            logger.warning("[GoogleNews] decode failed for %s: %s", goog_url[:60], exc)

        # Fallback: use the Google News URL as-is
        _DECODE_CACHE[goog_url] = goog_url
        return goog_url


# ─── Sync wrapper (kept for backward compat, prefer async version) ──────────────


def _decode_google_news_url(goog_url: str) -> str:
    """
    Synchronous decode — uses the shared cache.

    Prefer ``_decode_google_news_url_async`` in async contexts.
    """
    if not goog_url or "news.google.com" not in goog_url:
        return goog_url
    if goog_url in _DECODE_CACHE:
        return _DECODE_CACHE[goog_url]

    try:
        result = new_decoderv1(goog_url)
        if result.get("status"):
            decoded = result.get("decoded_url", "").strip()
            if decoded and decoded != goog_url:
                _DECODE_CACHE[goog_url] = decoded
                return decoded
    except Exception:
        pass

    _DECODE_CACHE[goog_url] = goog_url
    return goog_url


# ─── RSS parsing helpers ───────────────────────────────────────────────────────


def _select_entry_link(entry: dict, base_url: str) -> str:
    """Pick an article link from a feedparser entry and make it absolute.

    Two failures this exists to prevent, both seen in production:
      * relative or protocol-relative hrefs (`/2020/06/22/post.html`,
        `//host/path`) reaching the crawler verbatim, because
        ``feedparser.parse`` is handed a string and so has no base URI;
      * podcast enclosures being crawled as if they were articles.
    """
    candidates: list[tuple[str, str]] = []
    if entry.get("link"):
        candidates.append((entry["link"], ""))
    for link in entry.get("links") or []:
        href = link.get("href", "")
        if not href:
            continue
        if link.get("rel") == "enclosure":
            continue
        candidates.append((href, link.get("type", "") or ""))

    for href, link_type in candidates:
        # Strip the #ref=crwt... fragment that Google adds to every redirect URL
        href = href.split("#", 1)[0].strip()
        if not href or not is_probably_article(href, link_type):
            continue
        try:
            return resolve_crawl_url(href, base_url=base_url)
        except UnusableUrl:
            continue
    return ""


def _select_entry_content(entry: dict) -> str:
    """Return the entry's full article HTML, or "" if it only carries a teaser.

    Only `content` is considered — RSS `content:encoded` and Atom `<content>`
    both land there. `summary`/`description` is deliberately ignored: it is a
    teaser by convention, and storing it would silently replace articles with
    their own blurbs.
    """
    bodies = entry.get("content") or []
    if isinstance(bodies, dict):  # some feeds expose a single mapping
        bodies = [bodies]
    values = [
        (body.get("value") or "")
        for body in bodies
        if isinstance(body, dict)
        and (body.get("type") or "text/html") not in ("text/plain",)
    ]
    return max(values, key=len, default="")


def _parse_rss_entry(entry: dict, base_url: str = "") -> dict:
    """
    Extract a flat dict from a feedparser entry dict.

    Handles both RSS 2.0 and Atom formats transparently. ``base_url`` is the
    URL the feed was actually fetched from, used to resolve relative links.
    """
    raw_link = _select_entry_link(entry, base_url)

    raw_title: str = (entry.get("title") or "").strip()

    # Source / publisher
    raw_source: str = ""
    if entry.get("source"):
        raw_source = (entry["source"].get("title") or "").strip()
    elif entry.get("author"):
        raw_source = (entry.get("author") or "").strip()

    # Publication date
    pub_date = None
    for field in ("published", "updated", "issued"):
        raw_date = entry.get(field)
        if raw_date:
            try:
                pub_date = parsedate_to_datetime(raw_date)
            except Exception:
                try:
                    pub_date = datetime.fromisoformat(
                        str(raw_date).replace("Z", "+00:00")
                    )
                except (TypeError, ValueError):
                    continue
            if pub_date.tzinfo is None:
                pub_date = pub_date.replace(tzinfo=timezone.utc)
            break

    return {
        "title": raw_title,
        "feed_entry_url": raw_link,
        "source": raw_source,
        "published_date": pub_date,
        "content_html": _select_entry_content(entry),
    }


# ─── RSS fetch ─────────────────────────────────────────────────────────────────


class FeedFetch(NamedTuple):
    entries: list[dict]
    feed_title: str
    etag: str | None
    last_modified: str | None
    not_modified: bool
    # Final location when every hop of the redirect chain was permanent;
    # None when the feed answered directly or moved only temporarily.
    permanent_url: str | None = None
    entries_skipped_non_article: int = 0


# Status codes that make a feed URL permanently wrong: retrying cannot fix a
# feed that is gone (404/410) or one that demands credentials we do not have
# (401). 403 is deliberately absent — it is usually a bot filter, not a
# permanent verdict on the URL.
_PERMANENT_FEED_STATUSES = frozenset({401, 404, 410})
_PERMANENT_REDIRECT_STATUSES = frozenset({301, 308})


def _is_permanent_feed_error(exc: BaseException) -> bool:
    """Whether a feed fetch failed in a way no amount of retrying will fix."""
    return (
        isinstance(exc, httpx.HTTPStatusError)
        and exc.response.status_code in _PERMANENT_FEED_STATUSES
    )


def _permanent_redirect_url(response, requested_url: str) -> str | None:
    """The final URL when the whole redirect chain was permanent."""
    history = response.history
    if not history:
        return None
    if any(hop.status_code not in _PERMANENT_REDIRECT_STATUSES for hop in history):
        return None
    final_url = str(response.url)
    return final_url if final_url != requested_url else None


def _redirect_from_failure(exc: BaseException, requested_url: str) -> str | None:
    """The permanent redirect a failed fetch still proves.

    A 301 followed by a 403 tells us two independent things: where the feed
    now lives, and that we were turned away there. Discarding the first
    because of the second means re-learning the redirect on every poll, so
    the stale URL never leaves the rotation.
    """
    response = getattr(exc, "response", None)
    if response is None:
        return None
    return _permanent_redirect_url(response, requested_url)


async def _fetch_rss(
    url: str,
    client: httpx.AsyncClient,
    *,
    etag: str | None = None,
    last_modified: str | None = None,
) -> FeedFetch:
    """
    Fetch and parse a single RSS feed, using conditional GET when possible.

    A 304 must be checked before ``raise_for_status()`` runs: without
    following redirects, httpx treats any unhandled 3xx as an error for that
    call, so checking status after would misclassify every unchanged feed as
    a failure and eventually get it auto-disabled.
    """
    headers = {}
    if etag:
        headers["If-None-Match"] = etag
    if last_modified:
        headers["If-Modified-Since"] = last_modified
    try:
        response = await client.get(url, timeout=20.0, headers=headers or None)
        if response.status_code == 304:
            return FeedFetch(
                [],
                "",
                etag,
                last_modified,
                True,
                _permanent_redirect_url(response, url),
            )
        response.raise_for_status()
    except httpx.HTTPError as exc:
        logger.warning("[GoogleNews RSS] failed to fetch %s: %s", url, exc)
        raise

    feed = feedparser.parse(response.text)
    feed_title = (feed.feed.get("title") or "") if feed.feed else ""

    if feed.bozo and not feed.entries:
        raise ValueError(f"Malformed RSS/Atom feed: {feed.bozo_exception}")

    # Relative hrefs resolve against where the feed actually came from, which
    # is the post-redirect URL rather than the one we asked for.
    base_url = str(response.url)
    parsed = [_parse_rss_entry(e, base_url) for e in feed.entries]
    entries = [entry for entry in parsed if entry["feed_entry_url"]]
    skipped = len(parsed) - len(entries)
    if skipped:
        logger.info(
            "[GoogleNews RSS] %s — skipped %d/%d entries with no article link",
            url,
            skipped,
            len(parsed),
        )
    return FeedFetch(
        entries,
        feed_title,
        response.headers.get("ETag"),
        response.headers.get("Last-Modified"),
        False,
        _permanent_redirect_url(response, url),
        skipped,
    )


# ─── Google News RSS Searcher ─────────────────────────────────────────────────


class GoogleNewsRssSearcher(CrawlerProvider):
    """
    RSS-based implementation of the CrawlerProvider port.

    Reads RSS URLs from either a four-column TSV file or an OPML subscription
    export. For every entry found in every feed it:
      1. Decodes the Google News redirect URL → real article URL
      2. Crawls the real article URL using crawl4ai
      3. Attaches RSS metadata (query label, original feed-entry URL, source
         name, and publication date) to the resulting CrawlResult.

    Parameters
    ----------
    feed_path : str | Path, optional
        Path to a ``.tsv`` or ``.opml`` feed source file. Defaults to the
        legacy ``list_queries_ggnews.tsv`` location.
    http_client : httpx.AsyncClient, optional
        Shared HTTP client to use for RSS fetches.  If not supplied, a default
        client is created and closed after the run.
    """

    name = "rss_feed"

    def __init__(
        self,
        feed_path: Optional[str | Path] = None,
        http_client: Optional[httpx.AsyncClient] = None,
        max_concurrent_crawls: int = 2,
        max_concurrent_feeds: int = 2,
        crawl_batch_size: int = 10,
        crawl_provider: CrawlerProvider | None = None,
        *,
        tsv_path: Optional[str | Path] = None,
        feed_content_min_chars: int = MIN_FULL_CONTENT_CHARS,
        max_crawls_per_host: int = 1,
        crawl_delay_per_host: float = 1.0,
        user_agent: str = DEFAULT_USER_AGENT,
    ) -> None:
        if feed_path is not None and tsv_path is not None:
            raise ValueError("Pass only one of feed_path or tsv_path")
        self._feed_path = feed_path or tsv_path or (
            Path(__file__).parent / "list_queries_ggnews.tsv"
        )
        self._http_client = http_client
        self._max_concurrent_crawls = max_concurrent_crawls
        self._max_concurrent_feeds = max_concurrent_feeds
        self._crawl_batch_size = crawl_batch_size
        self._crawl_provider = crawl_provider
        self._feed_content_min_chars = feed_content_min_chars
        self._max_crawls_per_host = max_crawls_per_host
        self._crawl_delay_per_host = crawl_delay_per_host
        self._user_agent = user_agent or DEFAULT_USER_AGENT
        self._feed_offset = 0
        self._last_feed_offset = 0
        self._logger = get_logger(__name__)

    # ── public API ─────────────────────────────────────────────────────────────

    async def fetch_all(
        self,
        max_articles_per_feed: int = 20,
        max_feeds: int = 0,
    ) -> list[CrawlResult]:
        """Compatibility wrapper returning only crawler results."""
        batch = await self.fetch_batch(
            max_articles_per_feed=max_articles_per_feed,
            max_feeds=max_feeds,
        )
        return batch.results

    async def fetch_batch(
        self,
        max_articles_per_feed: int = 20,
        max_feeds: int = 0,
        sources: list[RssFeedSource] | None = None,
        feed_offset: int | None = None,
    ) -> CrawlBatch:
        """Compatibility wrapper that combines the bounded streamed batches."""
        combined = CrawlBatch()
        async for batch in self.iter_batches(
            max_articles_per_feed=max_articles_per_feed,
            max_feeds=max_feeds,
            sources=sources,
            feed_offset=feed_offset,
        ):
            self._merge_batch(combined, batch)
        return combined

    async def poll_feeds(
        self, targets: Sequence[FeedPollTarget]
    ) -> AsyncIterator[FeedPoll]:
        """Poll each target feed, honoring conditional GET; yield one FeedPoll per feed.

        Fetches in groups of ``max_concurrent_feeds`` like the old
        ``iter_batches`` did, but yields per-feed instead of merging a group
        into one discovery batch, so a caller can react to each feed's
        304/failure/fetched outcome independently.
        """
        own_client = self._http_client is None
        client = self._http_client or self._build_default_client()
        try:
            for feed_group in self._chunks(list(targets), self._max_concurrent_feeds):
                fetched = await asyncio.gather(
                    *[
                        _fetch_rss(
                            target.fetch_url,
                            client,
                            etag=target.etag,
                            last_modified=target.last_modified,
                        )
                        for target in feed_group
                    ],
                    return_exceptions=True,
                )
                for target, outcome in zip(feed_group, fetched, strict=True):
                    if isinstance(outcome, Exception):
                        yield FeedPoll(
                            target=target,
                            status="failed",
                            error=CrawlFailureOccurrence(
                                failure_kind="feed",
                                url=target.feed_url,
                                source=target.source_label,
                                error_type=type(outcome).__name__,
                                error_message=str(outcome),
                                permanent=_is_permanent_feed_error(outcome),
                            ),
                            resolved_url=_redirect_from_failure(
                                outcome, target.fetch_url
                            ),
                        )
                        continue

                    fetch = outcome
                    if fetch.not_modified:
                        yield FeedPoll(
                            target=target,
                            status="not_modified",
                            etag=fetch.etag,
                            last_modified=fetch.last_modified,
                            resolved_url=fetch.permanent_url,
                        )
                        continue

                    entries = [
                        DiscoveredEntry(
                            feed_url=target.feed_url,
                            query_label=target.source_label or "",
                            feed_title=fetch.feed_title,
                            entry_url=entry["feed_entry_url"],
                            title=entry["title"],
                            source=entry["source"],
                            published_at=entry["published_date"],
                            content_html=entry.get("content_html", ""),
                        )
                        for entry in fetch.entries
                    ]
                    yield FeedPoll(
                        target=target,
                        status="fetched",
                        entries=entries,
                        feed_title=fetch.feed_title,
                        etag=fetch.etag,
                        last_modified=fetch.last_modified,
                        resolved_url=fetch.permanent_url,
                        entries_skipped_non_article=fetch.entries_skipped_non_article,
                    )
        finally:
            if own_client:
                await client.aclose()

    async def crawl_entries(self, entries: Sequence[DiscoveredEntry]) -> CrawlBatch:
        """Crawl an already-filtered, already-bounded chunk of entries."""
        jobs = [
            _ArticleJob(
                {
                    "feed_entry_url": entry.entry_url,
                    "title": entry.title,
                    "source": entry.source,
                    "published_date": entry.published_at,
                    "content_html": entry.content_html,
                },
                entry.query_label,
                entry.feed_title,
            )
            for entry in entries
        ]
        return await self._crawl_job_batch(jobs)

    def _build_default_client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(
            follow_redirects=True,
            headers={"User-Agent": self._user_agent},
        )

    async def iter_batches(
        self,
        max_articles_per_feed: int = 20,
        max_feeds: int = 0,
        sources: list[RssFeedSource] | None = None,
        feed_offset: int | None = None,
    ) -> AsyncIterator[CrawlBatch]:
        """Yield bounded crawl batches so callers can persist incrementally.

        Built on poll_feeds/crawl_entries so `news-pipeline rss` and
        CrawlWorker (via RssDiscovery) share one fetch/crawl implementation.
        """
        configured = sources if sources is not None else self._load_queries()
        # Naming the feed file when the caller passed URLs directly reads as
        # if the arguments were ignored; say where the feeds actually came from.
        origin = (
            f"{len(configured)} explicit feed URL(s)"
            if sources is not None
            else str(self._feed_path)
        )
        queries = self._select_sources(
            configured,
            max_feeds=max_feeds,
            feed_offset=feed_offset,
            advance_offset=sources is None,
        )
        if not queries:
            self._logger.warning("[GoogleNews RSS] No RSS URLs found in %s", origin)
            return

        self._logger.info(
            "[GoogleNews RSS] Processing %d/%d RSS feeds from %s at offset %d",
            len(queries),
            len(configured),
            origin,
            self._last_feed_offset,
        )

        targets = [
            FeedPollTarget(
                feed_url=source.rss_url,
                source_label=source.query_label,
                etag=None,
                last_modified=None,
            )
            for source in queries
        ]
        async for poll in self.poll_feeds(targets):
            discovery = CrawlBatch(feeds_processed=1)
            if poll.status == "failed":
                assert poll.error is not None
                discovery.failures.append(poll.error)
                yield discovery
                continue

            discovery.resolved_failures.append(
                FailureTarget(failure_kind="feed", url=poll.target.feed_url)
            )
            if poll.status == "not_modified":
                yield discovery
                continue

            discovery.articles_discovered = len(poll.entries)
            selected = (
                poll.entries[:max_articles_per_feed]
                if max_articles_per_feed > 0
                else poll.entries
            )
            self._logger.info(
                "[GoogleNews RSS] %s — %d entries (%d total), crawling %d…",
                poll.target.source_label,
                len(selected),
                len(poll.entries),
                len(selected),
            )
            yield discovery
            for chunk in self._chunks(selected, self._crawl_batch_size):
                yield await self.crawl_entries(chunk)

    @property
    def next_feed_offset(self) -> int:
        return self._feed_offset

    def _select_sources(
        self,
        sources: list[RssFeedSource],
        *,
        max_feeds: int,
        feed_offset: int | None,
        advance_offset: bool,
    ) -> list[RssFeedSource]:
        if not sources:
            self._last_feed_offset = 0
            return []
        start = (self._feed_offset if feed_offset is None else feed_offset) % len(
            sources
        )
        self._last_feed_offset = start
        count = len(sources) if max_feeds == 0 else min(max_feeds, len(sources))
        selected = [sources[(start + index) % len(sources)] for index in range(count)]
        if advance_offset:
            self._feed_offset = (start + count) % len(sources)
        return selected

    @staticmethod
    def _chunks(items: list, size: int) -> Iterator[list]:
        for start in range(0, len(items), size):
            yield items[start : start + size]

    @staticmethod
    def _merge_batch(destination: CrawlBatch, source: CrawlBatch) -> None:
        destination.results.extend(source.results)
        destination.failures.extend(source.failures)
        destination.resolved_failures.extend(source.resolved_failures)
        destination.feeds_processed += source.feeds_processed
        destination.articles_discovered += source.articles_discovered

    def _provider(self) -> CrawlerProvider:
        if self._crawl_provider is None:
            from adapters.retrieve_news_data.crawler import Crawl4aiProvider

            self._crawl_provider = Crawl4aiProvider(
                max_per_host=self._max_crawls_per_host,
                per_host_delay_seconds=self._crawl_delay_per_host,
            )
        return self._crawl_provider

    async def _crawl_job_batch(
        self,
        jobs: list[_ArticleJob],
        *,
        max_concurrency: int | None = None,
    ) -> CrawlBatch:
        decoded_urls = await asyncio.gather(
            *[
                _decode_google_news_url_async(
                    job.entry.get("feed_entry_url")
                    or job.entry.get("google_news_url", "")
                )
                for job in jobs
            ]
        )
        outcomes: list[CrawlResult | None] = [None] * len(jobs)
        crawl_indexes: list[int] = []
        crawl_urls: list[str] = []
        for index, (job, real_url) in enumerate(
            zip(jobs, decoded_urls, strict=True)
        ):
            feed_entry_url = job.entry.get("feed_entry_url", "")
            from_feed = self._result_from_feed_content(job, real_url)
            if from_feed is not None:
                outcomes[index] = from_feed
            elif not real_url:
                outcomes[index] = CrawlResult(
                    url=feed_entry_url,
                    markdown="",
                    success=False,
                    error_message="Empty URL after decoding",
                )
            elif "news.google.com" in feed_entry_url and real_url == feed_entry_url:
                outcomes[index] = CrawlResult(
                    url=feed_entry_url,
                    markdown="",
                    success=False,
                    error_message="Unable to decode Google News redirect URL",
                )
            else:
                crawl_indexes.append(index)
                crawl_urls.append(real_url)

        if crawl_urls:
            try:
                crawled = await self._provider().crawl_batch(
                    crawl_urls,
                    max_concurrency=(
                        max_concurrency or self._max_concurrent_crawls
                    ),
                )
                if len(crawled) != len(crawl_urls):
                    raise RuntimeError(
                        "Crawler returned a different number of batch results"
                    )
            except Exception as exc:
                crawled = [
                    CrawlResult(
                        url=url,
                        markdown="",
                        success=False,
                        error_message=str(exc),
                    )
                    for url in crawl_urls
                ]
            for index, result in zip(crawl_indexes, crawled, strict=True):
                outcomes[index] = result

        batch = CrawlBatch()
        for job, real_url, result in zip(
            jobs, decoded_urls, outcomes, strict=True
        ):
            assert result is not None
            enriched = self._enrich_result(result, job, real_url)
            self._add_article_outcome(batch, job, enriched)
        return batch

    def _result_from_feed_content(
        self,
        job: _ArticleJob,
        real_url: str,
    ) -> CrawlResult | None:
        """Render the feed's own article body, or None to crawl the page.

        The URL recorded is the entry link rather than a final redirect target,
        since nothing was fetched to observe one. For feeds that ship full
        content this is the canonical article URL anyway.
        """
        if self._feed_content_min_chars <= 0:
            return None
        markdown = html_to_markdown(job.entry.get("content_html", ""))
        if not is_full_article(markdown, min_chars=self._feed_content_min_chars):
            return None

        url = real_url or job.entry.get("feed_entry_url", "")
        if not url:
            return None
        result = CrawlResult(url=url, markdown=markdown, success=True)
        result.context[FEED_CONTENT_KEY] = True
        return result

    def _enrich_result(
        self,
        result: CrawlResult,
        job: _ArticleJob,
        real_url: str,
    ) -> CrawlResult:
        feed_entry_url = job.entry.get("feed_entry_url", "")
        crawl_meta = result.metadata or CrawlMetadata()
        enriched = replace(
            result,
            metadata=CrawlMetadata(
                title=crawl_meta.title or job.entry.get("title", ""),
                description=crawl_meta.description,
                language=crawl_meta.language,
                author=crawl_meta.author,
                provider=(
                    "google_news_rss"
                    if "news.google.com" in feed_entry_url
                    else "rss_feed"
                ),
            ),
        )
        enriched.context.update(
            {
                "query_label": job.query_label,
                "feed_title": job.feed_title,
                "source_name": job.entry.get("source", ""),
                "feed_entry_url": feed_entry_url,
                "google_news_url": (
                    feed_entry_url if "news.google.com" in feed_entry_url else ""
                ),
                "published_date": job.entry.get("published_date"),
                "requested_url": real_url or feed_entry_url,
            }
        )
        return enriched

    @staticmethod
    def _add_article_outcome(
        batch: CrawlBatch,
        job: _ArticleJob,
        result: CrawlResult,
    ) -> None:
        feed_entry_url = job.entry.get("feed_entry_url", "")
        source = job.feed_title or job.query_label or None
        batch.results.append(result)
        if result.success:
            resolved_urls = {
                result.url,
                feed_entry_url,
                result.context.get("requested_url", ""),
            }
            batch.resolved_failures.extend(
                FailureTarget(failure_kind="article", url=url)
                for url in resolved_urls
                if url
            )
            return
        batch.failures.append(
            CrawlFailureOccurrence(
                failure_kind="article",
                url=result.url or feed_entry_url,
                source=source,
                title=job.entry.get("title") or None,
                published_at=job.entry.get("published_date"),
                error_type="CrawlError",
                error_message=result.error_message or "Unknown crawl failure",
            )
        )

    # ── CrawlerProvider contract ───────────────────────────────────────────────

    async def crawl(self, url: str) -> CrawlResult:
        """
        Decode a single Google News redirect URL and crawl the real article.

        If the URL does not contain ``news.google.com`` it is treated as a
        direct article URL and crawled as-is.
        """
        google_url = url if "news.google.com" in url else ""
        real_url = await _decode_google_news_url_async(url) if google_url else url

        if not real_url:
            return CrawlResult(
                url=url,
                markdown="",
                success=False,
                error_message="Empty URL after decoding",
            )
        if google_url and real_url == google_url:
            return CrawlResult(
                url=url,
                markdown="",
                success=False,
                error_message="Unable to decode Google News redirect URL",
            )

        return await self._do_crawl(
            real_url=real_url,
            feed_entry_url=url,
            query_label="",
            feed_title="",
            entry_title="",
            source_name="",
            published_date=None,
        )

    async def crawl_batch(
        self,
        urls: list[str],
        *,
        max_concurrency: int | None = None,
    ) -> list[CrawlResult]:
        """Crawl URLs in bounded chunks, sharing one browser per chunk."""
        results: list[CrawlResult] = []
        jobs = [
            _ArticleJob(
                {
                    "feed_entry_url": url,
                    "title": "",
                    "source": "",
                    "published_date": None,
                },
                "",
                "",
            )
            for url in urls
        ]
        for chunk in self._chunks(jobs, self._crawl_batch_size):
            batch = await self._crawl_job_batch(
                chunk,
                max_concurrency=max_concurrency,
            )
            results.extend(batch.results)
        return results

    # ── internal ──────────────────────────────────────────────────────────────

    async def _crawl_single_entry(
        self,
        entry: dict,
        query_label: str,
        feed_title: str,
    ) -> Optional[CrawlResult]:
        """Decode one RSS entry's Google URL and crawl it."""
        feed_entry_url: str = (
            entry.get("feed_entry_url") or entry.get("google_news_url", "")
        )
        real_url = await _decode_google_news_url_async(feed_entry_url)

        if not real_url:
            self._logger.warning(
                "[GoogleNews RSS] Could not decode URL for article: %s",
                entry.get("title", "(no title)"),
            )
            return None
        if "news.google.com" in feed_entry_url and real_url == feed_entry_url:
            return CrawlResult(
                url=feed_entry_url,
                markdown="",
                success=False,
                error_message="Unable to decode Google News redirect URL",
            )

        return await self._do_crawl(
            real_url=real_url,
            feed_entry_url=feed_entry_url,
            query_label=query_label,
            feed_title=feed_title,
            entry_title=entry.get("title", ""),
            source_name=entry.get("source", ""),
            published_date=entry.get("published_date"),
        )

    async def _do_crawl(
        self,
        *,
        real_url: str,
        feed_entry_url: str,
        query_label: str,
        feed_title: str,
        entry_title: str,
        source_name: str,
        published_date,  # datetime | None — no type annotation to avoid circular import
    ) -> CrawlResult:
        """
        Crawl the real article URL via crawl4ai and attach RSS metadata.

        If the crawl fails the returned CrawlResult has ``success=False``.
        """
        result = await self._provider().crawl(real_url)
        enriched_result = self._enrich_result(
            result,
            _ArticleJob(
                {
                    "feed_entry_url": feed_entry_url,
                    "title": entry_title,
                    "source": source_name,
                    "published_date": published_date,
                },
                query_label,
                feed_title,
            ),
            real_url,
        )

        if result.success:
            self._logger.debug(
                "[GoogleNews RSS] OK %s — %s", entry_title[:60], real_url
            )
        else:
            self._logger.warning(
                "[GoogleNews RSS] FAIL %s (%s): %s",
                entry_title[:60] or real_url,
                query_label,
                result.error_message,
            )

        return enriched_result

    # ── TSV loading ────────────────────────────────────────────────────────────

    @staticmethod
    def _load_queries_from_tsv(
        tsv_path: str | Path,
    ) -> list[RssFeedSource]:
        """
        Parse ``list_queries_ggnews.tsv`` and return ``[(query_label, rss_url), ...]``.

        TSV columns (0-indexed):
          col 2 — Query (English)   → query_label
          col 3 — Google News RSS URL → rss_url

        Blank lines and comment lines (starting with ``#``) are skipped.
        """
        queries: list[RssFeedSource] = []
        with open(tsv_path, newline="", encoding="utf-8") as fh:
            reader = csv.reader(fh, delimiter="\t")
            for row in reader:
                # Skip blank / comment rows
                if not row or row[0].startswith("#"):
                    continue
                if len(row) < 4:
                    continue
                label = row[2].strip().strip('"')
                url = row[3].strip().strip('"')
                if not url or not url.startswith("http"):
                    continue
                queries.append(RssFeedSource(query_label=label, rss_url=url))
        return queries

    @staticmethod
    def _load_queries_from_opml(
        opml_path: str | Path,
    ) -> list[RssFeedSource]:
        """Normalize OPML outlines into ``query_label`` and ``rss_url``."""
        try:
            root = ET.parse(opml_path).getroot()
        except ET.ParseError as exc:
            raise ValueError(f"Invalid OPML file {opml_path}: {exc}") from exc

        queries: list[RssFeedSource] = []
        for outline in root.iter():
            if outline.tag.rsplit("}", 1)[-1] != "outline":
                continue
            rss_url = (
                outline.attrib.get("xmlUrl")
                or outline.attrib.get("xmlurl")
                or ""
            ).strip()
            if not rss_url.startswith(("http://", "https://")):
                continue
            query_label = (
                outline.attrib.get("title")
                or outline.attrib.get("text")
                or rss_url
            ).strip()
            queries.append(
                RssFeedSource(query_label=query_label, rss_url=rss_url)
            )
        return queries

    def _load_queries(self) -> list[RssFeedSource]:
        feed_path = Path(self._feed_path)
        if not feed_path.is_file():
            raise FileNotFoundError(
                f"RSS feed source file not found: {feed_path}. "
                "Pass --feed-path/--tsv-path or set "
                "RSS_FEED_PATH/GOOGLE_NEWS_TSV_PATH."
            )
        suffix = feed_path.suffix.lower()
        if suffix == ".tsv":
            return self._load_queries_from_tsv(feed_path)
        if suffix == ".opml":
            return self._load_queries_from_opml(feed_path)
        raise ValueError(
            f"Unsupported feed source format {suffix or '(no extension)'}: "
            f"{feed_path}. Expected .tsv or .opml."
        )


# ─── Convenience factory ──────────────────────────────────────────────────────


# Preferred generic name; retain GoogleNewsRssSearcher for compatibility.
RssFeedSearcher = GoogleNewsRssSearcher


def create_google_news_searcher(
    feed_path: Optional[str | Path] = None,
    *,
    tsv_path: Optional[str | Path] = None,
) -> GoogleNewsRssSearcher:
    """
    Factory that returns a ready-to-use GoogleNewsRssSearcher.

    Parameters
    ----------
    feed_path : str | Path, optional
        Path to a TSV or OPML feed source file.
    """
    return GoogleNewsRssSearcher(feed_path=feed_path, tsv_path=tsv_path)
