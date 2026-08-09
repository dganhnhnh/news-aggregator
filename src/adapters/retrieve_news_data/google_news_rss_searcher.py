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
in src/models/search_and_crawl_models.py so they can be imported without
pulling in any I/O dependencies.
"""
import asyncio
import csv
import xml.etree.ElementTree as ET
from dataclasses import replace
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import NamedTuple, Optional

import feedparser
import httpx
from googlenewsdecoder import new_decoderv1

from core.logger import get_logger
from models.search_and_crawl_models import (
    CrawlMetadata,
    CrawlResult,
    CrawlerProvider,
)

logger = get_logger(__name__)


class RssFeedSource(NamedTuple):
    """Normalized feed configuration shared by TSV and OPML inputs."""

    query_label: str
    rss_url: str


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


def _parse_rss_entry(entry: dict) -> dict:
    """
    Extract a flat dict from a feedparser entry dict.

    Handles both RSS 2.0 and Atom formats transparently.
    """
    raw_link: str = (
        entry.get("link") or
        (entry.get("links", [{}])[0].get("href", "") if entry.get("links") else "") or
        ""
    )

    # Strip the #ref=crwt... fragment that Google adds to every redirect URL
    raw_link = raw_link.split("#", 1)[0]

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
                break
            except Exception:
                pass

    return {
        "title": raw_title,
        "feed_entry_url": raw_link,
        "source": raw_source,
        "published_date": pub_date,
    }


# ─── RSS fetch ─────────────────────────────────────────────────────────────────


async def _fetch_rss(url: str, client: httpx.AsyncClient) -> tuple[list[dict], str]:
    """
    Fetch and parse a single RSS feed.

    Returns (entries, feed_title).
    entries is a list of dicts from _parse_rss_entry.
    """
    try:
        response = await client.get(url, timeout=20.0)
        response.raise_for_status()
    except httpx.HTTPError as exc:
        logger.warning("[GoogleNews RSS] failed to fetch %s: %s", url, exc)
        raise

    feed = feedparser.parse(response.text)
    feed_title = (feed.feed.get("title") or "") if feed.feed else ""

    if feed.bozo and not feed.entries:
        logger.warning("[GoogleNews RSS] bozo feed (malformed) at %s: %s", url, feed.bozo_exception)

    entries = [_parse_rss_entry(e) for e in feed.entries]
    return entries, feed_title


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
        max_concurrent_crawls: int = 10,
        *,
        tsv_path: Optional[str | Path] = None,
    ) -> None:
        if feed_path is not None and tsv_path is not None:
            raise ValueError("Pass only one of feed_path or tsv_path")
        self._feed_path = feed_path or tsv_path or (
            Path(__file__).parent / "list_queries_ggnews.tsv"
        )
        self._http_client = http_client
        self._max_concurrent_crawls = max_concurrent_crawls
        self._logger = get_logger(__name__)

    # ── public API ─────────────────────────────────────────────────────────────

    async def fetch_all(
        self,
        max_articles_per_feed: int = 20,
        max_feeds: int = 0,
    ) -> list[CrawlResult]:
        """
        Fetch all configured RSS feeds and crawl every discovered article.

        Args:
            max_articles_per_feed: Maximum number of entries to process per
                RSS feed (most recent first). Set to 0 for no per-feed limit.
            max_feeds: Maximum number of configured feeds to process. Set to 0
                to process every feed.

        Returns:
            Flat list of CrawlResult objects, one per successfully crawled
            article.  Failed individual articles are logged and skipped; a
            fully-empty result list is returned only when no feeds were
            reachable.
        """
        queries = self._load_queries()
        total_queries = len(queries)
        if max_feeds > 0:
            queries = queries[:max_feeds]
        if not queries:
            self._logger.warning(
                "[GoogleNews RSS] No RSS URLs found in %s", self._feed_path
            )
            return []

        self._logger.info(
            "[GoogleNews RSS] Processing %d/%d RSS feeds from %s",
            len(queries),
            total_queries,
            self._feed_path,
        )

        own_client = self._http_client is None
        client = self._http_client or httpx.AsyncClient(
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (compatible; GoogleNewsBot/1.0; "
                    "+https://news.google.com)"
                )
            }
        )

        # Two-tier concurrency:
        #   - 5 feeds in parallel   (MAX_CONCURRENT_FEEDS)
        #   - up to max_concurrent_crawls article crawls per feed
        #     shared across ALL active feeds (single semaphore)
        _MAX_CONCURRENT_FEEDS = 5

        feed_sem = asyncio.Semaphore(_MAX_CONCURRENT_FEEDS)
        crawl_sem = asyncio.Semaphore(self._max_concurrent_crawls)

        async def _fetch_and_crawl_feed(
            query_label: str,
            rss_url: str,
        ) -> list[CrawlResult]:
            async with feed_sem:
                try:
                    entries, feed_title = await _fetch_rss(rss_url, client)
                except Exception as exc:
                    self._logger.warning(
                        "[GoogleNews RSS] Skipping feed %s (%s): %s",
                        query_label,
                        rss_url,
                        exc,
                    )
                    return []

                if not entries:
                    self._logger.debug(
                        "[GoogleNews RSS] No entries in feed %s", query_label
                    )
                    return []

                total_entries = len(entries)

                # Cap entries per feed
                if max_articles_per_feed > 0:
                    entries = entries[:max_articles_per_feed]

                self._logger.info(
                    "[GoogleNews RSS] %s — %d entries (%d total), crawling %d…",
                    query_label,
                    len(entries),
                    total_entries,
                    len(entries),
                )

                async def _crawl_one(entry: dict) -> Optional[CrawlResult]:
                    async with crawl_sem:
                        return await self._crawl_single_entry(
                            entry=entry,
                            query_label=query_label,
                            feed_title=feed_title,
                        )

                crawl_results = await asyncio.gather(
                    *[_crawl_one(e) for e in entries],
                    return_exceptions=True,
                )
                return [
                    r
                    for r in crawl_results
                    if r is not None and not isinstance(r, Exception)
                ]

        try:
            feed_results = await asyncio.gather(
                *[
                    _fetch_and_crawl_feed(label, url)
                    for label, url in queries
                ],
                return_exceptions=True,
            )
            results: list[CrawlResult] = []
            for feed_result in feed_results:
                if isinstance(feed_result, Exception):
                    self._logger.warning(
                        "[GoogleNews RSS] Unexpected error in feed task: %s",
                        feed_result,
                    )
                elif isinstance(feed_result, list):
                    results.extend(feed_result)

        finally:
            if own_client:
                await client.aclose()

        self._logger.info(
            "[GoogleNews RSS] Done — %d articles crawled across %d feeds",
            len(results),
            len(queries),
        )
        return results

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

        return await self._do_crawl(
            real_url=real_url,
            feed_entry_url=url,
            query_label="",
            feed_title="",
            entry_title="",
            source_name="",
            published_date=None,
        )

    async def crawl_batch(self, urls: list[str]) -> list[CrawlResult]:
        """Crawl a batch of Google News redirect URLs."""
        return [await self.crawl(u) for u in urls]

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
        # Import here to keep the module-level namespace clean
        from adapters.retrieve_news_data.crawler import Crawl4aiProvider

        provider = Crawl4aiProvider()
        result = await provider.crawl(real_url)

        # Attach Google News–specific metadata on top of crawl4ai metadata
        crawl_meta: CrawlMetadata = result.metadata or CrawlMetadata()
        enriched_meta = CrawlMetadata(
            title=crawl_meta.title or entry_title,
            description=crawl_meta.description,
            language=crawl_meta.language,
            author=crawl_meta.author,
            provider=(
                "google_news_rss"
                if "news.google.com" in feed_entry_url
                else "rss_feed"
            ),
        )

        # Preserve crawl content while attaching feed-level trace context.
        enriched_result = replace(
            result,
            metadata=enriched_meta,
        )
        enriched_result.context.update({
            "query_label": query_label,
            "feed_title": feed_title,
            "source_name": source_name,
            "feed_entry_url": feed_entry_url,
            "google_news_url": (
                feed_entry_url if "news.google.com" in feed_entry_url else ""
            ),
            "published_date": published_date,
        })

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
