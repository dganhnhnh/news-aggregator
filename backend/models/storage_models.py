"""Domain objects for raw storage and ingest operations."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Literal

from core.html_content import FEED_CONTENT_KEY, FEED_SUMMARY_KEY
from models.search_and_crawl_models import CrawlResult


FailureKind = Literal["feed", "article"]
# Where an article's stored `content` came from. 'feed_summary' means there is
# no content at all — only the dek survived.
ContentSource = Literal["feed_content", "crawl", "feed_summary"]
# Which text the embedder was actually handed.
TextKind = Literal["content", "feed_summary", "title"]
RunStatus = Literal["running", "succeeded", "partial", "failed", "abandoned"]
WorkerTrigger = Literal["scheduled", "manual", "backfill"]


@dataclass(frozen=True)
class FailureTarget:
    failure_kind: FailureKind
    url: str


@dataclass(frozen=True)
class CrawlFailureOccurrence:
    failure_kind: FailureKind
    url: str
    error_type: str
    error_message: str
    source: str | None = None
    title: str | None = None
    published_at: datetime | None = None
    # A failure retrying cannot fix (404/410/401 on a feed, a link that is not
    # an article). Storage dead-letters it on the first occurrence instead of
    # burning MAX_FAILURE_ATTEMPTS on a deterministic outcome.
    permanent: bool = False


@dataclass(frozen=True)
class StoredCrawlFailure(CrawlFailureOccurrence):
    attempt_count: int = 1
    first_failed_at: datetime | None = None
    last_failed_at: datetime | None = None
    next_retry_at: datetime | None = None
    dead_lettered_at: datetime | None = None


@dataclass(frozen=True)
class RawArticle:
    url: str
    url_original: str
    title: str | None
    source: str | None
    lang: str | None
    content: str
    content_hash: bytes
    published_at: datetime | None
    crawled_at: datetime
    feed_summary: str = ""
    content_source: ContentSource = "crawl"


@dataclass(frozen=True)
class FeedSeed:
    """What storage needs to seed a feed; no RSS-adapter dependency."""

    feed_url: str
    source_label: str


@dataclass(frozen=True)
class FeedPollTarget:
    """What storage hands back to a worker about to poll a feed."""

    feed_url: str
    source_label: str | None
    etag: str | None
    last_modified: str | None
    resolved_url: str | None = None

    @property
    def fetch_url(self) -> str:
        """Where to send the request; `feed_url` stays the row's identity."""
        return self.resolved_url or self.feed_url


@dataclass(frozen=True)
class FeedFailureOutcome:
    consecutive_failures: int
    disabled: bool


@dataclass(frozen=True)
class ArticleFeedItem:
    id: int
    url: str
    title: str | None
    excerpt: str
    source_slug: str | None
    source_name: str | None
    published_at: datetime | None
    crawled_at: datetime
    event_at: datetime


@dataclass(frozen=True)
class ArticleDetailItem:
    url: str
    url_original: str | None
    title: str | None
    content: str
    source_slug: str | None
    source_name: str | None
    published_at: datetime | None
    crawled_at: datetime
    event_at: datetime


@dataclass(frozen=True)
class SourceItem:
    slug: str
    name: str
    homepage: str | None


@dataclass
class CrawlBatch:
    results: list[CrawlResult] = field(default_factory=list)
    failures: list[CrawlFailureOccurrence] = field(default_factory=list)
    resolved_failures: list[FailureTarget] = field(default_factory=list)
    feeds_processed: int = 0
    articles_discovered: int = 0

    @property
    def crawl_succeeded(self) -> int:
        return sum(result.success for result in self.results)

    @property
    def crawl_failed(self) -> int:
        return len(self.failures)

    @property
    def articles_from_feed_content(self) -> int:
        """Results rendered from the feed body instead of a fetched page."""
        return sum(
            1
            for result in self.results
            if result.success and result.context.get(FEED_CONTENT_KEY)
        )

    @property
    def articles_summary_only(self) -> int:
        """Failed crawls the feed's dek rescues into a storable row."""
        return sum(
            1
            for result in self.results
            if not result.success and result.context.get(FEED_SUMMARY_KEY)
        )


@dataclass
class IngestRunStats:
    feeds_processed: int = 0
    articles_discovered: int = 0
    crawl_succeeded: int = 0
    crawl_failed: int = 0
    retries_attempted: int = 0
    retries_succeeded: int = 0
    articles_written: int = 0
    articles_unchanged: int = 0
    feeds_not_modified: int = 0
    feeds_failed: int = 0
    feeds_disabled: int = 0
    entries_skipped_out_of_window: int = 0
    entries_skipped_undated: int = 0
    entries_skipped_known: int = 0
    entries_skipped_non_article: int = 0
    feeds_redirected: int = 0
    stale_failures_purged: int = 0
    # Articles taken from the feed's own content:encoded body, so no page was
    # fetched for them at all.
    articles_from_feed_content: int = 0
    # Articles whose page could not be crawled but whose feed dek was stored
    # anyway, so they still carry text to search and embed.
    articles_summary_only: int = 0

    def add_batch(self, batch: CrawlBatch, *, retry: bool = False) -> None:
        self.feeds_processed += batch.feeds_processed
        self.articles_discovered += batch.articles_discovered
        self.crawl_succeeded += batch.crawl_succeeded
        self.crawl_failed += batch.crawl_failed
        self.articles_from_feed_content += batch.articles_from_feed_content
        self.articles_summary_only += batch.articles_summary_only
        if retry:
            self.retries_attempted += batch.crawl_succeeded + batch.crawl_failed
            self.retries_succeeded += batch.crawl_succeeded


@dataclass(frozen=True)
class StorageWriteSummary:
    received: int
    deduplicated: int
    written: int
    unchanged: int
    rejected: int


@dataclass(frozen=True)
class EmbeddingCandidate:
    """An article claimed for embedding, with its previous embedding state.

    Carries every candidate text rather than a pre-picked one: choosing among
    them is `services.text.article_text`'s job, so that embed and cluster
    cannot drift into two different answers.
    """

    article_id: int
    title: str | None
    content: str
    feed_summary: str
    content_source: ContentSource
    existing_model: str | None
    existing_content_hash: bytes | None


@dataclass(frozen=True)
class EmbeddingRecord:
    article_id: int
    model: str
    dim: int
    content_hash: bytes
    embedding: list[float]
    text_kind: TextKind = "content"


@dataclass
class EmbedRunStats:
    reclaimed: int = 0
    requeued: int = 0
    claimed: int = 0
    embedded: int = 0
    skipped_unchanged: int = 0
    failed: int = 0
