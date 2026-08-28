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
class StorySourceRef:
    slug: str
    name: str


@dataclass(frozen=True)
class StoryFeedItem:
    id: int
    headline: str | None
    first_seen_at: datetime
    last_seen_at: datetime
    article_count: int
    source_count: int
    sources: list[StorySourceRef]


@dataclass(frozen=True)
class StoryMember:
    article_id: int
    title: str | None
    url: str
    source_slug: str | None
    source_name: str | None
    published_at: datetime | None
    event_at: datetime


@dataclass(frozen=True)
class StoryDetailItem:
    id: int
    headline: str | None
    first_seen_at: datetime
    last_seen_at: datetime
    article_count: int
    source_count: int
    members: list[StoryMember]


@dataclass(frozen=True)
class HistoricalBlockPage:
    """A summary-only article whose title came from an old block page."""

    article_id: int
    url: str
    source: str | None
    published_at: datetime | None
    reason: str


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


@dataclass(frozen=True)
class ClusterArticle:
    article_id: int
    source_id: int | None
    title: str | None
    event_at: datetime
    model: str
    dim: int
    embedding: list[float]


@dataclass(frozen=True)
class ClusterCandidate:
    cluster_id: int
    centroid: list[float]
    centroid_count: int
    first_seen_at: datetime
    last_seen_at: datetime
    similarity: float


@dataclass(frozen=True)
class ClusterAttachResult:
    cluster_id: int
    attached: bool
    centroid_count: int


@dataclass(frozen=True)
class ClusterMembership:
    cluster_id: int
    # True when the article's embedding was rewritten after this membership
    # was recorded (article_embeddings.created_at > cluster_members.added_at),
    # so the centroid this article contributed to no longer reflects it.
    stale: bool


@dataclass(frozen=True)
class ClusterRebuildResult:
    cluster_id: int
    n_articles: int
    closed: bool


@dataclass(frozen=True)
class ClusterMergeResult:
    canonical_id: int
    loser_id: int
    merged: bool


@dataclass
class ClusterRunStats:
    reclaimed: int = 0
    requeued: int = 0
    claimed: int = 0
    created: int = 0
    attached: int = 0
    already_member: int = 0
    reevaluated: int = 0
    missing_embedding: int = 0
    merged: int = 0
    repaired: int = 0
    closed: int = 0
    failed: int = 0


@dataclass(frozen=True)
class TopicSignalSnapshot:
    kind: str  # exemplar | keyword
    value: str
    weight: float
    active: bool


@dataclass(frozen=True)
class TopicSnapshot:
    id: int
    slug: str
    display_name: str
    description: str | None
    threshold: float
    classifier_revision: int
    active: bool
    signals: list[TopicSignalSnapshot]


@dataclass(frozen=True)
class TaxonomyNodeSnapshot:
    slug: str
    display_name: str
    sort_order: int
    topic_slugs: list[str]
    children: list["TaxonomyNodeSnapshot"]


@dataclass(frozen=True)
class TaxonomySnapshot:
    slug: str
    display_name: str
    revision: int
    active: bool
    nodes: list[TaxonomyNodeSnapshot]


@dataclass(frozen=True)
class FlatTaxonomyNode:
    """One `taxonomy_nodes` row, flattened for pure tree-building."""

    id: int
    parent_id: int | None
    slug: str
    display_name: str
    sort_order: int
    topic_slugs: tuple[str, ...]


@dataclass(frozen=True)
class TopicImportOutcome:
    slug: str
    created: bool
    revision_bumped: bool
    old_revision: int
    new_revision: int


@dataclass(frozen=True)
class TaxonomyImportSummary:
    topics: list[TopicImportOutcome]
    taxonomies_created: list[str]
    taxonomies_updated: list[str]
    nodes_created: int
    nodes_updated: int
    pruned_topics: list[str]
    pruned_nodes: list[str]


@dataclass(frozen=True)
class TopicExemplarSet:
    """What the prototype compiler needs for one topic, plus its current
    prototype's provenance so an unchanged topic can be skipped."""

    topic_id: int
    slug: str
    active: bool
    exemplars: list[str]
    current_definition_hash: bytes | None
    current_model: str | None
    current_dim: int | None


@dataclass(frozen=True)
class TopicPrototypeRecord:
    topic_id: int
    model: str
    dim: int
    definition_hash: bytes
    embedding: list[float]


@dataclass
class PrototypeRebuildStats:
    considered: int = 0
    rebuilt: int = 0
    skipped_unchanged: int = 0
    skipped_insufficient_exemplars: int = 0


@dataclass(frozen=True)
class ClassifiableArticle:
    """An article claimed for classification, with its embedding and the
    texts keyword matching runs over.

    Carries every candidate text for the same reason `EmbeddingCandidate`
    does: which body represents the article is `services.text.article_text`'s
    decision, so classify cannot drift from what embed actually embedded.
    """

    article_id: int
    source_id: int | None
    title: str | None
    content: str
    feed_summary: str
    content_source: ContentSource
    model: str
    dim: int
    embedding: list[float]


@dataclass
class ClassifyRunStats:
    reclaimed: int = 0
    requeued: int = 0
    claimed: int = 0
    classified: int = 0
    assigned: int = 0
    uncategorized: int = 0
    clusters_refreshed: int = 0
    missing_embedding: int = 0
    failed: int = 0
