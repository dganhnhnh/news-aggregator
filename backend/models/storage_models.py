"""Domain objects for raw storage and ingest operations."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
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
    # The feed the entry was discovered in. Carried through the retry queue so
    # an article that only crawls on a later attempt is still linked to it.
    feed_url: str | None = None


@dataclass(frozen=True)
class StoredCrawlFailure(CrawlFailureOccurrence):
    priority: int = 1
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
    # Every feed this URL was discovered in during the batch. Persisted to
    # `article_feeds`, never onto the article row itself.
    feed_urls: tuple[str, ...] = ()


@dataclass(frozen=True)
class FeedSeed:
    """What storage needs to seed a feed; no RSS-adapter dependency."""

    feed_url: str
    source_label: str
    priority: int = 1


@dataclass(frozen=True)
class FeedStateItem:
    """Operator-facing state for one configured feed."""

    feed_url: str
    source_label: str | None
    priority: int
    resolved_url: str | None
    last_polled_at: datetime | None
    last_success_at: datetime | None
    next_poll_at: datetime
    consecutive_failures: int
    disabled_at: datetime | None
    disabled_reason: str | None
    updated_at: datetime

    @property
    def active(self) -> bool:
        return self.disabled_at is None


@dataclass(frozen=True)
class FeedPollTarget:
    """What storage hands back to a worker about to poll a feed."""

    feed_url: str
    source_label: str | None
    etag: str | None
    last_modified: str | None
    resolved_url: str | None = None
    poll_interval_seconds: int | None = None
    cadence_checked_at: datetime | None = None
    priority: int = 1

    @property
    def fetch_url(self) -> str:
        """Where to send the request; `feed_url` stays the row's identity."""
        return self.resolved_url or self.feed_url

    @property
    def needs_cadence_bootstrap(self) -> bool:
        """Whether the next fetch must return a body instead of a 304."""
        return self.cadence_checked_at is None


@dataclass(frozen=True)
class FeedFailureOutcome:
    consecutive_failures: int
    disabled: bool


@dataclass(frozen=True)
class TopicRefItem:
    id: int
    slug: str
    display_name: str


@dataclass(frozen=True)
class HighlightSegment:
    """One piece of a `ts_headline()` result: matched or not."""

    text: str
    highlight: bool


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
    topics: tuple[TopicRefItem, ...] = ()
    # Populated only when the feed was filtered by a search query.
    title_highlights: tuple[HighlightSegment, ...] | None = None
    excerpt_highlights: tuple[HighlightSegment, ...] | None = None


@dataclass(frozen=True)
class DailyArticleCount:
    """One UTC calendar day of a query's mention count, with its denominator.

    `articles` counts every article that day passing the same source/topic
    filters but *not* the query, so `matches / articles` is a share. That
    denominator is the point: daily crawl volume on this corpus swings by more
    than an order of magnitude, so a bare `matches` series is mostly a picture
    of how much the crawler fetched. `articles == 0` means nothing was crawled
    at all, which reads very differently from a day with articles and no
    matches, and the two must stay distinguishable.
    """

    day: date
    matches: int
    articles: int
    # The day is only partly inside the requested window. Normal at both ends,
    # and the last bucket is almost always partial since `until` defaults to now.
    partial: bool


@dataclass(frozen=True)
class ArticleDetailItem:
    url: str
    url_original: str | None
    title: str | None
    content: str
    feed_summary: str
    source_slug: str | None
    source_name: str | None
    published_at: datetime | None
    crawled_at: datetime
    event_at: datetime
    topics: tuple[TopicRefItem, ...] = ()


@dataclass(frozen=True)
class ArticleSearchHit:
    id: int
    url: str
    title: str | None
    snippet: str
    source_slug: str | None
    source_name: str | None
    published_at: datetime | None
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
    topics: tuple[TopicRefItem, ...] = ()


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
    topics: tuple[TopicRefItem, ...] = ()


@dataclass(frozen=True)
class StorylineMemberEvidence:
    article_id: int
    title: str | None
    url: str
    source_slug: str | None
    source_name: str | None
    published_at: datetime | None
    event_at: datetime
    snippet: str


@dataclass(frozen=True)
class StorylineSearchHit:
    id: int
    title: str | None
    updated_at: datetime
    article_count: int
    member_preview: tuple[StorylineMemberEvidence, ...]


@dataclass(frozen=True)
class StorylineDetailEvidence:
    id: int
    title: str | None
    first_seen_at: datetime
    updated_at: datetime
    article_count: int
    source_count: int
    members: tuple[StorylineMemberEvidence, ...]


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


# Reader-facing name for `feed_state.priority`: 0 is polled continuously by
# `instant-crawl`, anything above it by the periodic `crawl`.
FeedTier = Literal["instant", "normal"]


@dataclass(frozen=True)
class ArticleFeedSource:
    """A feed that has articles, as the sidebar lists it."""

    feed_url: str
    source_label: str | None
    tier: FeedTier
    article_count: int


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
    feeds_retired: int = 0
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
    source_id: int | None
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
    priority: int = 1


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
class TopicCatalogItem:
    id: int
    slug: str
    display_name: str
    description: str | None


@dataclass(frozen=True)
class TaxonomyCatalogItem:
    slug: str
    display_name: str
    revision: int
    topics: tuple[TopicCatalogItem, ...]
    nodes: tuple[TaxonomyNodeSnapshot, ...]


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


@dataclass
class BoilerplateRefreshStats:
    """What one `boilerplate-refresh` run did to `source_boilerplate`."""

    considered: int = 0
    refreshed: int = 0
    unchanged: int = 0
    skipped_too_few_articles: int = 0
    # Sources whose corpus is big enough but that repeat nothing; their profile
    # is stored empty so the run does not keep reconsidering them for free.
    empty_profiles: int = 0
    lines: int = 0


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


@dataclass(frozen=True)
class StageQueueStatus:
    """How much work is waiting for one `processing_jobs` stage.

    `oldest_pending_at` is the signal that matters operationally: a growing
    `pending` count with a recent oldest entry is a burst, while an old one
    means the stage has stopped draining.
    """

    stage: str
    pending: int = 0
    running: int = 0
    done: int = 0
    dead_letter: int = 0
    oldest_pending_at: datetime | None = None


@dataclass(frozen=True)
class FailureQueueStatus:
    failure_kind: str
    total: int = 0
    dead_lettered: int = 0
    due_now: int = 0
    max_attempts: int = 0


@dataclass(frozen=True)
class WorkerRunCount:
    worker: str
    status: RunStatus
    count: int


@dataclass(frozen=True)
class QueueStatusSnapshot:
    """A point-in-time read of every operational queue.

    Read-only by construction: nothing here writes, so a monitoring caller can
    run it as often as it likes.
    """

    captured_at: datetime
    runs_window_hours: int
    stages: tuple[StageQueueStatus, ...] = ()
    failures: tuple[FailureQueueStatus, ...] = ()
    runs: tuple[WorkerRunCount, ...] = ()

    @property
    def dead_letter_jobs(self) -> int:
        return sum(stage.dead_letter for stage in self.stages)

    @property
    def dead_lettered_failures(self) -> int:
        return sum(failure.dead_lettered for failure in self.failures)

    @property
    def abandoned_runs(self) -> int:
        return sum(run.count for run in self.runs if run.status == "abandoned")

    @property
    def failed_runs(self) -> int:
        return sum(run.count for run in self.runs if run.status == "failed")

    def alerts(self) -> list[str]:
        """Point-in-time conditions worth failing a monitoring task on.

        Queue growth is deliberately absent: it needs history, which belongs to
        whatever is doing the sampling, not to a single snapshot.
        """
        problems: list[str] = []
        if self.abandoned_runs:
            problems.append(
                f"{self.abandoned_runs} abandoned run(s) in the last "
                f"{self.runs_window_hours}h; a worker process died mid-run"
            )
        if self.failed_runs:
            problems.append(
                f"{self.failed_runs} failed run(s) in the last "
                f"{self.runs_window_hours}h"
            )
        for stage in self.stages:
            if stage.dead_letter:
                problems.append(
                    f"stage {stage.stage!r} has {stage.dead_letter} dead-lettered "
                    f"job(s); replay them with `news-worker replay --stage {stage.stage}`"
                )
        for failure in self.failures:
            if failure.dead_lettered:
                problems.append(
                    f"{failure.dead_lettered} dead-lettered {failure.failure_kind} "
                    "failure(s); replay them with `news-worker replay --failures`"
                )
        return problems


@dataclass(frozen=True)
class StageJobMetrics:
    """`processing_jobs` counts for one stage, as a Prometheus scrape sees them.

    Separate from `StageQueueStatus` because a scrape wants two things that a
    CLI report does not: the age of the oldest pending job rather than its
    timestamp (Prometheus compares numbers, not clocks), and how many leases
    have expired -- work a dead worker is still nominally holding.
    """

    stage: str
    pending: int = 0
    running: int = 0
    done: int = 0
    dead_letter: int = 0
    expired_leases: int = 0
    oldest_pending_age_seconds: float | None = None


@dataclass(frozen=True)
class WorkerHealthMetrics:
    """Per-worker liveness read off `worker_runs`.

    `last_success_at` is the one that matters for alerting: a worker that has
    not succeeded in several scheduling intervals is down, whether or not it
    has been failing loudly.
    """

    worker: str
    last_success_at: datetime | None = None
    last_run_duration_seconds: float | None = None
    running: int = 0
    oldest_heartbeat_age_seconds: float | None = None


@dataclass(frozen=True)
class FailureTypeCount:
    error_type: str
    total: int = 0
    dead_lettered: int = 0


@dataclass(frozen=True)
class FeedMetrics:
    """`feed_state` seen as a schedule rather than as a list of feeds.

    `overdue` is the one worth alerting on: feeds past their `next_poll_at` by
    more than a grace period mean the crawl worker is not keeping up with its
    own rotation, which is a different failure from a feed being broken.
    """

    total: int = 0
    active: int = 0
    disabled: int = 0
    due: int = 0
    overdue: int = 0
    max_consecutive_failures: int = 0


@dataclass(frozen=True)
class CorpusMetrics:
    """Table-state counts that describe what the pipeline has produced."""

    articles_by_content_source: tuple[tuple[str, int], ...] = ()
    embeddings_by_text_kind: tuple[tuple[str, int], ...] = ()
    clusters_by_status: tuple[tuple[str, int], ...] = ()
    cluster_members: int = 0
    # ('assigned', n) and ('uncategorized', n): an article whose classification
    # assigned no topic at all is the taxonomy-drift signal from spec.md §12.
    classified_articles: tuple[tuple[str, int], ...] = ()
    # Articles inside the recent window that never reached a stage. Distinct
    # from queue depth: a pending job is work that is queued, while these are
    # articles nothing has queued.
    recent_articles: int = 0
    recent_missing: tuple[tuple[str, int], ...] = ()


@dataclass(frozen=True)
class MetricsSnapshot:
    """One read-only pass over every operational table, for `/metrics`.

    Deliberately a plain value object: rendering it to the Prometheus text
    format is somebody else's job, so the queries can be tested without a
    Prometheus client and the rendering without a database.
    """

    captured_at: datetime
    recent_window_hours: int
    feed_overdue_grace_seconds: int
    stages: tuple[StageJobMetrics, ...] = ()
    workers: tuple[WorkerHealthMetrics, ...] = ()
    runs: tuple[WorkerRunCount, ...] = ()
    failures: tuple[FailureQueueStatus, ...] = ()
    failure_types: tuple[FailureTypeCount, ...] = ()
    feeds: FeedMetrics = FeedMetrics()
    corpus: CorpusMetrics = CorpusMetrics()


@dataclass(frozen=True)
class WorkerStatDelta:
    """A sum of one numeric `worker_runs.stats` key over a range of runs.

    `through_run_id` is what makes the sum resumable: the caller adds the
    delta to a running total and asks for runs after that id next time, so a
    scrape never re-aggregates history it has already counted.
    """

    worker: str
    stat: str
    total: float
    through_run_id: int


@dataclass(frozen=True)
class Distribution:
    """A bucketed distribution of rows that exist right now.

    Prometheus calls this shape a gauge histogram, and the distinction from an
    ordinary histogram is the point: these buckets describe current table
    state, so they fall as well as rise. Merging a cluster or retiring a feed
    genuinely removes an observation, which a counter is not allowed to do.

    `counts` is cumulative and aligned with `bounds` -- entry *i* is how many
    rows are at most `bounds[i]` -- and `total` is the row count, so the
    overflow bucket is `total - counts[-1]`.
    """

    bounds: tuple[float, ...] = ()
    counts: tuple[int, ...] = ()
    total: int = 0
    sum_value: float = 0.0


@dataclass(frozen=True)
class LagBucketCounts:
    """Cumulative `le` counts for one content source, plus their sum."""

    content_source: str
    counts: tuple[int, ...] = ()
    total_seconds: float = 0.0
    count: int = 0


@dataclass(frozen=True)
class IngestionLagDelta:
    """How long newly stored articles took to reach us, bucketed, since a mark.

    A resumable histogram delta -- the trick `WorkerStatDelta` plays for worker
    stats, applied to a latency. Each article is measured exactly once, at the
    first scrape that sees it, because `crawled_at` moves when a page is
    re-crawled and re-measuring would let an observation change bucket after
    the fact. Counters may not go backwards, so the watermark is what keeps
    this a legal histogram rather than an approximation of one.

    Articles the feed dated in the future are counted in
    `ahead_of_publication` rather than clamped to zero: a negative lag is a
    publisher clock or timezone bug, and hiding it in the 1-minute bucket
    would make the fastest feeds look like the healthiest ones.
    """

    bounds: tuple[float, ...] = ()
    through_article_id: int = 0
    series: tuple[LagBucketCounts, ...] = ()
    undated: int = 0
    ahead_of_publication: int = 0


@dataclass(frozen=True)
class QualityMetrics:
    """What the pipeline produced, as opposed to whether it ran.

    Everything here answers a question that queue depth cannot: articles keep
    being written while the crawler quietly degrades to summaries, clusters
    keep forming while they shatter into singletons, and classifications stay
    in place long after the taxonomy that produced them was edited.
    """

    recent_window_hours: int = 24
    recent_articles_by_content_source: tuple[tuple[str, int], ...] = ()
    # stage -> how long articles in the recent window waited for it.
    pipeline_lag: tuple[tuple[str, Distribution], ...] = ()
    cluster_sizes: Distribution = Distribution()
    cluster_sources: Distribution = Distribution()
    # Rows whose `classifier_set_hash` predates the active classifier set. The
    # denominator is already exposed as `news_radar_classified_articles`, so
    # it is not repeated here.
    stale_classifications: int = 0
    stale_prototypes: int = 0
    active_topics: int = 0
    feed_poll_intervals: Distribution = Distribution()
    # Active feeds that have never had their cadence bootstrap fetch, so they
    # have no tier yet and cannot appear in the distribution above.
    feeds_without_interval: int = 0
