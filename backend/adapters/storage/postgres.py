"""Async PostgreSQL repository for raw articles and ingest operations."""

from __future__ import annotations

import asyncio
import random
from collections import defaultdict
from collections.abc import AsyncIterator, Callable, Mapping, Sequence
from contextlib import asynccontextmanager
from dataclasses import asdict, is_dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import (
    case,
    delete,
    func,
    or_,
    select,
    text,
    true,
    tuple_,
    union,
    update,
)
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, create_async_engine

from adapters.storage.schema import (
    article_classifications,
    article_embeddings,
    article_topics,
    cluster_members,
    cluster_topics,
    clusters,
    crawl_failures,
    feed_state,
    processing_jobs,
    raw_articles,
    sources,
    taxonomies,
    taxonomy_node_topics,
    taxonomy_nodes,
    topic_prototypes,
    topic_signals,
    topics,
    worker_runs,
)
from core.content_quality import historical_summary_title_reason
from models.storage_models import (
    ArticleDetailItem,
    ArticleFeedItem,
    ArticleSearchHit,
    ClassifiableArticle,
    CrawlFailureOccurrence,
    ClusterArticle,
    ClusterAttachResult,
    ClusterCandidate,
    ClusterMembership,
    ClusterMergeResult,
    ClusterRebuildResult,
    EmbeddingCandidate,
    EmbeddingRecord,
    FailureKind,
    FailureTarget,
    FeedFailureOutcome,
    FailureQueueStatus,
    FeedPollTarget,
    FeedSeed,
    FeedStateItem,
    FlatTaxonomyNode,
    HistoricalBlockPage,
    QueueStatusSnapshot,
    RawArticle,
    RunStatus,
    SourceItem,
    StageQueueStatus,
    StoredCrawlFailure,
    StorageWriteSummary,
    StoryDetailItem,
    StoryFeedItem,
    StorylineDetailEvidence,
    StorylineMemberEvidence,
    StorylineSearchHit,
    StoryMember,
    StorySourceRef,
    TaxonomyCatalogItem,
    TaxonomyImportSummary,
    TaxonomySnapshot,
    TopicCatalogItem,
    TopicExemplarSet,
    TopicImportOutcome,
    TopicPrototypeRecord,
    TopicRefItem,
    TopicSignalSnapshot,
    TopicSnapshot,
    WorkerRunCount,
    WorkerTrigger,
)
from services.raw_storage import build_excerpt, source_slug
from services.classification import (
    ArticleClassification,
    ClassifierSnapshot,
    MemberTopicScores,
    TopicClassifier,
    TopicKeyword,
    aggregate_cluster_topics,
    classifier_set_hash,
)
from services.clustering import choose_canonical, recompute_centroid, running_centroid
from services.taxonomy import SeedTaxonomyNode, TaxonomySeed, build_node_tree, next_classifier_revision


class StorageError(RuntimeError):
    """Raised when raw storage is unavailable or not migrated."""


MAX_FAILURE_ATTEMPTS = 5
# How many representative sources a story summary carries for its avatar
# strip; the full distinct-source count is a separate, already-cached field.
STORY_SOURCE_LIMIT = 3
ARTICLE_SEARCH_CANDIDATE_LIMIT = 50
ARTICLE_SEARCH_LIMIT = 8
STORYLINE_SEARCH_LIMIT = 5
STORYLINE_PREVIEW_LIMIT = 3
RRF_K = 60


def reciprocal_rank_fusion(
    lexical_ids: Sequence[int],
    semantic_ids: Sequence[int],
    *,
    limit: int,
    k: int = RRF_K,
    tie_break: Mapping[int, tuple[datetime, int]] | None = None,
) -> list[int]:
    """Fuse two ranked lists without comparing incomparable raw scores."""
    scores: dict[int, float] = defaultdict(float)
    for ranked_ids in (lexical_ids, semantic_ids):
        for rank, article_id in enumerate(ranked_ids, start=1):
            scores[int(article_id)] += 1.0 / (k + rank)
    tie_break = tie_break or {}
    return sorted(
        scores,
        key=lambda article_id: (
            scores[article_id],
            *tie_break.get(
                article_id, (datetime.min.replace(tzinfo=timezone.utc), article_id)
            ),
        ),
        reverse=True,
    )[:limit]


def _vector_list(value: Any) -> list[float]:
    """Convert pgvector/numpy return values into domain-owned plain lists."""
    converted = value.tolist() if hasattr(value, "tolist") else value
    return [float(item) for item in converted]


async def _lock_article(connection: AsyncConnection, article_id: int) -> None:
    """Serialize competing create/attach decisions for one article."""
    await connection.execute(
        text("SELECT pg_advisory_xact_lock(CAST(:article_id AS BIGINT))"),
        {"article_id": article_id},
    )


async def _representative_headline(
    connection: AsyncConnection, cluster_id: int, centroid: Sequence[float]
) -> str | None:
    distance = article_embeddings.c.embedding.cosine_distance(list(centroid))
    return await connection.scalar(
        select(raw_articles.c.title)
        .select_from(
            cluster_members.join(
                raw_articles, cluster_members.c.article_id == raw_articles.c.id
            )
            .join(
                article_embeddings,
                cluster_members.c.article_id == article_embeddings.c.article_id,
            )
            .outerjoin(sources, raw_articles.c.source_id == sources.c.id)
        )
        .where(
            cluster_members.c.cluster_id == cluster_id,
            raw_articles.c.title.is_not(None),
            raw_articles.c.title != "",
        )
        .order_by(distance, sources.c.slug.nulls_last(), raw_articles.c.id)
        .limit(1)
    )


async def _refresh_cluster_topics_locked(
    connection: AsyncConnection, cluster_id: int
) -> int:
    """Recompute one cluster's labels from its members' `article_topics`.

    The single aggregate path (spec §8, T6.4): the classify worker, cluster
    attach, merge and repair all land here, so a story's label can never
    depend on which worker touched it last. `cluster_topics` is a
    materialized view of `article_topics` and is always fully replaced, never
    incrementally patched -- that is what makes it rebuildable (spec §2.7).
    """
    member_rows = (
        await connection.execute(
            select(cluster_members.c.article_id, raw_articles.c.source_id)
            .select_from(
                cluster_members.join(
                    raw_articles, cluster_members.c.article_id == raw_articles.c.id
                )
            )
            .where(cluster_members.c.cluster_id == cluster_id)
        )
    ).all()
    if not member_rows:
        await connection.execute(
            delete(cluster_topics).where(cluster_topics.c.cluster_id == cluster_id)
        )
        return 0

    article_ids = [int(row.article_id) for row in member_rows]
    score_rows = (
        await connection.execute(
            select(
                article_topics.c.article_id,
                article_topics.c.topic_id,
                article_topics.c.score,
            ).where(article_topics.c.article_id.in_(article_ids))
        )
    ).all()
    scores_by_article: dict[int, dict[int, float]] = defaultdict(dict)
    for row in score_rows:
        scores_by_article[int(row.article_id)][int(row.topic_id)] = float(row.score)

    members = [
        MemberTopicScores(
            article_id=int(row.article_id),
            source_id=int(row.source_id) if row.source_id is not None else None,
            scores=scores_by_article.get(int(row.article_id), {}),
        )
        for row in member_rows
    ]
    topic_ids = {topic_id for member in members for topic_id in member.scores}
    thresholds = {
        int(row.id): float(row.threshold)
        for row in (
            await connection.execute(
                select(topics.c.id, topics.c.threshold).where(topics.c.id.in_(topic_ids))
            )
        ).all()
    } if topic_ids else {}

    aggregated = aggregate_cluster_topics(members, thresholds)
    await connection.execute(
        delete(cluster_topics).where(cluster_topics.c.cluster_id == cluster_id)
    )
    if aggregated:
        now = datetime.now(timezone.utc)
        await connection.execute(
            cluster_topics.insert().values(
                [
                    {
                        "cluster_id": cluster_id,
                        "topic_id": item.topic_id,
                        "score": item.score,
                        "assigned": item.assigned,
                        "updated_at": now,
                    }
                    for item in aggregated
                ]
            )
        )
    return len(aggregated)


async def _rebuild_cluster_locked(
    connection: AsyncConnection, cluster_id: int
) -> ClusterRebuildResult:
    """Recompute one cluster's aggregate from `cluster_members`, the source of
    truth, independent of whatever running-mean history produced the prior
    row. Caller must already be inside a transaction; this takes its own row
    lock so it is safe to call after only locking the membership rows.

    A merged cluster is a redirect with no aggregate of its own, so it is
    left untouched. A cluster left with no members is closed rather than
    deleted, to keep `merged_into_id` references and job history intact; its
    counters and headline are cleared so no stale aggregate survives.
    """
    status = await connection.scalar(
        select(clusters.c.status).where(clusters.c.id == cluster_id).with_for_update()
    )
    if status is None:
        raise StorageError(f"cluster {cluster_id} not found")
    if status == "merged":
        n_articles = int(
            await connection.scalar(
                select(func.count())
                .select_from(cluster_members)
                .where(cluster_members.c.cluster_id == cluster_id)
            )
            or 0
        )
        return ClusterRebuildResult(cluster_id, n_articles, False)

    members = (
        await connection.execute(
            select(
                article_embeddings.c.embedding,
                raw_articles.c.source_id,
                raw_articles.c.event_at,
            )
            .select_from(
                cluster_members.join(
                    article_embeddings,
                    cluster_members.c.article_id == article_embeddings.c.article_id,
                ).join(
                    raw_articles,
                    cluster_members.c.article_id == raw_articles.c.id,
                )
            )
            .where(cluster_members.c.cluster_id == cluster_id)
        )
    ).all()

    if not members:
        await connection.execute(
            update(clusters)
            .where(clusters.c.id == cluster_id)
            .values(
                centroid_count=0,
                n_articles=0,
                n_distinct_sources=0,
                headline=None,
                status="closed",
            )
        )
        await _refresh_cluster_topics_locked(connection, cluster_id)
        return ClusterRebuildResult(cluster_id, 0, True)

    new_centroid = recompute_centroid([_vector_list(row.embedding) for row in members])
    source_ids = {row.source_id for row in members if row.source_id is not None}
    event_times = [row.event_at for row in members]
    headline = await _representative_headline(connection, cluster_id, new_centroid)
    await connection.execute(
        update(clusters)
        .where(clusters.c.id == cluster_id)
        .values(
            centroid=new_centroid,
            centroid_count=len(members),
            n_articles=len(members),
            n_distinct_sources=len(source_ids),
            first_seen_at=min(event_times),
            last_seen_at=max(event_times),
            headline=headline,
        )
    )
    await _refresh_cluster_topics_locked(connection, cluster_id)
    return ClusterRebuildResult(cluster_id, len(members), False)


def retry_delay(
    attempt_count: int,
    *,
    random_uniform: Callable[[float, float], float] = random.uniform,
) -> timedelta:
    """Exponential backoff with a one-day cap and full jitter.

    Full jitter (`uniform(0, cap)` rather than a fixed delay) spreads out
    retries that failed together instead of having them all wake up and
    retry in lockstep. `random_uniform` is injectable so callers can pass a
    deterministic stub in tests.
    """
    exponent = min(max(attempt_count - 1, 0), 9)
    cap = min(300 * (2**exponent), 86_400)
    return timedelta(seconds=random_uniform(0, cap))


class PostgresRawStorage:
    def __init__(self, database_url: str) -> None:
        if not database_url:
            raise StorageError("DATABASE_URL is required for persistence")
        self._engine: AsyncEngine = create_async_engine(
            database_url,
            pool_pre_ping=True,
        )

    @property
    def engine(self) -> AsyncEngine:
        return self._engine

    async def close(self) -> None:
        await self._engine.dispose()

    async def check_ready(self) -> None:
        try:
            async with self._engine.connect() as connection:
                tables = await connection.execute(
                    text(
                        "SELECT to_regclass('public.raw_articles'), "
                        "to_regclass('public.crawl_failures'), "
                        "to_regclass('public.worker_runs'), "
                        "to_regclass('public.sources'), "
                        "to_regclass('public.feed_state'), "
                        "to_regclass('public.processing_jobs'), "
                        "to_regclass('public.article_embeddings'), "
                        "to_regclass('public.clusters'), "
                        "to_regclass('public.cluster_members'), "
                        "to_regclass('public.topics'), "
                        "to_regclass('public.topic_signals'), "
                        "to_regclass('public.topic_prototypes'), "
                        "to_regclass('public.taxonomies'), "
                        "to_regclass('public.taxonomy_nodes'), "
                        "to_regclass('public.taxonomy_node_topics'), "
                        "to_regclass('public.article_classifications'), "
                        "to_regclass('public.article_topics'), "
                        "to_regclass('public.cluster_topics')"
                    )
                )
                if any(value is None for value in tables.one()):
                    raise StorageError(
                        "Database schema is missing; run `uv run alembic upgrade head`"
                    )
        except StorageError:
            raise
        except Exception as exc:
            raise StorageError(f"Database is unavailable: {exc}") from exc

    @asynccontextmanager
    async def worker_lock(self, name: str) -> AsyncIterator[bool]:
        """Hold a session-level advisory lock for the duration of a worker run.

        Yields whether the lock was acquired; a caller that gets ``False`` must
        exit instead of waiting, because overlapping cron schedules would
        otherwise pile up waiters. The lock lives on a dedicated AUTOCOMMIT
        connection: advisory locks do not need a transaction, and holding one
        open for a run that may last an hour would block autovacuum.
        """
        key = {"key": f"worker:{name}"}
        engine = self._engine.execution_options(isolation_level="AUTOCOMMIT")
        acquired = False
        async with engine.connect() as connection:
            try:
                acquired = bool(
                    await connection.scalar(
                        text("SELECT pg_try_advisory_lock(hashtext(:key)::bigint)"),
                        key,
                    )
                )
                yield acquired
            finally:
                if acquired:
                    await connection.execute(
                        text("SELECT pg_advisory_unlock(hashtext(:key)::bigint)"),
                        key,
                    )

    async def reap_abandoned(self, worker: str, stale_after: timedelta) -> int:
        """Close out runs whose process died without finishing them.

        Only relabels rows; a live run keeps its heartbeat fresh and is never
        touched. Returns how many runs were reaped so the caller can log it.
        """
        cutoff = datetime.now(timezone.utc) - stale_after
        async with self._engine.begin() as connection:
            result = await connection.execute(
                update(worker_runs)
                .where(
                    worker_runs.c.worker == worker,
                    worker_runs.c.status == "running",
                    worker_runs.c.heartbeat_at < cutoff,
                )
                .values(
                    status="abandoned",
                    finished_at=datetime.now(timezone.utc),
                    error_message="Heartbeat stopped; reaped by a later run",
                )
            )
            return int(result.rowcount or 0)

    async def start_run(
        self,
        *,
        worker: str,
        trigger: WorkerTrigger,
        window_start: datetime | None = None,
        window_end: datetime | None = None,
        params: Mapping[str, Any] | None = None,
    ) -> int:
        now = datetime.now(timezone.utc)
        async with self._engine.begin() as connection:
            result = await connection.execute(
                worker_runs.insert()
                .values(
                    worker=worker,
                    trigger=trigger,
                    status="running",
                    started_at=now,
                    heartbeat_at=now,
                    window_start=window_start,
                    window_end=window_end,
                    params=dict(params or {}),
                )
                .returning(worker_runs.c.id)
            )
            return int(result.scalar_one())

    async def finish_run(
        self,
        run_id: int,
        status: RunStatus,
        stats: Any,
        error_message: str | None = None,
    ) -> None:
        async with self._engine.begin() as connection:
            await connection.execute(
                update(worker_runs)
                .where(worker_runs.c.id == run_id)
                .values(
                    status=status,
                    finished_at=datetime.now(timezone.utc),
                    heartbeat_at=datetime.now(timezone.utc),
                    stats=self._stats_payload(stats),
                    error_message=(error_message or "")[:2000] or None,
                )
            )

    async def update_run(self, run_id: int, stats: Any) -> None:
        """Checkpoint stats and refresh the heartbeat in one write.

        Both express the same event -- "a chunk just finished" -- so keeping
        them in a single statement makes it impossible to record progress
        without also proving the run is alive.
        """
        async with self._engine.begin() as connection:
            await connection.execute(
                update(worker_runs)
                .where(worker_runs.c.id == run_id)
                .values(
                    stats=self._stats_payload(stats),
                    heartbeat_at=datetime.now(timezone.utc),
                )
            )

    @staticmethod
    def _stats_payload(stats: Any) -> dict[str, Any]:
        """Accept any counter dataclass so each worker can define its own."""
        if stats is None:
            return {}
        if is_dataclass(stats) and not isinstance(stats, type):
            return asdict(stats)
        return dict(stats)

    async def queue_status(self, *, runs_window_hours: int = 24) -> QueueStatusSnapshot:
        """Read every operational queue in one pass, without writing anything.

        Exists so monitoring -- a scheduler task, a smoke check -- goes through
        the repository like every other reader instead of opening its own
        connection and querying `processing_jobs` directly.
        """
        now = datetime.now(timezone.utc)
        cutoff = now - timedelta(hours=runs_window_hours)
        async with self._engine.connect() as connection:
            stage_rows = (
                await connection.execute(
                    select(
                        processing_jobs.c.stage,
                        processing_jobs.c.state,
                        func.count().label("count"),
                        func.min(processing_jobs.c.available_at).label("oldest"),
                    ).group_by(processing_jobs.c.stage, processing_jobs.c.state)
                )
            ).all()
            failure_rows = (
                await connection.execute(
                    select(
                        crawl_failures.c.failure_kind,
                        func.count().label("total"),
                        func.count()
                        .filter(crawl_failures.c.dead_lettered_at.isnot(None))
                        .label("dead_lettered"),
                        func.count()
                        .filter(
                            crawl_failures.c.dead_lettered_at.is_(None),
                            crawl_failures.c.next_retry_at <= now,
                        )
                        .label("due_now"),
                        func.max(crawl_failures.c.attempt_count).label("max_attempts"),
                    )
                    .group_by(crawl_failures.c.failure_kind)
                    .order_by(crawl_failures.c.failure_kind)
                )
            ).all()
            run_rows = (
                await connection.execute(
                    select(
                        worker_runs.c.worker,
                        worker_runs.c.status,
                        func.count().label("count"),
                    )
                    .where(worker_runs.c.started_at >= cutoff)
                    .group_by(worker_runs.c.worker, worker_runs.c.status)
                    .order_by(worker_runs.c.worker, worker_runs.c.status)
                )
            ).all()

        counts: dict[str, dict[str, int]] = defaultdict(dict)
        oldest_pending: dict[str, datetime] = {}
        for row in stage_rows:
            counts[row.stage][row.state] = int(row.count)
            if row.state == "pending" and row.oldest is not None:
                oldest_pending[row.stage] = row.oldest

        stages = tuple(
            StageQueueStatus(
                stage=stage,
                pending=states.get("pending", 0),
                running=states.get("running", 0),
                done=states.get("done", 0),
                dead_letter=states.get("dead_letter", 0),
                oldest_pending_at=oldest_pending.get(stage),
            )
            for stage, states in sorted(counts.items())
        )
        return QueueStatusSnapshot(
            captured_at=now,
            runs_window_hours=runs_window_hours,
            stages=stages,
            failures=tuple(
                FailureQueueStatus(
                    failure_kind=row.failure_kind,
                    total=int(row.total),
                    dead_lettered=int(row.dead_lettered),
                    due_now=int(row.due_now),
                    max_attempts=int(row.max_attempts or 0),
                )
                for row in failure_rows
            ),
            runs=tuple(
                WorkerRunCount(
                    worker=row.worker,
                    status=row.status,
                    count=int(row.count),
                )
                for row in run_rows
            ),
        )

    @staticmethod
    async def _ensure_sources(
        connection: AsyncConnection,
        slugs: set[str],
    ) -> dict[str, int]:
        """Create missing sources once per batch and return their stable IDs."""
        if not slugs:
            return {}
        values = [{"slug": slug, "name": slug} for slug in sorted(slugs)]
        await connection.execute(
            pg_insert(sources)
            .values(values)
            .on_conflict_do_nothing(index_elements=[sources.c.slug])
        )
        result = await connection.execute(
            select(sources.c.slug, sources.c.id).where(sources.c.slug.in_(slugs))
        )
        return {row.slug: int(row.id) for row in result}

    async def apply_batch(
        self,
        articles: list[RawArticle],
        failures: list[CrawlFailureOccurrence],
        resolved_failures: list[FailureTarget],
        *,
        received: int,
        rejected: int,
        priority: int = 1,
    ) -> StorageWriteSummary:
        written = 0
        written_article_ids: list[int] = []
        async with self._engine.begin() as connection:
            article_slugs = {
                article.url: source_slug(article.url) for article in articles
            }
            source_ids = await self._ensure_sources(
                connection,
                set(article_slugs.values()),
            )

            failure_by_key = {
                (failure.failure_kind, failure.url): failure for failure in failures
            }
            if failure_by_key:
                keys = list(failure_by_key)
                existing_rows = await connection.execute(
                    select(crawl_failures).where(
                        tuple_(
                            crawl_failures.c.failure_kind,
                            crawl_failures.c.url,
                        ).in_(keys)
                    )
                )
                existing = {
                    (row.failure_kind, row.url): row for row in existing_rows
                }
                now = datetime.now(timezone.utc)
                for key in sorted(failure_by_key):
                    failure = failure_by_key[key]
                    previous = existing.get(key)
                    attempt = (previous.attempt_count if previous else 0) + 1
                    exhausted = failure.permanent or attempt >= MAX_FAILURE_ATTEMPTS
                    dead_lettered_at = (
                        previous.dead_lettered_at
                        if previous and previous.dead_lettered_at
                        else (now if exhausted else None)
                    )
                    values = {
                        "failure_kind": failure.failure_kind,
                        "url": failure.url,
                        "source": (
                            failure.source
                            if failure.source is not None
                            else previous.source if previous else None
                        ),
                        "title": (
                            failure.title
                            if failure.title is not None
                            else previous.title if previous else None
                        ),
                        "published_at": (
                            failure.published_at
                            if failure.published_at is not None
                            else previous.published_at if previous else None
                        ),
                        "priority": min(
                            int(previous.priority) if previous else priority,
                            priority,
                        ),
                        "error_type": failure.error_type[:200],
                        "error_message": failure.error_message[:2000],
                        "attempt_count": attempt,
                        "first_failed_at": previous.first_failed_at if previous else now,
                        "last_failed_at": now,
                        "dead_lettered_at": dead_lettered_at,
                        # A dead-lettered row keeps whatever schedule it had;
                        # one dead-lettered on first sight (a permanent
                        # failure) has none to keep.
                        "next_retry_at": (
                            previous.next_retry_at
                            if dead_lettered_at and previous
                            else now + retry_delay(attempt)
                        ),
                    }
                    stmt = pg_insert(crawl_failures).values(**values)
                    await connection.execute(
                        stmt.on_conflict_do_update(
                            index_elements=[
                                crawl_failures.c.failure_kind,
                                crawl_failures.c.url,
                            ],
                            set_=values,
                        )
                    )

            if resolved_failures:
                keys = {(item.failure_kind, item.url) for item in resolved_failures}
                await connection.execute(
                    delete(crawl_failures).where(
                        tuple_(
                            crawl_failures.c.failure_kind,
                            crawl_failures.c.url,
                        ).in_(keys)
                    )
                )

            # Concurrent instant and periodic crawl workers can discover the
            # same URLs. Lock rows in a stable order to avoid deadlocks when
            # their batches overlap in a different discovery order.
            for article in sorted(articles, key=lambda item: item.url):
                stmt = pg_insert(raw_articles).values(
                    url=article.url,
                    url_original=article.url_original,
                    source_id=source_ids[article_slugs[article.url]],
                    title=article.title,
                    source=article.source,
                    lang=article.lang,
                    content=article.content,
                    content_hash=article.content_hash,
                    feed_summary=article.feed_summary,
                    content_source=article.content_source,
                    published_at=article.published_at,
                    crawled_at=article.crawled_at,
                )
                incoming = stmt.excluded
                merged_title = func.coalesce(incoming.title, raw_articles.c.title)
                merged_source = func.coalesce(incoming.source, raw_articles.c.source)
                merged_lang = func.coalesce(incoming.lang, raw_articles.c.lang)
                merged_published = func.coalesce(
                    incoming.published_at,
                    raw_articles.c.published_at,
                )
                # A summary-only row must never blank out a body we already
                # have: the same URL can arrive again from a retry that failed,
                # and the three content columns have to move together or not
                # at all.
                has_content = incoming.content != ""
                merged_content = case(
                    (has_content, incoming.content), else_=raw_articles.c.content
                )
                merged_content_hash = case(
                    (has_content, incoming.content_hash),
                    else_=raw_articles.c.content_hash,
                )
                merged_content_source = case(
                    (has_content, incoming.content_source),
                    else_=raw_articles.c.content_source,
                )
                # Same rule for the dek, in its own right: a retry carries no
                # feed entry, so its empty summary says "unknown", not "gone".
                merged_feed_summary = case(
                    (incoming.feed_summary != "", incoming.feed_summary),
                    else_=raw_articles.c.feed_summary,
                )
                statement = stmt.on_conflict_do_update(
                    index_elements=[raw_articles.c.url],
                    set_={
                        "url_original": incoming.url_original,
                        "title": merged_title,
                        "source": merged_source,
                        "source_id": incoming.source_id,
                        "lang": merged_lang,
                        "content": merged_content,
                        "content_hash": merged_content_hash,
                        "feed_summary": merged_feed_summary,
                        "content_source": merged_content_source,
                        "published_at": merged_published,
                        "crawled_at": incoming.crawled_at,
                        "updated_at": func.now(),
                    },
                    where=or_(
                        raw_articles.c.content_hash.is_distinct_from(merged_content_hash),
                        raw_articles.c.feed_summary.is_distinct_from(merged_feed_summary),
                        raw_articles.c.content_source.is_distinct_from(
                            merged_content_source
                        ),
                        raw_articles.c.title.is_distinct_from(merged_title),
                        raw_articles.c.source.is_distinct_from(merged_source),
                        raw_articles.c.source_id.is_distinct_from(incoming.source_id),
                        raw_articles.c.lang.is_distinct_from(merged_lang),
                        raw_articles.c.published_at.is_distinct_from(merged_published),
                        raw_articles.c.url_original.is_distinct_from(incoming.url_original),
                    ),
                ).returning(raw_articles.c.id)
                result = await connection.execute(statement)
                written_id = result.scalar_one_or_none()
                if written_id is not None:
                    written += 1
                    written_article_ids.append(int(written_id))

            if written_article_ids:
                await self._enqueue_embed_jobs(
                    connection, written_article_ids, priority=priority
                )

        deduplicated = max(received - rejected - len(articles), 0)
        unchanged = max(len(articles) - written, 0)
        return StorageWriteSummary(
            received=received,
            deduplicated=deduplicated,
            written=written,
            unchanged=unchanged,
            rejected=rejected,
        )

    @staticmethod
    async def _enqueue_embed_jobs(
        connection: AsyncConnection,
        article_ids: Sequence[int],
        *,
        priority: int = 1,
    ) -> None:
        """Queue (or re-queue) the embed stage for articles just written.

        On conflict, only rows already 'done' or 'dead_letter' are reset to
        'pending' -- an in-flight 'pending'/'running' job from a previous
        write in the same run must not be disturbed.
        """
        now = datetime.now(timezone.utc)
        await PostgresRawStorage._enqueue_processing_jobs(
            connection,
            article_ids,
            ("embed",),
            reset_completed=True,
            priority=priority,
            now=now,
        )

    @staticmethod
    async def _enqueue_processing_jobs(
        connection: AsyncConnection,
        article_ids: Sequence[int],
        stages: Sequence[str],
        *,
        reset_completed: bool,
        priority: int = 1,
        now: datetime | None = None,
    ) -> None:
        if not article_ids or not stages:
            return
        timestamp = now or datetime.now(timezone.utc)
        stmt = pg_insert(processing_jobs).values(
            [
                {
                    "article_id": article_id,
                    "stage": stage,
                    "priority": priority,
                }
                for article_id in article_ids
                for stage in stages
            ]
        )
        resettable = processing_jobs.c.state.in_(("done", "dead_letter"))
        if reset_completed:
            await connection.execute(
                stmt.on_conflict_do_update(
                    index_elements=[
                        processing_jobs.c.article_id,
                        processing_jobs.c.stage,
                    ],
                    set_={
                        # A smaller number is a higher priority. An instant
                        # rediscovery may upgrade an existing normal job, but
                        # a later normal crawl must never downgrade it.
                        "priority": func.least(
                            processing_jobs.c.priority, priority
                        ),
                        "state": case(
                            (resettable, "pending"),
                            else_=processing_jobs.c.state,
                        ),
                        "attempts": case(
                            (resettable, 0), else_=processing_jobs.c.attempts
                        ),
                        "available_at": case(
                            (resettable, timestamp),
                            else_=processing_jobs.c.available_at,
                        ),
                        "lease_until": case(
                            (resettable, None),
                            else_=processing_jobs.c.lease_until,
                        ),
                        "locked_by": case(
                            (resettable, None), else_=processing_jobs.c.locked_by
                        ),
                        "last_error": case(
                            (resettable, None), else_=processing_jobs.c.last_error
                        ),
                        "updated_at": timestamp,
                    },
                )
            )
            return

        # Creating a missing downstream job is enough in the common case.
        # When the row already exists, only upgrade its priority; do not replay
        # completed work or disturb an active lease.
        await connection.execute(
            stmt.on_conflict_do_update(
                index_elements=[processing_jobs.c.article_id, processing_jobs.c.stage],
                set_={
                    "priority": priority,
                    "updated_at": timestamp,
                },
                where=processing_jobs.c.priority > priority,
            )
        )

    async def ensure_processing_jobs(
        self,
        article_ids: Sequence[int],
        stages: Sequence[str],
        *,
        priority: int = 1,
    ) -> None:
        """Create missing downstream jobs without replaying completed work."""
        async with self._engine.begin() as connection:
            await self._enqueue_processing_jobs(
                connection,
                article_ids,
                stages,
                reset_completed=False,
                priority=priority,
            )

    async def embeddable_articles(
        self, article_ids: Sequence[int]
    ) -> dict[int, EmbeddingCandidate]:
        """Load claimed articles alongside their previous embedding, if any.

        Every candidate text comes back, not a pre-chosen one: which of them is
        embedded is `services.text.article_text`'s call, and the hash the
        worker compares against `existing_content_hash` is the hash of that
        chosen text rather than of `raw_articles.content`.
        """
        if not article_ids:
            return {}
        async with self._engine.connect() as connection:
            result = await connection.execute(
                select(
                    raw_articles.c.id,
                    raw_articles.c.title,
                    raw_articles.c.content,
                    raw_articles.c.feed_summary,
                    raw_articles.c.content_source,
                    article_embeddings.c.model.label("existing_model"),
                    article_embeddings.c.content_hash.label("existing_content_hash"),
                )
                .select_from(
                    raw_articles.outerjoin(
                        article_embeddings,
                        raw_articles.c.id == article_embeddings.c.article_id,
                    )
                )
                .where(raw_articles.c.id.in_(article_ids))
            )
            return {
                row.id: EmbeddingCandidate(
                    article_id=row.id,
                    title=row.title,
                    content=row.content,
                    feed_summary=row.feed_summary,
                    content_source=row.content_source,
                    existing_model=row.existing_model,
                    existing_content_hash=row.existing_content_hash,
                )
                for row in result
            }

    async def upsert_embeddings(self, records: Sequence[EmbeddingRecord]) -> None:
        if not records:
            return
        stmt = pg_insert(article_embeddings).values(
            [
                {
                    "article_id": record.article_id,
                    "model": record.model,
                    "dim": record.dim,
                    "content_hash": record.content_hash,
                    "text_kind": record.text_kind,
                    "embedding": record.embedding,
                }
                for record in records
            ]
        )
        async with self._engine.begin() as connection:
            await connection.execute(
                stmt.on_conflict_do_update(
                    index_elements=[article_embeddings.c.article_id],
                    set_={
                        "model": stmt.excluded.model,
                        "dim": stmt.excluded.dim,
                        "content_hash": stmt.excluded.content_hash,
                        "text_kind": stmt.excluded.text_kind,
                        "embedding": stmt.excluded.embedding,
                        "created_at": func.now(),
                    },
                )
            )
            # This transaction closes the crash gap between storing a vector
            # and making it visible to downstream stages. A changed embedding
            # deliberately replays completed cluster/classify jobs.
            # Preserve the priority carried by each claimed embed job. Group
            # by priority so the shared enqueue helper can still use bulk SQL.
            for priority in sorted({record.priority for record in records}):
                await self._enqueue_processing_jobs(
                    connection,
                    [
                        record.article_id
                        for record in records
                        if record.priority == priority
                    ],
                    ("classify", "cluster"),
                    reset_completed=True,
                    priority=priority,
                )

    async def clusterable_articles(
        self, article_ids: Sequence[int]
    ) -> dict[int, ClusterArticle]:
        if not article_ids:
            return {}
        async with self._engine.connect() as connection:
            rows = await connection.execute(
                select(
                    raw_articles.c.id,
                    raw_articles.c.source_id,
                    raw_articles.c.title,
                    raw_articles.c.event_at,
                    article_embeddings.c.model,
                    article_embeddings.c.dim,
                    article_embeddings.c.embedding,
                )
                .select_from(
                    raw_articles.join(
                        article_embeddings,
                        raw_articles.c.id == article_embeddings.c.article_id,
                    )
                )
                .where(raw_articles.c.id.in_(article_ids))
            )
            return {
                row.id: ClusterArticle(
                    article_id=row.id,
                    source_id=row.source_id,
                    title=row.title,
                    event_at=row.event_at,
                    model=row.model,
                    dim=row.dim,
                    embedding=_vector_list(row.embedding),
                )
                for row in rows
            }

    async def cluster_membership(self, article_id: int) -> ClusterMembership | None:
        """Look up an article's current cluster, flagging staleness.

        `stale=True` means the embedding was rewritten (article_embeddings.
        created_at bumped by a genuine re-embed) after this membership was
        recorded, so the centroid this article contributed to no longer
        reflects its current vector.
        """
        async with self._engine.connect() as connection:
            row = (
                await connection.execute(
                    select(
                        cluster_members.c.cluster_id,
                        cluster_members.c.added_at,
                        article_embeddings.c.created_at,
                    )
                    .select_from(
                        cluster_members.join(
                            article_embeddings,
                            cluster_members.c.article_id
                            == article_embeddings.c.article_id,
                        )
                    )
                    .where(cluster_members.c.article_id == article_id)
                )
            ).one_or_none()
            if row is None:
                return None
            return ClusterMembership(
                cluster_id=int(row.cluster_id),
                stale=row.created_at > row.added_at,
            )

    async def active_cluster_candidates(
        self,
        article: ClusterArticle,
        *,
        window: timedelta,
        limit: int,
    ) -> list[ClusterCandidate]:
        if limit <= 0:
            return []
        distance = clusters.c.centroid.cosine_distance(article.embedding)
        async with self._engine.connect() as connection:
            rows = await connection.execute(
                select(
                    clusters.c.id,
                    clusters.c.centroid,
                    clusters.c.centroid_count,
                    clusters.c.first_seen_at,
                    clusters.c.last_seen_at,
                    (1 - distance).label("similarity"),
                )
                .where(
                    clusters.c.status == "open",
                    clusters.c.last_seen_at >= article.event_at - window,
                    clusters.c.first_seen_at <= article.event_at + window,
                )
                .order_by(
                    distance,
                    func.abs(
                        func.extract(
                            "epoch", clusters.c.last_seen_at - article.event_at
                        )
                    ),
                    clusters.c.id,
                )
                .limit(limit)
            )
            return [
                ClusterCandidate(
                    cluster_id=row.id,
                    centroid=_vector_list(row.centroid),
                    centroid_count=row.centroid_count,
                    first_seen_at=row.first_seen_at,
                    last_seen_at=row.last_seen_at,
                    similarity=float(row.similarity),
                )
                for row in rows
            ]

    async def create_cluster(self, article: ClusterArticle) -> ClusterAttachResult:
        """Create a one-member cluster, idempotently per article."""
        async with self._engine.begin() as connection:
            await _lock_article(connection, article.article_id)
            existing = await connection.scalar(
                select(cluster_members.c.cluster_id).where(
                    cluster_members.c.article_id == article.article_id
                )
            )
            if existing is not None:
                count = await connection.scalar(
                    select(clusters.c.centroid_count).where(clusters.c.id == existing)
                )
                return ClusterAttachResult(int(existing), False, int(count or 0))

            cluster_id = int(
                (
                    await connection.execute(
                        clusters.insert()
                        .values(
                            centroid=article.embedding,
                            centroid_count=1,
                            first_seen_at=article.event_at,
                            last_seen_at=article.event_at,
                            n_articles=1,
                            n_distinct_sources=1 if article.source_id is not None else 0,
                            headline=article.title,
                            status="open",
                        )
                        .returning(clusters.c.id)
                    )
                ).scalar_one()
            )
            await connection.execute(
                cluster_members.insert().values(
                    cluster_id=cluster_id,
                    article_id=article.article_id,
                    similarity=1.0,
                )
            )
            # A brand-new cluster inherits whatever labels this article
            # already has: if classify ran first, the story is labelled the
            # moment it exists rather than waiting for the next refresh.
            await _refresh_cluster_topics_locked(connection, cluster_id)
            return ClusterAttachResult(cluster_id, True, 1)

    async def attach_cluster_member(
        self,
        cluster_id: int,
        article: ClusterArticle,
        *,
        similarity: float,
    ) -> ClusterAttachResult:
        """Attach once and update the exact running arithmetic mean atomically."""
        async with self._engine.begin() as connection:
            await _lock_article(connection, article.article_id)
            existing = await connection.scalar(
                select(cluster_members.c.cluster_id).where(
                    cluster_members.c.article_id == article.article_id
                )
            )
            if existing is not None:
                count = await connection.scalar(
                    select(clusters.c.centroid_count).where(clusters.c.id == existing)
                )
                return ClusterAttachResult(int(existing), False, int(count or 0))

            row = (
                await connection.execute(
                    select(
                        clusters.c.centroid,
                        clusters.c.centroid_count,
                        clusters.c.first_seen_at,
                        clusters.c.last_seen_at,
                    )
                    .where(clusters.c.id == cluster_id, clusters.c.status == "open")
                    .with_for_update()
                )
            ).one_or_none()
            if row is None:
                raise StorageError(f"open cluster {cluster_id} not found")

            new_centroid = running_centroid(
                _vector_list(row.centroid), row.centroid_count, article.embedding
            )
            new_count = row.centroid_count + 1
            await connection.execute(
                cluster_members.insert().values(
                    cluster_id=cluster_id,
                    article_id=article.article_id,
                    similarity=similarity,
                )
            )
            source_count = int(
                await connection.scalar(
                    select(func.count(func.distinct(raw_articles.c.source_id)))
                    .select_from(
                        cluster_members.join(
                            raw_articles,
                            cluster_members.c.article_id == raw_articles.c.id,
                        )
                    )
                    .where(cluster_members.c.cluster_id == cluster_id)
                )
                or 0
            )
            headline = await _representative_headline(
                connection, cluster_id, new_centroid
            )
            await connection.execute(
                update(clusters)
                .where(clusters.c.id == cluster_id)
                .values(
                    centroid=new_centroid,
                    centroid_count=new_count,
                    first_seen_at=min(row.first_seen_at, article.event_at),
                    last_seen_at=max(row.last_seen_at, article.event_at),
                    n_articles=new_count,
                    n_distinct_sources=source_count,
                    headline=headline,
                )
            )
            await _refresh_cluster_topics_locked(connection, cluster_id)
            return ClusterAttachResult(cluster_id, True, new_count)

    async def close_stale_clusters(self, *, before: datetime) -> int:
        async with self._engine.begin() as connection:
            result = await connection.execute(
                update(clusters)
                .where(clusters.c.status == "open", clusters.c.last_seen_at < before)
                .values(status="closed")
            )
            return int(result.rowcount or 0)

    async def reenqueue_clusters(self, *, since: datetime, until: datetime) -> int:
        """Queue embedded articles in a window that have no cluster membership."""
        async with self._engine.begin() as connection:
            ids = [
                int(row.article_id)
                for row in await connection.execute(
                    select(article_embeddings.c.article_id)
                    .join(
                        raw_articles,
                        raw_articles.c.id == article_embeddings.c.article_id,
                    )
                    .outerjoin(
                        cluster_members,
                        cluster_members.c.article_id == article_embeddings.c.article_id,
                    )
                    .where(
                        raw_articles.c.event_at >= since,
                        raw_articles.c.event_at < until,
                        cluster_members.c.article_id.is_(None),
                    )
                )
            ]
            await self._enqueue_processing_jobs(
                connection, ids, ("cluster",), reset_completed=True
            )
            return len(ids)

    async def rebuild_cluster(self, cluster_id: int) -> ClusterRebuildResult:
        """Recompute a cluster's aggregate from `cluster_members`, atomically."""
        async with self._engine.begin() as connection:
            return await _rebuild_cluster_locked(connection, cluster_id)

    async def remove_cluster_member(self, article_id: int) -> int | None:
        """Detach an article from its cluster and rebuild that cluster.

        The primitive every delete/re-evaluation path must use instead of
        relying on `ON DELETE CASCADE`, so the cluster's aggregate never goes
        stale silently. Returns the rebuilt cluster's id, or None if the
        article had no membership.
        """
        async with self._engine.begin() as connection:
            cluster_id = await connection.scalar(
                select(cluster_members.c.cluster_id)
                .where(cluster_members.c.article_id == article_id)
                .with_for_update()
            )
            if cluster_id is None:
                return None
            await connection.execute(
                delete(cluster_members).where(
                    cluster_members.c.article_id == article_id
                )
            )
            await _rebuild_cluster_locked(connection, int(cluster_id))
            return int(cluster_id)

    async def merge_candidates(
        self, *, threshold: float, limit: int
    ) -> list[tuple[int, int]]:
        """Pair-scan open clusters by time-window overlap and centroid
        similarity, returning (lower_id, higher_id) pairs to merge."""
        if limit <= 0:
            return []
        a = clusters.alias("cluster_a")
        b = clusters.alias("cluster_b")
        distance = a.c.centroid.cosine_distance(b.c.centroid)
        async with self._engine.connect() as connection:
            rows = await connection.execute(
                select(a.c.id, b.c.id)
                .select_from(a.join(b, a.c.id < b.c.id))
                .where(
                    a.c.status == "open",
                    b.c.status == "open",
                    a.c.first_seen_at <= b.c.last_seen_at,
                    b.c.first_seen_at <= a.c.last_seen_at,
                    (1 - distance) >= threshold,
                )
                .order_by(distance)
                .limit(limit)
            )
            return [(int(row[0]), int(row[1])) for row in rows]

    async def merge_clusters(
        self, cluster_a_id: int, cluster_b_id: int
    ) -> ClusterMergeResult:
        """Merge two open clusters into one, transactionally and idempotently.

        Locks both cluster rows in ascending-id order (never `WHERE id IN
        (a, b)`, whose lock-acquisition order is not guaranteed to follow
        `ORDER BY`) so a concurrent merge over an overlapping pair cannot
        deadlock. Rerunning with the same pair after a successful merge is a
        no-op: the loser is no longer `open`, so the precondition check below
        returns early.
        """
        lower_id, higher_id = sorted((cluster_a_id, cluster_b_id))
        async with self._engine.begin() as connection:
            lower = (
                await connection.execute(
                    select(
                        clusters.c.id, clusters.c.status, clusters.c.first_seen_at
                    )
                    .where(clusters.c.id == lower_id)
                    .with_for_update()
                )
            ).one_or_none()
            higher = (
                await connection.execute(
                    select(
                        clusters.c.id, clusters.c.status, clusters.c.first_seen_at
                    )
                    .where(clusters.c.id == higher_id)
                    .with_for_update()
                )
            ).one_or_none()
            if (
                lower is None
                or higher is None
                or lower.status != "open"
                or higher.status != "open"
            ):
                return ClusterMergeResult(cluster_a_id, cluster_b_id, False)

            canonical_id, loser_id = choose_canonical(
                lower.id, lower.first_seen_at, higher.id, higher.first_seen_at
            )
            await connection.execute(
                update(cluster_members)
                .where(cluster_members.c.cluster_id == loser_id)
                .values(cluster_id=canonical_id)
            )
            await connection.execute(
                update(clusters)
                .where(clusters.c.id == loser_id)
                .values(status="merged", merged_into_id=canonical_id)
            )
            await _rebuild_cluster_locked(connection, canonical_id)
            return ClusterMergeResult(canonical_id, loser_id, True)

    async def repair_clusters(self, *, limit: int) -> int:
        """Detect and fix drift between cached aggregates and the
        `cluster_members` source of truth, then rebuild affected clusters.

        Catches membership rows orphaned by something that deleted an
        article's embedding without going through `remove_cluster_member`
        (e.g. `quarantine_historical_block_pages`), and cached counters that
        no longer match a live `COUNT(cluster_members)`. Running this twice
        with no further drift in between is a no-op.
        """
        if limit <= 0:
            return 0
        async with self._engine.begin() as connection:
            orphan_rows = (
                await connection.execute(
                    select(cluster_members.c.article_id, cluster_members.c.cluster_id)
                    .select_from(
                        cluster_members.outerjoin(
                            article_embeddings,
                            cluster_members.c.article_id
                            == article_embeddings.c.article_id,
                        )
                    )
                    .where(article_embeddings.c.article_id.is_(None))
                )
            ).all()
            orphan_cluster_ids = {int(row.cluster_id) for row in orphan_rows}
            if orphan_rows:
                await connection.execute(
                    delete(cluster_members).where(
                        cluster_members.c.article_id.in_(
                            [row.article_id for row in orphan_rows]
                        )
                    )
                )

            member_counts = (
                select(
                    cluster_members.c.cluster_id,
                    func.count().label("live_count"),
                )
                .group_by(cluster_members.c.cluster_id)
                .subquery()
            )
            drifted_rows = await connection.execute(
                select(clusters.c.id)
                .select_from(
                    clusters.outerjoin(
                        member_counts, clusters.c.id == member_counts.c.cluster_id
                    )
                )
                .where(
                    clusters.c.status != "merged",
                    func.coalesce(member_counts.c.live_count, 0)
                    != clusters.c.centroid_count,
                )
            )
            drifted_ids = {int(row.id) for row in drifted_rows}

            affected = list(orphan_cluster_ids | drifted_ids)[:limit]
            for cluster_id in affected:
                await _rebuild_cluster_locked(connection, cluster_id)
            return len(affected)

    async def reenqueue_embeddings(
        self,
        *,
        since: datetime,
        until: datetime,
        stage: str = "embed",
        model: str | None = None,
    ) -> int:
        """Re-queue already-embedded articles for `--reprocess`/`--model-changed`.

        Filters on `raw_articles.event_at` (the window semantics every other
        worker uses) rather than `article_embeddings.created_at`, so a
        backfill window matches articles by when they happened, not by when
        they were last embedded.
        """
        now = datetime.now(timezone.utc)
        async with self._engine.begin() as connection:
            candidates = select(raw_articles.c.id).where(
                raw_articles.c.event_at >= since, raw_articles.c.event_at < until
            )
            if model is not None:
                candidates = candidates.where(
                    raw_articles.c.id.in_(
                        select(article_embeddings.c.article_id).where(
                            article_embeddings.c.model != model
                        )
                    )
                )
            article_ids = [row.id for row in await connection.execute(candidates)]
            if not article_ids:
                return 0
            stmt = pg_insert(processing_jobs).values(
                [
                    {"article_id": article_id, "stage": stage}
                    for article_id in article_ids
                ]
            )
            result = await connection.execute(
                stmt.on_conflict_do_update(
                    index_elements=[
                        processing_jobs.c.article_id,
                        processing_jobs.c.stage,
                    ],
                    set_={
                        "state": "pending",
                        "attempts": 0,
                        "available_at": now,
                        "lease_until": None,
                        "locked_by": None,
                        "last_error": None,
                        "updated_at": now,
                    },
                )
            )
            return int(result.rowcount or 0)

    async def replay_stage_jobs(self, stage: str, *, limit: int) -> int:
        """Requeue dead-lettered `processing_jobs` rows for another attempt."""
        now = datetime.now(timezone.utc)
        async with self._engine.begin() as connection:
            targets = (
                await connection.execute(
                    select(processing_jobs.c.article_id)
                    .where(
                        processing_jobs.c.stage == stage,
                        processing_jobs.c.state == "dead_letter",
                    )
                    .order_by(processing_jobs.c.updated_at)
                    .limit(limit)
                )
            ).all()
            if not targets:
                return 0
            article_ids = [row.article_id for row in targets]
            result = await connection.execute(
                update(processing_jobs)
                .where(
                    processing_jobs.c.stage == stage,
                    processing_jobs.c.article_id.in_(article_ids),
                )
                .values(
                    state="pending",
                    attempts=0,
                    available_at=now,
                    lease_until=None,
                    locked_by=None,
                    updated_at=now,
                )
            )
            return int(result.rowcount or 0)

    @staticmethod
    def _excerpt_source():
        """The text a list preview is cut from.

        Preview ranks differently from embedding: a dek is written to be read
        as a standalone blurb, so it wins outright here even when the crawled
        body is perfectly clean. The leading slice of `content` is the fallback,
        and both still go through `build_excerpt`, since a feed that ships its
        whole article in <description> would otherwise fill the card.
        """
        return func.coalesce(
            func.nullif(func.left(raw_articles.c.feed_summary, 400), ""),
            func.left(raw_articles.c.content, 400),
        )

    @staticmethod
    def _feed_filters(
        *,
        since: datetime,
        until: datetime,
        cursor: tuple[datetime, int] | None,
        query: str | None,
        topic_slugs: Sequence[str] | None = None,
    ) -> list[Any]:
        filters = [
            raw_articles.c.event_at >= since,
            raw_articles.c.event_at < until,
        ]
        if cursor is not None:
            filters.append(
                tuple_(raw_articles.c.event_at, raw_articles.c.id) < tuple_(*cursor)
            )
        if query:
            filters.append(
                raw_articles.c.search_tsv.op("@@")(
                    func.websearch_to_tsquery("english", query)
                )
            )
        if topic_slugs:
            filters.append(
                select(1)
                .select_from(
                    article_topics.join(topics, article_topics.c.topic_id == topics.c.id)
                )
                .where(
                    article_topics.c.article_id == raw_articles.c.id,
                    article_topics.c.assigned.is_(True),
                    topics.c.active.is_(True),
                    topics.c.slug.in_(topic_slugs),
                )
                .exists()
            )
        return filters

    @staticmethod
    async def _assigned_article_topics(
        connection: AsyncConnection, article_ids: Sequence[int]
    ) -> dict[int, tuple[TopicRefItem, ...]]:
        if not article_ids:
            return {}
        rows = await connection.execute(
            select(
                article_topics.c.article_id,
                topics.c.id,
                topics.c.slug,
                topics.c.display_name,
            )
            .select_from(article_topics.join(topics, article_topics.c.topic_id == topics.c.id))
            .where(
                article_topics.c.article_id.in_(article_ids),
                article_topics.c.assigned.is_(True),
                topics.c.active.is_(True),
            )
            .order_by(article_topics.c.article_id, article_topics.c.score.desc(), topics.c.slug)
        )
        grouped: dict[int, list[TopicRefItem]] = defaultdict(list)
        for row in rows:
            grouped[int(row.article_id)].append(
                TopicRefItem(id=int(row.id), slug=row.slug, display_name=row.display_name)
            )
        return {article_id: tuple(items) for article_id, items in grouped.items()}

    @staticmethod
    async def _assigned_cluster_topics(
        connection: AsyncConnection, cluster_ids: Sequence[int]
    ) -> dict[int, tuple[TopicRefItem, ...]]:
        if not cluster_ids:
            return {}
        rows = await connection.execute(
            select(
                cluster_topics.c.cluster_id,
                topics.c.id,
                topics.c.slug,
                topics.c.display_name,
            )
            .select_from(cluster_topics.join(topics, cluster_topics.c.topic_id == topics.c.id))
            .where(
                cluster_topics.c.cluster_id.in_(cluster_ids),
                cluster_topics.c.assigned.is_(True),
                topics.c.active.is_(True),
            )
            .order_by(cluster_topics.c.cluster_id, cluster_topics.c.score.desc(), topics.c.slug)
        )
        grouped: dict[int, list[TopicRefItem]] = defaultdict(list)
        for row in rows:
            grouped[int(row.cluster_id)].append(
                TopicRefItem(id=int(row.id), slug=row.slug, display_name=row.display_name)
            )
        return {cluster_id: tuple(items) for cluster_id, items in grouped.items()}

    async def feed_articles(
        self,
        *,
        since: datetime,
        until: datetime,
        limit: int,
        cursor: tuple[datetime, int] | None = None,
        source_slugs: Sequence[str] | None = None,
        topic_slugs: Sequence[str] | None = None,
        query: str | None = None,
        max_per_source: int = 3,
    ) -> list[ArticleFeedItem]:
        inner = (
            select(
                raw_articles.c.id,
                raw_articles.c.url,
                raw_articles.c.title,
                self._excerpt_source().label("excerpt_raw"),
                raw_articles.c.published_at,
                raw_articles.c.crawled_at,
                raw_articles.c.event_at,
            )
            .where(raw_articles.c.source_id == sources.c.id)
            .where(
                *self._feed_filters(
                    since=since,
                    until=until,
                    cursor=cursor,
                    query=query,
                    topic_slugs=topic_slugs,
                )
            )
            .order_by(raw_articles.c.event_at.desc(), raw_articles.c.id.desc())
            .limit(max_per_source)
            .lateral("a")
        )
        stmt = (
            select(
                inner.c.id,
                inner.c.url,
                inner.c.title,
                inner.c.excerpt_raw,
                inner.c.published_at,
                inner.c.crawled_at,
                inner.c.event_at,
                sources.c.slug,
                sources.c.name,
            )
            .select_from(sources.join(inner, true()))
        )
        if source_slugs:
            stmt = stmt.where(sources.c.slug.in_(source_slugs))
        stmt = stmt.order_by(inner.c.event_at.desc(), inner.c.id.desc()).limit(limit)

        async with self._engine.connect() as connection:
            rows = (await connection.execute(stmt)).all()
            topics_by_article = await self._assigned_article_topics(
                connection, [row.id for row in rows]
            )
            return [
                ArticleFeedItem(
                    id=row.id,
                    url=row.url,
                    title=row.title,
                    excerpt=build_excerpt(row.excerpt_raw or ""),
                    source_slug=row.slug,
                    source_name=row.name,
                    published_at=row.published_at,
                    crawled_at=row.crawled_at,
                    event_at=row.event_at,
                    topics=topics_by_article.get(int(row.id), ()),
                )
                for row in rows
            ]

    @staticmethod
    def _research_article_filters(
        *,
        since: datetime | None,
        until: datetime | None,
        source: str | None,
    ) -> list[Any]:
        filters: list[Any] = []
        if since is not None:
            filters.append(raw_articles.c.event_at >= since)
        if until is not None:
            filters.append(raw_articles.c.event_at < until)
        if source:
            normalized = source.casefold()
            filters.append(
                or_(
                    func.lower(sources.c.slug) == normalized,
                    func.lower(sources.c.name) == normalized,
                )
            )
        return filters

    async def _lexical_article_candidates(
        self,
        query: str,
        *,
        since: datetime | None,
        until: datetime | None,
        source: str | None,
        limit: int,
    ) -> list[Any]:
        tsquery = func.websearch_to_tsquery("english", query)
        rank = func.ts_rank_cd(raw_articles.c.search_tsv, tsquery)
        stmt = (
            select(
                raw_articles.c.id,
                raw_articles.c.url,
                raw_articles.c.title,
                self._excerpt_source().label("snippet_raw"),
                raw_articles.c.published_at,
                raw_articles.c.event_at,
                sources.c.slug.label("source_slug"),
                sources.c.name.label("source_name"),
            )
            .select_from(
                raw_articles.outerjoin(
                    sources, raw_articles.c.source_id == sources.c.id
                )
            )
            .where(
                raw_articles.c.search_tsv.op("@@")(tsquery),
                *self._research_article_filters(
                    since=since, until=until, source=source
                ),
            )
            .order_by(
                rank.desc(), raw_articles.c.event_at.desc(), raw_articles.c.id.desc()
            )
            .limit(limit)
        )
        async with self._engine.connect() as connection:
            return list((await connection.execute(stmt)).all())

    async def _semantic_article_candidates(
        self,
        embedding: Sequence[float],
        *,
        model: str,
        since: datetime | None,
        until: datetime | None,
        source: str | None,
        limit: int,
    ) -> list[Any]:
        base = (
            select(
                raw_articles.c.id,
                raw_articles.c.url,
                raw_articles.c.title,
                self._excerpt_source().label("snippet_raw"),
                raw_articles.c.published_at,
                raw_articles.c.event_at,
                sources.c.slug.label("source_slug"),
                sources.c.name.label("source_name"),
                article_embeddings.c.embedding,
            )
            .select_from(
                article_embeddings.join(
                    raw_articles,
                    article_embeddings.c.article_id == raw_articles.c.id,
                ).outerjoin(sources, raw_articles.c.source_id == sources.c.id)
            )
            .where(
                article_embeddings.c.model == model,
                article_embeddings.c.dim == len(embedding),
                *self._research_article_filters(
                    since=since, until=until, source=source
                ),
            )
        )
        if source or since is not None or until is not None:
            # pgvector applies ordinary WHERE filters after an approximate HNSW
            # scan, which can under-return for a selective source/time window.
            # Materializing the filtered subset makes that case exact; the
            # common unfiltered path below still uses the HNSW index.
            filtered = base.cte("filtered_article_embeddings").prefix_with(
                "MATERIALIZED"
            )
            distance = filtered.c.embedding.cosine_distance(list(embedding))
            stmt = (
                select(
                    filtered.c.id,
                    filtered.c.url,
                    filtered.c.title,
                    filtered.c.snippet_raw,
                    filtered.c.published_at,
                    filtered.c.event_at,
                    filtered.c.source_slug,
                    filtered.c.source_name,
                )
                .order_by(distance, filtered.c.event_at.desc(), filtered.c.id.desc())
                .limit(limit)
            )
        else:
            distance = article_embeddings.c.embedding.cosine_distance(list(embedding))
            stmt = base.order_by(
                distance, raw_articles.c.event_at.desc(), raw_articles.c.id.desc()
            ).limit(limit)
        async with self._engine.connect() as connection:
            return list((await connection.execute(stmt)).all())

    async def search_articles(
        self,
        query: str,
        embedding: Sequence[float],
        *,
        model: str,
        since: datetime | None = None,
        until: datetime | None = None,
        source: str | None = None,
        limit: int = ARTICLE_SEARCH_LIMIT,
        candidate_limit: int = ARTICLE_SEARCH_CANDIDATE_LIMIT,
    ) -> list[ArticleSearchHit]:
        """Hybrid full-text/vector search merged with reciprocal-rank fusion."""
        lexical_rows, semantic_rows = await asyncio.gather(
            self._lexical_article_candidates(
                query,
                since=since,
                until=until,
                source=source,
                limit=candidate_limit,
            ),
            self._semantic_article_candidates(
                embedding,
                model=model,
                since=since,
                until=until,
                source=source,
                limit=candidate_limit,
            ),
        )
        rows_by_id = {
            int(row.id): row for row in [*lexical_rows, *semantic_rows]
        }
        ranked_ids = reciprocal_rank_fusion(
            [int(row.id) for row in lexical_rows],
            [int(row.id) for row in semantic_rows],
            limit=limit,
            tie_break={
                article_id: (row.event_at, article_id)
                for article_id, row in rows_by_id.items()
            },
        )
        return [
            ArticleSearchHit(
                id=article_id,
                url=rows_by_id[article_id].url,
                title=rows_by_id[article_id].title,
                snippet=build_excerpt(rows_by_id[article_id].snippet_raw or ""),
                source_slug=rows_by_id[article_id].source_slug,
                source_name=rows_by_id[article_id].source_name,
                published_at=rows_by_id[article_id].published_at,
                event_at=rows_by_id[article_id].event_at,
            )
            for article_id in ranked_ids
        ]

    async def source_articles(
        self,
        slug: str,
        *,
        since: datetime,
        until: datetime,
        limit: int,
        cursor: tuple[datetime, int] | None = None,
        topic_slugs: Sequence[str] | None = None,
        query: str | None = None,
    ) -> list[ArticleFeedItem] | None:
        async with self._engine.connect() as connection:
            source_row = (
                await connection.execute(
                    select(sources.c.id, sources.c.slug, sources.c.name).where(
                        sources.c.slug == slug
                    )
                )
            ).first()
            if source_row is None:
                return None

            stmt = (
                select(
                    raw_articles.c.id,
                    raw_articles.c.url,
                    raw_articles.c.title,
                    self._excerpt_source().label("excerpt_raw"),
                    raw_articles.c.published_at,
                    raw_articles.c.crawled_at,
                    raw_articles.c.event_at,
                )
                .where(raw_articles.c.source_id == source_row.id)
                .where(
                    *self._feed_filters(
                        since=since,
                        until=until,
                        cursor=cursor,
                        query=query,
                        topic_slugs=topic_slugs,
                    )
                )
                .order_by(raw_articles.c.event_at.desc(), raw_articles.c.id.desc())
                .limit(limit)
            )
            rows = (await connection.execute(stmt)).all()
            topics_by_article = await self._assigned_article_topics(
                connection, [row.id for row in rows]
            )
            return [
                ArticleFeedItem(
                    id=row.id,
                    url=row.url,
                    title=row.title,
                    excerpt=build_excerpt(row.excerpt_raw or ""),
                    source_slug=source_row.slug,
                    source_name=source_row.name,
                    published_at=row.published_at,
                    crawled_at=row.crawled_at,
                    event_at=row.event_at,
                    topics=topics_by_article.get(int(row.id), ()),
                )
                for row in rows
            ]

    async def list_sources(
        self,
        *,
        since: datetime | None = None,
        until: datetime | None = None,
    ) -> list[SourceItem]:
        stmt = select(sources.c.slug, sources.c.name, sources.c.homepage)
        if since is not None:
            stmt = (
                stmt.select_from(
                    sources.join(raw_articles, raw_articles.c.source_id == sources.c.id)
                )
                .where(raw_articles.c.event_at >= since)
                .distinct()
            )
            if until is not None:
                stmt = stmt.where(raw_articles.c.event_at < until)
        stmt = stmt.order_by(sources.c.name)

        async with self._engine.connect() as connection:
            result = await connection.execute(stmt)
            return [SourceItem(**dict(row._mapping)) for row in result]

    async def get_taxonomy_catalog(
        self, slug: str = "default"
    ) -> TaxonomyCatalogItem | None:
        """Return one active navigation tree plus its active topic registry.

        This is deliberately narrower than ``export_taxonomy``: the public API
        never needs classifier signals, thresholds, prototypes or inactive
        definitions.
        """
        async with self._engine.connect() as connection:
            taxonomy_row = (
                await connection.execute(
                    select(taxonomies).where(
                        taxonomies.c.slug == slug, taxonomies.c.active.is_(True)
                    )
                )
            ).first()
            if taxonomy_row is None:
                return None

            topic_rows = (
                await connection.execute(
                    select(
                        topics.c.id,
                        topics.c.slug,
                        topics.c.display_name,
                        topics.c.description,
                    )
                    .where(topics.c.active.is_(True))
                    .order_by(topics.c.display_name, topics.c.slug)
                )
            ).all()
            active_slugs = {row.slug for row in topic_rows}

            node_rows = (
                await connection.execute(
                    select(taxonomy_nodes)
                    .where(taxonomy_nodes.c.taxonomy_id == taxonomy_row.id)
                    .order_by(taxonomy_nodes.c.sort_order, taxonomy_nodes.c.slug)
                )
            ).all()
            node_ids = [int(row.id) for row in node_rows]
            links_by_node: dict[int, list[str]] = defaultdict(list)
            if node_ids:
                link_rows = await connection.execute(
                    select(taxonomy_node_topics.c.node_id, topics.c.slug)
                    .select_from(
                        taxonomy_node_topics.join(
                            topics, taxonomy_node_topics.c.topic_id == topics.c.id
                        )
                    )
                    .where(
                        taxonomy_node_topics.c.node_id.in_(node_ids),
                        topics.c.active.is_(True),
                    )
                    .order_by(taxonomy_node_topics.c.node_id, topics.c.slug)
                )
                for row in link_rows:
                    if row.slug in active_slugs:
                        links_by_node[int(row.node_id)].append(row.slug)

            flat_nodes = [
                FlatTaxonomyNode(
                    id=int(row.id),
                    parent_id=int(row.parent_id) if row.parent_id is not None else None,
                    slug=row.slug,
                    display_name=row.display_name,
                    sort_order=int(row.sort_order),
                    topic_slugs=tuple(links_by_node.get(int(row.id), [])),
                )
                for row in node_rows
            ]
            return TaxonomyCatalogItem(
                slug=taxonomy_row.slug,
                display_name=taxonomy_row.display_name,
                revision=int(taxonomy_row.revision),
                topics=tuple(
                    TopicCatalogItem(
                        id=int(row.id),
                        slug=row.slug,
                        display_name=row.display_name,
                        description=row.description,
                    )
                    for row in topic_rows
                ),
                nodes=tuple(build_node_tree(flat_nodes)),
            )

    async def get_article(self, article_id: int) -> ArticleDetailItem | None:
        async with self._engine.connect() as connection:
            result = await connection.execute(
                select(
                    raw_articles.c.url,
                    raw_articles.c.url_original,
                    raw_articles.c.title,
                    raw_articles.c.content,
                    raw_articles.c.feed_summary,
                    raw_articles.c.published_at,
                    raw_articles.c.crawled_at,
                    raw_articles.c.event_at,
                    sources.c.slug.label("source_slug"),
                    sources.c.name.label("source_name"),
                )
                .select_from(
                    raw_articles.outerjoin(sources, raw_articles.c.source_id == sources.c.id)
                )
                .where(raw_articles.c.id == article_id)
            )
            row = result.first()
            if row is None:
                return None
            topics_by_article = await self._assigned_article_topics(connection, [article_id])
            return ArticleDetailItem(
                **dict(row._mapping), topics=topics_by_article.get(article_id, ())
            )

    @staticmethod
    async def _top_story_sources(
        connection: AsyncConnection, cluster_ids: Sequence[int]
    ) -> dict[int, list[StorySourceRef]]:
        """Up to `STORY_SOURCE_LIMIT` sources per cluster, ranked by article
        count. One query for the whole page, not one per story."""
        counts = (
            select(
                cluster_members.c.cluster_id,
                sources.c.slug,
                sources.c.name,
                func.count().label("article_count"),
            )
            .select_from(
                cluster_members.join(
                    raw_articles, cluster_members.c.article_id == raw_articles.c.id
                ).join(sources, raw_articles.c.source_id == sources.c.id)
            )
            .where(cluster_members.c.cluster_id.in_(cluster_ids))
            .group_by(cluster_members.c.cluster_id, sources.c.slug, sources.c.name)
            .subquery()
        )
        ranked = (
            select(
                counts.c.cluster_id,
                counts.c.slug,
                counts.c.name,
                func.row_number()
                .over(
                    partition_by=counts.c.cluster_id,
                    order_by=(counts.c.article_count.desc(), counts.c.name),
                )
                .label("rn"),
            )
        ).subquery()
        rows = await connection.execute(
            select(ranked.c.cluster_id, ranked.c.slug, ranked.c.name)
            .where(ranked.c.rn <= STORY_SOURCE_LIMIT)
            .order_by(ranked.c.cluster_id, ranked.c.rn)
        )
        result: dict[int, list[StorySourceRef]] = {}
        for row in rows:
            result.setdefault(int(row.cluster_id), []).append(
                StorySourceRef(slug=row.slug, name=row.name)
            )
        return result

    async def list_stories(
        self,
        *,
        since: datetime,
        until: datetime,
        limit: int,
        cursor: tuple[datetime, int] | None = None,
        source_slugs: Sequence[str] | None = None,
        topic_slugs: Sequence[str] | None = None,
        min_sources: int = 1,
        min_articles: int = 1,
    ) -> list[StoryFeedItem]:
        """List canonical stories (open or closed, never a merge loser),
        newest by `last_seen_at` first. `source_slugs` filters to stories with
        at least one member article from any of those sources, done with an
        `EXISTS` so filtering can never surface duplicate rows."""
        filters: list[Any] = [
            clusters.c.status != "merged",
            clusters.c.last_seen_at >= since,
            clusters.c.last_seen_at < until,
            clusters.c.n_distinct_sources >= min_sources,
            clusters.c.n_articles >= min_articles,
        ]
        if cursor is not None:
            filters.append(
                tuple_(clusters.c.last_seen_at, clusters.c.id) < tuple_(*cursor)
            )
        if source_slugs:
            filters.append(
                select(1)
                .select_from(
                    cluster_members.join(
                        raw_articles,
                        cluster_members.c.article_id == raw_articles.c.id,
                    ).join(sources, raw_articles.c.source_id == sources.c.id)
                )
                .where(
                    cluster_members.c.cluster_id == clusters.c.id,
                    sources.c.slug.in_(source_slugs),
                )
                .exists()
            )
        if topic_slugs:
            filters.append(
                select(1)
                .select_from(
                    cluster_topics.join(topics, cluster_topics.c.topic_id == topics.c.id)
                )
                .where(
                    cluster_topics.c.cluster_id == clusters.c.id,
                    cluster_topics.c.assigned.is_(True),
                    topics.c.active.is_(True),
                    topics.c.slug.in_(topic_slugs),
                )
                .exists()
            )

        stmt = (
            select(
                clusters.c.id,
                clusters.c.headline,
                clusters.c.first_seen_at,
                clusters.c.last_seen_at,
                clusters.c.n_articles,
                clusters.c.n_distinct_sources,
            )
            .where(*filters)
            .order_by(clusters.c.last_seen_at.desc(), clusters.c.id.desc())
            .limit(limit)
        )

        async with self._engine.connect() as connection:
            rows = (await connection.execute(stmt)).all()
            if not rows:
                return []
            sources_by_cluster = await self._top_story_sources(
                connection, [row.id for row in rows]
            )
            topics_by_cluster = await self._assigned_cluster_topics(
                connection, [row.id for row in rows]
            )
            return [
                StoryFeedItem(
                    id=row.id,
                    headline=row.headline,
                    first_seen_at=row.first_seen_at,
                    last_seen_at=row.last_seen_at,
                    article_count=row.n_articles,
                    source_count=row.n_distinct_sources,
                    sources=sources_by_cluster.get(row.id, []),
                    topics=topics_by_cluster.get(int(row.id), ()),
                )
                for row in rows
            ]

    async def get_story(self, story_id: int) -> StoryDetailItem | None:
        """A story's members, resolving a merge loser to its canonical
        cluster. Merges are always a single hop -- only an `open` cluster can
        become a merge loser, so a `merged_into_id` never chains."""
        async with self._engine.connect() as connection:
            row = (
                await connection.execute(
                    select(clusters).where(clusters.c.id == story_id)
                )
            ).first()
            if row is None:
                return None
            if row.merged_into_id is not None:
                row = (
                    await connection.execute(
                        select(clusters).where(clusters.c.id == row.merged_into_id)
                    )
                ).first()
                if row is None:
                    return None

            member_rows = await connection.execute(
                select(
                    raw_articles.c.id,
                    raw_articles.c.title,
                    raw_articles.c.url,
                    raw_articles.c.published_at,
                    raw_articles.c.event_at,
                    sources.c.slug.label("source_slug"),
                    sources.c.name.label("source_name"),
                )
                .select_from(
                    cluster_members.join(
                        raw_articles,
                        cluster_members.c.article_id == raw_articles.c.id,
                    ).outerjoin(sources, raw_articles.c.source_id == sources.c.id)
                )
                .where(cluster_members.c.cluster_id == row.id)
                .order_by(raw_articles.c.event_at.desc(), raw_articles.c.id.desc())
            )
            topics_by_cluster = await self._assigned_cluster_topics(connection, [int(row.id)])
            return StoryDetailItem(
                id=int(row.id),
                headline=row.headline,
                first_seen_at=row.first_seen_at,
                last_seen_at=row.last_seen_at,
                article_count=row.n_articles,
                source_count=row.n_distinct_sources,
                topics=topics_by_cluster.get(int(row.id), ()),
                members=[
                    StoryMember(
                        article_id=member.id,
                        title=member.title,
                        url=member.url,
                        source_slug=member.source_slug,
                        source_name=member.source_name,
                        published_at=member.published_at,
                        event_at=member.event_at,
                    )
                    for member in member_rows
                ],
            )

    async def _storyline_member_evidence(
        self,
        connection: AsyncConnection,
        story_ids: Sequence[int],
        *,
        per_story_limit: int | None,
    ) -> dict[int, tuple[StorylineMemberEvidence, ...]]:
        if not story_ids:
            return {}
        ranked = (
            select(
                cluster_members.c.cluster_id,
                raw_articles.c.id.label("article_id"),
                raw_articles.c.title,
                raw_articles.c.url,
                raw_articles.c.published_at,
                raw_articles.c.event_at,
                sources.c.slug.label("source_slug"),
                sources.c.name.label("source_name"),
                self._excerpt_source().label("snippet_raw"),
                func.row_number()
                .over(
                    partition_by=cluster_members.c.cluster_id,
                    order_by=(
                        raw_articles.c.event_at.desc(),
                        raw_articles.c.id.desc(),
                    ),
                )
                .label("rn"),
            )
            .select_from(
                cluster_members.join(
                    raw_articles,
                    cluster_members.c.article_id == raw_articles.c.id,
                ).outerjoin(sources, raw_articles.c.source_id == sources.c.id)
            )
            .where(cluster_members.c.cluster_id.in_(story_ids))
            .subquery()
        )
        stmt = select(ranked).order_by(
            ranked.c.cluster_id, ranked.c.event_at.desc(), ranked.c.article_id.desc()
        )
        if per_story_limit is not None:
            stmt = stmt.where(ranked.c.rn <= per_story_limit)
        rows = (await connection.execute(stmt)).all()
        grouped: dict[int, list[StorylineMemberEvidence]] = defaultdict(list)
        for row in rows:
            grouped[int(row.cluster_id)].append(
                StorylineMemberEvidence(
                    article_id=int(row.article_id),
                    title=row.title,
                    url=row.url,
                    source_slug=row.source_slug,
                    source_name=row.source_name,
                    published_at=row.published_at,
                    event_at=row.event_at,
                    snippet=build_excerpt(row.snippet_raw or ""),
                )
            )
        return {
            story_id: tuple(members) for story_id, members in grouped.items()
        }

    async def search_storylines(
        self,
        embedding: Sequence[float],
        *,
        since: datetime | None = None,
        until: datetime | None = None,
        limit: int = STORYLINE_SEARCH_LIMIT,
        preview_limit: int = STORYLINE_PREVIEW_LIMIT,
    ) -> list[StorylineSearchHit]:
        """Semantic search over canonical event clusters exposed as storylines."""
        filters: list[Any] = [clusters.c.status != "merged"]
        if since is not None:
            filters.append(clusters.c.last_seen_at >= since)
        if until is not None:
            filters.append(clusters.c.last_seen_at < until)
        distance = clusters.c.centroid.cosine_distance(list(embedding))
        stmt = (
            select(
                clusters.c.id,
                clusters.c.headline,
                clusters.c.last_seen_at,
                clusters.c.n_articles,
            )
            .where(*filters)
            .order_by(distance, clusters.c.last_seen_at.desc(), clusters.c.id.desc())
            .limit(limit)
        )
        async with self._engine.connect() as connection:
            rows = (await connection.execute(stmt)).all()
            previews = await self._storyline_member_evidence(
                connection,
                [int(row.id) for row in rows],
                per_story_limit=preview_limit,
            )
            return [
                StorylineSearchHit(
                    id=int(row.id),
                    title=row.headline,
                    updated_at=row.last_seen_at,
                    article_count=int(row.n_articles),
                    member_preview=previews.get(int(row.id), ()),
                )
                for row in rows
            ]

    async def get_storyline_evidence(
        self, storyline_id: int
    ) -> StorylineDetailEvidence | None:
        """Return one canonical cluster and all member previews for research."""
        async with self._engine.connect() as connection:
            row = (
                await connection.execute(
                    select(clusters).where(clusters.c.id == storyline_id)
                )
            ).first()
            if row is None:
                return None
            if row.merged_into_id is not None:
                row = (
                    await connection.execute(
                        select(clusters).where(clusters.c.id == row.merged_into_id)
                    )
                ).first()
                if row is None:
                    return None
            members = await self._storyline_member_evidence(
                connection, [int(row.id)], per_story_limit=None
            )
            return StorylineDetailEvidence(
                id=int(row.id),
                title=row.headline,
                first_seen_at=row.first_seen_at,
                updated_at=row.last_seen_at,
                article_count=int(row.n_articles),
                source_count=int(row.n_distinct_sources),
                members=members.get(int(row.id), ()),
            )

    async def due_failures(
        self, limit: int, *, priority: int | None = None
    ) -> list[StoredCrawlFailure]:
        now = datetime.now(timezone.utc)
        async with self._engine.connect() as connection:
            stmt = select(crawl_failures).where(
                crawl_failures.c.next_retry_at <= now,
                crawl_failures.c.dead_lettered_at.is_(None),
            )
            if priority is not None:
                stmt = stmt.where(crawl_failures.c.priority == priority)
            result = await connection.execute(
                stmt.order_by(
                    crawl_failures.c.priority,
                    crawl_failures.c.next_retry_at,
                ).limit(limit)
            )
            return [StoredCrawlFailure(**dict(row._mapping)) for row in result]

    async def purge_stale_feed_failures(self) -> int:
        """Drop feed failures the feed itself has since disproved.

        A feed-kind row is resolved by the next successful poll, but a run cut
        short between the poll and the resolving write leaves the row behind
        forever: nothing else revisits it. Reconciling against
        `feed_state.last_success_at` keeps the queue honest without waiting
        for a coincidence.
        """
        async with self._engine.begin() as connection:
            result = await connection.execute(
                delete(crawl_failures).where(
                    crawl_failures.c.failure_kind == "feed",
                    crawl_failures.c.url.in_(
                        select(feed_state.c.feed_url).where(
                            feed_state.c.last_success_at.is_not(None),
                            feed_state.c.last_success_at
                            > crawl_failures.c.last_failed_at,
                        )
                    ),
                )
            )
            return int(result.rowcount or 0)

    async def replay_failures(
        self, *, kind: FailureKind | None = None, limit: int
    ) -> int:
        """Requeue dead-lettered `crawl_failures` rows for another attempt.

        Resets `attempt_count` so a replayed row gets a fresh backoff cycle
        instead of being immediately eligible for dead-lettering again on
        its first renewed failure.
        """
        now = datetime.now(timezone.utc)
        async with self._engine.begin() as connection:
            filters = [crawl_failures.c.dead_lettered_at.is_not(None)]
            if kind is not None:
                filters.append(crawl_failures.c.failure_kind == kind)
            targets = (
                await connection.execute(
                    select(crawl_failures.c.failure_kind, crawl_failures.c.url)
                    .where(*filters)
                    .order_by(crawl_failures.c.last_failed_at)
                    .limit(limit)
                )
            ).all()
            if not targets:
                return 0
            keys = [(row.failure_kind, row.url) for row in targets]
            result = await connection.execute(
                update(crawl_failures)
                .where(
                    tuple_(
                        crawl_failures.c.failure_kind, crawl_failures.c.url
                    ).in_(keys)
                )
                .values(attempt_count=0, dead_lettered_at=None, next_retry_at=now)
            )
            return int(result.rowcount or 0)

    async def historical_block_pages(
        self, *, limit: int = 100
    ) -> list[HistoricalBlockPage]:
        """Find legacy summary-only rows that inherited a block-page title.

        The body of these rows was already discarded, so their feed summary is
        still trustworthy. Only the title needs quarantine and the original URL
        needs another crawl attempt.
        """
        async with self._engine.connect() as connection:
            rows = await connection.execute(
                select(
                    raw_articles.c.id,
                    raw_articles.c.url,
                    raw_articles.c.title,
                    raw_articles.c.source,
                    raw_articles.c.published_at,
                )
                .where(
                    raw_articles.c.content_source == "feed_summary",
                    raw_articles.c.content == "",
                    raw_articles.c.feed_summary != "",
                    raw_articles.c.title.is_not(None),
                )
                .order_by(raw_articles.c.id)
            )

            findings: list[HistoricalBlockPage] = []
            for row in rows:
                reason = historical_summary_title_reason(row.title, row.url)
                if not reason:
                    continue
                findings.append(
                    HistoricalBlockPage(
                        article_id=int(row.id),
                        url=row.url,
                        source=row.source,
                        published_at=row.published_at,
                        reason=reason,
                    )
                )
                if len(findings) >= limit:
                    break
            return findings

    async def quarantine_historical_block_pages(
        self, findings: Sequence[HistoricalBlockPage]
    ) -> int:
        """Clear untrusted titles and make their URLs immediately retryable.

        Feed summaries remain in ``raw_articles`` and are re-embedded without
        the interstitial title. Existing embeddings are invalidated. A durable
        crawl failure is also created (or replayed), allowing a future crawl to
        upgrade the row with the real title and body.
        """
        if not findings:
            return 0

        requested = {item.article_id for item in findings}
        now = datetime.now(timezone.utc)
        async with self._engine.begin() as connection:
            rows = (
                await connection.execute(
                    select(
                        raw_articles.c.id,
                        raw_articles.c.url,
                        raw_articles.c.title,
                        raw_articles.c.source,
                        raw_articles.c.published_at,
                    )
                    .where(
                        raw_articles.c.id.in_(requested),
                        raw_articles.c.content_source == "feed_summary",
                        raw_articles.c.content == "",
                        raw_articles.c.feed_summary != "",
                    )
                    .order_by(raw_articles.c.id)
                    .with_for_update()
                )
            ).all()

            eligible: list[tuple[Any, str]] = []
            for row in rows:
                reason = historical_summary_title_reason(row.title, row.url)
                if reason:
                    eligible.append((row, reason))
            if not eligible:
                return 0

            article_ids = [int(row.id) for row, _ in eligible]
            await connection.execute(
                update(raw_articles)
                .where(raw_articles.c.id.in_(article_ids))
                .values(title=None, updated_at=now)
            )
            await connection.execute(
                delete(article_embeddings).where(
                    article_embeddings.c.article_id.in_(article_ids)
                )
            )

            for row, reason in eligible:
                stmt = pg_insert(crawl_failures).values(
                    failure_kind="article",
                    url=row.url,
                    source=row.source,
                    title=None,
                    published_at=row.published_at,
                    error_type="BlockedPage",
                    error_message=f"Historical block-page metadata: {reason}"[:2000],
                    attempt_count=0,
                    first_failed_at=now,
                    last_failed_at=now,
                    next_retry_at=now,
                    dead_lettered_at=None,
                )
                await connection.execute(
                    stmt.on_conflict_do_update(
                        index_elements=[
                            crawl_failures.c.failure_kind,
                            crawl_failures.c.url,
                        ],
                        set_={
                            "source": stmt.excluded.source,
                            "title": None,
                            "published_at": func.coalesce(
                                stmt.excluded.published_at,
                                crawl_failures.c.published_at,
                            ),
                            "error_type": stmt.excluded.error_type,
                            "error_message": stmt.excluded.error_message,
                            "attempt_count": 0,
                            "last_failed_at": now,
                            "next_retry_at": now,
                            "dead_lettered_at": None,
                        },
                    )
                )

            await self._enqueue_embed_jobs(connection, article_ids)
            return len(article_ids)

    async def enable_feed(self, feed_url: str) -> bool:
        async with self._engine.begin() as connection:
            result = await connection.execute(
                update(feed_state)
                .where(feed_state.c.feed_url == feed_url)
                .values(
                    disabled_at=None,
                    disabled_reason=None,
                    updated_at=datetime.now(timezone.utc),
                )
            )
            return bool(result.rowcount)

    async def disable_feed(self, feed_url: str, *, reason: str) -> bool:
        async with self._engine.begin() as connection:
            result = await connection.execute(
                update(feed_state)
                .where(feed_state.c.feed_url == feed_url)
                .values(
                    disabled_at=datetime.now(timezone.utc),
                    disabled_reason=reason[:2000],
                    updated_at=datetime.now(timezone.utc),
                )
            )
            return bool(result.rowcount)

    async def seed_feeds(self, seeds: Sequence[FeedSeed]) -> int:
        """Insert feeds unknown to `feed_state`; never touch existing rows.

        DO NOTHING rather than DO UPDATE: an operator's manual `disabled_at`
        must outlive the feed disappearing from an OPML file, and a feed
        reappearing later must not silently resurrect it.
        """
        if not seeds:
            return 0
        async with self._engine.begin() as connection:
            result = await connection.execute(
                pg_insert(feed_state)
                .values(
                    [
                        {
                            "feed_url": seed.feed_url,
                            "source_label": seed.source_label,
                            "priority": seed.priority,
                        }
                        for seed in seeds
                    ]
                )
                .on_conflict_do_nothing(index_elements=[feed_state.c.feed_url])
                .returning(feed_state.c.feed_url)
            )
            return len(result.fetchall())

    async def create_feed(self, seed: FeedSeed) -> FeedStateItem | None:
        """Insert one operator-managed feed, returning None on duplicate URL."""
        async with self._engine.begin() as connection:
            row = (
                await connection.execute(
                    pg_insert(feed_state)
                    .values(
                        feed_url=seed.feed_url,
                        source_label=seed.source_label,
                        priority=seed.priority,
                    )
                    .on_conflict_do_nothing(index_elements=[feed_state.c.feed_url])
                    .returning(
                        feed_state.c.feed_url,
                        feed_state.c.source_label,
                        feed_state.c.priority,
                        feed_state.c.resolved_url,
                        feed_state.c.last_polled_at,
                        feed_state.c.last_success_at,
                        feed_state.c.next_poll_at,
                        feed_state.c.consecutive_failures,
                        feed_state.c.disabled_at,
                        feed_state.c.disabled_reason,
                        feed_state.c.updated_at,
                    )
                )
            ).first()
            return FeedStateItem(**dict(row._mapping)) if row is not None else None

    async def list_feeds(self, *, active: bool | None = True) -> list[FeedStateItem]:
        stmt = select(
            feed_state.c.feed_url,
            feed_state.c.source_label,
            feed_state.c.priority,
            feed_state.c.resolved_url,
            feed_state.c.last_polled_at,
            feed_state.c.last_success_at,
            feed_state.c.next_poll_at,
            feed_state.c.consecutive_failures,
            feed_state.c.disabled_at,
            feed_state.c.disabled_reason,
            feed_state.c.updated_at,
        )
        if active is True:
            stmt = stmt.where(feed_state.c.disabled_at.is_(None))
        elif active is False:
            stmt = stmt.where(feed_state.c.disabled_at.is_not(None))
        stmt = stmt.order_by(
            case((feed_state.c.disabled_at.is_(None), 0), else_=1),
            feed_state.c.priority,
            feed_state.c.source_label.asc().nulls_last(),
            feed_state.c.feed_url,
        )
        async with self._engine.connect() as connection:
            rows = (await connection.execute(stmt)).all()
            return [FeedStateItem(**dict(row._mapping)) for row in rows]

    async def update_feed(
        self,
        feed_url: str,
        *,
        priority: int | None = None,
        active: bool | None = None,
        disabled_reason: str = "Disabled from feed management UI",
    ) -> FeedStateItem | None:
        """Atomically update an operator-managed feed and return its new state."""
        now = datetime.now(timezone.utc)
        values: dict[str, Any] = {"updated_at": now}
        if priority is not None:
            values.update(
                priority=priority,
                next_poll_at=now,
                poll_interval_seconds=None,
                cadence_checked_at=None,
            )
        if active is True:
            values.update(disabled_at=None, disabled_reason=None)
        elif active is False:
            values.update(disabled_at=now, disabled_reason=disabled_reason[:2000])

        async with self._engine.begin() as connection:
            row = (
                await connection.execute(
                    update(feed_state)
                    .where(feed_state.c.feed_url == feed_url)
                    .values(**values)
                    .returning(
                        feed_state.c.feed_url,
                        feed_state.c.source_label,
                        feed_state.c.priority,
                        feed_state.c.resolved_url,
                        feed_state.c.last_polled_at,
                        feed_state.c.last_success_at,
                        feed_state.c.next_poll_at,
                        feed_state.c.consecutive_failures,
                        feed_state.c.disabled_at,
                        feed_state.c.disabled_reason,
                        feed_state.c.updated_at,
                    )
                )
            ).first()
            return FeedStateItem(**dict(row._mapping)) if row is not None else None

    async def due_feeds(
        self,
        *,
        limit: int,
        feed_urls: Sequence[str] | None = None,
        ignore_schedule: bool = False,
        priority: int | None = None,
        now: datetime | None = None,
    ) -> list[FeedPollTarget]:
        now = now or datetime.now(timezone.utc)
        stmt = select(
            feed_state.c.feed_url,
            feed_state.c.source_label,
            feed_state.c.etag,
            feed_state.c.last_modified,
            feed_state.c.resolved_url,
            feed_state.c.poll_interval_seconds,
            feed_state.c.cadence_checked_at,
            feed_state.c.priority,
        )
        if priority is not None:
            stmt = stmt.where(feed_state.c.priority == priority)
        if feed_urls:
            stmt = stmt.where(feed_state.c.feed_url.in_(list(feed_urls)))
        else:
            stmt = stmt.where(feed_state.c.disabled_at.is_(None))
            if not ignore_schedule:
                stmt = stmt.where(feed_state.c.next_poll_at <= now)
        stmt = stmt.order_by(feed_state.c.next_poll_at)
        if limit:
            stmt = stmt.limit(limit)
        async with self._engine.connect() as connection:
            result = await connection.execute(stmt)
            return [FeedPollTarget(**dict(row._mapping)) for row in result]

    async def set_feed_priority(self, feed_url: str, priority: int) -> bool:
        """Set a feed's crawl tier; smaller values are processed first."""
        now = datetime.now(timezone.utc)
        async with self._engine.begin() as connection:
            result = await connection.execute(
                update(feed_state)
                .where(feed_state.c.feed_url == feed_url)
                .values(
                    priority=priority,
                    # Make the new owner see the feed immediately. Resetting
                    # cadence also prevents a 30-second instant interval from
                    # leaking into adaptive normal polling (or vice versa).
                    next_poll_at=now,
                    poll_interval_seconds=None,
                    cadence_checked_at=None,
                    updated_at=now,
                )
            )
            return bool(result.rowcount)

    async def record_feed_poll(
        self,
        feed_url: str,
        *,
        next_poll_at: datetime,
        not_modified: bool = False,
        etag: str | None = None,
        last_modified: str | None = None,
        last_entry_published_at: datetime | None = None,
        poll_interval_seconds: int | None = None,
        observed_publish_gap_seconds: float | None = None,
        observed_publish_gap_count: int = 0,
        resolved_url: str | None = None,
    ) -> None:
        now = datetime.now(timezone.utc)
        values: dict[str, Any] = {
            "last_polled_at": now,
            "last_success_at": now,
            "consecutive_failures": 0,
            "next_poll_at": next_poll_at,
            "updated_at": now,
        }
        if resolved_url:
            # Remember where a permanent redirect lands so the next poll goes
            # straight there; `feed_url` keeps matching the OPML/TSV entry.
            values["resolved_url"] = resolved_url
        if not not_modified:
            # A 304 carries no new headers/entries, so leave the existing
            # validators and cadence evidence untouched.
            values["etag"] = etag
            values["last_modified"] = last_modified
            values["last_entry_published_at"] = last_entry_published_at
            values["poll_interval_seconds"] = poll_interval_seconds
            values["observed_publish_gap_seconds"] = (
                observed_publish_gap_seconds
            )
            values["observed_publish_gap_count"] = observed_publish_gap_count
            values["cadence_checked_at"] = now
        async with self._engine.begin() as connection:
            await connection.execute(
                update(feed_state)
                .where(feed_state.c.feed_url == feed_url)
                .values(**values)
            )

    async def record_feed_failure(
        self,
        feed_url: str,
        *,
        error_message: str,
        disable_after: int,
        permanent: bool = False,
        resolved_url: str | None = None,
    ) -> FeedFailureOutcome:
        """Record one failed poll; `permanent` disables the feed immediately.

        Counting to `disable_after` only makes sense for failures that might
        stop happening. A 404/410/401 will not, so waiting ten polls to admit
        it just keeps a dead URL in the rotation.
        """
        now = datetime.now(timezone.utc)
        async with self._engine.begin() as connection:
            row = (
                await connection.execute(
                    select(feed_state.c.consecutive_failures)
                    .where(feed_state.c.feed_url == feed_url)
                    .with_for_update()
                )
            ).one()
            consecutive_failures = int(row.consecutive_failures) + 1
            disabled = permanent or consecutive_failures >= disable_after
            values: dict[str, Any] = {
                "last_polled_at": now,
                "consecutive_failures": consecutive_failures,
                "next_poll_at": now + retry_delay(consecutive_failures),
                "updated_at": now,
            }
            if resolved_url:
                # A failed poll can still have proved a permanent redirect on
                # the way to the error; remember it so the retry starts from
                # the new URL instead of paying for the redirect every time.
                values["resolved_url"] = resolved_url
            if disabled:
                values["disabled_at"] = now
                values["disabled_reason"] = error_message[:2000]
            await connection.execute(
                update(feed_state)
                .where(feed_state.c.feed_url == feed_url)
                .values(**values)
            )
        return FeedFailureOutcome(
            consecutive_failures=consecutive_failures, disabled=disabled
        )

    async def known_urls(
        self,
        candidates: Sequence[str],
        *,
        now: datetime | None = None,
    ) -> set[str]:
        """Return candidates already stored, in backoff, or dead-lettered.

        Checks both `raw_articles` columns because a feed's entry URL often
        differs from the final URL after redirects. Failures still in backoff
        and dead-lettered failures are also treated as known so a fresh feed
        discovery cannot bypass either retry policy and crawl the same broken
        URL on every poll.
        """
        if not candidates:
            return set()
        now = now or datetime.now(timezone.utc)
        urls = list(candidates)
        stmt = union(
            select(raw_articles.c.url).where(raw_articles.c.url.in_(urls)),
            select(raw_articles.c.url_original).where(
                raw_articles.c.url_original.in_(urls)
            ),
            select(crawl_failures.c.url).where(
                crawl_failures.c.url.in_(urls),
                or_(
                    crawl_failures.c.next_retry_at > now,
                    crawl_failures.c.dead_lettered_at.is_not(None),
                ),
            ),
        )
        async with self._engine.connect() as connection:
            result = await connection.execute(stmt)
            return {row[0] for row in result if row[0] is not None}

    async def import_taxonomy(
        self, seed: TaxonomySeed, *, prune: bool = False
    ) -> TaxonomyImportSummary:
        """Import topics and taxonomy trees, idempotently.

        Runs as one transaction so a partially-applied seed can never be
        observed: either every topic/node lands, or none does. `prune=False`
        (the default) only ever adds or updates -- nothing is ever removed by
        a seed simply omitting it, matching the spec's opt-in deletion rule.
        """
        async with self._engine.begin() as connection:
            existing_topic_rows = (await connection.execute(select(topics))).all()
            existing_topics_by_slug = {row.slug: row for row in existing_topic_rows}
            existing_signal_rows = (await connection.execute(select(topic_signals))).all()
            signals_by_topic_id: dict[int, list] = defaultdict(list)
            for row in existing_signal_rows:
                signals_by_topic_id[row.topic_id].append(row)

            topic_outcomes: list[TopicImportOutcome] = []
            topic_ids_by_slug: dict[str, int] = {}
            seed_topic_slugs = {topic.slug for topic in seed.topics}

            for seed_topic in seed.topics:
                existing_row = existing_topics_by_slug.get(seed_topic.slug)
                existing_snapshot: TopicSnapshot | None = None
                if existing_row is not None:
                    existing_snapshot = TopicSnapshot(
                        id=existing_row.id,
                        slug=existing_row.slug,
                        display_name=existing_row.display_name,
                        description=existing_row.description,
                        threshold=existing_row.threshold,
                        classifier_revision=existing_row.classifier_revision,
                        active=existing_row.active,
                        signals=[
                            TopicSignalSnapshot(
                                kind=s.kind, value=s.value, weight=s.weight, active=s.active
                            )
                            for s in signals_by_topic_id.get(existing_row.id, [])
                        ],
                    )
                old_revision = existing_snapshot.classifier_revision if existing_snapshot else 0
                new_revision = next_classifier_revision(existing_snapshot, seed_topic)

                stmt = pg_insert(topics).values(
                    slug=seed_topic.slug,
                    display_name=seed_topic.name,
                    description=seed_topic.description,
                    threshold=seed_topic.threshold,
                    classifier_revision=new_revision,
                    active=seed_topic.active,
                )
                result = await connection.execute(
                    stmt.on_conflict_do_update(
                        index_elements=[topics.c.slug],
                        set_={
                            "display_name": stmt.excluded.display_name,
                            "description": stmt.excluded.description,
                            "threshold": stmt.excluded.threshold,
                            "classifier_revision": new_revision,
                            "active": stmt.excluded.active,
                            "updated_at": func.now(),
                        },
                    ).returning(topics.c.id)
                )
                topic_id = int(result.scalar_one())
                topic_ids_by_slug[seed_topic.slug] = topic_id

                desired_signals = [
                    {
                        "topic_id": topic_id,
                        "kind": "exemplar",
                        "value": exemplar,
                        "weight": 1.0,
                        "language": None,
                        "active": True,
                    }
                    for exemplar in seed_topic.exemplars
                ] + [
                    {
                        "topic_id": topic_id,
                        "kind": "keyword",
                        "value": keyword.value,
                        "weight": keyword.weight,
                        "language": None,
                        "active": True,
                    }
                    for keyword in seed_topic.keywords
                ]
                if desired_signals:
                    signal_stmt = pg_insert(topic_signals).values(desired_signals)
                    await connection.execute(
                        signal_stmt.on_conflict_do_update(
                            index_elements=[
                                topic_signals.c.topic_id,
                                topic_signals.c.kind,
                                topic_signals.c.value,
                            ],
                            set_={
                                "weight": signal_stmt.excluded.weight,
                                "language": signal_stmt.excluded.language,
                                "active": True,
                            },
                        )
                    )
                desired_keys = {(s["kind"], s["value"]) for s in desired_signals}
                stale_keys = [
                    (s.kind, s.value)
                    for s in signals_by_topic_id.get(topic_id, [])
                    if s.active and (s.kind, s.value) not in desired_keys
                ]
                if stale_keys:
                    await connection.execute(
                        update(topic_signals)
                        .where(
                            topic_signals.c.topic_id == topic_id,
                            tuple_(topic_signals.c.kind, topic_signals.c.value).in_(
                                stale_keys
                            ),
                        )
                        .values(active=False)
                    )

                topic_outcomes.append(
                    TopicImportOutcome(
                        slug=seed_topic.slug,
                        created=existing_row is None,
                        revision_bumped=new_revision != old_revision,
                        old_revision=old_revision,
                        new_revision=new_revision,
                    )
                )

            pruned_topics: list[str] = []
            if prune:
                stale_topic_rows = [
                    row for row in existing_topic_rows if row.slug not in seed_topic_slugs
                ]
                if stale_topic_rows:
                    await connection.execute(
                        update(topics)
                        .where(topics.c.id.in_([row.id for row in stale_topic_rows]))
                        .values(active=False, updated_at=func.now())
                    )
                    pruned_topics = [row.slug for row in stale_topic_rows]

            taxonomies_created: list[str] = []
            taxonomies_updated: list[str] = []
            nodes_created = 0
            nodes_updated = 0
            pruned_nodes: list[str] = []

            for seed_taxonomy in seed.taxonomies:
                existing_taxonomy_row = (
                    await connection.execute(
                        select(taxonomies).where(taxonomies.c.slug == seed_taxonomy.slug)
                    )
                ).first()
                taxonomy_stmt = pg_insert(taxonomies).values(
                    slug=seed_taxonomy.slug, display_name=seed_taxonomy.name, active=True
                )
                taxonomy_result = await connection.execute(
                    taxonomy_stmt.on_conflict_do_update(
                        index_elements=[taxonomies.c.slug],
                        set_={"display_name": taxonomy_stmt.excluded.display_name},
                    ).returning(taxonomies.c.id)
                )
                taxonomy_id = int(taxonomy_result.scalar_one())
                if existing_taxonomy_row is None:
                    taxonomies_created.append(seed_taxonomy.slug)
                else:
                    taxonomies_updated.append(seed_taxonomy.slug)

                existing_node_rows = (
                    await connection.execute(
                        select(taxonomy_nodes).where(
                            taxonomy_nodes.c.taxonomy_id == taxonomy_id
                        )
                    )
                ).all()
                existing_nodes_by_key = {
                    (row.parent_id, row.slug): row for row in existing_node_rows
                }

                seen_node_ids: set[int] = set()
                created_count, updated_count = await self._upsert_nodes(
                    connection,
                    taxonomy_id,
                    None,
                    seed_taxonomy.nodes,
                    topic_ids_by_slug,
                    existing_nodes_by_key,
                    seen_node_ids,
                )
                nodes_created += created_count
                nodes_updated += updated_count

                if prune:
                    stale_nodes = [
                        row for row in existing_node_rows if row.id not in seen_node_ids
                    ]
                    if stale_nodes:
                        await connection.execute(
                            delete(taxonomy_nodes).where(
                                taxonomy_nodes.c.id.in_([row.id for row in stale_nodes])
                            )
                        )
                        pruned_nodes.extend(row.slug for row in stale_nodes)

            return TaxonomyImportSummary(
                topics=topic_outcomes,
                taxonomies_created=taxonomies_created,
                taxonomies_updated=taxonomies_updated,
                nodes_created=nodes_created,
                nodes_updated=nodes_updated,
                pruned_topics=pruned_topics,
                pruned_nodes=pruned_nodes,
            )

    @staticmethod
    async def _upsert_nodes(
        connection: AsyncConnection,
        taxonomy_id: int,
        parent_id: int | None,
        nodes: Sequence[SeedTaxonomyNode],
        topic_ids_by_slug: Mapping[str, int],
        existing_nodes_by_key: Mapping[tuple[int | None, str], Any],
        seen_node_ids: set[int],
    ) -> tuple[int, int]:
        """Recursively upsert one taxonomy level, reconciling node-topic links.

        Two different ON CONFLICT targets are needed because Postgres treats
        NULL parent_id values as distinct: root nodes conflict on the partial
        unique index over (taxonomy_id, slug) WHERE parent_id IS NULL, and
        non-root nodes conflict on the named (taxonomy_id, parent_id, slug)
        constraint.
        """
        created = 0
        updated = 0
        for index, node in enumerate(nodes):
            existing = existing_nodes_by_key.get((parent_id, node.slug))
            stmt = pg_insert(taxonomy_nodes).values(
                taxonomy_id=taxonomy_id,
                parent_id=parent_id,
                slug=node.slug,
                display_name=node.name,
                sort_order=index,
            )
            if parent_id is None:
                conflict = stmt.on_conflict_do_update(
                    index_elements=[taxonomy_nodes.c.taxonomy_id, taxonomy_nodes.c.slug],
                    index_where=taxonomy_nodes.c.parent_id.is_(None),
                    set_={
                        "display_name": stmt.excluded.display_name,
                        "sort_order": index,
                    },
                )
            else:
                conflict = stmt.on_conflict_do_update(
                    constraint="uq_taxonomy_nodes_taxonomy_parent_slug",
                    set_={
                        "display_name": stmt.excluded.display_name,
                        "sort_order": index,
                    },
                )
            result = await connection.execute(conflict.returning(taxonomy_nodes.c.id))
            node_id = int(result.scalar_one())
            seen_node_ids.add(node_id)
            if existing is None:
                created += 1
            else:
                updated += 1

            desired_topic_ids = {
                topic_ids_by_slug[slug]
                for slug in node.topics
                if slug in topic_ids_by_slug
            }
            current_links = {
                row.topic_id
                for row in (
                    await connection.execute(
                        select(taxonomy_node_topics.c.topic_id).where(
                            taxonomy_node_topics.c.node_id == node_id
                        )
                    )
                ).all()
            }
            to_add = desired_topic_ids - current_links
            to_remove = current_links - desired_topic_ids
            if to_add:
                await connection.execute(
                    taxonomy_node_topics.insert().values(
                        [{"node_id": node_id, "topic_id": tid} for tid in to_add]
                    )
                )
            if to_remove:
                await connection.execute(
                    delete(taxonomy_node_topics).where(
                        taxonomy_node_topics.c.node_id == node_id,
                        taxonomy_node_topics.c.topic_id.in_(to_remove),
                    )
                )

            child_created, child_updated = await PostgresRawStorage._upsert_nodes(
                connection,
                taxonomy_id,
                node_id,
                node.children,
                topic_ids_by_slug,
                existing_nodes_by_key,
                seen_node_ids,
            )
            created += child_created
            updated += child_updated
        return created, updated

    async def export_taxonomy(self) -> tuple[list[TopicSnapshot], list[TaxonomySnapshot]]:
        """Read the full topic registry and taxonomy trees back out, in the
        same shape `services.taxonomy.to_seed_dict` expects."""
        async with self._engine.connect() as connection:
            topic_rows = (
                await connection.execute(select(topics).order_by(topics.c.slug))
            ).all()
            signal_rows = (await connection.execute(select(topic_signals))).all()
            signals_by_topic: dict[int, list[TopicSignalSnapshot]] = defaultdict(list)
            for row in signal_rows:
                signals_by_topic[row.topic_id].append(
                    TopicSignalSnapshot(
                        kind=row.kind, value=row.value, weight=row.weight, active=row.active
                    )
                )
            topic_snapshots = [
                TopicSnapshot(
                    id=row.id,
                    slug=row.slug,
                    display_name=row.display_name,
                    description=row.description,
                    threshold=row.threshold,
                    classifier_revision=row.classifier_revision,
                    active=row.active,
                    signals=signals_by_topic.get(row.id, []),
                )
                for row in topic_rows
            ]
            topic_slug_by_id = {row.id: row.slug for row in topic_rows}

            taxonomy_rows = (
                await connection.execute(select(taxonomies).order_by(taxonomies.c.slug))
            ).all()
            node_rows = (await connection.execute(select(taxonomy_nodes))).all()
            link_rows = (await connection.execute(select(taxonomy_node_topics))).all()
            topic_slugs_by_node: dict[int, list[str]] = defaultdict(list)
            for row in link_rows:
                slug = topic_slug_by_id.get(row.topic_id)
                if slug:
                    topic_slugs_by_node[row.node_id].append(slug)
            nodes_by_taxonomy: dict[int, list] = defaultdict(list)
            for row in node_rows:
                nodes_by_taxonomy[row.taxonomy_id].append(row)

            taxonomy_snapshots = []
            for row in taxonomy_rows:
                flat_nodes = [
                    FlatTaxonomyNode(
                        id=node.id,
                        parent_id=node.parent_id,
                        slug=node.slug,
                        display_name=node.display_name,
                        sort_order=node.sort_order,
                        topic_slugs=tuple(sorted(topic_slugs_by_node.get(node.id, []))),
                    )
                    for node in nodes_by_taxonomy.get(row.id, [])
                ]
                taxonomy_snapshots.append(
                    TaxonomySnapshot(
                        slug=row.slug,
                        display_name=row.display_name,
                        revision=row.revision,
                        active=row.active,
                        nodes=build_node_tree(flat_nodes),
                    )
                )
            return topic_snapshots, taxonomy_snapshots

    async def topics_for_prototype_rebuild(
        self, slug: str | None = None
    ) -> list[TopicExemplarSet]:
        """Active topics' current exemplars, plus their prototype's
        provenance so the caller can skip a topic whose exemplars haven't
        changed since the last compile."""
        async with self._engine.connect() as connection:
            stmt = select(topics).where(topics.c.active.is_(True))
            if slug is not None:
                stmt = stmt.where(topics.c.slug == slug)
            topic_rows = (await connection.execute(stmt)).all()
            if not topic_rows:
                return []
            topic_ids = [row.id for row in topic_rows]
            signal_rows = (
                await connection.execute(
                    select(topic_signals.c.topic_id, topic_signals.c.value).where(
                        topic_signals.c.topic_id.in_(topic_ids),
                        topic_signals.c.kind == "exemplar",
                        topic_signals.c.active.is_(True),
                    )
                )
            ).all()
            exemplars_by_topic: dict[int, list[str]] = defaultdict(list)
            for row in signal_rows:
                exemplars_by_topic[row.topic_id].append(row.value)
            prototype_rows = (
                await connection.execute(
                    select(topic_prototypes).where(
                        topic_prototypes.c.topic_id.in_(topic_ids)
                    )
                )
            ).all()
            prototypes_by_topic = {row.topic_id: row for row in prototype_rows}

            results: list[TopicExemplarSet] = []
            for row in topic_rows:
                prototype = prototypes_by_topic.get(row.id)
                results.append(
                    TopicExemplarSet(
                        topic_id=row.id,
                        slug=row.slug,
                        active=row.active,
                        exemplars=exemplars_by_topic.get(row.id, []),
                        current_definition_hash=(
                            prototype.definition_hash if prototype else None
                        ),
                        current_model=prototype.model if prototype else None,
                        current_dim=prototype.dim if prototype else None,
                    )
                )
            return results

    async def upsert_topic_prototypes(
        self, records: Sequence[TopicPrototypeRecord]
    ) -> None:
        if not records:
            return
        stmt = pg_insert(topic_prototypes).values(
            [
                {
                    "topic_id": record.topic_id,
                    "model": record.model,
                    "dim": record.dim,
                    "definition_hash": record.definition_hash,
                    "embedding": record.embedding,
                }
                for record in records
            ]
        )
        async with self._engine.begin() as connection:
            await connection.execute(
                stmt.on_conflict_do_update(
                    index_elements=[topic_prototypes.c.topic_id],
                    set_={
                        "model": stmt.excluded.model,
                        "dim": stmt.excluded.dim,
                        "definition_hash": stmt.excluded.definition_hash,
                        "embedding": stmt.excluded.embedding,
                        "generated_at": func.now(),
                    },
                )
            )

    async def classifier_snapshot(self) -> ClassifierSnapshot:
        """Load the active classifier set once per run.

        Only active topics with a compiled prototype can score anything; an
        active topic still missing one is returned separately so the worker
        can tell the operator to run `taxonomy-rebuild` instead of silently
        producing classifications that topic could never win.
        """
        async with self._engine.connect() as connection:
            topic_rows = (
                await connection.execute(
                    select(topics)
                    .where(topics.c.active.is_(True))
                    .order_by(topics.c.slug)
                )
            ).all()
            if not topic_rows:
                return ClassifierSnapshot(
                    classifiers=(), set_hash=classifier_set_hash([])
                )
            topic_ids = [row.id for row in topic_rows]
            prototypes = {
                int(row.topic_id): row
                for row in (
                    await connection.execute(
                        select(topic_prototypes).where(
                            topic_prototypes.c.topic_id.in_(topic_ids)
                        )
                    )
                ).all()
            }
            keyword_rows = (
                await connection.execute(
                    select(
                        topic_signals.c.topic_id,
                        topic_signals.c.value,
                        topic_signals.c.weight,
                    ).where(
                        topic_signals.c.topic_id.in_(topic_ids),
                        topic_signals.c.kind == "keyword",
                        topic_signals.c.active.is_(True),
                    )
                )
            ).all()
            keywords_by_topic: dict[int, list[TopicKeyword]] = defaultdict(list)
            for row in keyword_rows:
                keywords_by_topic[int(row.topic_id)].append(
                    TopicKeyword(value=row.value, weight=float(row.weight))
                )

            classifiers: list[TopicClassifier] = []
            missing: list[str] = []
            for row in topic_rows:
                prototype = prototypes.get(int(row.id))
                if prototype is None:
                    missing.append(row.slug)
                    continue
                classifiers.append(
                    TopicClassifier(
                        topic_id=int(row.id),
                        slug=row.slug,
                        threshold=float(row.threshold),
                        classifier_revision=int(row.classifier_revision),
                        model=prototype.model,
                        dim=int(prototype.dim),
                        prototype=tuple(_vector_list(prototype.embedding)),
                        definition_hash=prototype.definition_hash,
                        keywords=tuple(
                            sorted(
                                keywords_by_topic.get(int(row.id), []),
                                key=lambda item: item.value,
                            )
                        ),
                    )
                )
            return ClassifierSnapshot(
                classifiers=tuple(classifiers),
                set_hash=classifier_set_hash(classifiers),
                topics_without_prototype=tuple(missing),
            )

    async def classifiable_articles(
        self, article_ids: Sequence[int]
    ) -> dict[int, ClassifiableArticle]:
        """Load claimed articles with their embedding and keyword-match texts.

        An article with no embedding yet is simply absent from the result:
        classification is defined against the vector, so the worker completes
        that job rather than failing it and waits for embed to catch up.
        """
        if not article_ids:
            return {}
        async with self._engine.connect() as connection:
            result = await connection.execute(
                select(
                    raw_articles.c.id,
                    raw_articles.c.source_id,
                    raw_articles.c.title,
                    raw_articles.c.content,
                    raw_articles.c.feed_summary,
                    raw_articles.c.content_source,
                    article_embeddings.c.model,
                    article_embeddings.c.dim,
                    article_embeddings.c.embedding,
                )
                .select_from(
                    raw_articles.join(
                        article_embeddings,
                        raw_articles.c.id == article_embeddings.c.article_id,
                    )
                )
                .where(raw_articles.c.id.in_(article_ids))
            )
            return {
                row.id: ClassifiableArticle(
                    article_id=row.id,
                    source_id=row.source_id,
                    title=row.title,
                    content=row.content,
                    feed_summary=row.feed_summary,
                    content_source=row.content_source,
                    model=row.model,
                    dim=int(row.dim),
                    embedding=_vector_list(row.embedding),
                )
                for row in result
            }

    async def replace_article_classification(
        self,
        article_id: int,
        classification: ArticleClassification,
        *,
        model: str,
        set_hash: bytes,
    ) -> int | None:
        """Replace one article's scores and refresh its cluster, atomically.

        The whole score set is replaced rather than upserted per topic, so a
        topic that has since been deactivated cannot leave a stale row behind.
        Returns the cluster id that was refreshed, if the article is already a
        member of one -- when clustering has not run yet, the cluster picks
        these scores up through `_refresh_cluster_topics_locked` at attach
        time, which is what makes the two worker orders converge.
        """
        async with self._engine.begin() as connection:
            await connection.execute(
                delete(article_topics).where(article_topics.c.article_id == article_id)
            )
            if classification.scores:
                await connection.execute(
                    article_topics.insert().values(
                        [
                            {
                                "article_id": article_id,
                                "topic_id": score.topic_id,
                                "score": score.score,
                                "prototype_score": score.prototype_score,
                                "keyword_score": score.keyword_score,
                                "source_prior": None,
                                "assigned": score.assigned,
                                "classifier_revision": score.classifier_revision,
                            }
                            for score in classification.scores
                        ]
                    )
                )
            stmt = pg_insert(article_classifications).values(
                article_id=article_id,
                model=model,
                classifier_set_hash=set_hash,
                max_score=classification.max_score,
                assigned_count=classification.assigned_count,
            )
            await connection.execute(
                stmt.on_conflict_do_update(
                    index_elements=[article_classifications.c.article_id],
                    set_={
                        "model": stmt.excluded.model,
                        "classifier_set_hash": stmt.excluded.classifier_set_hash,
                        "max_score": stmt.excluded.max_score,
                        "assigned_count": stmt.excluded.assigned_count,
                        "classified_at": func.now(),
                    },
                )
            )
            cluster_id = await connection.scalar(
                select(cluster_members.c.cluster_id).where(
                    cluster_members.c.article_id == article_id
                )
            )
            if cluster_id is None:
                return None
            await _refresh_cluster_topics_locked(connection, int(cluster_id))
            return int(cluster_id)

    async def refresh_cluster_topics(self, cluster_id: int) -> int:
        """Rebuild one cluster's labels from `article_topics` on demand."""
        async with self._engine.begin() as connection:
            return await _refresh_cluster_topics_locked(connection, cluster_id)

    async def reenqueue_classifications(
        self,
        *,
        since: datetime,
        until: datetime,
        set_hash: bytes | None = None,
    ) -> int:
        """Queue embedded articles in a window for (re)classification.

        With `set_hash`, only *stale* articles are queued: those never
        classified, or classified against a different classifier set. That is
        the cheap path after a `taxonomy-import` bumps one topic's revision --
        it re-scores the window without disturbing articles already scored
        against the current set.
        """
        async with self._engine.begin() as connection:
            stmt = (
                select(article_embeddings.c.article_id)
                .join(
                    raw_articles, raw_articles.c.id == article_embeddings.c.article_id
                )
                .outerjoin(
                    article_classifications,
                    article_classifications.c.article_id
                    == article_embeddings.c.article_id,
                )
                .where(
                    raw_articles.c.event_at >= since,
                    raw_articles.c.event_at < until,
                )
            )
            if set_hash is not None:
                stmt = stmt.where(
                    or_(
                        article_classifications.c.article_id.is_(None),
                        article_classifications.c.classifier_set_hash != set_hash,
                    )
                )
            ids = [int(row.article_id) for row in await connection.execute(stmt)]
            await self._enqueue_processing_jobs(
                connection, ids, ("classify",), reset_completed=True
            )
            return len(ids)

    async def reenqueue_cluster_topic_refresh(
        self, *, since: datetime, until: datetime
    ) -> int:
        """Rebuild `cluster_topics` for every cluster active in a window.

        `cluster_topics` is derived data with no source of truth of its own
        (spec §2.7), so it must be reconstructible without touching
        `article_topics`; this is the command that proves it.
        """
        async with self._engine.begin() as connection:
            cluster_ids = [
                int(row.id)
                for row in await connection.execute(
                    select(clusters.c.id).where(
                        clusters.c.status != "merged",
                        clusters.c.last_seen_at >= since,
                        clusters.c.last_seen_at < until,
                    )
                )
            ]
            for cluster_id in cluster_ids:
                await _refresh_cluster_topics_locked(connection, cluster_id)
            return len(cluster_ids)
