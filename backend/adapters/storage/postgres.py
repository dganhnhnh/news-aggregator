"""Async PostgreSQL repository for raw articles and ingest operations."""

from __future__ import annotations

import random
from collections.abc import AsyncIterator, Callable, Mapping, Sequence
from contextlib import asynccontextmanager
from dataclasses import asdict, is_dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import delete, func, or_, select, text, true, tuple_, union, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, create_async_engine

from adapters.storage.schema import (
    article_embeddings,
    crawl_failures,
    feed_state,
    processing_jobs,
    raw_articles,
    sources,
    worker_runs,
)
from models.storage_models import (
    ArticleDetailItem,
    ArticleFeedItem,
    CrawlFailureOccurrence,
    EmbeddingCandidate,
    EmbeddingRecord,
    FailureKind,
    FailureTarget,
    FeedFailureOutcome,
    FeedPollTarget,
    FeedSeed,
    RawArticle,
    RunStatus,
    SourceItem,
    StoredCrawlFailure,
    StorageWriteSummary,
    WorkerTrigger,
)
from services.raw_storage import build_excerpt, source_slug


class StorageError(RuntimeError):
    """Raised when raw storage is unavailable or not migrated."""


MAX_FAILURE_ATTEMPTS = 5


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
                        "to_regclass('public.article_embeddings')"
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
                for key, failure in failure_by_key.items():
                    previous = existing.get(key)
                    attempt = (previous.attempt_count if previous else 0) + 1
                    dead_lettered_at = (
                        previous.dead_lettered_at
                        if previous and previous.dead_lettered_at
                        else (now if attempt >= MAX_FAILURE_ATTEMPTS else None)
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
                        "error_type": failure.error_type[:200],
                        "error_message": failure.error_message[:2000],
                        "attempt_count": attempt,
                        "first_failed_at": previous.first_failed_at if previous else now,
                        "last_failed_at": now,
                        "dead_lettered_at": dead_lettered_at,
                        "next_retry_at": (
                            previous.next_retry_at
                            if dead_lettered_at
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

            for article in articles:
                stmt = pg_insert(raw_articles).values(
                    url=article.url,
                    url_original=article.url_original,
                    source_id=source_ids[article_slugs[article.url]],
                    title=article.title,
                    source=article.source,
                    lang=article.lang,
                    content=article.content,
                    content_hash=article.content_hash,
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
                statement = stmt.on_conflict_do_update(
                    index_elements=[raw_articles.c.url],
                    set_={
                        "url_original": incoming.url_original,
                        "title": merged_title,
                        "source": merged_source,
                        "source_id": incoming.source_id,
                        "lang": merged_lang,
                        "content": incoming.content,
                        "content_hash": incoming.content_hash,
                        "published_at": merged_published,
                        "crawled_at": incoming.crawled_at,
                        "updated_at": func.now(),
                    },
                    where=or_(
                        raw_articles.c.content_hash.is_distinct_from(incoming.content_hash),
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
                await self._enqueue_embed_jobs(connection, written_article_ids)

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
        connection: AsyncConnection, article_ids: Sequence[int]
    ) -> None:
        """Queue (or re-queue) the embed stage for articles just written.

        On conflict, only rows already 'done' or 'dead_letter' are reset to
        'pending' -- an in-flight 'pending'/'running' job from a previous
        write in the same run must not be disturbed.
        """
        now = datetime.now(timezone.utc)
        stmt = pg_insert(processing_jobs).values(
            [
                {"article_id": article_id, "stage": "embed"}
                for article_id in article_ids
            ]
        )
        await connection.execute(
            stmt.on_conflict_do_update(
                index_elements=[processing_jobs.c.article_id, processing_jobs.c.stage],
                set_={
                    "state": "pending",
                    "attempts": 0,
                    "available_at": now,
                    "lease_until": None,
                    "locked_by": None,
                    "last_error": None,
                    "updated_at": now,
                },
                where=processing_jobs.c.state.in_(("done", "dead_letter")),
            )
        )

    async def embeddable_articles(
        self, article_ids: Sequence[int]
    ) -> dict[int, EmbeddingCandidate]:
        """Load claimed articles alongside their previous embedding, if any.

        Lets `EmbedWorker` skip re-embedding when `content_hash` and `model`
        both match what's already stored.
        """
        if not article_ids:
            return {}
        async with self._engine.connect() as connection:
            result = await connection.execute(
                select(
                    raw_articles.c.id,
                    raw_articles.c.title,
                    raw_articles.c.content,
                    raw_articles.c.content_hash,
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
                    content_hash=row.content_hash,
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
                        "embedding": stmt.excluded.embedding,
                        "created_at": func.now(),
                    },
                )
            )

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
    def _feed_filters(
        *,
        since: datetime,
        until: datetime,
        cursor: tuple[datetime, int] | None,
        query: str | None,
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
        return filters

    async def feed_articles(
        self,
        *,
        since: datetime,
        until: datetime,
        limit: int,
        cursor: tuple[datetime, int] | None = None,
        source_slugs: Sequence[str] | None = None,
        query: str | None = None,
        max_per_source: int = 3,
    ) -> list[ArticleFeedItem]:
        inner = (
            select(
                raw_articles.c.id,
                raw_articles.c.url,
                raw_articles.c.title,
                func.left(raw_articles.c.content, 400).label("excerpt_raw"),
                raw_articles.c.published_at,
                raw_articles.c.crawled_at,
                raw_articles.c.event_at,
            )
            .where(raw_articles.c.source_id == sources.c.id)
            .where(*self._feed_filters(since=since, until=until, cursor=cursor, query=query))
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
            result = await connection.execute(stmt)
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
                )
                for row in result
            ]

    async def source_articles(
        self,
        slug: str,
        *,
        since: datetime,
        until: datetime,
        limit: int,
        cursor: tuple[datetime, int] | None = None,
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
                    func.left(raw_articles.c.content, 400).label("excerpt_raw"),
                    raw_articles.c.published_at,
                    raw_articles.c.crawled_at,
                    raw_articles.c.event_at,
                )
                .where(raw_articles.c.source_id == source_row.id)
                .where(*self._feed_filters(since=since, until=until, cursor=cursor, query=query))
                .order_by(raw_articles.c.event_at.desc(), raw_articles.c.id.desc())
                .limit(limit)
            )
            result = await connection.execute(stmt)
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
                )
                for row in result
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

    async def get_article(self, article_id: int) -> ArticleDetailItem | None:
        async with self._engine.connect() as connection:
            result = await connection.execute(
                select(
                    raw_articles.c.url,
                    raw_articles.c.url_original,
                    raw_articles.c.title,
                    raw_articles.c.content,
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
            return ArticleDetailItem(**dict(row._mapping)) if row is not None else None

    async def due_failures(self, limit: int) -> list[StoredCrawlFailure]:
        now = datetime.now(timezone.utc)
        async with self._engine.connect() as connection:
            result = await connection.execute(
                select(crawl_failures)
                .where(
                    crawl_failures.c.next_retry_at <= now,
                    crawl_failures.c.dead_lettered_at.is_(None),
                )
                .order_by(crawl_failures.c.next_retry_at)
                .limit(limit)
            )
            return [StoredCrawlFailure(**dict(row._mapping)) for row in result]

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
                        {"feed_url": seed.feed_url, "source_label": seed.source_label}
                        for seed in seeds
                    ]
                )
                .on_conflict_do_nothing(index_elements=[feed_state.c.feed_url])
                .returning(feed_state.c.feed_url)
            )
            return len(result.fetchall())

    async def due_feeds(
        self,
        *,
        limit: int,
        feed_urls: Sequence[str] | None = None,
        ignore_schedule: bool = False,
        now: datetime | None = None,
    ) -> list[FeedPollTarget]:
        now = now or datetime.now(timezone.utc)
        stmt = select(
            feed_state.c.feed_url,
            feed_state.c.source_label,
            feed_state.c.etag,
            feed_state.c.last_modified,
        )
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

    async def record_feed_poll(
        self,
        feed_url: str,
        *,
        next_poll_at: datetime,
        not_modified: bool = False,
        etag: str | None = None,
        last_modified: str | None = None,
        last_entry_published_at: datetime | None = None,
    ) -> None:
        now = datetime.now(timezone.utc)
        values: dict[str, Any] = {
            "last_polled_at": now,
            "last_success_at": now,
            "consecutive_failures": 0,
            "next_poll_at": next_poll_at,
            "updated_at": now,
        }
        if not not_modified:
            # A 304 carries no new headers/entries, so leave the existing
            # etag/last_modified/last_entry_published_at untouched.
            values["etag"] = etag
            values["last_modified"] = last_modified
            values["last_entry_published_at"] = last_entry_published_at
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
    ) -> FeedFailureOutcome:
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
            disabled = consecutive_failures >= disable_after
            values: dict[str, Any] = {
                "last_polled_at": now,
                "consecutive_failures": consecutive_failures,
                "next_poll_at": now + retry_delay(consecutive_failures),
                "updated_at": now,
            }
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
        """Return the subset of `candidates` already stored or in backoff.

        Checks both `raw_articles` columns because a feed's entry URL often
        differs from the final URL after redirects, and includes
        `crawl_failures` still in backoff so the exponential delay in
        `retry_delay()` is actually honored instead of being defeated by a
        fresh discovery pass rediscovering the same broken URL every poll.
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
                crawl_failures.c.next_retry_at > now,
            ),
        )
        async with self._engine.connect() as connection:
            result = await connection.execute(stmt)
            return {row[0] for row in result if row[0] is not None}
