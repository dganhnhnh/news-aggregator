"""Async PostgreSQL repository for raw articles and ingest operations."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from sqlalchemy import delete, func, or_, select, text, tuple_, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, create_async_engine

from adapters.storage.schema import crawl_failures, ingest_runs, raw_articles, sources
from models.storage_models import (
    ArticleListItem,
    CrawlFailureOccurrence,
    FailureTarget,
    IngestRunStats,
    RawArticle,
    StoredCrawlFailure,
    StorageWriteSummary,
)
from services.raw_storage import source_slug


class StorageError(RuntimeError):
    """Raised when raw storage is unavailable or not migrated."""


def retry_delay(attempt_count: int) -> timedelta:
    exponent = min(max(attempt_count - 1, 0), 9)
    seconds = min(300 * (2**exponent), 86_400)
    return timedelta(seconds=seconds)


class PostgresRawStorage:
    def __init__(self, database_url: str) -> None:
        if not database_url:
            raise StorageError("DATABASE_URL is required for persistence")
        self._engine: AsyncEngine = create_async_engine(
            database_url,
            pool_pre_ping=True,
        )

    async def close(self) -> None:
        await self._engine.dispose()

    async def check_ready(self) -> None:
        try:
            async with self._engine.connect() as connection:
                tables = await connection.execute(
                    text(
                        "SELECT to_regclass('public.raw_articles'), "
                        "to_regclass('public.crawl_failures'), "
                        "to_regclass('public.ingest_runs'), "
                        "to_regclass('public.sources')"
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

    async def start_run(self, job_type: str) -> int:
        now = datetime.now(timezone.utc)
        async with self._engine.begin() as connection:
            result = await connection.execute(
                ingest_runs.insert()
                .values(job_type=job_type, status="running", started_at=now)
                .returning(ingest_runs.c.id)
            )
            return int(result.scalar_one())

    async def finish_run(
        self,
        run_id: int,
        status: str,
        stats: IngestRunStats,
        error_message: str | None = None,
    ) -> None:
        async with self._engine.begin() as connection:
            await connection.execute(
                update(ingest_runs)
                .where(ingest_runs.c.id == run_id)
                .values(
                    status=status,
                    finished_at=datetime.now(timezone.utc),
                    **self._stats_values(stats),
                    error_message=(error_message or "")[:2000] or None,
                )
            )

    async def update_run(self, run_id: int, stats: IngestRunStats) -> None:
        """Checkpoint counters while the run is still in progress."""
        async with self._engine.begin() as connection:
            await connection.execute(
                update(ingest_runs)
                .where(ingest_runs.c.id == run_id)
                .values(**self._stats_values(stats))
            )

    @staticmethod
    def _stats_values(stats: IngestRunStats) -> dict[str, int]:
        return {
            "feeds_processed": stats.feeds_processed,
            "articles_discovered": stats.articles_discovered,
            "crawl_succeeded": stats.crawl_succeeded,
            "crawl_failed": stats.crawl_failed,
            "retries_attempted": stats.retries_attempted,
            "retries_succeeded": stats.retries_succeeded,
            "articles_written": stats.articles_written,
            "articles_unchanged": stats.articles_unchanged,
        }

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
                        "next_retry_at": now + retry_delay(attempt),
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
                if result.scalar_one_or_none() is not None:
                    written += 1

        deduplicated = max(received - rejected - len(articles), 0)
        unchanged = max(len(articles) - written, 0)
        return StorageWriteSummary(
            received=received,
            deduplicated=deduplicated,
            written=written,
            unchanged=unchanged,
            rejected=rejected,
        )

    async def list_articles(
        self,
        *,
        limit: int,
        offset: int,
        source: str | None = None,
        query: str | None = None,
    ) -> tuple[list[ArticleListItem], int]:
        list_columns = (
            raw_articles.c.id,
            raw_articles.c.url,
            raw_articles.c.title,
            raw_articles.c.source,
            raw_articles.c.published_at,
            raw_articles.c.crawled_at,
        )
        filters = []
        if source:
            filters.append(raw_articles.c.source == source)
        if query:
            filters.append(raw_articles.c.title.ilike(f"%{query}%"))

        async with self._engine.connect() as connection:
            count_stmt = select(func.count()).select_from(raw_articles)
            for condition in filters:
                count_stmt = count_stmt.where(condition)
            total = (await connection.execute(count_stmt)).scalar_one()

            list_stmt = select(*list_columns)
            for condition in filters:
                list_stmt = list_stmt.where(condition)
            list_stmt = (
                list_stmt.order_by(raw_articles.c.crawled_at.desc())
                .limit(limit)
                .offset(offset)
            )
            result = await connection.execute(list_stmt)
            items = [ArticleListItem(**dict(row._mapping)) for row in result]
            return items, int(total)

    async def get_article(self, article_id: int) -> RawArticle | None:
        columns = (
            raw_articles.c.url,
            raw_articles.c.url_original,
            raw_articles.c.title,
            raw_articles.c.source,
            raw_articles.c.lang,
            raw_articles.c.content,
            raw_articles.c.content_hash,
            raw_articles.c.published_at,
            raw_articles.c.crawled_at,
        )
        async with self._engine.connect() as connection:
            result = await connection.execute(
                select(*columns).where(raw_articles.c.id == article_id)
            )
            row = result.first()
            return RawArticle(**dict(row._mapping)) if row is not None else None

    async def due_failures(self, limit: int) -> list[StoredCrawlFailure]:
        now = datetime.now(timezone.utc)
        async with self._engine.connect() as connection:
            result = await connection.execute(
                select(crawl_failures)
                .where(crawl_failures.c.next_retry_at <= now)
                .order_by(crawl_failures.c.next_retry_at)
                .limit(limit)
            )
            return [StoredCrawlFailure(**dict(row._mapping)) for row in result]
