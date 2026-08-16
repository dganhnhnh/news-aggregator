import asyncio
from datetime import datetime, timezone
import os

import pytest
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import create_async_engine

from adapters.storage.postgres import PostgresRawStorage
from adapters.storage.schema import crawl_failures, ingest_runs, raw_articles, sources
from models.storage_models import (
    ArticleListItem,
    CrawlFailureOccurrence,
    FailureTarget,
    IngestRunStats,
    RawArticle,
)


DATABASE_URL = os.getenv("TEST_DATABASE_URL")
pytestmark = pytest.mark.integration


def test_postgres_upsert_and_failure_state() -> None:
    if not DATABASE_URL:
        pytest.skip("TEST_DATABASE_URL is not set")
    asyncio.run(_test_postgres_upsert_and_failure_state())


async def _test_postgres_upsert_and_failure_state() -> None:
    url = "https://integration.example/article"
    sibling_url = "https://www.integration.example/second"
    source_slug = "integration.example"
    storage = PostgresRawStorage(DATABASE_URL)
    engine = create_async_engine(DATABASE_URL)
    now = datetime.now(timezone.utc)
    run_ids: list[int] = []
    ready = False
    try:
        await storage.check_ready()
        ready = True
        async with engine.begin() as connection:
            await connection.execute(
                delete(raw_articles).where(
                    raw_articles.c.url.in_([url, sibling_url])
                )
            )
            await connection.execute(delete(crawl_failures).where(crawl_failures.c.url == url))
            await connection.execute(delete(sources).where(sources.c.slug == source_slug))

        original = RawArticle(
            url=url,
            title="Original",
            source="Example",
            content="one",
            content_hash=b"1" * 32,
            published_at=now,
            crawled_at=now,
        )
        inserted = await storage.apply_batch([original], [], [], received=1, rejected=0)
        sibling = RawArticle(
            url=sibling_url,
            title="Second article",
            source="A different feed label",
            content="second",
            content_hash=b"3" * 32,
            published_at=now,
            crawled_at=now,
        )
        await storage.apply_batch([sibling], [], [], received=1, rejected=0)
        async with engine.connect() as connection:
            inserted_row = (
                await connection.execute(
                    select(raw_articles).where(raw_articles.c.url == url)
                )
            ).one()
            source_rows = (
                await connection.execute(
                    select(sources).where(sources.c.slug == source_slug)
                )
            ).all()
            article_source_ids = (
                await connection.execute(
                    select(raw_articles.c.source_id).where(
                        raw_articles.c.url.in_([url, sibling_url])
                    )
                )
            ).scalars().all()
        assert len(source_rows) == 1
        assert source_rows[0].name == source_slug
        assert len(set(article_source_ids)) == 1
        assert article_source_ids[0] is not None
        unchanged = await storage.apply_batch([original], [], [], received=1, rejected=0)
        async with engine.connect() as connection:
            unchanged_row = (
                await connection.execute(
                    select(raw_articles).where(raw_articles.c.url == url)
                )
            ).one()
        changed = RawArticle(
            url=url,
            title=None,
            source=None,
            content="two",
            content_hash=b"2" * 32,
            published_at=None,
            crawled_at=now,
        )
        updated = await storage.apply_batch([changed], [], [], received=1, rejected=0)

        assert inserted.written == 1
        assert unchanged.unchanged == 1
        assert updated.written == 1
        assert unchanged_row.updated_at == inserted_row.updated_at

        metadata_change = RawArticle(
            url=url,
            title="Renamed",
            source=None,
            content="two",
            content_hash=b"2" * 32,
            published_at=None,
            crawled_at=now,
        )
        metadata_updated = await storage.apply_batch(
            [metadata_change], [], [], received=1, rejected=0
        )
        assert metadata_updated.written == 1

        failure = CrawlFailureOccurrence(
            failure_kind="article",
            url=url,
            source="Example",
            error_type="TimeoutError",
            error_message="timeout",
        )
        await storage.apply_batch([], [failure], [], received=0, rejected=0)
        failure_without_context = CrawlFailureOccurrence(
            failure_kind="article",
            url=url,
            error_type="TimeoutError",
            error_message="timeout again",
        )
        await storage.apply_batch(
            [], [failure_without_context], [], received=0, rejected=0
        )

        async with engine.connect() as connection:
            article = (
                await connection.execute(
                    select(raw_articles).where(raw_articles.c.url == url)
                )
            ).one()
            failed = (
                await connection.execute(
                    select(crawl_failures).where(crawl_failures.c.url == url)
                )
            ).one()
        assert article.title == "Renamed"
        assert article.source == "Example"
        assert article.content == "two"
        assert failed.attempt_count == 2
        assert failed.source == "Example"
        assert (
            failed.next_retry_at - failed.last_failed_at
        ).total_seconds() == pytest.approx(600, abs=1)

        fetched = await storage.get_article(article.id)
        assert fetched is not None
        assert fetched.url == url
        assert fetched.content == "two"
        assert await storage.get_article(-1) is None

        listed, total = await storage.list_articles(
            limit=50, offset=0, query="Renamed", source="Example"
        )
        assert total == 1
        assert listed == [
            ArticleListItem(
                id=article.id,
                url=url,
                title="Renamed",
                source="Example",
                published_at=now,
                crawled_at=now,
            )
        ]
        no_match, no_match_total = await storage.list_articles(
            limit=50, offset=0, query="no-such-title-xyz"
        )
        assert no_match == []
        assert no_match_total == 0

        await storage.apply_batch(
            [],
            [],
            [FailureTarget(failure_kind="article", url=url)],
            received=0,
            rejected=0,
        )
        succeeded_id = await storage.start_run("pipeline_crawl")
        run_ids.append(succeeded_id)
        async with engine.connect() as connection:
            running_status = await connection.scalar(
                select(ingest_runs.c.status).where(
                    ingest_runs.c.id == succeeded_id
                )
            )
        assert running_status == "running"

        checkpoint_stats = IngestRunStats(
            feeds_processed=2,
            articles_written=1,
        )
        await storage.update_run(succeeded_id, checkpoint_stats)
        async with engine.connect() as connection:
            checkpointed = (
                await connection.execute(
                    select(ingest_runs).where(
                        ingest_runs.c.id == succeeded_id
                    )
                )
            ).one()
        assert checkpointed.status == "running"
        assert checkpointed.feeds_processed == 2
        assert checkpointed.articles_written == 1

        await storage.finish_run(succeeded_id, "succeeded", checkpoint_stats)
        partial_id = await storage.start_run("pipeline_rss")
        run_ids.append(partial_id)
        await storage.finish_run(partial_id, "partial", IngestRunStats())
        failed_id = await storage.start_run("service")
        run_ids.append(failed_id)
        await storage.finish_run(
            failed_id,
            "failed",
            IngestRunStats(),
            "fatal error",
        )
        async with engine.connect() as connection:
            resolved = await connection.scalar(
                select(crawl_failures.c.url).where(crawl_failures.c.url == url)
            )
            runs = await connection.execute(
                select(ingest_runs.c.id, ingest_runs.c.status).where(
                    ingest_runs.c.id.in_(run_ids)
                )
            )
        assert resolved is None
        assert dict(runs.all()) == {
            succeeded_id: "succeeded",
            partial_id: "partial",
            failed_id: "failed",
        }
    finally:
        if ready:
            async with engine.begin() as connection:
                await connection.execute(
                    delete(raw_articles).where(
                        raw_articles.c.url.in_([url, sibling_url])
                    )
                )
                await connection.execute(
                    delete(crawl_failures).where(crawl_failures.c.url == url)
                )
                if run_ids:
                    await connection.execute(
                        delete(ingest_runs).where(ingest_runs.c.id.in_(run_ids))
                    )
                await connection.execute(
                    delete(sources).where(sources.c.slug == source_slug)
                )
        await engine.dispose()
        await storage.close()
