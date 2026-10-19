import asyncio
from datetime import datetime, timedelta, timezone
import os

import pytest
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import create_async_engine

from adapters.storage.postgres import PostgresRawStorage
from adapters.storage.schema import (
    crawl_failures,
    processing_jobs,
    raw_articles,
    sources,
    worker_runs,
)
from models.storage_models import (
    CrawlFailureOccurrence,
    FailureTarget,
    IngestRunStats,
    RawArticle,
)
from services.raw_storage import content_hash


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
            url_original=url + "?utm_source=rss",
            title="Original",
            source="Example",
            lang="en",
            content="one",
            content_hash=b"1" * 32,
            published_at=now,
            crawled_at=now,
        )
        inserted = await storage.apply_batch([original], [], [], received=1, rejected=0)
        sibling = RawArticle(
            url=sibling_url,
            url_original=sibling_url,
            title="Second article",
            source="A different feed label",
            lang="en",
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
            url_original=url,
            title=None,
            source=None,
            lang=None,
            content="two",
            content_hash=b"2" * 32,
            published_at=None,
            crawled_at=now,
        )
        updated = await storage.apply_batch([changed], [], [], received=1, rejected=0)

        assert inserted.written == 1
        assert inserted.full_content_written == 1
        assert inserted.summary_only_written == 0
        assert unchanged.unchanged == 1
        assert unchanged.full_content_written == unchanged.summary_only_written == 0
        assert updated.written == 1
        assert updated.full_content_written == 1
        assert unchanged_row.updated_at == inserted_row.updated_at

        metadata_change = RawArticle(
            url=url,
            url_original=url,
            title="Renamed",
            source=None,
            lang=None,
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
        assert article.lang == "en"
        assert article.url_original == url
        assert failed.attempt_count == 2
        assert failed.source == "Example"
        # Full jitter: the delay is uniform(0, cap), cap = 10 minutes at attempt 2.
        delay_seconds = (failed.next_retry_at - failed.last_failed_at).total_seconds()
        assert 0 <= delay_seconds <= 600
        assert failed.dead_lettered_at is None

        fetched = await storage.get_article(article.id)
        assert fetched is not None
        assert fetched.url == url
        assert fetched.content == "two"
        assert fetched.source_slug == source_slug
        assert await storage.get_article(-1) is None

        listed = await storage.feed_articles(
            since=now - timedelta(days=1),
            until=now + timedelta(days=1),
            limit=50,
            source_slugs=[source_slug],
            query="Renamed",
        )
        assert [item.id for item in listed] == [article.id]
        assert listed[0].url == url
        assert listed[0].title == "Renamed"
        assert listed[0].source_slug == source_slug

        listed_sources = await storage.list_sources(
            since=now - timedelta(days=1),
            until=now + timedelta(days=1),
        )
        assert [item.slug for item in listed_sources if item.slug == source_slug] == [
            source_slug
        ]
        future_sources = await storage.list_sources(
            since=now + timedelta(days=1),
            until=now + timedelta(days=2),
        )
        assert source_slug not in [item.slug for item in future_sources]

        no_match = await storage.feed_articles(
            since=now - timedelta(days=1),
            until=now + timedelta(days=1),
            limit=50,
            query="no-such-title-xyz",
        )
        assert no_match == []

        await storage.apply_batch(
            [],
            [],
            [FailureTarget(failure_kind="article", url=url)],
            received=0,
            rejected=0,
        )
        window_start = now - timedelta(days=3)
        succeeded_id = await storage.start_run(
            worker="crawl",
            trigger="manual",
            window_start=window_start,
            window_end=now,
            params={"entrypoint": "test"},
        )
        run_ids.append(succeeded_id)
        async with engine.connect() as connection:
            started = (
                await connection.execute(
                    select(worker_runs).where(worker_runs.c.id == succeeded_id)
                )
            ).one()
        assert started.status == "running"
        assert started.params == {"entrypoint": "test"}
        assert started.window_start == window_start
        assert started.heartbeat_at == started.started_at

        checkpoint_stats = IngestRunStats(
            feeds_processed=2,
            articles_written=1,
        )
        await storage.update_run(succeeded_id, checkpoint_stats)
        async with engine.connect() as connection:
            checkpointed = (
                await connection.execute(
                    select(worker_runs).where(
                        worker_runs.c.id == succeeded_id
                    )
                )
            ).one()
        assert checkpointed.status == "running"
        assert checkpointed.stats["feeds_processed"] == 2
        assert checkpointed.stats["articles_written"] == 1
        # A checkpoint is also the run's proof of life.
        assert checkpointed.heartbeat_at > started.heartbeat_at

        await storage.finish_run(succeeded_id, "succeeded", checkpoint_stats)
        partial_id = await storage.start_run(worker="crawl", trigger="manual")
        run_ids.append(partial_id)
        await storage.finish_run(partial_id, "partial", IngestRunStats())
        failed_id = await storage.start_run(worker="crawl", trigger="scheduled")
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
                select(worker_runs.c.id, worker_runs.c.status).where(
                    worker_runs.c.id.in_(run_ids)
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
                        delete(worker_runs).where(worker_runs.c.id.in_(run_ids))
                    )
                await connection.execute(
                    delete(sources).where(sources.c.slug == source_slug)
                )
        await engine.dispose()
        await storage.close()


def test_a_summary_only_row_is_searchable_and_never_blanks_a_crawled_body() -> None:
    if not DATABASE_URL:
        pytest.skip("TEST_DATABASE_URL is not set")
    asyncio.run(_test_a_summary_only_row_is_searchable_and_never_blanks_a_crawled_body())


async def _test_a_summary_only_row_is_searchable_and_never_blanks_a_crawled_body() -> None:
    url = "https://integration.example/summary-only"
    source_slug = "integration.example"
    summary = "Hugging Face pushes for openness after the zestily unusual breach."
    storage = PostgresRawStorage(DATABASE_URL)
    engine = create_async_engine(DATABASE_URL)
    now = datetime.now(timezone.utc)
    ready = False

    def article(*, content: str, feed_summary: str, content_source: str) -> RawArticle:
        return RawArticle(
            url=url,
            url_original=url,
            title="Hugging Face breach",
            source="Example",
            lang="en",
            content=content,
            content_hash=content_hash(content),
            feed_summary=feed_summary,
            content_source=content_source,
            published_at=now,
            crawled_at=now,
        )

    try:
        await storage.check_ready()
        ready = True
        async with engine.begin() as connection:
            await connection.execute(delete(raw_articles).where(raw_articles.c.url == url))
            await connection.execute(delete(sources).where(sources.c.slug == source_slug))

        summary_only = article(
            content="", feed_summary=summary, content_source="feed_summary"
        )
        rescued = await storage.apply_batch([summary_only], [], [], received=1, rejected=0)
        assert rescued.written == rescued.summary_only_written == 1
        assert rescued.full_content_written == 0

        # A row with no content is still findable: search_tsv covers the dek.
        found = await storage.feed_articles(
            since=now - timedelta(days=1),
            until=now + timedelta(days=1),
            limit=50,
            source_slugs=[source_slug],
            query="zestily",
        )
        assert [item.url for item in found] == [url]
        assert "zestily" in found[0].excerpt

        crawled = article(
            content="The real article body.",
            feed_summary=summary,
            content_source="crawl",
        )
        upgraded = await storage.apply_batch([crawled], [], [], received=1, rejected=0)
        assert upgraded.written == upgraded.full_content_written == 1
        assert upgraded.summary_only_written == 0

        # The retry queue can hand back the same failure long after the crawl
        # succeeded. Re-applying the summary-only row must be a no-op, not a
        # regression that erases the body.
        replayed = await storage.apply_batch(
            [summary_only], [], [], received=1, rejected=0
        )
        async with engine.connect() as connection:
            row = (
                await connection.execute(
                    select(raw_articles).where(raw_articles.c.url == url)
                )
            ).one()

        assert replayed.unchanged == 1
        assert row.content == "The real article body."
        assert row.content_hash == content_hash("The real article body.")
        assert row.content_source == "crawl"
        assert row.feed_summary == summary
    finally:
        if ready:
            async with engine.begin() as connection:
                await connection.execute(
                    delete(raw_articles).where(raw_articles.c.url == url)
                )
                await connection.execute(
                    delete(sources).where(sources.c.slug == source_slug)
                )
        await engine.dispose()
        await storage.close()


def test_historical_block_page_cleanup_preserves_summary_and_requeues_crawl() -> None:
    if not DATABASE_URL:
        pytest.skip("TEST_DATABASE_URL is not set")
    asyncio.run(_test_historical_block_page_cleanup())


async def _test_historical_block_page_cleanup() -> None:
    url = "https://cleanup.integration.example/article"
    source_slug = "cleanup.integration.example"
    summary = "A trusted feed summary that remains useful while the page is retried."
    storage = PostgresRawStorage(DATABASE_URL)
    engine = create_async_engine(DATABASE_URL)
    now = datetime.now(timezone.utc)
    ready = False
    try:
        await storage.check_ready()
        ready = True
        async with engine.begin() as connection:
            await connection.execute(
                delete(raw_articles).where(raw_articles.c.url == url)
            )
            await connection.execute(
                delete(crawl_failures).where(crawl_failures.c.url == url)
            )
            await connection.execute(
                delete(sources).where(sources.c.slug == source_slug)
            )

        article = RawArticle(
            url=url,
            url_original=url,
            title="Human Verification",
            source="Cleanup Integration",
            lang=None,
            content="",
            content_hash=content_hash(""),
            feed_summary=summary,
            content_source="feed_summary",
            published_at=now,
            crawled_at=now,
        )
        await storage.apply_batch([article], [], [], received=1, rejected=0)

        findings = await storage.historical_block_pages(limit=10_000)
        target = [item for item in findings if item.url == url]
        assert len(target) == 1
        assert await storage.quarantine_historical_block_pages(target) == 1
        assert await storage.quarantine_historical_block_pages(target) == 0

        async with engine.connect() as connection:
            row = (
                await connection.execute(
                    select(
                        raw_articles.c.title,
                        raw_articles.c.feed_summary,
                        processing_jobs.c.state,
                    )
                    .select_from(
                        raw_articles.join(
                            processing_jobs,
                            (processing_jobs.c.article_id == raw_articles.c.id)
                            & (processing_jobs.c.stage == "embed"),
                        )
                    )
                    .where(raw_articles.c.url == url)
                )
            ).one()
            failure = (
                await connection.execute(
                    select(crawl_failures).where(
                        crawl_failures.c.failure_kind == "article",
                        crawl_failures.c.url == url,
                    )
                )
            ).one()

        assert row.title is None
        assert row.feed_summary == summary
        assert row.state == "pending"
        assert failure.error_type == "BlockedPage"
        assert failure.title is None
        assert failure.dead_lettered_at is None
    finally:
        if ready:
            async with engine.begin() as connection:
                await connection.execute(
                    delete(raw_articles).where(raw_articles.c.url == url)
                )
                await connection.execute(
                    delete(crawl_failures).where(crawl_failures.c.url == url)
                )
                await connection.execute(
                    delete(sources).where(sources.c.slug == source_slug)
                )
        await engine.dispose()
        await storage.close()


def test_crawl_failure_dead_letters_after_max_attempts_and_replay_resets() -> None:
    if not DATABASE_URL:
        pytest.skip("TEST_DATABASE_URL is not set")
    asyncio.run(_test_crawl_failure_dead_letters_after_max_attempts_and_replay_resets())


async def _test_crawl_failure_dead_letters_after_max_attempts_and_replay_resets() -> None:
    url = "https://integration.example/dead-letter"
    storage = PostgresRawStorage(DATABASE_URL)
    engine = create_async_engine(DATABASE_URL)
    ready = False
    try:
        await storage.check_ready()
        ready = True
        async with engine.begin() as connection:
            await connection.execute(
                delete(crawl_failures).where(crawl_failures.c.url == url)
            )

        failure = CrawlFailureOccurrence(
            failure_kind="article",
            url=url,
            error_type="TimeoutError",
            error_message="timeout",
        )
        for _ in range(5):
            await storage.apply_batch([], [failure], [], received=0, rejected=0)

        async with engine.connect() as connection:
            row = (
                await connection.execute(
                    select(crawl_failures).where(crawl_failures.c.url == url)
                )
            ).one()
        assert row.attempt_count == 5
        assert row.dead_lettered_at is not None

        # Mirror due_failures()'s WHERE clause scoped to our url: a real DB
        # can carry far more than any page limit of unrelated due rows, so
        # asserting against a size-bounded due_failures() page would be
        # fragile. Confirming the row itself fails the filter is equivalent.
        async with engine.connect() as connection:
            still_due = (
                await connection.execute(
                    select(crawl_failures.c.url).where(
                        crawl_failures.c.url == url,
                        crawl_failures.c.next_retry_at <= datetime.now(timezone.utc),
                        crawl_failures.c.dead_lettered_at.is_(None),
                    )
                )
            ).first()
        assert still_due is None

        # A sixth failure while dead-lettered must not advance next_retry_at
        # further, nor clear the dead-letter.
        await storage.apply_batch([], [failure], [], received=0, rejected=0)
        async with engine.connect() as connection:
            still_dead = (
                await connection.execute(
                    select(crawl_failures).where(crawl_failures.c.url == url)
                )
            ).one()
        assert still_dead.attempt_count == 6
        assert still_dead.dead_lettered_at == row.dead_lettered_at
        assert still_dead.next_retry_at == row.next_retry_at

        replayed = await storage.replay_failures(kind="article", limit=200)
        assert replayed == 1

        async with engine.connect() as connection:
            replayed_row = (
                await connection.execute(
                    select(crawl_failures).where(crawl_failures.c.url == url)
                )
            ).one()
        assert replayed_row.dead_lettered_at is None
        assert replayed_row.attempt_count == 0
        # Replay makes the row immediately due; a real DB can carry far more
        # than `limit` other due rows, so check the row's own fields against
        # due_failures()'s filter instead of asserting it appears in a
        # size-bounded page of production data.
        assert replayed_row.next_retry_at <= datetime.now(timezone.utc)
    finally:
        if ready:
            async with engine.begin() as connection:
                await connection.execute(
                    delete(crawl_failures).where(crawl_failures.c.url == url)
                )
        await engine.dispose()
        await storage.close()
