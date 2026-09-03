import asyncio
from datetime import datetime, timedelta, timezone
import os

import pytest
from sqlalchemy import delete, select, update
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
    FailureQueueStatus,
    RawArticle,
    StageQueueStatus,
)


DATABASE_URL = os.getenv("TEST_DATABASE_URL")
pytestmark = pytest.mark.integration


def test_worker_lock_admits_one_holder_at_a_time() -> None:
    if not DATABASE_URL:
        pytest.skip("TEST_DATABASE_URL is not set")
    asyncio.run(_test_worker_lock_admits_one_holder_at_a_time())


async def _test_worker_lock_admits_one_holder_at_a_time() -> None:
    # Two storages stand in for two processes: advisory locks are per session,
    # so a single engine would happily hand the same lock to itself.
    first = PostgresRawStorage(DATABASE_URL)
    second = PostgresRawStorage(DATABASE_URL)
    try:
        await first.check_ready()
        async with first.worker_lock("test-lock") as held:
            assert held is True
            async with second.worker_lock("test-lock") as blocked:
                assert blocked is False
            # A different worker name is a different lock.
            async with second.worker_lock("test-lock-other") as unrelated:
                assert unrelated is True

        async with second.worker_lock("test-lock") as reacquired:
            assert reacquired is True
    finally:
        await first.close()
        await second.close()


def test_reaper_only_touches_stale_runs_of_its_own_worker() -> None:
    if not DATABASE_URL:
        pytest.skip("TEST_DATABASE_URL is not set")
    asyncio.run(_test_reaper_only_touches_stale_runs_of_its_own_worker())


async def _test_reaper_only_touches_stale_runs_of_its_own_worker() -> None:
    storage = PostgresRawStorage(DATABASE_URL)
    engine = create_async_engine(DATABASE_URL)
    run_ids: list[int] = []
    ready = False
    try:
        await storage.check_ready()
        ready = True
        stale_id = await storage.start_run(worker="reaper-test", trigger="scheduled")
        fresh_id = await storage.start_run(worker="reaper-test", trigger="scheduled")
        other_id = await storage.start_run(
            worker="reaper-test-other", trigger="scheduled"
        )
        run_ids = [stale_id, fresh_id, other_id]

        long_ago = datetime.now(timezone.utc) - timedelta(hours=2)
        async with engine.begin() as connection:
            await connection.execute(
                update(worker_runs)
                .where(worker_runs.c.id.in_([stale_id, other_id]))
                .values(heartbeat_at=long_ago)
            )

        reaped = await storage.reap_abandoned("reaper-test", timedelta(seconds=900))
        assert reaped == 1

        async with engine.connect() as connection:
            rows = await connection.execute(
                select(
                    worker_runs.c.id,
                    worker_runs.c.status,
                    worker_runs.c.finished_at,
                    worker_runs.c.error_message,
                ).where(worker_runs.c.id.in_(run_ids))
            )
            by_id = {row.id: row for row in rows}

        assert by_id[stale_id].status == "abandoned"
        assert by_id[stale_id].finished_at is not None
        assert "Heartbeat" in by_id[stale_id].error_message
        assert by_id[fresh_id].status == "running"
        # Stale, but belongs to another worker: not this reaper's business.
        assert by_id[other_id].status == "running"

        # A second pass has nothing left to do.
        assert await storage.reap_abandoned("reaper-test", timedelta(seconds=900)) == 0
    finally:
        if ready and run_ids:
            async with engine.begin() as connection:
                await connection.execute(
                    delete(worker_runs).where(worker_runs.c.id.in_(run_ids))
                )
        await engine.dispose()
        await storage.close()


def _stage(snapshot, name: str) -> StageQueueStatus:
    for stage in snapshot.stages:
        if stage.stage == name:
            return stage
    return StageQueueStatus(stage=name)


def _failures(snapshot, kind: str) -> FailureQueueStatus:
    for failure in snapshot.failures:
        if failure.failure_kind == kind:
            return failure
    return FailureQueueStatus(failure_kind=kind)


def _runs(snapshot, worker: str, status: str) -> int:
    return sum(
        row.count
        for row in snapshot.runs
        if row.worker == worker and row.status == status
    )


def test_queue_status_counts_every_queue_and_raises_the_right_alerts() -> None:
    if not DATABASE_URL:
        pytest.skip("TEST_DATABASE_URL is not set")
    asyncio.run(_test_queue_status_counts_every_queue_and_raises_the_right_alerts())


async def _test_queue_status_counts_every_queue_and_raises_the_right_alerts() -> None:
    url = "https://queue-status-integration.example/a"
    slug = "queue-status-integration.example"
    failure_url = "https://queue-status-integration.example/dead"
    storage = PostgresRawStorage(DATABASE_URL)
    engine = create_async_engine(DATABASE_URL)
    now = datetime.now(timezone.utc)
    run_ids: list[int] = []
    ready = False
    try:
        await storage.check_ready()
        ready = True
        await _purge_queue_status_fixtures(engine, url, slug, failure_url)
        before = await storage.queue_status(runs_window_hours=1)

        await storage.apply_batch(
            [
                RawArticle(
                    url=url,
                    url_original=url,
                    title="Title",
                    source="Example",
                    lang="en",
                    content="body",
                    content_hash=b"q" * 32,
                    published_at=now,
                    crawled_at=now,
                )
            ],
            [],
            [],
            received=1,
            rejected=0,
        )
        async with engine.begin() as connection:
            article_id = await connection.scalar(
                select(raw_articles.c.id).where(raw_articles.c.url == url)
            )
            await connection.execute(
                update(processing_jobs)
                .where(
                    processing_jobs.c.article_id == article_id,
                    processing_jobs.c.stage == "embed",
                )
                .values(state="dead_letter", attempts=5)
            )
            await connection.execute(
                crawl_failures.insert().values(
                    failure_kind="article",
                    url=failure_url,
                    error_type="Timeout",
                    error_message="took too long",
                    attempt_count=7,
                    first_failed_at=now,
                    last_failed_at=now,
                    next_retry_at=now,
                    dead_lettered_at=now,
                )
            )

        run_ids.append(
            await storage.start_run(worker="queue-status-test", trigger="scheduled")
        )
        await storage.finish_run(run_ids[0], "abandoned", None)
        run_ids.append(
            await storage.start_run(worker="queue-status-test", trigger="scheduled")
        )
        await storage.finish_run(run_ids[1], "succeeded", None)

        after = await storage.queue_status(runs_window_hours=1)

        assert (
            _stage(after, "embed").dead_letter - _stage(before, "embed").dead_letter
        ) == 1
        assert (
            _failures(after, "article").dead_lettered
            - _failures(before, "article").dead_lettered
        ) == 1
        assert _failures(after, "article").max_attempts >= 7
        assert _runs(after, "queue-status-test", "abandoned") == 1
        assert _runs(after, "queue-status-test", "succeeded") == 1

        alerts = after.alerts()
        assert any("news-worker replay --stage embed" in alert for alert in alerts)
        assert any("news-worker replay --failures" in alert for alert in alerts)
        assert any("abandoned run" in alert for alert in alerts)
        # A succeeded run is never an alert on its own.
        assert not any("succeeded" in alert for alert in alerts)

        # The window is a real filter, not decoration.
        async with engine.begin() as connection:
            await connection.execute(
                update(worker_runs)
                .where(worker_runs.c.id.in_(run_ids))
                .values(started_at=now - timedelta(hours=6))
            )
        narrowed = await storage.queue_status(runs_window_hours=1)
        assert _runs(narrowed, "queue-status-test", "abandoned") == 0
    finally:
        if ready:
            if run_ids:
                async with engine.begin() as connection:
                    await connection.execute(
                        delete(worker_runs).where(worker_runs.c.id.in_(run_ids))
                    )
            await _purge_queue_status_fixtures(engine, url, slug, failure_url)
        await engine.dispose()
        await storage.close()


async def _purge_queue_status_fixtures(
    engine, url: str, slug: str, failure_url: str
) -> None:
    async with engine.begin() as connection:
        article_ids = [
            row.id
            for row in await connection.execute(
                select(raw_articles.c.id).where(raw_articles.c.url == url)
            )
        ]
        if article_ids:
            await connection.execute(
                delete(processing_jobs).where(
                    processing_jobs.c.article_id.in_(article_ids)
                )
            )
        await connection.execute(delete(raw_articles).where(raw_articles.c.url == url))
        await connection.execute(delete(sources).where(sources.c.slug == slug))
        await connection.execute(
            delete(crawl_failures).where(crawl_failures.c.url == failure_url)
        )
