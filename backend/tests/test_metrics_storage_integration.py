"""PostgreSQL integration tests for the metrics read methods."""

import asyncio
from datetime import datetime, timedelta, timezone
import os

import pytest
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import create_async_engine

from adapters.storage.postgres import PostgresRawStorage
from adapters.storage.schema import (
    processing_jobs,
    raw_articles,
    sources,
    worker_runs,
)
from models.storage_models import IngestRunStats, RawArticle


DATABASE_URL = os.getenv("TEST_DATABASE_URL")
pytestmark = pytest.mark.integration

WORKER = "metrics-integration"
SOURCE_SLUG = "metrics.example"
ARTICLE_URL = "https://metrics.example/article"


def test_metrics_snapshot_counts_new_rows() -> None:
    if not DATABASE_URL:
        pytest.skip("TEST_DATABASE_URL is not set")
    asyncio.run(_test_metrics_snapshot_counts_new_rows())


async def _test_metrics_snapshot_counts_new_rows() -> None:
    storage = PostgresRawStorage(DATABASE_URL)
    engine = create_async_engine(DATABASE_URL)
    now = datetime.now(timezone.utc)
    ready = False
    try:
        await storage.check_ready()
        ready = True
        await _cleanup(engine)

        # Compared against a baseline rather than asserted absolutely: the
        # test database carries rows from every other integration test.
        before = await storage.metrics_snapshot()

        article = RawArticle(
            url=ARTICLE_URL,
            url_original=ARTICLE_URL,
            title="Metrics article",
            source="Metrics example",
            lang="en",
            content="body",
            content_hash=b"m" * 32,
            published_at=now,
            crawled_at=now,
        )
        await storage.apply_batch([article], [], [], received=1, rejected=0)
        async with engine.connect() as connection:
            article_id = await connection.scalar(
                select(raw_articles.c.id).where(raw_articles.c.url == ARTICLE_URL)
            )
        await storage.ensure_processing_jobs([article_id], ["embed"])

        after = await storage.metrics_snapshot()

        assert _pending(after, "embed") == _pending(before, "embed") + 1
        assert _corpus(after, "crawl") == _corpus(before, "crawl") + 1
        assert after.corpus.recent_articles == before.corpus.recent_articles + 1
        # The article has a job but no embedding yet, so it counts as backlog
        # at every downstream stage.
        missing_before = dict(before.corpus.recent_missing)
        missing_after = dict(after.corpus.recent_missing)
        for stage in ("embedding", "classification", "cluster"):
            assert missing_after[stage] == missing_before[stage] + 1
        assert after.captured_at >= before.captured_at
        assert after.recent_window_hours == 24
    finally:
        if ready:
            await _cleanup(engine)
        await storage.close()
        await engine.dispose()


def test_worker_stat_totals_resume_from_a_watermark() -> None:
    if not DATABASE_URL:
        pytest.skip("TEST_DATABASE_URL is not set")
    asyncio.run(_test_worker_stat_totals_resume_from_a_watermark())


async def _test_worker_stat_totals_resume_from_a_watermark() -> None:
    storage = PostgresRawStorage(DATABASE_URL)
    engine = create_async_engine(DATABASE_URL)
    ready = False
    try:
        await storage.check_ready()
        ready = True
        await _cleanup(engine)

        first_id = await storage.start_run(worker=WORKER, trigger="manual")
        await storage.finish_run(
            first_id, "succeeded", IngestRunStats(articles_written=3)
        )
        second_id = await storage.start_run(worker=WORKER, trigger="manual")
        await storage.finish_run(
            second_id, "succeeded", IngestRunStats(articles_written=4)
        )

        everything = _written(await storage.worker_stat_totals({}))
        assert everything is not None
        assert everything.total == 7.0
        assert everything.through_run_id == second_id

        # The whole point of the watermark: a second collection must see only
        # what finished since the first, so the caller can add rather than
        # replace and the exposed counter climbs.
        delta = _written(await storage.worker_stat_totals({WORKER: first_id}))
        assert delta is not None
        assert delta.total == 4.0
        assert delta.through_run_id == second_id

        # A run still in flight reports a checkpoint, not a total; counting it
        # now would count it twice when it finishes.
        running_id = await storage.start_run(worker=WORKER, trigger="manual")
        await storage.update_run(running_id, IngestRunStats(articles_written=99))
        assert _written(await storage.worker_stat_totals({WORKER: second_id})) is None

        await storage.finish_run(
            running_id, "failed", IngestRunStats(articles_written=1), "boom"
        )
        # Status is irrelevant: a failed run still did the work it reports.
        failed = _written(await storage.worker_stat_totals({WORKER: second_id}))
        assert failed is not None
        assert failed.total == 1.0
    finally:
        if ready:
            await _cleanup(engine)
        await storage.close()
        await engine.dispose()


def _pending(snapshot, stage: str) -> int:
    for item in snapshot.stages:
        if item.stage == stage:
            return item.pending
    return 0


def _corpus(snapshot, content_source: str) -> int:
    return dict(snapshot.corpus.articles_by_content_source).get(content_source, 0)


def _written(totals):
    for item in totals:
        if item.worker == WORKER and item.stat == "articles_written":
            return item
    return None


async def _cleanup(engine) -> None:
    async with engine.begin() as connection:
        article_id = await connection.scalar(
            select(raw_articles.c.id).where(raw_articles.c.url == ARTICLE_URL)
        )
        if article_id is not None:
            await connection.execute(
                delete(processing_jobs).where(
                    processing_jobs.c.article_id == article_id
                )
            )
            await connection.execute(
                delete(raw_articles).where(raw_articles.c.id == article_id)
            )
        await connection.execute(delete(sources).where(sources.c.slug == SOURCE_SLUG))
        await connection.execute(
            delete(worker_runs).where(worker_runs.c.worker == WORKER)
        )
