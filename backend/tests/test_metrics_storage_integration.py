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


def test_live_stats_preserve_checkpoints_and_distinguish_completion():
    if not DATABASE_URL:
        pytest.skip("TEST_DATABASE_URL is not set")
    asyncio.run(_live_stats_preserve_checkpoints())


async def _live_stats_preserve_checkpoints():
    storage = PostgresRawStorage(DATABASE_URL)
    engine = create_async_engine(DATABASE_URL)
    try:
        await storage.check_ready()
        await _cleanup(engine)
        first = await storage.start_run(worker=WORKER, trigger="manual")
        initial = next(w for w in (await storage.metrics_snapshot()).workers if w.worker == WORKER)
        assert initial.last_completed_at is None and initial.last_progress_at is None
        await storage.update_run(first, IngestRunStats(
            articles_written=3, failure_counts={"article:Timeout": 1},
        ))
        checkpoint = next(w for w in (await storage.metrics_snapshot()).workers if w.worker == WORKER)
        assert checkpoint.last_progress_at is not None
        assert checkpoint.last_completed_at is None
        observed = [r for r in await storage.worker_stat_observations({}) if r.worker == WORKER]
        assert len(observed) == 1 and not observed[0].finished
        assert observed[0].stats["articles_written"] == 3

        # A newer run finishing must not hide an older shared worker's row.
        second = await storage.start_run(worker=WORKER, trigger="manual")
        await storage.finish_run(second, "partial", IngestRunStats(articles_written=4))
        health = next(w for w in (await storage.metrics_snapshot()).workers if w.worker == WORKER)
        assert health.last_completed_at is not None and health.last_success_at is None
        assert health.last_progress_at == checkpoint.last_progress_at
        newer = [r for r in await storage.worker_stat_observations({WORKER: first}, [first]) if r.worker == WORKER]
        assert {r.run_id for r in newer} == {first, second}

        await storage.finish_run(first, "failed", None, "next chunk failed")
        late = [r for r in await storage.worker_stat_observations({WORKER: second}, [first]) if r.worker == WORKER]
        assert len(late) == 1 and late[0].finished
        assert late[0].stats["articles_written"] == 3
        assert late[0].stats["failure_counts"] == {"article:Timeout": 1}
        assert late[0].stats["_progress_at"] == observed[0].stats["_progress_at"]
        assert not [r for r in await storage.worker_stat_observations({WORKER: second}) if r.worker == WORKER]
    finally:
        await _cleanup(engine)
        await storage.close()
        await engine.dispose()


def test_ingestion_lag_buckets_each_article_once() -> None:
    if not DATABASE_URL:
        pytest.skip("TEST_DATABASE_URL is not set")
    asyncio.run(_test_ingestion_lag_buckets_each_article_once())


async def _test_ingestion_lag_buckets_each_article_once() -> None:
    storage = PostgresRawStorage(DATABASE_URL)
    engine = create_async_engine(DATABASE_URL)
    now = datetime.now(timezone.utc)
    ready = False
    try:
        await storage.check_ready()
        ready = True
        await _cleanup(engine)

        baseline = await storage.ingestion_lag_since(0)

        # Published ten minutes before the crawl stored it: past the 300s
        # bound, inside the 900s one.
        article = RawArticle(
            url=ARTICLE_URL,
            url_original=ARTICLE_URL,
            title="Metrics article",
            source="Metrics example",
            lang="en",
            content="body",
            content_hash=b"m" * 32,
            published_at=now - timedelta(minutes=10),
            crawled_at=now,
        )
        await storage.apply_batch([article], [], [], received=1, rejected=0)

        delta = await storage.ingestion_lag_since(baseline.through_article_id)
        crawl = _series(delta, "crawl")
        assert crawl is not None
        assert crawl.count == 1
        bounds = list(delta.bounds)
        assert crawl.counts[bounds.index(300.0)] == 0
        assert crawl.counts[bounds.index(900.0)] == 1
        assert 500.0 < crawl.total_seconds < 700.0
        assert delta.through_article_id > baseline.through_article_id

        # The mark is what makes this a legal histogram: reading again from
        # the new mark must return nothing, so the caller's counter holds
        # instead of counting the same article a second time.
        assert _series(
            await storage.ingestion_lag_since(delta.through_article_id), "crawl"
        ) is None

        # An empty delta must not rewind the mark, or every later scrape would
        # re-count the whole corpus.
        empty = await storage.ingestion_lag_since(delta.through_article_id)
        assert empty.through_article_id == delta.through_article_id
    finally:
        if ready:
            await _cleanup(engine)
        await storage.close()
        await engine.dispose()


def test_quality_snapshot_reads_current_table_state() -> None:
    if not DATABASE_URL:
        pytest.skip("TEST_DATABASE_URL is not set")
    asyncio.run(_test_quality_snapshot_reads_current_table_state())


async def _test_quality_snapshot_reads_current_table_state() -> None:
    storage = PostgresRawStorage(DATABASE_URL)
    engine = create_async_engine(DATABASE_URL)
    now = datetime.now(timezone.utc)
    ready = False
    try:
        await storage.check_ready()
        ready = True
        await _cleanup(engine)

        before = await storage.quality_snapshot()

        article = RawArticle(
            url=ARTICLE_URL,
            url_original=ARTICLE_URL,
            title="Metrics article",
            source="Metrics example",
            lang="en",
            content="",
            content_hash=b"m" * 32,
            feed_summary="dek only",
            content_source="feed_summary",
            published_at=now,
            crawled_at=now,
        )
        await storage.apply_batch([article], [], [], received=1, rejected=0)

        after = await storage.quality_snapshot()

        assert dict(after.recent_articles_by_content_source).get(
            "feed_summary", 0
        ) == dict(before.recent_articles_by_content_source).get("feed_summary", 0) + 1
        assert after.recent_window_hours == 24
        assert [stage for stage, _ in after.pipeline_lag] == [
            "embed",
            "cluster",
            "classify",
        ]
        # Cumulative counts, so every distribution must be non-decreasing and
        # capped by its own total; a violation here is an illegal histogram.
        for distribution in (
            after.cluster_sizes,
            after.cluster_sources,
            after.feed_poll_intervals,
            *(item for _, item in after.pipeline_lag),
        ):
            assert len(distribution.counts) == len(distribution.bounds)
            assert list(distribution.counts) == sorted(distribution.counts)
            assert distribution.counts[-1] <= distribution.total
        assert after.stale_prototypes <= after.active_topics
    finally:
        if ready:
            await _cleanup(engine)
        await storage.close()
        await engine.dispose()


def _series(delta, content_source: str):
    for item in delta.series:
        if item.content_source == content_source:
            return item
    return None


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
