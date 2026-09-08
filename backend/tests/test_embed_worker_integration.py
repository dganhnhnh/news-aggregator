import asyncio
from datetime import datetime, timedelta, timezone
import os

import pytest
from sqlalchemy import delete, select, update
from sqlalchemy.ext.asyncio import create_async_engine

from adapters.storage.postgres import PostgresRawStorage
from adapters.storage.schema import article_embeddings, processing_jobs, raw_articles, sources
from models.storage_models import EmbeddingRecord, RawArticle
from workers.queue import PostgresJobQueue


DATABASE_URL = os.getenv("TEST_DATABASE_URL")
pytestmark = pytest.mark.integration


def _article(url: str, content_hash: bytes, *, now: datetime) -> RawArticle:
    return RawArticle(
        url=url,
        url_original=url,
        title="Title",
        source="Example",
        lang="en",
        content="body",
        content_hash=content_hash,
        published_at=now,
        crawled_at=now,
    )


async def _cleanup(engine, urls: list[str], slugs: list[str]) -> None:
    async with engine.begin() as connection:
        article_ids = [
            row.id
            for row in await connection.execute(
                select(raw_articles.c.id).where(raw_articles.c.url.in_(urls))
            )
        ]
        if article_ids:
            await connection.execute(
                delete(processing_jobs).where(processing_jobs.c.article_id.in_(article_ids))
            )
            await connection.execute(
                delete(article_embeddings).where(
                    article_embeddings.c.article_id.in_(article_ids)
                )
            )
        await connection.execute(delete(raw_articles).where(raw_articles.c.url.in_(urls)))
        await connection.execute(delete(sources).where(sources.c.slug.in_(slugs)))


def test_apply_batch_enqueues_embed_jobs_on_write_and_content_change() -> None:
    if not DATABASE_URL:
        pytest.skip("TEST_DATABASE_URL is not set")
    asyncio.run(_test_apply_batch_enqueues_embed_jobs_on_write_and_content_change())


async def _test_apply_batch_enqueues_embed_jobs_on_write_and_content_change() -> None:
    url = "https://embed-integration.example/a"
    slug = "embed-integration.example"
    storage = PostgresRawStorage(DATABASE_URL)
    engine = create_async_engine(DATABASE_URL)
    now = datetime.now(timezone.utc)
    try:
        await storage.check_ready()
        await _cleanup(engine, [url], [slug])

        await storage.apply_batch([_article(url, b"1" * 32, now=now)], [], [], received=1, rejected=0)
        async with engine.connect() as connection:
            article_id = (
                await connection.execute(
                    select(raw_articles.c.id).where(raw_articles.c.url == url)
                )
            ).scalar_one()
            job = (
                await connection.execute(
                    select(processing_jobs).where(
                        processing_jobs.c.article_id == article_id,
                        processing_jobs.c.stage == "embed",
                    )
                )
            ).one()
        assert job.state == "pending"

        # Mark done, as EmbedWorker would after a successful embed.
        async with engine.begin() as connection:
            await connection.execute(
                update(processing_jobs)
                .where(processing_jobs.c.article_id == article_id)
                .values(state="done")
            )

        # Rewriting with the same content must not disturb the done job.
        await storage.apply_batch([_article(url, b"1" * 32, now=now)], [], [], received=1, rejected=0)
        async with engine.connect() as connection:
            job = (
                await connection.execute(
                    select(processing_jobs).where(processing_jobs.c.article_id == article_id)
                )
            ).one()
        assert job.state == "done"

        # A genuine content change resets the job to pending.
        await storage.apply_batch([_article(url, b"2" * 32, now=now)], [], [], received=1, rejected=0)
        async with engine.connect() as connection:
            job = (
                await connection.execute(
                    select(processing_jobs).where(processing_jobs.c.article_id == article_id)
                )
            ).one()
        assert job.state == "pending"
    finally:
        await _cleanup(engine, [url], [slug])
        await engine.dispose()
        await storage.close()


def test_job_queue_claim_complete_fail_and_reclaim() -> None:
    if not DATABASE_URL:
        pytest.skip("TEST_DATABASE_URL is not set")
    asyncio.run(_test_job_queue_claim_complete_fail_and_reclaim())


async def _test_job_queue_claim_complete_fail_and_reclaim() -> None:
    url = "https://embed-integration.example/queue"
    slug = "embed-integration.example"
    storage = PostgresRawStorage(DATABASE_URL)
    engine = create_async_engine(DATABASE_URL)
    now = datetime.now(timezone.utc)
    try:
        await storage.check_ready()
        await _cleanup(engine, [url], [slug])
        await storage.apply_batch([_article(url, b"3" * 32, now=now)], [], [], received=1, rejected=0)
        async with engine.connect() as connection:
            article_id = (
                await connection.execute(
                    select(raw_articles.c.id).where(raw_articles.c.url == url)
                )
            ).scalar_one()

        queue = PostgresJobQueue(engine)

        # claim() moves the row to running and skips already-locked rows.
        jobs = await queue.claim("embed", 10, locked_by="worker-a", lease=timedelta(minutes=10))
        assert [job.article_id for job in jobs] == [article_id]
        assert await queue.claim("embed", 10, locked_by="worker-b", lease=timedelta(minutes=10)) == []

        # A lease that has already expired is returned to pending.
        async with engine.begin() as connection:
            await connection.execute(
                update(processing_jobs)
                .where(processing_jobs.c.article_id == article_id)
                .values(lease_until=now - timedelta(minutes=1))
            )
        reclaimed = await queue.reclaim_expired("embed")
        assert reclaimed == 1
        async with engine.connect() as connection:
            state = (
                await connection.execute(
                    select(processing_jobs.c.state, processing_jobs.c.locked_by).where(
                        processing_jobs.c.article_id == article_id
                    )
                )
            ).one()
        assert state.state == "pending"
        assert state.locked_by is None

        # fail() below max_attempts goes back to pending with backoff.
        jobs = await queue.claim("embed", 10, locked_by="worker-a", lease=timedelta(minutes=10))
        assert len(jobs) == 1
        await queue.fail(article_id, "embed", "boom", max_attempts=3)
        async with engine.connect() as connection:
            row = (
                await connection.execute(
                    select(processing_jobs.c.state, processing_jobs.c.attempts).where(
                        processing_jobs.c.article_id == article_id
                    )
                )
            ).one()
        assert row.state == "pending"
        assert row.attempts == 1

        # fail() at max_attempts dead-letters the job.
        await queue.claim("embed", 10, locked_by="worker-a", lease=timedelta(minutes=10))
        await queue.fail(article_id, "embed", "boom again", max_attempts=2)
        async with engine.connect() as connection:
            row = (
                await connection.execute(
                    select(processing_jobs.c.state, processing_jobs.c.attempts).where(
                        processing_jobs.c.article_id == article_id
                    )
                )
            ).one()
        assert row.state == "dead_letter"
        assert row.attempts == 2

        # replay_stage_jobs resets dead-lettered rows for another attempt.
        replayed = await storage.replay_stage_jobs("embed", limit=10)
        assert replayed == 1
        async with engine.connect() as connection:
            row = (
                await connection.execute(
                    select(processing_jobs.c.state, processing_jobs.c.attempts).where(
                        processing_jobs.c.article_id == article_id
                    )
                )
            ).one()
        assert row.state == "pending"
        assert row.attempts == 0

        # complete() marks the job done.
        await queue.claim("embed", 10, locked_by="worker-a", lease=timedelta(minutes=10))
        await queue.complete(article_id, "embed")
        async with engine.connect() as connection:
            row = (
                await connection.execute(
                    select(processing_jobs.c.state).where(
                        processing_jobs.c.article_id == article_id
                    )
                )
            ).one()
        assert row.state == "done"
    finally:
        await _cleanup(engine, [url], [slug])
        await engine.dispose()
        await storage.close()


def test_embeddable_articles_and_upsert_embeddings_round_trip() -> None:
    if not DATABASE_URL:
        pytest.skip("TEST_DATABASE_URL is not set")
    asyncio.run(_test_embeddable_articles_and_upsert_embeddings_round_trip())


async def _test_embeddable_articles_and_upsert_embeddings_round_trip() -> None:
    url = "https://embed-integration.example/round-trip"
    slug = "embed-integration.example"
    storage = PostgresRawStorage(DATABASE_URL)
    engine = create_async_engine(DATABASE_URL)
    now = datetime.now(timezone.utc)
    try:
        await storage.check_ready()
        await _cleanup(engine, [url], [slug])
        await storage.apply_batch([_article(url, b"4" * 32, now=now)], [], [], received=1, rejected=0)
        async with engine.connect() as connection:
            article_id = (
                await connection.execute(
                    select(raw_articles.c.id).where(raw_articles.c.url == url)
                )
            ).scalar_one()

        candidates = await storage.embeddable_articles([article_id])
        assert candidates[article_id].existing_model is None

        await storage.upsert_embeddings(
            [
                EmbeddingRecord(
                    article_id=article_id,
                    model="fake-model",
                    dim=384,
                    content_hash=b"4" * 32,
                    embedding=[0.1] * 384,
                )
            ]
        )
        candidates = await storage.embeddable_articles([article_id])
        assert candidates[article_id].existing_model == "fake-model"
        assert candidates[article_id].existing_content_hash == b"4" * 32
    finally:
        await _cleanup(engine, [url], [slug])
        await engine.dispose()
        await storage.close()


def test_processing_priority_is_preserved_and_claimed_first() -> None:
    if not DATABASE_URL:
        pytest.skip("TEST_DATABASE_URL is not set")
    asyncio.run(_test_processing_priority_is_preserved_and_claimed_first())


async def _test_processing_priority_is_preserved_and_claimed_first() -> None:
    urls = [
        "https://embed-priority.integration.example/normal",
        "https://embed-priority.integration.example/instant",
    ]
    slug = "embed-priority.integration.example"
    storage = PostgresRawStorage(DATABASE_URL)
    engine = create_async_engine(DATABASE_URL)
    now = datetime.now(timezone.utc)
    stage = "priority-integration"
    try:
        await storage.check_ready()
        await _cleanup(engine, urls, [slug])
        await storage.apply_batch(
            [_article(urls[0], b"n" * 32, now=now)],
            [],
            [],
            received=1,
            rejected=0,
            priority=1,
        )
        await storage.apply_batch(
            [_article(urls[1], b"i" * 32, now=now)],
            [],
            [],
            received=1,
            rejected=0,
            priority=0,
        )
        async with engine.connect() as connection:
            rows = (
                await connection.execute(
                    select(raw_articles.c.id, raw_articles.c.url).where(
                        raw_articles.c.url.in_(urls)
                    )
                )
            ).all()
        ids = {row.url: int(row.id) for row in rows}

        await storage.ensure_processing_jobs([ids[urls[0]]], [stage], priority=1)
        await storage.ensure_processing_jobs([ids[urls[1]]], [stage], priority=0)
        # A normal producer cannot downgrade a job already marked instant.
        await storage.ensure_processing_jobs([ids[urls[1]]], [stage], priority=1)

        queue = PostgresJobQueue(engine)
        claimed = await queue.claim(
            stage, 1, locked_by="priority-test", lease=timedelta(minutes=1)
        )
        assert [(job.article_id, job.priority) for job in claimed] == [
            (ids[urls[1]], 0)
        ]

        await storage.upsert_embeddings(
            [
                EmbeddingRecord(
                    article_id=ids[urls[1]],
                    model="priority-test",
                    dim=384,
                    content_hash=b"i" * 32,
                    embedding=[0.1] * 384,
                    priority=0,
                )
            ]
        )
        async with engine.connect() as connection:
            downstream = (
                await connection.execute(
                    select(processing_jobs.c.stage, processing_jobs.c.priority).where(
                        processing_jobs.c.article_id == ids[urls[1]],
                        processing_jobs.c.stage.in_(("classify", "cluster")),
                    )
                )
            ).all()
        assert {(row.stage, row.priority) for row in downstream} == {
            ("classify", 0),
            ("cluster", 0),
        }
    finally:
        await _cleanup(engine, urls, [slug])
        await engine.dispose()
        await storage.close()
