import argparse
import asyncio
from datetime import datetime, timedelta, timezone
import os

import pytest
from sqlalchemy import delete, select, update
from sqlalchemy.ext.asyncio import create_async_engine

from adapters.storage.postgres import PostgresRawStorage
from adapters.storage.schema import (
    article_embeddings,
    cluster_members,
    clusters,
    processing_jobs,
    raw_articles,
    sources,
)
from models.storage_models import EmbeddingRecord, RawArticle
from workers.base import RunContext
from workers.cluster import ClusterWorker
from workers.queue import PostgresJobQueue
from workers.window import RunWindow


DATABASE_URL = os.getenv("TEST_DATABASE_URL")
pytestmark = pytest.mark.integration


def _article(url: str, title: str, now: datetime) -> RawArticle:
    return RawArticle(
        url=url,
        url_original=url,
        title=title,
        source="Cluster integration",
        lang="en",
        content=title,
        content_hash=title.encode().ljust(32, b"0")[:32],
        published_at=now,
        crawled_at=now,
    )


async def _cleanup(engine, urls: list[str], slug: str) -> None:
    async with engine.begin() as connection:
        article_ids = list(
            (
                await connection.execute(
                    select(raw_articles.c.id).where(raw_articles.c.url.in_(urls))
                )
            ).scalars()
        )
        cluster_ids = list(
            (
                await connection.execute(
                    select(cluster_members.c.cluster_id).where(
                        cluster_members.c.article_id.in_(article_ids)
                    )
                )
            ).scalars()
        )
        if cluster_ids:
            await connection.execute(delete(clusters).where(clusters.c.id.in_(cluster_ids)))
        if article_ids:
            await connection.execute(
                delete(processing_jobs).where(
                    processing_jobs.c.article_id.in_(article_ids)
                )
            )
            await connection.execute(
                delete(article_embeddings).where(
                    article_embeddings.c.article_id.in_(article_ids)
                )
            )
        await connection.execute(delete(raw_articles).where(raw_articles.c.url.in_(urls)))
        await connection.execute(delete(sources).where(sources.c.slug == slug))


def test_cluster_worker_running_mean_and_retry_are_idempotent() -> None:
    if not DATABASE_URL:
        pytest.skip("TEST_DATABASE_URL is not set")
    asyncio.run(_test_cluster_worker_running_mean_and_retry_are_idempotent())


async def _test_cluster_worker_running_mean_and_retry_are_idempotent() -> None:
    urls = [
        "https://cluster-integration.example/one",
        "https://cluster-integration.example/two",
    ]
    slug = "cluster-integration.example"
    now = datetime.now(timezone.utc)
    storage = PostgresRawStorage(DATABASE_URL)
    engine = create_async_engine(DATABASE_URL)
    try:
        await storage.check_ready()
        await _cleanup(engine, urls, slug)
        await storage.apply_batch(
            [_article(urls[0], "First", now), _article(urls[1], "Second", now)],
            [],
            [],
            received=2,
            rejected=0,
        )
        async with engine.connect() as connection:
            article_ids = list(
                (
                    await connection.execute(
                        select(raw_articles.c.id)
                        .where(raw_articles.c.url.in_(urls))
                        .order_by(raw_articles.c.id)
                    )
                ).scalars()
            )

        first = [1.0, 0.0] + [0.0] * 382
        second = [0.8, 0.6] + [0.0] * 382
        await storage.upsert_embeddings(
            [
                EmbeddingRecord(article_ids[0], "test", 384, b"1" * 32, first),
                EmbeddingRecord(article_ids[1], "test", 384, b"2" * 32, second),
            ]
        )
        async with engine.connect() as connection:
            downstream = {
                (row.article_id, row.stage, row.state)
                for row in await connection.execute(
                    select(
                        processing_jobs.c.article_id,
                        processing_jobs.c.stage,
                        processing_jobs.c.state,
                    ).where(
                        processing_jobs.c.article_id.in_(article_ids),
                        processing_jobs.c.stage.in_(("classify", "cluster")),
                    )
                )
            }
        assert downstream == {
            (article_id, stage, "pending")
            for article_id in article_ids
            for stage in ("classify", "cluster")
        }
        args = argparse.Namespace(
            batch_size=10,
            candidate_limit=20,
            cluster_threshold=0.75,
            cluster_window_hours=72,
            reprocess=False,
            limit=0,
        )
        ctx = RunContext(
            run_id=1,
            window=RunWindow(now - timedelta(days=3), now + timedelta(seconds=1), False),
            shutdown=asyncio.Event(),
            args=args,
            storage=storage,
        )
        outcome = await ClusterWorker(queue=PostgresJobQueue(engine)).execute(ctx)
        assert outcome.status == "succeeded"

        async with engine.connect() as connection:
            cluster = (
                await connection.execute(
                    select(clusters)
                    .join(
                        cluster_members,
                        cluster_members.c.cluster_id == clusters.c.id,
                    )
                    .where(cluster_members.c.article_id == article_ids[0])
                )
            ).one()
            members = (
                await connection.execute(
                    select(cluster_members).where(
                        cluster_members.c.cluster_id == cluster.id
                    )
                )
            ).all()
        assert len(members) == 2
        assert cluster.centroid_count == 2
        assert cluster.n_articles == 2
        assert cluster.n_distinct_sources == 1
        assert list(cluster.centroid)[:2] == pytest.approx([0.9, 0.3])

        async with engine.begin() as connection:
            await connection.execute(
                update(processing_jobs)
                .where(
                    processing_jobs.c.article_id.in_(article_ids),
                    processing_jobs.c.stage == "cluster",
                )
                .values(state="pending")
            )
        retry = await ClusterWorker(queue=PostgresJobQueue(engine)).execute(ctx)
        assert retry.stats.already_member == 2
        async with engine.connect() as connection:
            count_after_retry = await connection.scalar(
                select(clusters.c.centroid_count).where(clusters.c.id == cluster.id)
            )
        assert count_after_retry == 2
    finally:
        await _cleanup(engine, urls, slug)
        await engine.dispose()
        await storage.close()
