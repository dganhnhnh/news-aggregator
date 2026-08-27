import argparse
import asyncio
from datetime import datetime, timedelta, timezone
import os

import pytest
from sqlalchemy import delete, func, select, update
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
            merge_threshold=0.85,
            merge_only=False,
            repair=False,
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


def test_merge_clusters_converges_and_is_idempotent() -> None:
    if not DATABASE_URL:
        pytest.skip("TEST_DATABASE_URL is not set")
    asyncio.run(_test_merge_clusters_converges_and_is_idempotent())


async def _test_merge_clusters_converges_and_is_idempotent() -> None:
    urls = [
        "https://cluster-integration.example/merge-one",
        "https://cluster-integration.example/merge-two",
    ]
    slug = "cluster-integration.example"
    now = datetime.now(timezone.utc)
    storage = PostgresRawStorage(DATABASE_URL)
    engine = create_async_engine(DATABASE_URL)
    try:
        await storage.check_ready()
        await _cleanup(engine, urls, slug)
        await storage.apply_batch(
            [_article(urls[0], "Merge one", now), _article(urls[1], "Merge two", now)],
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

        # Similar but not identical embeddings, so the pair clears the merge
        # threshold (0.85) without going through the matching worker.
        first = [1.0, 0.0] + [0.0] * 382
        second = [0.95, 0.05] + [0.0] * 382
        await storage.upsert_embeddings(
            [
                EmbeddingRecord(article_ids[0], "test", 384, b"m1".ljust(32, b"0"), first),
                EmbeddingRecord(article_ids[1], "test", 384, b"m2".ljust(32, b"0"), second),
            ]
        )
        articles = await storage.clusterable_articles(article_ids)
        result_a = await storage.create_cluster(articles[article_ids[0]])
        result_b = await storage.create_cluster(articles[article_ids[1]])
        assert result_a.cluster_id != result_b.cluster_id

        pairs = await storage.merge_candidates(threshold=0.85, limit=10)
        assert (
            min(result_a.cluster_id, result_b.cluster_id),
            max(result_a.cluster_id, result_b.cluster_id),
        ) in pairs

        merge = await storage.merge_clusters(result_a.cluster_id, result_b.cluster_id)
        assert merge.merged is True
        canonical_id, loser_id = merge.canonical_id, merge.loser_id
        assert {canonical_id, loser_id} == {result_a.cluster_id, result_b.cluster_id}

        async with engine.connect() as connection:
            loser_row = (
                await connection.execute(
                    select(clusters.c.status, clusters.c.merged_into_id).where(
                        clusters.c.id == loser_id
                    )
                )
            ).one()
            canonical_row = (
                await connection.execute(
                    select(clusters).where(clusters.c.id == canonical_id)
                )
            ).one()
            members = (
                await connection.execute(
                    select(cluster_members.c.article_id).where(
                        cluster_members.c.cluster_id == canonical_id
                    )
                )
            ).scalars().all()
        assert loser_row.status == "merged"
        assert loser_row.merged_into_id == canonical_id
        assert set(members) == set(article_ids)
        assert canonical_row.centroid_count == 2
        assert canonical_row.n_articles == 2

        # Rerunning the same merge is a no-op: the loser is no longer open.
        rerun = await storage.merge_clusters(result_a.cluster_id, result_b.cluster_id)
        assert rerun.merged is False

        # A merged pair is no longer proposed as a merge candidate.
        pairs_after = await storage.merge_candidates(threshold=0.85, limit=10)
        assert pairs_after == []
    finally:
        async with engine.begin() as connection:
            await connection.execute(
                delete(clusters).where(
                    clusters.c.id.in_([result_a.cluster_id, result_b.cluster_id])
                )
            )
        await _cleanup(engine, urls, slug)
        await engine.dispose()
        await storage.close()


def test_reembedding_and_removal_keep_cluster_aggregate_exact() -> None:
    if not DATABASE_URL:
        pytest.skip("TEST_DATABASE_URL is not set")
    asyncio.run(_test_reembedding_and_removal_keep_cluster_aggregate_exact())


async def _test_reembedding_and_removal_keep_cluster_aggregate_exact() -> None:
    urls = [
        "https://cluster-integration.example/reembed-one",
        "https://cluster-integration.example/reembed-two",
    ]
    slug = "cluster-integration.example"
    now = datetime.now(timezone.utc)
    storage = PostgresRawStorage(DATABASE_URL)
    engine = create_async_engine(DATABASE_URL)
    try:
        await storage.check_ready()
        await _cleanup(engine, urls, slug)
        await storage.apply_batch(
            [
                _article(urls[0], "Reembed one", now),
                _article(urls[1], "Reembed two", now),
            ],
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
        second = [0.9, 0.1] + [0.0] * 382
        await storage.upsert_embeddings(
            [
                EmbeddingRecord(article_ids[0], "test", 384, b"r1".ljust(32, b"0"), first),
                EmbeddingRecord(article_ids[1], "test", 384, b"r2".ljust(32, b"0"), second),
            ]
        )
        articles = await storage.clusterable_articles(article_ids)
        created = await storage.create_cluster(articles[article_ids[0]])
        attached = await storage.attach_cluster_member(
            created.cluster_id, articles[article_ids[1]], similarity=0.9
        )
        assert attached.attached is True
        cluster_id = created.cluster_id

        membership = await storage.cluster_membership(article_ids[0])
        assert membership is not None and membership.stale is False

        # A genuine re-embed bumps article_embeddings.created_at past
        # cluster_members.added_at, marking the membership stale.
        new_vector = [0.0, 1.0] + [0.0] * 382
        await storage.upsert_embeddings(
            [
                EmbeddingRecord(
                    article_ids[0], "test", 384, b"r1b".ljust(32, b"0"), new_vector
                )
            ]
        )
        membership_after_reembed = await storage.cluster_membership(article_ids[0])
        assert membership_after_reembed is not None
        assert membership_after_reembed.stale is True

        removed_cluster_id = await storage.remove_cluster_member(article_ids[0])
        assert removed_cluster_id == cluster_id
        assert await storage.cluster_membership(article_ids[0]) is None
        # Idempotent: removing an already-removed member is a no-op.
        assert await storage.remove_cluster_member(article_ids[0]) is None

        async with engine.connect() as connection:
            cluster_row = (
                await connection.execute(
                    select(clusters).where(clusters.c.id == cluster_id)
                )
            ).one()
            member_count = await connection.scalar(
                select(func.count())
                .select_from(cluster_members)
                .where(cluster_members.c.cluster_id == cluster_id)
            )
        assert member_count == 1
        assert cluster_row.centroid_count == 1
        assert cluster_row.n_articles == 1
        assert cluster_row.n_distinct_sources == 1
        assert list(cluster_row.centroid)[:2] == pytest.approx([0.9, 0.1])
        assert cluster_row.status == "open"
    finally:
        await _cleanup(engine, urls, slug)
        await engine.dispose()
        await storage.close()


def test_repair_clusters_fixes_orphaned_membership_and_is_idempotent() -> None:
    if not DATABASE_URL:
        pytest.skip("TEST_DATABASE_URL is not set")
    asyncio.run(_test_repair_clusters_fixes_orphaned_membership_and_is_idempotent())


async def _test_repair_clusters_fixes_orphaned_membership_and_is_idempotent() -> None:
    urls = [
        "https://cluster-integration.example/repair-one",
        "https://cluster-integration.example/repair-two",
    ]
    slug = "cluster-integration.example"
    now = datetime.now(timezone.utc)
    storage = PostgresRawStorage(DATABASE_URL)
    engine = create_async_engine(DATABASE_URL)
    try:
        await storage.check_ready()
        await _cleanup(engine, urls, slug)
        await storage.apply_batch(
            [
                _article(urls[0], "Repair one", now),
                _article(urls[1], "Repair two", now),
            ],
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
        second = [0.9, 0.1] + [0.0] * 382
        await storage.upsert_embeddings(
            [
                EmbeddingRecord(article_ids[0], "test", 384, b"p1".ljust(32, b"0"), first),
                EmbeddingRecord(article_ids[1], "test", 384, b"p2".ljust(32, b"0"), second),
            ]
        )
        articles = await storage.clusterable_articles(article_ids)
        created = await storage.create_cluster(articles[article_ids[0]])
        await storage.attach_cluster_member(
            created.cluster_id, articles[article_ids[1]], similarity=0.9
        )
        cluster_id = created.cluster_id

        # Simulate a delete path that bypasses remove_cluster_member, like
        # quarantine_historical_block_pages: drop the embedding but leave the
        # membership row and cached aggregate stale.
        async with engine.begin() as connection:
            await connection.execute(
                delete(article_embeddings).where(
                    article_embeddings.c.article_id == article_ids[0]
                )
            )

        fixed_first = await storage.repair_clusters(limit=50)
        assert fixed_first >= 1

        async with engine.connect() as connection:
            remaining_members = (
                await connection.execute(
                    select(cluster_members.c.article_id).where(
                        cluster_members.c.cluster_id == cluster_id
                    )
                )
            ).scalars().all()
            cluster_row = (
                await connection.execute(
                    select(clusters).where(clusters.c.id == cluster_id)
                )
            ).one()
        assert list(remaining_members) == [article_ids[1]]
        assert cluster_row.centroid_count == 1
        assert cluster_row.n_articles == 1

        fixed_second = await storage.repair_clusters(limit=50)
        assert fixed_second == 0
    finally:
        await _cleanup(engine, urls, slug)
        await engine.dispose()
        await storage.close()
