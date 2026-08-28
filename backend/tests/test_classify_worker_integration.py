import argparse
import asyncio
import os
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import delete, select, update
from sqlalchemy.ext.asyncio import create_async_engine

from adapters.embedding.fastembed_embedder import EMBEDDING_DIM, MODEL_NAME
from adapters.storage.postgres import PostgresRawStorage
from adapters.storage.schema import (
    article_classifications,
    article_embeddings,
    article_topics,
    cluster_members,
    cluster_topics,
    clusters,
    processing_jobs,
    raw_articles,
    sources,
    topic_prototypes,
    topics,
)
from models.storage_models import EmbeddingRecord, RawArticle
from workers.base import RunContext
from workers.classify import ClassifyWorker
from workers.cluster import ClusterWorker
from workers.queue import PostgresJobQueue
from workers.window import RunWindow


DATABASE_URL = os.getenv("TEST_DATABASE_URL")
pytestmark = pytest.mark.integration

# The fixture shares the production model/dimension on purpose: the classifier
# snapshot covers every active topic in the database, so a fixture prototype
# built on a made-up model name would make the whole run a ClassifierMismatch
# against whatever real taxonomy the development database happens to hold.
# Assertions therefore scope themselves to `TOPIC_PREFIX` rather than assuming
# these are the only topics.
MODEL = MODEL_NAME
DIM = EMBEDDING_DIM
TOPIC_PREFIX = "classify-it-"
# Two publishers so the source-balanced vote has more than one voter.
HOSTS = ("classify-it-a.example", "classify-it-b.example")
URLS = (f"https://{HOSTS[0]}/story", f"https://{HOSTS[1]}/story")


def _vector(*leading: float) -> list[float]:
    return list(leading) + [0.0] * (DIM - len(leading))


def _article(url: str, title: str, now: datetime) -> RawArticle:
    return RawArticle(
        url=url,
        url_original=url,
        title=title,
        source=url,
        lang="en",
        content=title,
        content_hash=title.encode().ljust(32, b"0")[:32],
        published_at=now,
        crawled_at=now,
    )


async def _cleanup(engine) -> None:
    async with engine.begin() as connection:
        article_ids = list(
            (
                await connection.execute(
                    select(raw_articles.c.id).where(raw_articles.c.url.in_(URLS))
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
            await connection.execute(
                delete(clusters).where(clusters.c.id.in_(cluster_ids))
            )
        if article_ids:
            for table in (processing_jobs, article_embeddings):
                await connection.execute(
                    delete(table).where(table.c.article_id.in_(article_ids))
                )
        await connection.execute(delete(raw_articles).where(raw_articles.c.url.in_(URLS)))
        await connection.execute(
            delete(sources).where(sources.c.slug.in_(HOSTS))
        )
        # topic_prototypes / article_topics / cluster_topics cascade from here.
        await connection.execute(
            delete(topics).where(topics.c.slug.like(f"{TOPIC_PREFIX}%"))
        )


async def _seed_topics(engine) -> dict[str, int]:
    """Two active topics: one the fixture articles point at, one orthogonal."""
    prototypes = {
        f"{TOPIC_PREFIX}near": _vector(1.0, 0.0, 0.0),
        f"{TOPIC_PREFIX}far": _vector(0.0, 0.0, 1.0),
    }
    topic_ids: dict[str, int] = {}
    async with engine.begin() as connection:
        for slug, prototype in prototypes.items():
            topic_id = await connection.scalar(
                topics.insert()
                .values(
                    slug=slug,
                    display_name=slug,
                    threshold=0.55,
                    classifier_revision=1,
                    active=True,
                )
                .returning(topics.c.id)
            )
            await connection.execute(
                topic_prototypes.insert().values(
                    topic_id=topic_id,
                    model=MODEL,
                    dim=DIM,
                    definition_hash=slug.encode().ljust(32, b"0")[:32],
                    embedding=prototype,
                )
            )
            topic_ids[slug] = int(topic_id)
    return topic_ids


async def _seed_articles(storage, engine, now: datetime) -> list[int]:
    await storage.apply_batch(
        [_article(URLS[0], "First", now), _article(URLS[1], "Second", now)],
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
                    .where(raw_articles.c.url.in_(URLS))
                    .order_by(raw_articles.c.id)
                )
            ).scalars()
        )
    # cos(first, second) = 0.8: similar enough to cluster, different enough
    # that the two sources do not cast an identical vote.
    await storage.upsert_embeddings(
        [
            EmbeddingRecord(article_ids[0], MODEL, DIM, b"1" * 32, _vector(1.0, 0.0)),
            EmbeddingRecord(article_ids[1], MODEL, DIM, b"2" * 32, _vector(0.8, 0.6)),
        ]
    )
    return article_ids


def _classify_context(storage, now: datetime, **overrides) -> RunContext:
    args = argparse.Namespace(
        batch_size=10,
        limit=0,
        reprocess=False,
        stale=False,
        refresh_clusters=False,
    )
    for key, value in overrides.items():
        setattr(args, key, value)
    return RunContext(
        run_id=1,
        window=RunWindow(now - timedelta(days=3), now + timedelta(seconds=1), False),
        shutdown=asyncio.Event(),
        args=args,
        storage=storage,
    )


def _cluster_context(storage, now: datetime) -> RunContext:
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
    return RunContext(
        run_id=2,
        window=RunWindow(now - timedelta(days=3), now + timedelta(seconds=1), False),
        shutdown=asyncio.Event(),
        args=args,
        storage=storage,
    )


async def _classified_at(engine, article_ids) -> dict[int, datetime]:
    """When each fixture article was last scored.

    Run-wide counters cannot tell a fixture apart from the development
    database's own backlog, which the worker also drains; this can.
    """
    async with engine.connect() as connection:
        rows = (
            await connection.execute(
                select(
                    article_classifications.c.article_id,
                    article_classifications.c.classified_at,
                ).where(article_classifications.c.article_id.in_(article_ids))
            )
        ).all()
    return {int(row.article_id): row.classified_at for row in rows}


async def _cluster_labels(engine, article_id: int) -> list[tuple[str, float, bool]]:
    async with engine.connect() as connection:
        cluster_id = await connection.scalar(
            select(cluster_members.c.cluster_id).where(
                cluster_members.c.article_id == article_id
            )
        )
        assert cluster_id is not None
        rows = (
            await connection.execute(
                select(topics.c.slug, cluster_topics.c.score, cluster_topics.c.assigned)
                .join(topics, topics.c.id == cluster_topics.c.topic_id)
                .where(
                    cluster_topics.c.cluster_id == cluster_id,
                    topics.c.slug.like(f"{TOPIC_PREFIX}%"),
                )
                .order_by(topics.c.slug)
            )
        ).all()
    return [(row.slug, round(float(row.score), 9), row.assigned) for row in rows]


def test_classify_then_cluster_matches_cluster_then_classify() -> None:
    if not DATABASE_URL:
        pytest.skip("TEST_DATABASE_URL is not set")
    asyncio.run(_test_classify_then_cluster_matches_cluster_then_classify())


async def _test_classify_then_cluster_matches_cluster_then_classify() -> None:
    """Both queue orders must land on the same story label set (spec §12.1).

    Whether an article is scored before or after it joins a cluster is a race
    between two independent workers, so the pipeline is only deterministic if
    the two orders converge.
    """
    now = datetime.now(timezone.utc)
    storage = PostgresRawStorage(DATABASE_URL)
    engine = create_async_engine(DATABASE_URL)
    results: dict[str, list[tuple[str, float, bool]]] = {}
    try:
        await storage.check_ready()
        for order in ("classify-first", "cluster-first"):
            await _cleanup(engine)
            await _seed_topics(engine)
            article_ids = await _seed_articles(storage, engine, now)
            classify = ClassifyWorker(queue=PostgresJobQueue(engine))
            cluster = ClusterWorker(queue=PostgresJobQueue(engine))
            if order == "classify-first":
                stages = (
                    (classify, _classify_context(storage, now)),
                    (cluster, _cluster_context(storage, now)),
                )
            else:
                stages = (
                    (cluster, _cluster_context(storage, now)),
                    (classify, _classify_context(storage, now)),
                )
            for worker, ctx in stages:
                outcome = await worker.execute(ctx)
                assert outcome.status == "succeeded", outcome
            results[order] = await _cluster_labels(engine, article_ids[0])

        assert results["classify-first"] == results["cluster-first"]
        labels = dict(
            (slug, (score, assigned)) for slug, score, assigned in results["classify-first"]
        )
        # 0.8 * 1.0 and 0.8 * 0.8 from two distinct sources: (0.8 + 0.64) / 2.
        assert labels[f"{TOPIC_PREFIX}near"] == (pytest.approx(0.72), True)
        assert labels[f"{TOPIC_PREFIX}far"] == (pytest.approx(0.0), False)
    finally:
        await _cleanup(engine)
        await engine.dispose()
        await storage.close()


def test_every_embedded_article_gets_a_classification_row() -> None:
    if not DATABASE_URL:
        pytest.skip("TEST_DATABASE_URL is not set")
    asyncio.run(_test_every_embedded_article_gets_a_classification_row())


async def _test_every_embedded_article_gets_a_classification_row() -> None:
    now = datetime.now(timezone.utc)
    storage = PostgresRawStorage(DATABASE_URL)
    engine = create_async_engine(DATABASE_URL)
    try:
        await storage.check_ready()
        await _cleanup(engine)
        await _seed_topics(engine)
        article_ids = await _seed_articles(storage, engine, now)
        snapshot = await storage.classifier_snapshot()

        outcome = await ClassifyWorker(queue=PostgresJobQueue(engine)).execute(
            _classify_context(storage, now)
        )
        # The worker drains whatever the queue holds, which on a development
        # database is more than this fixture; assert on the fixture's own rows
        # rather than on run-wide counters.
        assert outcome.stats.classified >= len(article_ids)
        assert outcome.stats.failed == 0

        async with engine.connect() as connection:
            rows = (
                await connection.execute(
                    select(article_classifications).where(
                        article_classifications.c.article_id.in_(article_ids)
                    )
                )
            ).all()
            scored = (
                await connection.execute(
                    select(article_topics.c.assigned)
                    .join(topics, topics.c.id == article_topics.c.topic_id)
                    .where(
                        article_topics.c.article_id.in_(article_ids),
                        topics.c.slug.like(f"{TOPIC_PREFIX}%"),
                    )
                )
            ).all()
            done = (
                await connection.execute(
                    select(processing_jobs.c.state).where(
                        processing_jobs.c.article_id.in_(article_ids),
                        processing_jobs.c.stage == "classify",
                    )
                )
            ).scalars()
        assert {row.article_id for row in rows} == set(article_ids)
        assert all(row.classifier_set_hash == snapshot.set_hash for row in rows)
        assert all(row.assigned_count == 1 for row in rows)
        # Near-misses are kept, so both topics are scored for both articles.
        assert len(scored) == 4
        assert {row.assigned for row in scored} == {True, False}
        assert set(done) == {"done"}

        # A second run has nothing pending and must change nothing.
        again = await ClassifyWorker(queue=PostgresJobQueue(engine)).execute(
            _classify_context(storage, now)
        )
        assert again.stats.claimed == 0
    finally:
        await _cleanup(engine)
        await engine.dispose()
        await storage.close()


def test_stale_reprocess_only_requeues_articles_scored_against_another_set() -> None:
    if not DATABASE_URL:
        pytest.skip("TEST_DATABASE_URL is not set")
    asyncio.run(
        _test_stale_reprocess_only_requeues_articles_scored_against_another_set()
    )


async def _test_stale_reprocess_only_requeues_articles_scored_against_another_set() -> (
    None
):
    now = datetime.now(timezone.utc)
    storage = PostgresRawStorage(DATABASE_URL)
    engine = create_async_engine(DATABASE_URL)
    try:
        await storage.check_ready()
        await _cleanup(engine)
        await _seed_topics(engine)
        article_ids = await _seed_articles(storage, engine, now)
        await ClassifyWorker(queue=PostgresJobQueue(engine)).execute(
            _classify_context(storage, now)
        )

        first_pass = await _classified_at(engine, article_ids)

        # Nothing changed in the registry, so these two are not stale and
        # --stale must leave them exactly as they are.
        await ClassifyWorker(queue=PostgresJobQueue(engine)).execute(
            _classify_context(storage, now, stale=True)
        )
        assert await _classified_at(engine, article_ids) == first_pass

        # Retuning one topic changes the set hash, so both articles are stale.
        async with engine.begin() as connection:
            await connection.execute(
                update(topics)
                .where(topics.c.slug == f"{TOPIC_PREFIX}near")
                .values(threshold=0.9, classifier_revision=2)
            )
        await ClassifyWorker(queue=PostgresJobQueue(engine)).execute(
            _classify_context(storage, now, stale=True)
        )
        assert await _classified_at(engine, article_ids) != first_pass
        # 0.8 and 0.64 both fall under the new 0.9 threshold.
        async with engine.connect() as connection:
            near = (
                await connection.execute(
                    select(article_topics.c.assigned, article_topics.c.classifier_revision)
                    .join(topics, topics.c.id == article_topics.c.topic_id)
                    .where(
                        article_topics.c.article_id.in_(article_ids),
                        topics.c.slug == f"{TOPIC_PREFIX}near",
                    )
                )
            ).all()
        assert [(row.assigned, row.classifier_revision) for row in near] == [
            (False, 2)
        ] * len(article_ids)

        # --reprocess ignores the hash and rescores the window regardless.
        second_pass = await _classified_at(engine, article_ids)
        await ClassifyWorker(queue=PostgresJobQueue(engine)).execute(
            _classify_context(storage, now, reprocess=True)
        )
        assert await _classified_at(engine, article_ids) != second_pass
    finally:
        await _cleanup(engine)
        await engine.dispose()
        await storage.close()


def test_refresh_clusters_rebuilds_labels_from_article_topics() -> None:
    if not DATABASE_URL:
        pytest.skip("TEST_DATABASE_URL is not set")
    asyncio.run(_test_refresh_clusters_rebuilds_labels_from_article_topics())


async def _test_refresh_clusters_rebuilds_labels_from_article_topics() -> None:
    """`cluster_topics` is derived: deleting it must be fully recoverable."""
    now = datetime.now(timezone.utc)
    storage = PostgresRawStorage(DATABASE_URL)
    engine = create_async_engine(DATABASE_URL)
    try:
        await storage.check_ready()
        await _cleanup(engine)
        await _seed_topics(engine)
        article_ids = await _seed_articles(storage, engine, now)
        await ClassifyWorker(queue=PostgresJobQueue(engine)).execute(
            _classify_context(storage, now)
        )
        await ClusterWorker(queue=PostgresJobQueue(engine)).execute(
            _cluster_context(storage, now)
        )
        before = await _cluster_labels(engine, article_ids[0])
        assert before

        async with engine.connect() as connection:
            cluster_id = await connection.scalar(
                select(cluster_members.c.cluster_id).where(
                    cluster_members.c.article_id == article_ids[0]
                )
            )
        async with engine.begin() as connection:
            await connection.execute(
                delete(cluster_topics).where(cluster_topics.c.cluster_id == cluster_id)
            )
        assert await _cluster_labels(engine, article_ids[0]) == []

        outcome = await ClassifyWorker(queue=PostgresJobQueue(engine)).execute(
            _classify_context(storage, now, refresh_clusters=True)
        )
        assert outcome.stats.clusters_refreshed >= 1
        assert await _cluster_labels(engine, article_ids[0]) == before
    finally:
        await _cleanup(engine)
        await engine.dispose()
        await storage.close()
