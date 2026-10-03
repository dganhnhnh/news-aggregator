"""All writes are isolated in a temporary schema, even on a populated test DB."""
import asyncio
import os
import uuid
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import select, text, func
from sqlalchemy.ext.asyncio import create_async_engine
from adapters.storage.postgres import PostgresRawStorage
from adapters.storage.schema import (
    metadata, raw_articles, article_embeddings, worker_runs, clusters, cluster_members, topics, topic_prototypes,
    topic_signals, taxonomy_node_topics,
)
from services.taxonomy import SeedTopic, SeedKeyword, TaxonomySeed

pytestmark = pytest.mark.integration


def test_discovery_storage_atomic_additive_and_windowed():
    url = os.getenv("TEST_DATABASE_URL")
    if not url:
        pytest.skip("TEST_DATABASE_URL is not set")
    asyncio.run(_exercise(url))


async def _exercise(url):
    schema = "taxonomy_test_" + uuid.uuid4().hex
    admin = create_async_engine(url)
    storage = PostgresRawStorage(url)
    await storage.close()
    storage._engine = create_async_engine(url, connect_args={"server_settings": {"search_path": f"{schema},public"}})
    try:
        async with admin.begin() as connection:
            await connection.execute(text(f'CREATE SCHEMA "{schema}"'))
        async with storage.engine.begin() as connection:
            await connection.run_sync(lambda conn: metadata.create_all(conn, checkfirst=False))
        topic = SeedTopic("test-economics", "Economics", "Monetary policy and employment", .4,
                          ("Central bank cuts rates", "Employment growth slows", "Inflation rises again"),
                          (SeedKeyword("inflation"),))
        seed = TaxonomySeed(1, (topic,), ())
        vector = [1.] + [0.] * 383
        assert await storage.create_discovered_topics(seed, {topic.slug: vector}, model="test", dim=384) == [topic.slug]
        assert await storage.create_discovered_topics(seed, {topic.slug: vector}, model="test", dim=384) == []
        snapshot = await storage.classifier_snapshot()
        assert len(snapshot.classifiers) == 1
        assert not snapshot.topics_without_prototype
        assert snapshot.classifiers[0].classifier_revision == 1
        async with storage.engine.connect() as connection:
            assert await connection.scalar(select(func.count()).select_from(topic_signals)) == 4
            assert await connection.scalar(select(func.count()).select_from(taxonomy_node_topics)) == 1
        # A later bad vector must roll back the earlier topic in the same batch.
        from dataclasses import replace
        one, two = replace(topic, slug="rollback-one"), replace(topic, slug="rollback-two")
        with pytest.raises(Exception):
            await storage.create_discovered_topics(TaxonomySeed(1, (one, two), ()),
                {one.slug: vector, two.slug: ["invalid"] * 384}, model="test", dim=384)
        async with storage.engine.connect() as connection:
            assert await connection.scalar(select(func.count()).select_from(topics)) == 1
            assert await connection.scalar(select(func.count()).select_from(topic_prototypes)) == 1
        now = datetime.now(timezone.utc)
        async with storage.engine.begin() as connection:
            ids = []
            for i in range(5):
                ids.append((await connection.execute(raw_articles.insert().values(
                    url=f"https://example.test/{i}", title=f"Article {i}", content="Body",
                    content_hash=bytes([i]), crawled_at=now-timedelta(days=9 if i == 4 else 1),
                ).returning(raw_articles.c.id))).scalar_one())
            await connection.execute(article_embeddings.insert(), [
                dict(article_id=aid, model="test", dim=384, content_hash=b"hash", embedding=vector)
                for aid in ids
            ])
            for status in ("succeeded", "failed"):
                await connection.execute(worker_runs.insert().values(
                    worker="taxonomy-discover", trigger="manual", status=status,
                    started_at=now, heartbeat_at=now,
                    stats={"input_hash": "test-input", "proposal": {"topics": [] if status == "succeeded" else ["bad"]}},
                ))
            # Drifted n_articles says 1; real membership of 3 still qualifies.
            for members in (ids[:3], ids[3:]):
                cid = (await connection.execute(clusters.insert().values(
                    centroid=vector, centroid_count=1, n_articles=1, n_distinct_sources=1,
                    first_seen_at=now, last_seen_at=now, headline="Headline", status="closed",
                ).returning(clusters.c.id))).scalar_one()
                await connection.execute(cluster_members.insert(), [dict(cluster_id=cid, article_id=aid, similarity=.9) for aid in members])
        assert await storage.taxonomy_discovery_cached("test-input") == {"topics": []}
        assert await storage.taxonomy_discovery_cached("missing") is None
        assert await storage.classification_evaluation_ids(
            since=now-timedelta(days=7), until=now, after_id=ids[0], limit=2
        ) == ids[1:3]
        stories = await storage.taxonomy_discovery_stories(since=now-timedelta(days=7), until=now, limit=100)
        assert len(stories) == 1  # The other cluster has only one member inside the window.
        assert len(stories[0]["members"]) == 3
        assert stories[0]["n_articles"] == 3
        assert await storage.taxonomy_discovery_stories(
            since=now-timedelta(days=7), until=now, story_ids=[stories[0]["id"]]
        ) == stories
        assert await storage.taxonomy_discovery_stories(
            since=now-timedelta(days=7), until=now, story_ids=[]
        ) == []
    finally:
        await storage.close()
        async with admin.begin() as connection:
            await connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        await admin.dispose()
