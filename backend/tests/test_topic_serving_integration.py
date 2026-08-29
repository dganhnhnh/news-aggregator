import asyncio
from datetime import datetime, timedelta, timezone
import os

import pytest
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import create_async_engine

from adapters.storage.postgres import PostgresRawStorage
from adapters.storage.schema import (
    article_embeddings,
    article_topics,
    cluster_topics,
    clusters,
    processing_jobs,
    raw_articles,
    sources,
    taxonomies,
    taxonomy_node_topics,
    taxonomy_nodes,
    topics,
)
from models.storage_models import EmbeddingRecord, RawArticle


DATABASE_URL = os.getenv("TEST_DATABASE_URL")
pytestmark = pytest.mark.integration


def _article(url: str, title: str, source: str, now: datetime) -> RawArticle:
    return RawArticle(
        url=url,
        url_original=url,
        title=title,
        source=source,
        lang="en",
        content=title,
        content_hash=title.encode().ljust(32, b"0")[:32],
        published_at=now,
        crawled_at=now,
    )


async def _cleanup(engine, urls: list[str], source_slugs: list[str], topic_slugs: list[str]) -> None:
    async with engine.begin() as connection:
        article_ids = list(
            (
                await connection.execute(
                    select(raw_articles.c.id).where(raw_articles.c.url.in_(urls))
                )
            ).scalars()
        )
        if article_ids:
            await connection.execute(
                delete(processing_jobs).where(processing_jobs.c.article_id.in_(article_ids))
            )
            await connection.execute(
                delete(article_embeddings).where(article_embeddings.c.article_id.in_(article_ids))
            )
        await connection.execute(delete(raw_articles).where(raw_articles.c.url.in_(urls)))
        await connection.execute(
            delete(taxonomies).where(taxonomies.c.slug == "topic-serving-taxonomy")
        )
        await connection.execute(delete(topics).where(topics.c.slug.in_(topic_slugs)))
        await connection.execute(delete(sources).where(sources.c.slug.in_(source_slugs)))


def test_topic_catalog_article_and_story_serving() -> None:
    if not DATABASE_URL:
        pytest.skip("TEST_DATABASE_URL is not set")
    asyncio.run(_test_topic_catalog_article_and_story_serving())


async def _test_topic_catalog_article_and_story_serving() -> None:
    urls = [
        "https://topic-serving-a.example/one",
        "https://topic-serving-b.example/two",
    ]
    source_slugs = ["topic-serving-a.example", "topic-serving-b.example"]
    topic_slugs = ["topic-serving-alpha", "topic-serving-beta"]
    now = datetime.now(timezone.utc)
    storage = PostgresRawStorage(DATABASE_URL)
    engine = create_async_engine(DATABASE_URL)
    cluster_ids: list[int] = []
    try:
        await storage.check_ready()
        await _cleanup(engine, urls, source_slugs, topic_slugs)
        await storage.apply_batch(
            [
                _article(urls[0], "Topic alpha", "A", now),
                _article(urls[1], "Topic beta", "B", now),
            ],
            [],
            [],
            received=2,
            rejected=0,
        )
        async with engine.connect() as connection:
            article_rows = (
                await connection.execute(
                    select(raw_articles.c.id, raw_articles.c.url)
                    .where(raw_articles.c.url.in_(urls))
                    .order_by(raw_articles.c.url)
                )
            ).all()
        article_ids = [int(row.id) for row in article_rows]
        vectors = [
            [1.0, 0.0] + [0.0] * 382,
            [0.0, 1.0] + [0.0] * 382,
        ]
        await storage.upsert_embeddings(
            [
                EmbeddingRecord(
                    article_id,
                    "test",
                    384,
                    f"topic-serving-{index}".encode().ljust(32, b"0")[:32],
                    vectors[index],
                )
                for index, article_id in enumerate(article_ids)
            ]
        )
        clusterable = await storage.clusterable_articles(article_ids)
        cluster_ids = [
            (await storage.create_cluster(clusterable[article_id])).cluster_id
            for article_id in article_ids
        ]

        async with engine.begin() as connection:
            topic_ids = []
            for slug, name in zip(topic_slugs, ("Alpha", "Beta")):
                topic_ids.append(
                    int(
                        await connection.scalar(
                            topics.insert()
                            .values(
                                slug=slug,
                                display_name=name,
                                description=f"{name} description",
                                threshold=0.4,
                                classifier_revision=1,
                                active=True,
                            )
                            .returning(topics.c.id)
                        )
                    )
                )
            await connection.execute(
                article_topics.insert().values(
                    [
                        {
                            "article_id": article_ids[0],
                            "topic_id": topic_ids[0],
                            "score": 0.6,
                            "prototype_score": 0.7,
                            "keyword_score": 0.2,
                            "source_prior": None,
                            "assigned": True,
                            "classifier_revision": 1,
                        },
                        {
                            "article_id": article_ids[0],
                            "topic_id": topic_ids[1],
                            "score": 0.7,
                            "prototype_score": 0.8,
                            "keyword_score": 0.3,
                            "source_prior": None,
                            "assigned": True,
                            "classifier_revision": 1,
                        },
                        {
                            "article_id": article_ids[1],
                            "topic_id": topic_ids[0],
                            "score": 0.39,
                            "prototype_score": 0.45,
                            "keyword_score": 0.15,
                            "source_prior": None,
                            "assigned": False,
                            "classifier_revision": 1,
                        },
                        {
                            "article_id": article_ids[1],
                            "topic_id": topic_ids[1],
                            "score": 0.65,
                            "prototype_score": 0.75,
                            "keyword_score": 0.25,
                            "source_prior": None,
                            "assigned": True,
                            "classifier_revision": 1,
                        },
                    ]
                )
            )
            await connection.execute(
                cluster_topics.insert().values(
                    [
                        {
                            "cluster_id": cluster_ids[0],
                            "topic_id": topic_ids[0],
                            "score": 0.6,
                            "assigned": True,
                        },
                        {
                            "cluster_id": cluster_ids[0],
                            "topic_id": topic_ids[1],
                            "score": 0.7,
                            "assigned": True,
                        },
                        {
                            "cluster_id": cluster_ids[1],
                            "topic_id": topic_ids[0],
                            "score": 0.39,
                            "assigned": False,
                        },
                        {
                            "cluster_id": cluster_ids[1],
                            "topic_id": topic_ids[1],
                            "score": 0.65,
                            "assigned": True,
                        },
                    ]
                )
            )
            taxonomy_id = int(
                await connection.scalar(
                    taxonomies.insert()
                    .values(
                        slug="topic-serving-taxonomy",
                        display_name="Topic Serving",
                        revision=3,
                        active=True,
                    )
                    .returning(taxonomies.c.id)
                )
            )
            node_id = int(
                await connection.scalar(
                    taxonomy_nodes.insert()
                    .values(
                        taxonomy_id=taxonomy_id,
                        parent_id=None,
                        slug="root",
                        display_name="Root",
                        sort_order=0,
                    )
                    .returning(taxonomy_nodes.c.id)
                )
            )
            await connection.execute(
                taxonomy_node_topics.insert().values(
                    [{"node_id": node_id, "topic_id": topic_id} for topic_id in topic_ids]
                )
            )

        since = now - timedelta(days=1)
        until = now + timedelta(days=1)
        alpha_articles = await storage.feed_articles(
            since=since,
            until=until,
            limit=20,
            topic_slugs=[topic_slugs[0]],
            max_per_source=20,
        )
        assert [item.id for item in alpha_articles if item.id in article_ids] == [article_ids[0]]
        assert [topic.slug for topic in alpha_articles[0].topics] == [
            topic_slugs[1],
            topic_slugs[0],
        ]

        alpha_stories = await storage.list_stories(
            since=since, until=until, limit=20, topic_slugs=[topic_slugs[0]]
        )
        assert [item.id for item in alpha_stories if item.id in cluster_ids] == [cluster_ids[0]]
        story = await storage.get_story(cluster_ids[0])
        assert story is not None
        assert [topic.slug for topic in story.topics] == [topic_slugs[1], topic_slugs[0]]

        source_and_topic = await storage.list_stories(
            since=since,
            until=until,
            limit=20,
            source_slugs=[source_slugs[1]],
            topic_slugs=topic_slugs,
        )
        assert [item.id for item in source_and_topic if item.id in cluster_ids] == [cluster_ids[1]]

        catalog = await storage.get_taxonomy_catalog("topic-serving-taxonomy")
        assert catalog is not None
        assert catalog.revision == 3
        assert [topic.slug for topic in catalog.topics if topic.slug in topic_slugs] == topic_slugs
        assert catalog.nodes[0].topic_slugs == topic_slugs
    finally:
        if cluster_ids:
            async with engine.begin() as connection:
                await connection.execute(delete(clusters).where(clusters.c.id.in_(cluster_ids)))
        await _cleanup(engine, urls, source_slugs, topic_slugs)
        await engine.dispose()
        await storage.close()
