import asyncio
from datetime import datetime, timedelta, timezone
import os

import pytest
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import create_async_engine

from adapters.storage.postgres import PostgresRawStorage
from adapters.storage.schema import (
    article_embeddings,
    clusters,
    processing_jobs,
    raw_articles,
    sources,
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


async def _cleanup(engine, urls: list[str], slugs: list[str]) -> None:
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
        await connection.execute(delete(sources).where(sources.c.slug.in_(slugs)))


def test_list_stories_filters_by_source_without_duplicates_or_n_plus_1() -> None:
    if not DATABASE_URL:
        pytest.skip("TEST_DATABASE_URL is not set")
    asyncio.run(_test_list_stories_filters_by_source_without_duplicates_or_n_plus_1())


async def _test_list_stories_filters_by_source_without_duplicates_or_n_plus_1() -> None:
    urls = [
        "https://stories-integration-a.example/one",
        "https://stories-integration-b.example/two",
    ]
    slugs = ["stories-integration-a.example", "stories-integration-b.example"]
    now = datetime.now(timezone.utc)
    storage = PostgresRawStorage(DATABASE_URL)
    engine = create_async_engine(DATABASE_URL)
    cluster_ids: list[int] = []
    try:
        await storage.check_ready()
        await _cleanup(engine, urls, slugs)
        await storage.apply_batch(
            [
                _article(urls[0], "Story one", "A", now),
                _article(urls[1], "Story two", "B", now),
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
        second = [0.0, 1.0] + [0.0] * 382
        await storage.upsert_embeddings(
            [
                EmbeddingRecord(article_ids[0], "test", 384, b"s1".ljust(32, b"0"), first),
                EmbeddingRecord(article_ids[1], "test", 384, b"s2".ljust(32, b"0"), second),
            ]
        )
        articles = await storage.clusterable_articles(article_ids)
        created_a = await storage.create_cluster(articles[article_ids[0]])
        created_b = await storage.create_cluster(articles[article_ids[1]])
        cluster_ids = [created_a.cluster_id, created_b.cluster_id]

        since = now - timedelta(days=1)
        until = now + timedelta(days=1)

        both = await storage.list_stories(since=since, until=until, limit=50)
        matched_ids = {item.id for item in both}
        assert cluster_ids[0] in matched_ids
        assert cluster_ids[1] in matched_ids

        only_a = await storage.list_stories(
            since=since, until=until, limit=50, source_slugs=[slugs[0]]
        )
        # Filtering by one source returns exactly its story once, never a
        # duplicate row from the EXISTS join and never the other source's.
        assert [item.id for item in only_a if item.id in cluster_ids] == [
            cluster_ids[0]
        ]
        story_a = next(item for item in only_a if item.id == cluster_ids[0])
        assert story_a.article_count == 1
        assert story_a.source_count == 1
        assert [s.slug for s in story_a.sources] == [slugs[0]]
    finally:
        if cluster_ids:
            async with engine.begin() as connection:
                await connection.execute(
                    delete(clusters).where(clusters.c.id.in_(cluster_ids))
                )
        await _cleanup(engine, urls, slugs)
        await engine.dispose()
        await storage.close()


def test_get_story_resolves_merged_cluster_to_canonical() -> None:
    if not DATABASE_URL:
        pytest.skip("TEST_DATABASE_URL is not set")
    asyncio.run(_test_get_story_resolves_merged_cluster_to_canonical())


async def _test_get_story_resolves_merged_cluster_to_canonical() -> None:
    urls = [
        "https://stories-integration-merge.example/one",
        "https://stories-integration-merge.example/two",
    ]
    slugs = ["stories-integration-merge.example"]
    now = datetime.now(timezone.utc)
    storage = PostgresRawStorage(DATABASE_URL)
    engine = create_async_engine(DATABASE_URL)
    cluster_ids: list[int] = []
    try:
        await storage.check_ready()
        await _cleanup(engine, urls, slugs)
        await storage.apply_batch(
            [
                _article(urls[0], "Merge one", "Same source", now),
                _article(urls[1], "Merge two", "Same source", now),
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
        second = [0.95, 0.05] + [0.0] * 382
        await storage.upsert_embeddings(
            [
                EmbeddingRecord(article_ids[0], "test", 384, b"g1".ljust(32, b"0"), first),
                EmbeddingRecord(article_ids[1], "test", 384, b"g2".ljust(32, b"0"), second),
            ]
        )
        articles = await storage.clusterable_articles(article_ids)
        result_a = await storage.create_cluster(articles[article_ids[0]])
        result_b = await storage.create_cluster(articles[article_ids[1]])
        cluster_ids = [result_a.cluster_id, result_b.cluster_id]

        merge = await storage.merge_clusters(result_a.cluster_id, result_b.cluster_id)
        assert merge.merged is True

        canonical = await storage.get_story(merge.canonical_id)
        assert canonical is not None
        assert canonical.id == merge.canonical_id
        assert {member.article_id for member in canonical.members} == set(article_ids)

        via_loser = await storage.get_story(merge.loser_id)
        assert via_loser is not None
        assert via_loser.id == merge.canonical_id
        assert {member.article_id for member in via_loser.members} == set(article_ids)
    finally:
        if cluster_ids:
            async with engine.begin() as connection:
                await connection.execute(
                    delete(clusters).where(clusters.c.id.in_(cluster_ids))
                )
        await _cleanup(engine, urls, slugs)
        await engine.dispose()
        await storage.close()
