from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
import os

import pytest
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import create_async_engine

from adapters.storage.postgres import PostgresRawStorage
from adapters.storage.schema import clusters, raw_articles, sources
from models.storage_models import EmbeddingRecord, RawArticle


DATABASE_URL = os.getenv("TEST_DATABASE_URL")
pytestmark = pytest.mark.integration
MODEL = "research-integration-model"


def _article(url: str, title: str, source: str, at: datetime) -> RawArticle:
    return RawArticle(
        url=url,
        url_original=url,
        title=title,
        source=source,
        lang="en",
        content=f"{title}. Full article body for research integration testing.",
        content_hash=title.encode().ljust(32, b"0")[:32],
        published_at=at,
        crawled_at=at,
    )


async def _cleanup(engine, urls: list[str], cluster_ids: list[int]) -> None:
    async with engine.begin() as connection:
        if cluster_ids:
            await connection.execute(delete(clusters).where(clusters.c.id.in_(cluster_ids)))
        await connection.execute(delete(raw_articles).where(raw_articles.c.url.in_(urls)))
        await connection.execute(
            delete(sources).where(sources.c.slug.like("research-search-integration-%"))
        )


def test_hybrid_article_and_storyline_research_queries() -> None:
    if not DATABASE_URL:
        pytest.skip("TEST_DATABASE_URL is not set")
    asyncio.run(_test_hybrid_article_and_storyline_research_queries())


async def _test_hybrid_article_and_storyline_research_queries() -> None:
    now = datetime.now(timezone.utc)
    urls = [
        "https://research-search-integration-a.example/lexical",
        "https://research-search-integration-b.example/semantic",
        "https://research-search-integration-c.example/both",
    ]
    storage = PostgresRawStorage(DATABASE_URL)
    engine = create_async_engine(DATABASE_URL)
    cluster_ids: list[int] = []
    try:
        await storage.check_ready()
        await _cleanup(engine, urls, cluster_ids)
        await storage.apply_batch(
            [
                _article(urls[0], "Quasarlex policy report", "Lexical Source", now),
                _article(urls[1], "Unrelated headline", "Semantic Source", now),
                _article(urls[2], "Quasarlex combined report", "Combined Source", now),
            ],
            [],
            [],
            received=3,
            rejected=0,
        )
        async with engine.connect() as connection:
            rows = (
                await connection.execute(
                    select(raw_articles.c.id, raw_articles.c.url)
                    .where(raw_articles.c.url.in_(urls))
                    .order_by(raw_articles.c.url)
                )
            ).all()
        ids_by_url = {row.url: int(row.id) for row in rows}
        query_vector = [1.0, 0.0] + [0.0] * 382
        opposite = [0.0, 1.0] + [0.0] * 382
        await storage.upsert_embeddings(
            [
                EmbeddingRecord(
                    ids_by_url[urls[1]], MODEL, 384, b"semantic".ljust(32, b"0"), query_vector
                ),
                EmbeddingRecord(
                    ids_by_url[urls[2]], MODEL, 384, b"both".ljust(32, b"0"), query_vector
                ),
                EmbeddingRecord(
                    ids_by_url[urls[0]], MODEL, 384, b"lexical".ljust(32, b"0"), opposite
                ),
            ]
        )

        hits = await storage.search_articles("quasarlex", query_vector, model=MODEL)
        matched = [hit.id for hit in hits if hit.id in ids_by_url.values()]
        assert matched[0] == ids_by_url[urls[2]]
        assert set(matched) == set(ids_by_url.values())

        source_hits = await storage.search_articles(
            "quasarlex",
            query_vector,
            model=MODEL,
            source="research-search-integration-b.example",
            since=now - timedelta(hours=1),
            until=now + timedelta(hours=1),
        )
        assert [hit.id for hit in source_hits] == [ids_by_url[urls[1]]]

        clusterable = await storage.clusterable_articles(list(ids_by_url.values()))
        created_a = await storage.create_cluster(clusterable[ids_by_url[urls[1]]])
        created_b = await storage.create_cluster(clusterable[ids_by_url[urls[2]]])
        cluster_ids.extend([created_a.cluster_id, created_b.cluster_id])

        story_hits = await storage.search_storylines(query_vector)
        assert created_a.cluster_id in {hit.id for hit in story_hits}
        assert all(len(hit.member_preview) <= 3 for hit in story_hits)

        merge = await storage.merge_clusters(created_a.cluster_id, created_b.cluster_id)
        assert merge.merged
        canonical_hits = await storage.search_storylines(query_vector)
        assert merge.loser_id not in {hit.id for hit in canonical_hits}
        detail = await storage.get_storyline_evidence(merge.loser_id)
        assert detail is not None
        assert detail.id == merge.canonical_id
        assert {member.article_id for member in detail.members} == {
            ids_by_url[urls[1]],
            ids_by_url[urls[2]],
        }
    finally:
        await _cleanup(engine, urls, cluster_ids)
        await engine.dispose()
        await storage.close()
