"""PostgreSQL integration tests for the daily mention-count aggregate."""

import asyncio
import os
from datetime import date, datetime, timezone

import pytest
from sqlalchemy import delete
from sqlalchemy.ext.asyncio import create_async_engine

from adapters.storage.postgres import PostgresRawStorage
from adapters.storage.schema import raw_articles, sources
from models.storage_models import RawArticle


DATABASE_URL = os.getenv("TEST_DATABASE_URL")
pytestmark = pytest.mark.integration

SOURCE_SLUG = "trends.integration.example"
BASE = "https://trends.integration.example/"


def _article(path: str, *, title: str, content: str, published_at: datetime) -> RawArticle:
    url = BASE + path
    return RawArticle(
        url=url,
        url_original=url,
        title=title,
        source="Trends integration feed",
        lang="en",
        content=content,
        content_hash=path.encode().ljust(32, b"0"),
        published_at=published_at,
        crawled_at=published_at,
    )


def test_daily_article_counts() -> None:
    if not DATABASE_URL:
        pytest.skip("TEST_DATABASE_URL is not set")
    asyncio.run(_test_daily_article_counts())


async def _test_daily_article_counts() -> None:
    # Boundary rows at 00:30Z and 23:30Z: they land in a different calendar day
    # under any non-UTC session TimeZone, so they catch a bucket that follows
    # the server's locale. On a UTC server they pass either way, which is why
    # test_trends_query.py also asserts the explicit 'UTC' in the SQL.
    fixtures = [
        _article(
            "early",
            title="Quantum computing milestone",
            content="quantum body",
            published_at=datetime(2026, 3, 2, 0, 30, tzinfo=timezone.utc),
        ),
        _article(
            "midday",
            title="More quantum research",
            content="quantum body",
            published_at=datetime(2026, 3, 2, 12, 0, tzinfo=timezone.utc),
        ),
        # No "quantum" anywhere: proves `articles` counts the whole window and
        # is not a second copy of `matches`.
        _article(
            "unrelated",
            title="Gardening tips for spring",
            content="mulch and compost",
            published_at=datetime(2026, 3, 2, 12, 30, tzinfo=timezone.utc),
        ),
        # Nothing at all on 2026-03-03 -- the zero-fill case.
        _article(
            "late",
            title="Quantum chips ship",
            content="quantum body",
            published_at=datetime(2026, 3, 4, 23, 30, tzinfo=timezone.utc),
        ),
    ]
    urls = [article.url for article in fixtures]
    storage = PostgresRawStorage(DATABASE_URL)
    engine = create_async_engine(DATABASE_URL)
    ready = False
    try:
        await storage.check_ready()
        ready = True
        async with engine.begin() as connection:
            await connection.execute(delete(raw_articles).where(raw_articles.c.url.in_(urls)))
            await connection.execute(delete(sources).where(sources.c.slug == SOURCE_SLUG))

        await storage.apply_batch(fixtures, [], [], received=len(fixtures), rejected=0)

        # Scoped to this source: a real test database carries production rows,
        # so an unscoped denominator would be unassertable.
        days = await storage.daily_article_counts(
            since=datetime(2026, 3, 2, tzinfo=timezone.utc),
            until=datetime(2026, 3, 5, tzinfo=timezone.utc),
            query="quantum",
            source_slugs=[SOURCE_SLUG],
        )
        # One assertion covering: UTC bucketing at both day boundaries, the
        # q-independent denominator, the zero-filled gap day, the
        # nothing-was-crawled case, and the absence of any per-source cap.
        assert [(d.day, d.matches, d.articles, d.partial) for d in days] == [
            (date(2026, 3, 2), 2, 3, False),
            (date(2026, 3, 3), 0, 0, False),
            (date(2026, 3, 4), 1, 1, False),
        ]

        # A window starting mid-day flags the edge as partial and drops the
        # 00:30Z row from both numerator and denominator.
        partial = await storage.daily_article_counts(
            since=datetime(2026, 3, 2, 6, tzinfo=timezone.utc),
            until=datetime(2026, 3, 3, tzinfo=timezone.utc),
            query="quantum",
            source_slugs=[SOURCE_SLUG],
        )
        assert [(d.day, d.matches, d.articles, d.partial) for d in partial] == [
            (date(2026, 3, 2), 1, 2, True),
        ]

        # A query nothing matches still returns the dense window with its
        # denominators intact, rather than an empty list.
        no_match = await storage.daily_article_counts(
            since=datetime(2026, 3, 2, tzinfo=timezone.utc),
            until=datetime(2026, 3, 3, tzinfo=timezone.utc),
            query="no-such-term-xyz",
            source_slugs=[SOURCE_SLUG],
        )
        assert [(d.day, d.matches, d.articles) for d in no_match] == [
            (date(2026, 3, 2), 0, 3),
        ]

        # An unmatched source filter zeroes both numbers, never the row count.
        other_source = await storage.daily_article_counts(
            since=datetime(2026, 3, 2, tzinfo=timezone.utc),
            until=datetime(2026, 3, 3, tzinfo=timezone.utc),
            query="quantum",
            source_slugs=["no-such-source.example"],
        )
        assert [(d.day, d.matches, d.articles) for d in other_source] == [
            (date(2026, 3, 2), 0, 0),
        ]
    finally:
        if ready:
            async with engine.begin() as connection:
                await connection.execute(
                    delete(raw_articles).where(raw_articles.c.url.in_(urls))
                )
                await connection.execute(
                    delete(sources).where(sources.c.slug == SOURCE_SLUG)
                )
        await engine.dispose()
        await storage.close()
