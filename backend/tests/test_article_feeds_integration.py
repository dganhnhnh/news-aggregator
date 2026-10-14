import asyncio
from datetime import datetime, timedelta, timezone
import os

import pytest
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import create_async_engine

from adapters.storage.postgres import PostgresRawStorage
from adapters.storage.schema import (
    article_feeds,
    crawl_failures,
    feed_state,
    raw_articles,
    sources,
)
from models.storage_models import CrawlFailureOccurrence, FeedSeed, RawArticle
from services.raw_storage import content_hash


DATABASE_URL = os.getenv("TEST_DATABASE_URL")
pytestmark = pytest.mark.integration

INSTANT_FEED = "https://article-feeds.integration.example/instant.xml"
NORMAL_FEED = "https://article-feeds.integration.example/normal.xml"
UNSEEDED_FEED = "https://article-feeds.integration.example/unseeded.xml"
DOMAIN = "article-feeds.integration.example"
URL_PREFIX = f"https://{DOMAIN}/"


def _article(slug: str, *, feeds: tuple[str, ...], at: datetime) -> RawArticle:
    return RawArticle(
        url=f"{URL_PREFIX}{slug}",
        url_original=f"{URL_PREFIX}{slug}",
        title=slug,
        source="Integration",
        lang="en",
        content=f"body of {slug}",
        content_hash=content_hash(f"body of {slug}"),
        published_at=at,
        crawled_at=at,
        feed_urls=feeds,
    )


async def _cleanup(engine) -> None:
    async with engine.begin() as connection:
        await connection.execute(
            delete(raw_articles).where(raw_articles.c.url.like(f"{URL_PREFIX}%"))
        )
        await connection.execute(delete(sources).where(sources.c.slug == DOMAIN))
        await connection.execute(
            delete(feed_state).where(
                feed_state.c.feed_url.in_([INSTANT_FEED, NORMAL_FEED, UNSEEDED_FEED])
            )
        )
        await connection.execute(
            delete(crawl_failures).where(crawl_failures.c.url.like(f"{URL_PREFIX}%"))
        )


def test_articles_are_linked_to_feeds_and_listed_by_tier() -> None:
    if not DATABASE_URL:
        pytest.skip("TEST_DATABASE_URL is not set")
    asyncio.run(_test_articles_are_linked_to_feeds_and_listed_by_tier())


async def _test_articles_are_linked_to_feeds_and_listed_by_tier() -> None:
    storage = PostgresRawStorage(DATABASE_URL)
    engine = create_async_engine(DATABASE_URL)
    now = datetime.now(timezone.utc)
    window = {"since": now - timedelta(days=1), "until": now + timedelta(days=1)}
    ready = False
    try:
        await storage.check_ready()
        ready = True
        await _cleanup(engine)
        await storage.seed_feeds(
            [
                FeedSeed(feed_url=INSTANT_FEED, source_label="Instant", priority=0),
                FeedSeed(feed_url=NORMAL_FEED, source_label="Normal", priority=1),
            ]
        )

        # Five links from one domain: the instant tier must not be cut to the
        # three-per-domain cap the aggregated list applies.
        instant_only = [
            _article(f"instant-{i}", feeds=(INSTANT_FEED,), at=now - timedelta(minutes=i))
            for i in range(5)
        ]
        both = _article("both", feeds=(INSTANT_FEED, NORMAL_FEED), at=now)
        normal_only = _article("normal", feeds=(NORMAL_FEED,), at=now)
        unseeded = _article("unseeded", feeds=(UNSEEDED_FEED,), at=now)
        unlinked = _article("unlinked", feeds=(), at=now)
        await storage.apply_batch(
            [*instant_only, both, normal_only, unseeded, unlinked],
            [],
            [],
            received=9,
            rejected=0,
        )

        feeds = {
            item.feed_url: item for item in await storage.list_article_feeds(**window)
        }
        assert feeds[INSTANT_FEED].tier == "instant"
        assert feeds[INSTANT_FEED].article_count == 6
        assert feeds[INSTANT_FEED].source_label == "Instant"
        assert feeds[NORMAL_FEED].tier == "normal"
        assert feeds[NORMAL_FEED].article_count == 2
        # A feed that was never seeded has no priority, so it reads as normal.
        assert feeds[UNSEEDED_FEED].tier == "normal"
        assert feeds[UNSEEDED_FEED].source_label is None

        only_instant = await storage.list_article_feeds(**window, tier="instant")
        assert INSTANT_FEED in {item.feed_url for item in only_instant}
        assert NORMAL_FEED not in {item.feed_url for item in only_instant}

        async def urls(**scope) -> set[str]:
            items = await storage.feed_articles(
                **window, limit=100, source_slugs=[DOMAIN], **scope
            )
            return {item.url.removeprefix(URL_PREFIX) for item in items}

        # The aggregated list keeps its three-per-domain cap.
        assert len(await urls()) == 3
        assert await urls(tier="instant") == {
            "both",
            *(f"instant-{i}" for i in range(5)),
        }
        assert await urls(tier="normal") == {"both", "normal", "unseeded"}
        assert await urls(feed_url=NORMAL_FEED) == {"both", "normal"}
        # Scope is per feed row: a normal feed has nothing in the instant tier,
        # even for an article another, instant feed also carries.
        assert await urls(feed_url=NORMAL_FEED, tier="instant") == set()
        assert await urls(feed_url=INSTANT_FEED, tier="instant") >= {"both"}

        # Re-seeing an unchanged article in another feed still links it.
        replay = await storage.apply_batch(
            [_article("normal", feeds=(NORMAL_FEED, INSTANT_FEED), at=now)],
            [],
            [],
            received=1,
            rejected=0,
        )
        assert replay.unchanged == 1
        assert "normal" in await urls(feed_url=INSTANT_FEED)

        # The tier is read live: moving a feed re-sorts its whole history.
        assert await storage.set_feed_priority(INSTANT_FEED, 1)
        assert await urls(tier="instant") == set()
        moved = await storage.list_article_feeds(**window, tier="instant")
        assert INSTANT_FEED not in {item.feed_url for item in moved}
    finally:
        if ready:
            await _cleanup(engine)
        await engine.dispose()
        await storage.close()


def test_deleting_an_article_removes_its_feed_links() -> None:
    if not DATABASE_URL:
        pytest.skip("TEST_DATABASE_URL is not set")
    asyncio.run(_test_deleting_an_article_removes_its_feed_links())


async def _test_deleting_an_article_removes_its_feed_links() -> None:
    storage = PostgresRawStorage(DATABASE_URL)
    engine = create_async_engine(DATABASE_URL)
    now = datetime.now(timezone.utc)
    ready = False
    try:
        await storage.check_ready()
        ready = True
        await _cleanup(engine)
        await storage.apply_batch(
            [_article("doomed", feeds=(NORMAL_FEED,), at=now)],
            [],
            [],
            received=1,
            rejected=0,
        )
        async with engine.begin() as connection:
            await connection.execute(
                delete(raw_articles).where(raw_articles.c.url == f"{URL_PREFIX}doomed")
            )
            remaining = (
                await connection.execute(
                    select(article_feeds).where(article_feeds.c.feed_url == NORMAL_FEED)
                )
            ).all()
        assert remaining == []
    finally:
        if ready:
            await _cleanup(engine)
        await engine.dispose()
        await storage.close()


def test_a_failure_remembers_its_feed_across_occurrences() -> None:
    if not DATABASE_URL:
        pytest.skip("TEST_DATABASE_URL is not set")
    asyncio.run(_test_a_failure_remembers_its_feed_across_occurrences())


async def _test_a_failure_remembers_its_feed_across_occurrences() -> None:
    storage = PostgresRawStorage(DATABASE_URL)
    engine = create_async_engine(DATABASE_URL)
    url = f"{URL_PREFIX}flaky"
    ready = False

    def occurrence(feed_url: str | None) -> CrawlFailureOccurrence:
        return CrawlFailureOccurrence(
            failure_kind="article",
            url=url,
            error_type="TimeoutError",
            error_message="timeout",
            feed_url=feed_url,
        )

    async def stored_feed() -> str | None:
        async with engine.connect() as connection:
            return (
                await connection.execute(
                    select(crawl_failures.c.feed_url).where(crawl_failures.c.url == url)
                )
            ).scalar_one()

    try:
        await storage.check_ready()
        ready = True
        await _cleanup(engine)
        await storage.apply_batch([], [occurrence(NORMAL_FEED)], [], received=0, rejected=0)
        assert await stored_feed() == NORMAL_FEED

        # A later occurrence that does not know the feed must not erase it.
        await storage.apply_batch([], [occurrence(None)], [], received=0, rejected=0)
        assert await stored_feed() == NORMAL_FEED
    finally:
        if ready:
            await _cleanup(engine)
        await engine.dispose()
        await storage.close()
