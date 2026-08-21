import asyncio
from datetime import datetime, timedelta, timezone
import os

import pytest
from sqlalchemy import delete, select, update
from sqlalchemy.ext.asyncio import create_async_engine

from adapters.storage.postgres import PostgresRawStorage, retry_delay
from adapters.storage.schema import crawl_failures, feed_state, raw_articles, sources
from models.storage_models import CrawlFailureOccurrence, FeedSeed, RawArticle


DATABASE_URL = os.getenv("TEST_DATABASE_URL")
pytestmark = pytest.mark.integration


def test_seed_feeds_is_idempotent_and_preserves_manual_disable() -> None:
    if not DATABASE_URL:
        pytest.skip("TEST_DATABASE_URL is not set")
    asyncio.run(_test_seed_feeds_is_idempotent_and_preserves_manual_disable())


async def _test_seed_feeds_is_idempotent_and_preserves_manual_disable() -> None:
    feed_a = "https://feed-state.integration.example/a.xml"
    feed_b = "https://feed-state.integration.example/b.xml"
    storage = PostgresRawStorage(DATABASE_URL)
    engine = create_async_engine(DATABASE_URL)
    ready = False
    try:
        await storage.check_ready()
        ready = True
        await _delete_feeds(engine, [feed_a, feed_b])

        seeds = [
            FeedSeed(feed_url=feed_a, source_label="Feed A"),
            FeedSeed(feed_url=feed_b, source_label="Feed B"),
        ]
        assert await storage.seed_feeds(seeds) == 2
        assert await storage.seed_feeds(seeds) == 0

        async with engine.begin() as connection:
            await connection.execute(
                update(feed_state)
                .where(feed_state.c.feed_url == feed_a)
                .values(disabled_at=datetime.now(timezone.utc), disabled_reason="manual")
            )

        assert await storage.seed_feeds(seeds) == 0

        async with engine.connect() as connection:
            row = (
                await connection.execute(
                    select(feed_state.c.disabled_at, feed_state.c.disabled_reason).where(
                        feed_state.c.feed_url == feed_a
                    )
                )
            ).one()
        assert row.disabled_at is not None
        assert row.disabled_reason == "manual"
    finally:
        if ready:
            await _delete_feeds(engine, [feed_a, feed_b])
        await engine.dispose()
        await storage.close()


def test_due_feeds_ordering_filtering_and_explicit_urls_bypass_disabled() -> None:
    if not DATABASE_URL:
        pytest.skip("TEST_DATABASE_URL is not set")
    asyncio.run(_test_due_feeds_ordering_filtering_and_explicit_urls_bypass_disabled())


async def _test_due_feeds_ordering_filtering_and_explicit_urls_bypass_disabled() -> None:
    due_soon = "https://feed-state.integration.example/due-soon.xml"
    due_later = "https://feed-state.integration.example/due-later.xml"
    disabled = "https://feed-state.integration.example/disabled.xml"
    not_due = "https://feed-state.integration.example/not-due.xml"
    all_urls = [due_soon, due_later, disabled, not_due]
    storage = PostgresRawStorage(DATABASE_URL)
    engine = create_async_engine(DATABASE_URL)
    ready = False
    now = datetime.now(timezone.utc)
    try:
        await storage.check_ready()
        ready = True
        await _delete_feeds(engine, all_urls)
        await storage.seed_feeds(
            [FeedSeed(feed_url=url, source_label=url) for url in all_urls]
        )
        async with engine.begin() as connection:
            await connection.execute(
                update(feed_state)
                .where(feed_state.c.feed_url == due_soon)
                .values(next_poll_at=now - timedelta(minutes=10))
            )
            await connection.execute(
                update(feed_state)
                .where(feed_state.c.feed_url == due_later)
                .values(next_poll_at=now - timedelta(minutes=5))
            )
            await connection.execute(
                update(feed_state)
                .where(feed_state.c.feed_url == disabled)
                .values(
                    next_poll_at=now - timedelta(minutes=1),
                    disabled_at=now,
                    disabled_reason="too many failures",
                )
            )
            await connection.execute(
                update(feed_state)
                .where(feed_state.c.feed_url == not_due)
                .values(next_poll_at=now + timedelta(hours=1))
            )

        targets = await storage.due_feeds(limit=0, now=now)
        due_urls = [t.feed_url for t in targets if t.feed_url in all_urls]
        assert due_urls == [due_soon, due_later]

        bypassed = await storage.due_feeds(limit=0, feed_urls=[disabled], now=now)
        assert [t.feed_url for t in bypassed] == [disabled]
    finally:
        if ready:
            await _delete_feeds(engine, all_urls)
        await engine.dispose()
        await storage.close()


def test_record_feed_failure_backoff_and_disable_threshold_then_poll_resets() -> None:
    if not DATABASE_URL:
        pytest.skip("TEST_DATABASE_URL is not set")
    asyncio.run(_test_record_feed_failure_backoff_and_disable_threshold_then_poll_resets())


async def _test_record_feed_failure_backoff_and_disable_threshold_then_poll_resets() -> None:
    feed_url = "https://feed-state.integration.example/flaky.xml"
    storage = PostgresRawStorage(DATABASE_URL)
    engine = create_async_engine(DATABASE_URL)
    ready = False
    try:
        await storage.check_ready()
        ready = True
        await _delete_feeds(engine, [feed_url])
        await storage.seed_feeds([FeedSeed(feed_url=feed_url, source_label="Flaky")])

        disable_after = 3
        for attempt in range(1, disable_after + 1):
            before = datetime.now(timezone.utc)
            outcome = await storage.record_feed_failure(
                feed_url, error_message=f"boom {attempt}", disable_after=disable_after
            )
            assert outcome.consecutive_failures == attempt
            assert outcome.disabled == (attempt >= disable_after)

            async with engine.connect() as connection:
                row = (
                    await connection.execute(
                        select(feed_state.c.next_poll_at, feed_state.c.disabled_at).where(
                            feed_state.c.feed_url == feed_url
                        )
                    )
                ).one()
            # Full jitter means the exact delay is random; only its bounds
            # (`[0, cap]`, per `retry_delay()`) are deterministic.
            cap = retry_delay(attempt, random_uniform=lambda lo, hi: hi)
            assert row.next_poll_at >= before - timedelta(seconds=5)
            assert row.next_poll_at <= before + cap + timedelta(seconds=30)
            if attempt >= disable_after:
                assert row.disabled_at is not None
            else:
                assert row.disabled_at is None

        await storage.record_feed_poll(
            feed_url, next_poll_at=datetime.now(timezone.utc) + timedelta(minutes=30)
        )
        async with engine.connect() as connection:
            row = (
                await connection.execute(
                    select(feed_state.c.consecutive_failures).where(
                        feed_state.c.feed_url == feed_url
                    )
                )
            ).one()
        assert row.consecutive_failures == 0
    finally:
        if ready:
            await _delete_feeds(engine, [feed_url])
        await engine.dispose()
        await storage.close()


def test_known_urls_matches_stored_and_pending_but_not_due_failures() -> None:
    if not DATABASE_URL:
        pytest.skip("TEST_DATABASE_URL is not set")
    asyncio.run(_test_known_urls_matches_stored_and_pending_but_not_due_failures())


async def _test_known_urls_matches_stored_and_pending_but_not_due_failures() -> None:
    stored_url = "https://feed-state.integration.example/stored"
    stored_original = stored_url + "?utm_source=rss"
    pending_failure_url = "https://feed-state.integration.example/pending-failure"
    due_failure_url = "https://feed-state.integration.example/due-failure"
    unrelated_url = "https://feed-state.integration.example/never-seen"
    source_slug = "feed-state.integration.example"

    storage = PostgresRawStorage(DATABASE_URL)
    engine = create_async_engine(DATABASE_URL)
    ready = False
    now = datetime.now(timezone.utc)
    try:
        await storage.check_ready()
        ready = True
        async with engine.begin() as connection:
            await connection.execute(
                delete(raw_articles).where(raw_articles.c.url == stored_url)
            )
            await connection.execute(
                delete(crawl_failures).where(
                    crawl_failures.c.url.in_([pending_failure_url, due_failure_url])
                )
            )
            await connection.execute(delete(sources).where(sources.c.slug == source_slug))

        await storage.apply_batch(
            [
                RawArticle(
                    url=stored_url,
                    url_original=stored_original,
                    title="Stored",
                    source="Example",
                    lang="en",
                    content="body",
                    content_hash=b"1" * 32,
                    published_at=now,
                    crawled_at=now,
                )
            ],
            [
                CrawlFailureOccurrence(
                    failure_kind="article",
                    url=pending_failure_url,
                    error_type="TimeoutError",
                    error_message="timeout",
                ),
                CrawlFailureOccurrence(
                    failure_kind="article",
                    url=due_failure_url,
                    error_type="TimeoutError",
                    error_message="timeout",
                ),
            ],
            [],
            received=3,
            rejected=0,
        )
        # apply_batch always sets next_retry_at in the future; force one
        # failure back into "already due" so the filter has both cases.
        async with engine.begin() as connection:
            await connection.execute(
                update(crawl_failures)
                .where(crawl_failures.c.url == due_failure_url)
                .values(next_retry_at=now - timedelta(minutes=1))
            )

        candidates = [
            stored_url,
            stored_original,
            pending_failure_url,
            due_failure_url,
            unrelated_url,
        ]
        known = await storage.known_urls(candidates, now=now)

        assert known == {stored_url, stored_original, pending_failure_url}
    finally:
        if ready:
            async with engine.begin() as connection:
                await connection.execute(
                    delete(raw_articles).where(raw_articles.c.url == stored_url)
                )
                await connection.execute(
                    delete(crawl_failures).where(
                        crawl_failures.c.url.in_([pending_failure_url, due_failure_url])
                    )
                )
                await connection.execute(
                    delete(sources).where(sources.c.slug == source_slug)
                )
        await engine.dispose()
        await storage.close()


def test_disable_feed_and_enable_feed_toggle_disabled_state() -> None:
    if not DATABASE_URL:
        pytest.skip("TEST_DATABASE_URL is not set")
    asyncio.run(_test_disable_feed_and_enable_feed_toggle_disabled_state())


async def _test_disable_feed_and_enable_feed_toggle_disabled_state() -> None:
    feed_url = "https://feed-state.integration.example/toggle.xml"
    storage = PostgresRawStorage(DATABASE_URL)
    engine = create_async_engine(DATABASE_URL)
    ready = False
    try:
        await storage.check_ready()
        ready = True
        await _delete_feeds(engine, [feed_url])
        await storage.seed_feeds([FeedSeed(feed_url=feed_url, source_label="Toggle")])

        assert await storage.disable_feed(feed_url, reason="manual pause") is True
        async with engine.connect() as connection:
            row = (
                await connection.execute(
                    select(feed_state.c.disabled_at, feed_state.c.disabled_reason).where(
                        feed_state.c.feed_url == feed_url
                    )
                )
            ).one()
        assert row.disabled_at is not None
        assert row.disabled_reason == "manual pause"

        assert await storage.enable_feed(feed_url) is True
        async with engine.connect() as connection:
            row = (
                await connection.execute(
                    select(feed_state.c.disabled_at, feed_state.c.disabled_reason).where(
                        feed_state.c.feed_url == feed_url
                    )
                )
            ).one()
        assert row.disabled_at is None
        assert row.disabled_reason is None

        assert await storage.enable_feed("https://not-seeded.example/none.xml") is False
    finally:
        if ready:
            await _delete_feeds(engine, [feed_url])
        await engine.dispose()
        await storage.close()


async def _delete_feeds(engine, urls: list[str]) -> None:
    async with engine.begin() as connection:
        await connection.execute(delete(feed_state).where(feed_state.c.feed_url.in_(urls)))
