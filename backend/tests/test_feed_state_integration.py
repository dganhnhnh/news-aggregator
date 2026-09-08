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
                .values(
                    next_poll_at=now - timedelta(minutes=10),
                    poll_interval_seconds=1800,
                    cadence_checked_at=now - timedelta(days=1),
                )
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
        due_soon_target = next(t for t in targets if t.feed_url == due_soon)
        assert due_soon_target.poll_interval_seconds == 1800
        assert due_soon_target.cadence_checked_at == now - timedelta(days=1)

        bypassed = await storage.due_feeds(limit=0, feed_urls=[disabled], now=now)
        assert [t.feed_url for t in bypassed] == [disabled]
    finally:
        if ready:
            await _delete_feeds(engine, all_urls)
        await engine.dispose()
        await storage.close()


def test_feed_priority_partitions_instant_and_normal_polling() -> None:
    if not DATABASE_URL:
        pytest.skip("TEST_DATABASE_URL is not set")
    asyncio.run(_test_feed_priority_partitions_instant_and_normal_polling())


async def _test_feed_priority_partitions_instant_and_normal_polling() -> None:
    instant = "https://feed-state.integration.example/instant.xml"
    normal = "https://feed-state.integration.example/normal.xml"
    storage = PostgresRawStorage(DATABASE_URL)
    engine = create_async_engine(DATABASE_URL)
    ready = False
    try:
        await storage.check_ready()
        ready = True
        await _delete_feeds(engine, [instant, normal])
        await storage.seed_feeds(
            [
                FeedSeed(instant, "Instant", priority=0),
                FeedSeed(normal, "Normal", priority=1),
            ]
        )

        instant_targets = await storage.due_feeds(
            limit=0, priority=0, ignore_schedule=True
        )
        normal_targets = await storage.due_feeds(
            limit=0, priority=1, ignore_schedule=True
        )
        assert [item.feed_url for item in instant_targets if item.feed_url == instant] == [
            instant
        ]
        assert normal not in {item.feed_url for item in instant_targets}
        assert instant not in {item.feed_url for item in normal_targets}

        assert await storage.set_feed_priority(normal, 0) is True
        moved = await storage.due_feeds(limit=0, priority=0, ignore_schedule=True)
        assert {item.feed_url for item in moved}.issuperset({instant, normal})
    finally:
        if ready:
            await _delete_feeds(engine, [instant, normal])
        await engine.dispose()
        await storage.close()


def test_operator_feed_create_list_and_update_lifecycle() -> None:
    if not DATABASE_URL:
        pytest.skip("TEST_DATABASE_URL is not set")
    asyncio.run(_test_operator_feed_create_list_and_update_lifecycle())


async def _test_operator_feed_create_list_and_update_lifecycle() -> None:
    feed_url = "https://feed-state.integration.example/operator.xml"
    storage = PostgresRawStorage(DATABASE_URL)
    engine = create_async_engine(DATABASE_URL)
    ready = False
    try:
        await storage.check_ready()
        ready = True
        await _delete_feeds(engine, [feed_url])

        created = await storage.create_feed(
            FeedSeed(feed_url=feed_url, source_label="Operator", priority=1)
        )
        assert created is not None
        assert created.active is True
        assert created.priority == 1
        assert await storage.create_feed(
            FeedSeed(feed_url=feed_url, source_label="Duplicate", priority=0)
        ) is None

        active_items = await storage.list_feeds(active=True)
        assert feed_url in {item.feed_url for item in active_items}
        normal_due = await storage.due_feeds(limit=0, priority=1)
        assert feed_url in {item.feed_url for item in normal_due}

        before_priority_change = datetime.now(timezone.utc)
        updated = await storage.update_feed(feed_url, priority=0, active=False)
        assert updated is not None
        assert updated.priority == 0
        assert updated.active is False
        assert updated.next_poll_at >= before_priority_change

        inactive_items = await storage.list_feeds(active=False)
        assert feed_url in {item.feed_url for item in inactive_items}
        assert feed_url not in {
            item.feed_url for item in await storage.due_feeds(limit=0, priority=0)
        }

        enabled = await storage.update_feed(feed_url, active=True)
        assert enabled is not None
        assert enabled.active is True
        assert feed_url in {
            item.feed_url for item in await storage.due_feeds(limit=0, priority=0)
        }
    finally:
        if ready:
            await _delete_feeds(engine, [feed_url])
        await engine.dispose()
        await storage.close()


def test_crawl_failure_keeps_highest_priority_for_retry() -> None:
    if not DATABASE_URL:
        pytest.skip("TEST_DATABASE_URL is not set")
    asyncio.run(_test_crawl_failure_keeps_highest_priority_for_retry())


async def _test_crawl_failure_keeps_highest_priority_for_retry() -> None:
    url = "https://feed-state.integration.example/instant-article"
    storage = PostgresRawStorage(DATABASE_URL)
    engine = create_async_engine(DATABASE_URL)
    failure = CrawlFailureOccurrence(
        failure_kind="article",
        url=url,
        error_type="Timeout",
        error_message="temporary",
    )
    try:
        await storage.check_ready()
        async with engine.begin() as connection:
            await connection.execute(
                delete(crawl_failures).where(crawl_failures.c.url == url)
            )

        await storage.apply_batch(
            [], [failure], [], received=0, rejected=0, priority=0
        )
        await storage.apply_batch(
            [], [failure], [], received=0, rejected=0, priority=1
        )
        async with engine.connect() as connection:
            priority = await connection.scalar(
                select(crawl_failures.c.priority).where(crawl_failures.c.url == url)
            )
        assert priority == 0
    finally:
        async with engine.begin() as connection:
            await connection.execute(
                delete(crawl_failures).where(crawl_failures.c.url == url)
            )
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
            feed_url,
            next_poll_at=datetime.now(timezone.utc) + timedelta(minutes=30),
            poll_interval_seconds=1800,
            observed_publish_gap_seconds=7200,
            observed_publish_gap_count=8,
        )
        async with engine.connect() as connection:
            row = (
                await connection.execute(
                    select(
                        feed_state.c.consecutive_failures,
                        feed_state.c.poll_interval_seconds,
                        feed_state.c.observed_publish_gap_seconds,
                        feed_state.c.observed_publish_gap_count,
                        feed_state.c.cadence_checked_at,
                    ).where(feed_state.c.feed_url == feed_url)
                )
            ).one()
        assert row.consecutive_failures == 0
        assert row.poll_interval_seconds == 1800
        assert row.observed_publish_gap_seconds == 7200
        assert row.observed_publish_gap_count == 8
        assert row.cadence_checked_at is not None
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


def test_permanent_feed_failure_disables_immediately_and_dead_letters_the_row() -> None:
    if not DATABASE_URL:
        pytest.skip("TEST_DATABASE_URL is not set")
    asyncio.run(_test_permanent_feed_failure_disables_immediately())


async def _test_permanent_feed_failure_disables_immediately() -> None:
    feed_url = "https://feed-state.integration.example/gone.xml"
    storage = PostgresRawStorage(DATABASE_URL)
    engine = create_async_engine(DATABASE_URL)
    ready = False
    try:
        await storage.check_ready()
        ready = True
        await _delete_feeds(engine, [feed_url])
        await _delete_failures(engine, [feed_url])
        await storage.seed_feeds([FeedSeed(feed_url=feed_url, source_label="Gone")])

        outcome = await storage.record_feed_failure(
            feed_url,
            error_message="Client error '404 Not Found'",
            disable_after=10,
            permanent=True,
        )
        # One failure, not ten: a 404 is a verdict, not a streak.
        assert outcome.consecutive_failures == 1
        assert outcome.disabled is True

        await storage.apply_batch(
            [],
            [
                CrawlFailureOccurrence(
                    failure_kind="feed",
                    url=feed_url,
                    error_type="HTTPStatusError",
                    error_message="Client error '404 Not Found'",
                    permanent=True,
                )
            ],
            [],
            received=0,
            rejected=0,
        )

        async with engine.connect() as connection:
            failure = (
                await connection.execute(
                    select(
                        crawl_failures.c.attempt_count,
                        crawl_failures.c.dead_lettered_at,
                    ).where(crawl_failures.c.url == feed_url)
                )
            ).one()
        assert failure.attempt_count == 1
        assert failure.dead_lettered_at is not None
        assert await storage.due_feeds(limit=0, ignore_schedule=True) is not None
    finally:
        if ready:
            await _delete_feeds(engine, [feed_url])
            await _delete_failures(engine, [feed_url])
        await engine.dispose()
        await storage.close()


def test_record_feed_poll_stores_the_resolved_url_for_later_polls() -> None:
    if not DATABASE_URL:
        pytest.skip("TEST_DATABASE_URL is not set")
    asyncio.run(_test_record_feed_poll_stores_the_resolved_url())


async def _test_record_feed_poll_stores_the_resolved_url() -> None:
    feed_url = "https://feed-state.integration.example/moved.xml"
    moved_to = "https://feed-state.integration.example/new/moved.xml"
    storage = PostgresRawStorage(DATABASE_URL)
    engine = create_async_engine(DATABASE_URL)
    ready = False
    try:
        await storage.check_ready()
        ready = True
        await _delete_feeds(engine, [feed_url])
        await storage.seed_feeds([FeedSeed(feed_url=feed_url, source_label="Moved")])

        await storage.record_feed_poll(
            feed_url,
            next_poll_at=datetime.now(timezone.utc) - timedelta(minutes=1),
            resolved_url=moved_to,
        )

        target = next(
            t
            for t in await storage.due_feeds(limit=0, feed_urls=[feed_url])
            if t.feed_url == feed_url
        )
        assert target.resolved_url == moved_to
        # Identity still matches the feed file, so re-seeding cannot duplicate it.
        assert target.fetch_url == moved_to

        # A later poll that reports no redirect leaves the recorded home alone.
        await storage.record_feed_poll(
            feed_url, next_poll_at=datetime.now(timezone.utc) - timedelta(minutes=1)
        )
        target = next(
            t
            for t in await storage.due_feeds(limit=0, feed_urls=[feed_url])
            if t.feed_url == feed_url
        )
        assert target.resolved_url == moved_to
    finally:
        if ready:
            await _delete_feeds(engine, [feed_url])
        await engine.dispose()
        await storage.close()


def test_purge_stale_feed_failures_drops_rows_a_later_success_disproved() -> None:
    if not DATABASE_URL:
        pytest.skip("TEST_DATABASE_URL is not set")
    asyncio.run(_test_purge_stale_feed_failures())


async def _test_purge_stale_feed_failures() -> None:
    recovered = "https://feed-state.integration.example/recovered.xml"
    still_broken = "https://feed-state.integration.example/still-broken.xml"
    storage = PostgresRawStorage(DATABASE_URL)
    engine = create_async_engine(DATABASE_URL)
    ready = False
    urls = [recovered, still_broken]
    try:
        await storage.check_ready()
        ready = True
        await _delete_feeds(engine, urls)
        await _delete_failures(engine, urls)
        await storage.seed_feeds(
            [FeedSeed(feed_url=url, source_label="Feed") for url in urls]
        )
        await storage.apply_batch(
            [],
            [
                CrawlFailureOccurrence(
                    failure_kind="feed",
                    url=url,
                    error_type="ConnectError",
                    error_message="boom",
                )
                for url in urls
            ],
            [],
            received=0,
            rejected=0,
        )

        # Only one of the two has since been polled successfully.
        await storage.record_feed_poll(
            recovered, next_poll_at=datetime.now(timezone.utc) + timedelta(minutes=30)
        )

        # The count covers whatever else the database had to reconcile, so
        # assert on this test's own rows rather than on the total.
        assert await storage.purge_stale_feed_failures() >= 1

        async with engine.connect() as connection:
            remaining = [
                row.url
                for row in await connection.execute(
                    select(crawl_failures.c.url).where(crawl_failures.c.url.in_(urls))
                )
            ]
        assert remaining == [still_broken]

        # Nothing left to reconcile on a second pass.
        assert await storage.purge_stale_feed_failures() == 0
    finally:
        if ready:
            await _delete_feeds(engine, urls)
            await _delete_failures(engine, urls)
        await engine.dispose()
        await storage.close()


async def _delete_feeds(engine, urls: list[str]) -> None:
    async with engine.begin() as connection:
        await connection.execute(delete(feed_state).where(feed_state.c.feed_url.in_(urls)))


async def _delete_failures(engine, urls: list[str]) -> None:
    async with engine.begin() as connection:
        await connection.execute(
            delete(crawl_failures).where(crawl_failures.c.url.in_(urls))
        )
