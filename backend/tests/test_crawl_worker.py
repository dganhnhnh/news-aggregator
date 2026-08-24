import argparse
import asyncio
from collections.abc import AsyncIterator, Sequence
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest

import workers.crawl as crawl_module
from models.discovery_models import DiscoveredEntry, FeedPoll
from models.storage_models import (
    CrawlBatch,
    CrawlFailureOccurrence,
    CrawlResult,
    FeedFailureOutcome,
    FeedPollTarget,
    StorageWriteSummary,
)
from workers.base import RunContext
from workers.crawl import CrawlWorker
from workers.window import RunWindow


NOW = datetime(2026, 8, 19, 12, 0, tzinfo=timezone.utc)


def _window(*, explicit: bool = False, days: int = 3) -> RunWindow:
    return RunWindow(start=NOW - timedelta(days=days), end=NOW, explicit=explicit)


def _args(**overrides: Any) -> argparse.Namespace:
    defaults: dict[str, Any] = {
        "feed_path": None,
        "feeds": [],
        "ignore_schedule": False,
        "max_feeds": 0,
        "max_articles_per_feed": 0,
        "max_concurrent_feeds": 2,
        "max_concurrent_crawls": 2,
        "crawl_batch_size": 10,
        "max_crawls_per_host": 1,
        "crawl_delay_per_host": 0.0,
        "feed_content_min_chars": 1200,
        "user_agent": "news-radar/1.0",
        "poll_interval": 1800,
        "disable_after_failures": 3,
        "max_retries_per_run": 50,
        "emit_jsonl": False,
    }
    defaults.update(overrides)
    return argparse.Namespace(**defaults)


def _target(url: str = "https://feed.example/a.xml") -> FeedPollTarget:
    return FeedPollTarget(
        feed_url=url, source_label="Feed A", etag=None, last_modified=None
    )


def _entry(
    *,
    feed_url: str = "https://feed.example/a.xml",
    entry_url: str = "https://article.example/1",
    published_at: datetime | None = NOW,
) -> DiscoveredEntry:
    return DiscoveredEntry(
        feed_url=feed_url,
        query_label="Feed A",
        feed_title="Feed A",
        entry_url=entry_url,
        title="Title",
        source="Feed A",
        published_at=published_at,
    )


class FakeStorage:
    """Records call order; only implements what CrawlWorker.execute touches."""

    def __init__(self, *, disable_after_failures_map: dict[str, int] | None = None) -> None:
        self.calls: list[Any] = []
        self._due_feeds: list[FeedPollTarget] = []
        self._known: set[str] = set()
        self._consecutive_failures: dict[str, int] = {}
        self._disabled: set[str] = set()
        self.resolved_urls: dict[str, str] = {}
        self.stale_failures_purged = 0

    def set_due_feeds(self, targets: list[FeedPollTarget]) -> None:
        self._due_feeds = targets

    def set_known_urls(self, urls: set[str]) -> None:
        self._known = urls

    async def due_failures(self, limit: int) -> list:
        return []

    async def purge_stale_feed_failures(self) -> int:
        self.calls.append(("purge_stale_feed_failures",))
        return self.stale_failures_purged

    async def seed_feeds(self, seeds) -> int:
        return 0

    async def due_feeds(
        self,
        *,
        limit: int,
        feed_urls: Sequence[str] | None = None,
        ignore_schedule: bool = False,
    ) -> list[FeedPollTarget]:
        self.calls.append(("due_feeds", tuple(feed_urls) if feed_urls else None))
        return [t for t in self._due_feeds if t.feed_url not in self._disabled]

    async def known_urls(self, candidates: Sequence[str]) -> set[str]:
        self.calls.append(("known_urls", tuple(candidates)))
        return {url for url in candidates if url in self._known}

    async def record_feed_poll(
        self,
        feed_url: str,
        *,
        next_poll_at: datetime,
        not_modified: bool = False,
        etag: str | None = None,
        last_modified: str | None = None,
        last_entry_published_at: datetime | None = None,
        resolved_url: str | None = None,
    ) -> None:
        self.calls.append(
            ("record_feed_poll", feed_url, not_modified, next_poll_at, resolved_url)
        )
        self._consecutive_failures[feed_url] = 0

    async def record_feed_failure(
        self,
        feed_url: str,
        *,
        error_message: str,
        disable_after: int,
        permanent: bool = False,
        resolved_url: str | None = None,
    ) -> FeedFailureOutcome:
        if resolved_url:
            self.resolved_urls[feed_url] = resolved_url
        count = self._consecutive_failures.get(feed_url, 0) + 1
        self._consecutive_failures[feed_url] = count
        disabled = permanent or count >= disable_after
        if disabled:
            self._disabled.add(feed_url)
        self.calls.append(("record_feed_failure", feed_url, count, disabled))
        return FeedFailureOutcome(consecutive_failures=count, disabled=disabled)

    async def apply_batch(
        self,
        articles,
        failures,
        resolved_failures,
        *,
        received: int,
        rejected: int,
    ) -> StorageWriteSummary:
        self.calls.append(("apply_batch", len(articles), len(failures)))
        return StorageWriteSummary(received, 0, len(articles), 0, rejected)

    async def update_run(self, run_id: int, stats: Any) -> None:
        self.calls.append(("checkpoint", stats.articles_written))


class FakeDiscovery:
    def __init__(self, polls: list[FeedPoll], *, crawl_batches: list[CrawlBatch] | None = None) -> None:
        self._polls = polls
        self._crawl_batches = crawl_batches or []
        self.crawl_entries_calls: list[list[DiscoveredEntry]] = []

    async def poll(self, targets: Sequence[FeedPollTarget]) -> AsyncIterator[FeedPoll]:
        for poll in self._polls:
            yield poll

    async def crawl_entries(self, entries: Sequence[DiscoveredEntry]) -> CrawlBatch:
        self.crawl_entries_calls.append(list(entries))
        if self._crawl_batches:
            return self._crawl_batches.pop(0)
        return CrawlBatch()


def _run(
    storage: FakeStorage,
    discovery: FakeDiscovery,
    args: argparse.Namespace,
    *,
    window: RunWindow | None = None,
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setattr(crawl_module, "RssDiscovery", lambda searcher: discovery)
    ctx = RunContext(
        run_id=1,
        window=window or _window(),
        shutdown=asyncio.Event(),
        args=args,
        storage=storage,
    )
    return asyncio.run(CrawlWorker().execute(ctx))


def test_not_modified_feed_skips_crawl_and_advances_schedule(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = _target()
    storage = FakeStorage()
    storage.set_due_feeds([target])
    discovery = FakeDiscovery(
        [FeedPoll(target=target, status="not_modified", etag="W/abc")]
    )

    outcome = _run(storage, discovery, _args(), monkeypatch=monkeypatch)

    assert discovery.crawl_entries_calls == []
    assert outcome.stats.feeds_not_modified == 1
    poll_calls = [c for c in storage.calls if c[0] == "record_feed_poll"]
    assert len(poll_calls) == 1
    _, feed_url, not_modified, next_poll_at, _resolved = poll_calls[0]
    assert feed_url == target.feed_url
    assert not_modified is True
    assert next_poll_at > NOW


def test_undated_entry_passes_implicit_window(monkeypatch: pytest.MonkeyPatch) -> None:
    target = _target()
    undated = _entry(entry_url="https://article.example/undated", published_at=None)
    storage = FakeStorage()
    storage.set_due_feeds([target])
    discovery = FakeDiscovery(
        [FeedPoll(target=target, status="fetched", entries=[undated])]
    )

    outcome = _run(
        storage, discovery, _args(), window=_window(explicit=False), monkeypatch=monkeypatch
    )

    assert outcome.stats.entries_skipped_undated == 1
    assert outcome.stats.entries_skipped_out_of_window == 0
    assert discovery.crawl_entries_calls == [[undated]]


def test_undated_entry_excluded_from_explicit_window(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = _target()
    undated = _entry(entry_url="https://article.example/undated", published_at=None)
    storage = FakeStorage()
    storage.set_due_feeds([target])
    discovery = FakeDiscovery(
        [FeedPoll(target=target, status="fetched", entries=[undated])]
    )

    outcome = _run(
        storage, discovery, _args(), window=_window(explicit=True), monkeypatch=monkeypatch
    )

    assert outcome.stats.entries_skipped_undated == 1
    assert outcome.stats.entries_skipped_out_of_window == 1
    assert discovery.crawl_entries_calls == []


def test_entry_outside_window_is_filtered(monkeypatch: pytest.MonkeyPatch) -> None:
    target = _target()
    too_old = _entry(
        entry_url="https://article.example/old",
        published_at=NOW - timedelta(days=30),
    )
    storage = FakeStorage()
    storage.set_due_feeds([target])
    discovery = FakeDiscovery(
        [FeedPoll(target=target, status="fetched", entries=[too_old])]
    )

    outcome = _run(storage, discovery, _args(), monkeypatch=monkeypatch)

    assert outcome.stats.entries_skipped_out_of_window == 1
    assert discovery.crawl_entries_calls == []


def test_known_url_never_reaches_crawl_entries(monkeypatch: pytest.MonkeyPatch) -> None:
    target = _target()
    known = _entry(entry_url="https://article.example/known")
    fresh = _entry(entry_url="https://article.example/fresh")
    storage = FakeStorage()
    storage.set_due_feeds([target])
    storage.set_known_urls({"https://article.example/known"})
    discovery = FakeDiscovery(
        [FeedPoll(target=target, status="fetched", entries=[known, fresh])]
    )

    outcome = _run(storage, discovery, _args(), monkeypatch=monkeypatch)

    assert outcome.stats.entries_skipped_known == 1
    assert len(discovery.crawl_entries_calls) == 1
    crawled_urls = {entry.entry_url for entry in discovery.crawl_entries_calls[0]}
    assert crawled_urls == {"https://article.example/fresh"}


def test_feed_failure_escalates_to_disabled_after_n_failures(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = _target()
    storage = FakeStorage()
    storage.set_due_feeds([target])
    error = CrawlFailureOccurrence(
        failure_kind="feed",
        url=target.feed_url,
        error_type="ConnectError",
        error_message="boom",
    )
    args = _args(disable_after_failures=3)

    # Two failing runs: not yet disabled.
    for _ in range(2):
        discovery = FakeDiscovery(
            [FeedPoll(target=target, status="failed", error=error)]
        )
        outcome = _run(storage, discovery, args, monkeypatch=monkeypatch)
        assert outcome.stats.feeds_disabled == 0

    # Third failing run crosses disable_after_failures.
    discovery = FakeDiscovery([FeedPoll(target=target, status="failed", error=error)])
    outcome = _run(storage, discovery, args, monkeypatch=monkeypatch)
    assert outcome.stats.feeds_disabled == 1

    # The feed no longer comes back from due_feeds.
    remaining = asyncio.run(storage.due_feeds(limit=0))
    assert remaining == []


def test_permanently_missing_feed_is_disabled_on_its_first_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = _target()
    storage = FakeStorage()
    storage.set_due_feeds([target])
    discovery = FakeDiscovery(
        [
            FeedPoll(
                target=target,
                status="failed",
                error=CrawlFailureOccurrence(
                    failure_kind="feed",
                    url=target.feed_url,
                    error_type="HTTPStatusError",
                    error_message="Client error '404 Not Found'",
                    permanent=True,
                ),
            )
        ]
    )

    outcome = _run(
        storage, discovery, _args(disable_after_failures=10), monkeypatch=monkeypatch
    )

    assert outcome.stats.feeds_disabled == 1
    assert asyncio.run(storage.due_feeds(limit=0)) == []


def test_a_redirect_proved_by_a_failed_poll_is_still_persisted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # 301 to the new URL, 403 once we get there: the feed is still moved, and
    # the next run should start from where it moved to.
    target = _target()
    storage = FakeStorage()
    storage.set_due_feeds([target])
    discovery = FakeDiscovery(
        [
            FeedPoll(
                target=target,
                status="failed",
                error=CrawlFailureOccurrence(
                    failure_kind="feed",
                    url=target.feed_url,
                    error_type="HTTPStatusError",
                    error_message="Client error '403 Forbidden'",
                    permanent=False,
                ),
                resolved_url="https://example.com/moved.xml",
            )
        ]
    )

    outcome = _run(storage, discovery, _args(), monkeypatch=monkeypatch)

    assert storage.resolved_urls[target.feed_url] == "https://example.com/moved.xml"
    assert outcome.stats.feeds_redirected == 1
    assert outcome.stats.feeds_failed == 1


def test_permanent_redirect_is_recorded_with_the_poll(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = _target()
    storage = FakeStorage()
    storage.set_due_feeds([target])
    discovery = FakeDiscovery(
        [
            FeedPoll(
                target=target,
                status="fetched",
                resolved_url="https://feed.example/moved.xml",
            )
        ]
    )

    outcome = _run(storage, discovery, _args(), monkeypatch=monkeypatch)

    assert outcome.stats.feeds_redirected == 1
    poll_calls = [c for c in storage.calls if c[0] == "record_feed_poll"]
    assert poll_calls[0][-1] == "https://feed.example/moved.xml"


def test_stale_feed_failures_are_purged_before_the_run(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    storage = FakeStorage()
    storage.stale_failures_purged = 4
    discovery = FakeDiscovery([])

    outcome = _run(storage, discovery, _args(), monkeypatch=monkeypatch)

    assert outcome.stats.stale_failures_purged == 4
    assert storage.calls[0][0] == "purge_stale_feed_failures"


def test_non_article_entries_skipped_by_the_feed_are_reported(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = _target()
    storage = FakeStorage()
    storage.set_due_feeds([target])
    discovery = FakeDiscovery(
        [
            FeedPoll(
                target=target,
                status="fetched",
                entries=[_entry()],
                entries_skipped_non_article=3,
            )
        ]
    )

    outcome = _run(storage, discovery, _args(), monkeypatch=monkeypatch)

    assert outcome.stats.entries_skipped_non_article == 3


def test_record_feed_poll_runs_after_the_last_crawl_chunk_is_persisted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = _target()
    first = _entry(entry_url="https://article.example/1")
    second = _entry(entry_url="https://article.example/2")
    storage = FakeStorage()
    storage.set_due_feeds([target])
    discovery = FakeDiscovery(
        [FeedPoll(target=target, status="fetched", entries=[first, second])]
    )

    _run(storage, discovery, _args(crawl_batch_size=1), monkeypatch=monkeypatch)

    assert len(discovery.crawl_entries_calls) == 2
    call_kinds = [call[0] for call in storage.calls]
    apply_indexes = [i for i, kind in enumerate(call_kinds) if kind == "apply_batch"]
    poll_indexes = [i for i, kind in enumerate(call_kinds) if kind == "record_feed_poll"]
    assert len(apply_indexes) >= 2
    assert len(poll_indexes) == 1
    assert max(apply_indexes) < poll_indexes[0]
