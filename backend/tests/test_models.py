from datetime import datetime, timedelta, timezone

from models.search_and_crawl_models import CrawlResult, SearchResult
from models.storage_models import (
    FailureQueueStatus,
    QueueStatusSnapshot,
    StageQueueStatus,
    WorkerRunCount,
)


NOW = datetime(2026, 8, 30, 12, 0, tzinfo=timezone.utc)


def _snapshot(**overrides: object) -> QueueStatusSnapshot:
    fields: dict[str, object] = {
        "captured_at": NOW,
        "runs_window_hours": 24,
        "stages": (StageQueueStatus(stage="embed", pending=12, done=400),),
        "failures": (FailureQueueStatus(failure_kind="article", total=3, due_now=1),),
        "runs": (WorkerRunCount(worker="crawl", status="succeeded", count=48),),
    }
    fields.update(overrides)
    return QueueStatusSnapshot(**fields)  # type: ignore[arg-type]


def test_a_healthy_queue_raises_no_alerts() -> None:
    # A backlog on its own is normal: work arrives faster than it drains for
    # minutes at a time. Only a dead end or a dead process is an alert.
    assert _snapshot().alerts() == []


def test_partial_runs_and_pending_backlog_are_not_alerts() -> None:
    snapshot = _snapshot(
        stages=(StageQueueStatus(stage="embed", pending=5_000),),
        runs=(WorkerRunCount(worker="crawl", status="partial", count=9),),
    )

    assert snapshot.alerts() == []


def test_dead_ends_and_dead_processes_each_raise_one_alert() -> None:
    snapshot = _snapshot(
        stages=(StageQueueStatus(stage="embed", pending=1, dead_letter=4),),
        failures=(FailureQueueStatus(failure_kind="feed", total=6, dead_lettered=2),),
        runs=(
            WorkerRunCount(worker="cluster", status="abandoned", count=1),
            WorkerRunCount(worker="embed", status="failed", count=3),
        ),
    )

    alerts = snapshot.alerts()

    assert len(alerts) == 4
    assert snapshot.dead_letter_jobs == 4
    assert snapshot.dead_lettered_failures == 2
    assert (snapshot.abandoned_runs, snapshot.failed_runs) == (1, 3)
    assert any("news-worker replay --stage embed" in alert for alert in alerts)
    assert any("news-worker replay --failures" in alert for alert in alerts)


def test_search_result_recency() -> None:
    recent = SearchResult(
        id="1",
        url="https://example.com/news",
        title="News",
        content_snippet="content",
        published_date=datetime.now(timezone.utc) - timedelta(minutes=5),
    )
    assert recent.is_recent


def test_crawl_result_has_isolated_context() -> None:
    first = CrawlResult(url="https://example.com/1", markdown="one")
    second = CrawlResult(url="https://example.com/2", markdown="two")

    first.context["feed"] = "gold"

    assert second.context == {}
    assert first.content_length == 3


def test_ingest_counts_full_bodies_separately_from_summary_rescues():
    from core.html_content import FEED_CONTENT_KEY, FEED_SUMMARY_KEY
    from models.storage_models import CrawlBatch, CrawlFailureOccurrence, IngestRunStats

    batch = CrawlBatch(results=[
        CrawlResult("https://example.com/crawl", "body"),
        CrawlResult("https://example.com/feed", "body", context={FEED_CONTENT_KEY: True}),
        CrawlResult("https://example.com/summary", "", success=False, context={FEED_SUMMARY_KEY: "dek"}),
    ], failures=[CrawlFailureOccurrence("article", "https://example.com/summary", "Timeout", "slow")])
    stats = IngestRunStats()
    stats.add_batch(batch)
    assert stats.articles_full_content == 2
    assert stats.browser_crawl_succeeded == 1
    assert stats.articles_from_feed_content == 1
    assert stats.articles_summary_only == 1
    assert stats.failure_counts == {"article:Timeout": 1}
