"""Unit tests for the Prometheus exporter; no database required."""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

from fastapi.testclient import TestClient

from adapters.storage import StorageError
from admin_api.deps import get_metrics_collector
from admin_api.main import create_app
from admin_api.metrics import MetricsCollector, build_families, render
from models.storage_models import (
    CorpusMetrics,
    FailureQueueStatus,
    FailureTypeCount,
    FeedMetrics,
    MetricsSnapshot,
    StageJobMetrics,
    WorkerHealthMetrics,
    WorkerRunCount,
    WorkerStatDelta,
)


NOW = datetime(2026, 9, 16, 12, 0, tzinfo=timezone.utc)


def _snapshot(**overrides) -> MetricsSnapshot:
    defaults = dict(
        captured_at=NOW,
        recent_window_hours=24,
        feed_overdue_grace_seconds=300,
        stages=(
            StageJobMetrics(
                stage="embed",
                pending=3,
                running=1,
                done=90,
                dead_letter=2,
                expired_leases=1,
                oldest_pending_age_seconds=42.5,
            ),
        ),
        workers=(
            WorkerHealthMetrics(
                worker="crawl",
                last_success_at=NOW - timedelta(minutes=5),
                last_run_duration_seconds=12.0,
                running=1,
                oldest_heartbeat_age_seconds=7.0,
            ),
        ),
        runs=(WorkerRunCount("crawl", "succeeded", 10),),
        failures=(
            FailureQueueStatus(
                failure_kind="article",
                total=5,
                dead_lettered=1,
                due_now=2,
                max_attempts=4,
            ),
        ),
        failure_types=(FailureTypeCount("BlockedPage", 4, 1),),
        feeds=FeedMetrics(
            total=10, active=9, disabled=1, due=2, overdue=1, max_consecutive_failures=3
        ),
        corpus=CorpusMetrics(
            articles_by_content_source=(("crawl", 100), ("feed_summary", 20)),
            embeddings_by_text_kind=(("content", 90),),
            clusters_by_status=(("open", 7),),
            cluster_members=80,
            classified_articles=(("assigned", 60), ("uncategorized", 30)),
            recent_articles=25,
            recent_missing=(("embedding", 2), ("classification", 3), ("cluster", 4)),
        ),
    )
    return MetricsSnapshot(**{**defaults, **overrides})


def _samples(snapshot: MetricsSnapshot, **kwargs) -> dict[str, float]:
    families = build_families(snapshot, **kwargs)
    return {
        sample.name + str(sorted(sample.labels.items())): sample.value
        for family in families
        for sample in family.samples
    }


def test_job_gauges_keep_every_state_even_at_zero() -> None:
    snapshot = _snapshot(
        stages=(StageJobMetrics(stage="classify", pending=0, running=0),)
    )
    samples = _samples(snapshot)

    assert samples["news_radar_jobs[('stage', 'classify'), ('state', 'pending')]"] == 0.0
    assert (
        samples["news_radar_jobs[('stage', 'classify'), ('state', 'dead_letter')]"]
        == 0.0
    )
    # No pending job means no age at all; a zero would claim one is waiting.
    assert not any(
        name.startswith("news_radar_job_oldest_pending_age_seconds")
        for name in samples
    )


def test_worker_stats_render_as_counters() -> None:
    text = render(
        build_families(_snapshot(), {("crawl", "articles_persisted"): 1234.0})
    ).decode()

    assert "# TYPE news_radar_worker_stat_total counter" in text
    assert (
        'news_radar_worker_stat_total{stat="articles_persisted",worker="crawl"} 1234.0'
        in text
    )
    assert "# TYPE news_radar_worker_runs_total counter" in text


def test_failure_counts_split_open_from_dead_lettered() -> None:
    samples = _samples(_snapshot())

    assert (
        samples["news_radar_crawl_failures[('failure_kind', 'article'), ('state', 'open')]"]
        == 4.0
    )
    assert (
        samples[
            "news_radar_crawl_failures"
            "[('failure_kind', 'article'), ('state', 'dead_lettered')]"
        ]
        == 1.0
    )


def test_idle_worker_reports_no_heartbeat_age() -> None:
    snapshot = _snapshot(
        workers=(
            WorkerHealthMetrics(worker="embed", last_success_at=NOW, running=0),
        )
    )
    samples = _samples(snapshot)

    assert samples["news_radar_worker_running[('worker', 'embed')]"] == 0.0
    assert not any(
        name.startswith("news_radar_worker_running_heartbeat_age_seconds")
        for name in samples
    )


class FakeMetricsStorage:
    def __init__(self, deltas: list[tuple[WorkerStatDelta, ...]] | None = None) -> None:
        self.snapshot_calls = 0
        self.watermarks: list[dict[str, int]] = []
        self._deltas = deltas or [()]

    async def metrics_snapshot(self, **kwargs) -> MetricsSnapshot:
        self.snapshot_calls += 1
        return _snapshot()

    async def worker_stat_totals(self, after):
        self.watermarks.append(dict(after))
        index = min(len(self.watermarks) - 1, len(self._deltas) - 1)
        return self._deltas[index]


class FrozenClock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now


def test_collector_serves_the_cache_until_it_expires() -> None:
    storage = FakeMetricsStorage()
    clock = FrozenClock()
    collector = MetricsCollector(storage, cache_seconds=15.0, clock=clock)

    async def scenario() -> None:
        await collector.expose()
        await collector.expose()
        assert storage.snapshot_calls == 1
        clock.now = 20.0
        await collector.expose()

    asyncio.run(scenario())
    assert storage.snapshot_calls == 2


def test_collector_accumulates_stats_and_advances_watermarks() -> None:
    storage = FakeMetricsStorage(
        [
            (WorkerStatDelta("crawl", "articles_persisted", 10.0, 7),),
            (WorkerStatDelta("crawl", "articles_persisted", 4.0, 9),),
        ]
    )
    clock = FrozenClock()
    collector = MetricsCollector(storage, cache_seconds=0.0, clock=clock)

    async def scenario() -> tuple[str, str]:
        first = (await collector.expose()).decode()
        clock.now = 1.0
        return first, (await collector.expose()).decode()

    first, second = asyncio.run(scenario())

    assert "articles_persisted\",worker=\"crawl\"} 10.0" in first
    # Counters may only climb: the second collection adds its delta rather
    # than replacing the total, which is what a restart-free counter needs.
    assert "articles_persisted\",worker=\"crawl\"} 14.0" in second
    assert storage.watermarks == [{}, {"crawl": 7}]


class FakeCollector:
    def __init__(self, error: Exception | None = None) -> None:
        self.error = error

    async def expose(self) -> bytes:
        if self.error is not None:
            raise self.error
        return b"news_radar_recent_articles 25.0\n"


def _client(collector: FakeCollector) -> TestClient:
    app = create_app()
    app.dependency_overrides[get_metrics_collector] = lambda: collector
    return TestClient(app)


def test_metrics_endpoint_serves_the_prometheus_text_format() -> None:
    response = _client(FakeCollector()).get("/metrics")

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/plain")
    assert "news_radar_recent_articles" in response.text


def test_metrics_endpoint_fails_the_scrape_when_the_database_is_down() -> None:
    response = _client(FakeCollector(StorageError("connection refused"))).get("/metrics")

    assert response.status_code == 503
    assert "connection refused" in response.json()["detail"]


def test_metrics_endpoint_stays_out_of_the_openapi_schema() -> None:
    schema = _client(FakeCollector()).get("/openapi.json").json()

    assert "/metrics" not in schema["paths"]
