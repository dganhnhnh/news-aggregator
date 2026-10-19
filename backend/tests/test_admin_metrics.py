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
    Distribution,
    FailureQueueStatus,
    FailureTypeCount,
    FeedMetrics,
    IngestionLagDelta,
    LagBucketCounts,
    MetricsSnapshot,
    QualityMetrics,
    StageJobMetrics,
    WorkerHealthMetrics,
    WorkerRunCount,
    WorkerStatObservation,
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


# Deliberately short and round: the assertions below are about bucket
# arithmetic, not about the production tiers.
BOUNDS = (60.0, 300.0, 900.0)


def _quality(**overrides) -> QualityMetrics:
    defaults = dict(
        recent_window_hours=24,
        recent_articles_by_content_source=(("crawl", 20), ("feed_summary", 5)),
        pipeline_lag=(
            (
                "embed",
                Distribution(
                    bounds=BOUNDS, counts=(1, 4, 6), total=8, sum_value=9000.0
                ),
            ),
        ),
        cluster_sizes=Distribution(
            bounds=(1.0, 2.0), counts=(30, 45), total=50, sum_value=120.0
        ),
        cluster_sources=Distribution(
            bounds=(1.0, 2.0), counts=(40, 48), total=50, sum_value=70.0
        ),
        stale_classifications=12,
        stale_prototypes=1,
        active_topics=10,
        feed_poll_intervals=Distribution(
            bounds=(300.0, 1800.0), counts=(2, 6), total=9, sum_value=54000.0
        ),
        feeds_without_interval=3,
    )
    return QualityMetrics(**{**defaults, **overrides})


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


def test_partial_completion_and_progress_are_separate_from_strict_success():
    samples = _samples(_snapshot(workers=(WorkerHealthMetrics(
        worker="crawl", last_completed_at=NOW, last_progress_at=NOW - timedelta(seconds=5),
    ),)))
    assert samples["news_radar_worker_last_completed_timestamp_seconds[('worker', 'crawl')]"] == NOW.timestamp()
    assert samples["news_radar_worker_last_progress_timestamp_seconds[('worker', 'crawl')]"] == NOW.timestamp() - 5
    assert not any(key.startswith("news_radar_worker_last_success_timestamp") for key in samples)


def test_live_checkpoints_and_late_finishes_are_counted_once():
    storage = FakeMetricsStorage([
        (WorkerStatObservation("crawl", 1, {"crawl_failed": 2, "failure_counts": {"article:Timeout": 2}}, False),),
        (
            WorkerStatObservation("crawl", 1, {"crawl_failed": 3, "failure_counts": {"article:Timeout": 3}}, False),
            WorkerStatObservation("crawl", 2, {"crawl_failed": 4}, True),
        ),
        (WorkerStatObservation("crawl", 1, {"crawl_failed": 5, "failure_counts": {"article:Timeout": 5}}, True),),
        (),
    ])
    collector = MetricsCollector(storage, cache_seconds=0)

    async def scenario():
        texts = [(await collector.expose()).decode() for _ in range(4)]
        assert collector._active_stats == {}
        return texts

    texts = asyncio.run(scenario())
    for text, count in zip(texts, (2, 7, 9, 9), strict=True):
        assert f'news_radar_worker_stat_total{{stat="crawl_failed",worker="crawl"}} {count}.0' in text
    assert 'news_radar_worker_errors_total{error_type="Timeout",failure_kind="article",worker="crawl"} 5.0' in texts[-1]
    assert storage.active_run_ids == [(), (1,), (1,), ()]


def test_counter_high_water_survives_a_decreased_checkpoint():
    collector = MetricsCollector(FakeMetricsStorage())
    for count in (5, 2, 6):
        collector._accumulate([WorkerStatObservation("crawl", 1, {"crawl_failed": count}, False)])
    assert collector._totals[("crawl", "crawl_failed")] == 6


def test_ingestion_lag_renders_as_a_counter_histogram() -> None:
    ingestion = IngestionLagDelta(
        bounds=BOUNDS,
        through_article_id=400,
        series=(LagBucketCounts("crawl", counts=(2, 5, 9), total_seconds=4000.0, count=11),),
        undated=3,
        ahead_of_publication=1,
    )
    text = render(build_families(_snapshot(), ingestion=ingestion)).decode()

    assert "# TYPE news_radar_ingestion_lag_seconds histogram" in text
    assert 'news_radar_ingestion_lag_seconds_bucket{content_source="crawl",le="900.0"} 9.0' in text
    # Two articles took longer than the last bound, so `+Inf` exceeds it and
    # equals the count -- the bucket every histogram is required to carry.
    assert 'news_radar_ingestion_lag_seconds_bucket{content_source="crawl",le="+Inf"} 11.0' in text
    assert 'news_radar_ingestion_lag_seconds_count{content_source="crawl"} 11.0' in text
    # Undated articles are outside the histogram entirely: with no publication
    # date there is no lag to bucket, and zero would be a lie.
    assert "news_radar_articles_undated_total 3.0" in text
    assert "news_radar_articles_published_ahead_total 1.0" in text


def test_ingestion_buckets_accumulate_and_hold_sources_absent_from_a_delta() -> None:
    storage = FakeMetricsStorage(
        lag_deltas=[
            # The corpus as it stood at startup. It sets the mark and is then
            # discarded: this histogram counts what arrives from here on.
            IngestionLagDelta(
                bounds=BOUNDS,
                through_article_id=5,
                series=(LagBucketCounts("crawl", (900, 900, 900), 9e6, 900),),
                undated=40,
            ),
            IngestionLagDelta(
                bounds=BOUNDS,
                through_article_id=10,
                series=(
                    LagBucketCounts("crawl", (1, 2, 3), 500.0, 4),
                    LagBucketCounts("feed_summary", (0, 1, 1), 700.0, 2),
                ),
                undated=1,
            ),
            IngestionLagDelta(
                bounds=BOUNDS,
                through_article_id=17,
                series=(LagBucketCounts("crawl", (2, 2, 5), 100.0, 5),),
                undated=2,
            ),
        ]
    )
    clock = FrozenClock()
    collector = MetricsCollector(storage, cache_seconds=0.0, clock=clock)

    async def scenario() -> tuple[str, str, str]:
        expositions = []
        for tick in range(3):
            clock.now = float(tick)
            expositions.append((await collector.expose()).decode())
        return tuple(expositions)

    primed, first, second = asyncio.run(scenario())

    # Nothing has been ingested while this process was up, so there is no
    # histogram yet -- not 900 observations of historical lag.
    assert "news_radar_ingestion_lag_seconds_bucket" not in primed
    assert "news_radar_articles_undated_total 0.0" in primed

    assert 'news_radar_ingestion_lag_seconds_count{content_source="crawl"} 4.0' in first
    assert 'news_radar_ingestion_lag_seconds_count{content_source="crawl"} 9.0' in second
    assert 'news_radar_ingestion_lag_seconds_bucket{content_source="crawl",le="60.0"} 3.0' in second
    assert "news_radar_articles_undated_total 3.0" in second
    # The second delta brought no feed_summary rows. Its totals must survive
    # untouched: a bucket that fell back would read as a counter reset.
    assert (
        'news_radar_ingestion_lag_seconds_count{content_source="feed_summary"} 2.0'
        in second
    )
    # Each article is measured once, so the mark carries between scrapes.
    assert storage.lag_watermarks == [0, 5, 10]


def test_current_state_distributions_render_as_gauge_histograms() -> None:
    text = render(build_families(_snapshot(), quality=_quality())).decode()

    assert 'news_radar_cluster_size_bucket{le="1.0"} 30.0' in text
    assert 'news_radar_cluster_size_bucket{le="+Inf"} 50.0' in text
    assert 'news_radar_pipeline_lag_seconds_bucket{le="300.0",stage="embed"} 4.0' in text
    # The 0.0.4 text format has no gauge histogram, so the totals come out as
    # `_gcount`/`_gsum` gauges and there is deliberately no `_count`/`_sum`.
    # That absence is the signal: these buckets describe rows that exist now,
    # they fall when a cluster is merged, and nothing may `rate()` them.
    assert "news_radar_cluster_size_gcount 50.0" in text
    assert "news_radar_cluster_size_gsum 120.0" in text
    assert 'news_radar_pipeline_lag_seconds_gcount{stage="embed"} 8.0' in text
    assert "news_radar_cluster_size_count " not in text
    assert "news_radar_pipeline_lag_seconds_sum{" not in text


def test_quality_gauges_name_what_needs_a_rebuild() -> None:
    samples = _samples(_snapshot(), quality=_quality())

    assert samples["news_radar_stale_classifications[]"] == 12.0
    assert samples["news_radar_stale_prototypes[]"] == 1.0
    assert samples["news_radar_active_topics[]"] == 10.0
    assert samples["news_radar_feeds_without_poll_interval[]"] == 3.0
    assert (
        samples[
            "news_radar_recent_articles_by_content_source"
            "[('content_source', 'feed_summary')]"
        ]
        == 5.0
    )


def test_quality_families_are_absent_when_nothing_was_collected() -> None:
    samples = _samples(_snapshot())

    assert not any(name.startswith("news_radar_cluster_size") for name in samples)
    assert not any(name.startswith("news_radar_ingestion_lag") for name in samples)


class FakeMetricsStorage:
    def __init__(
        self,
        deltas: list[tuple[WorkerStatObservation, ...]] | None = None,
        lag_deltas: list[IngestionLagDelta] | None = None,
    ) -> None:
        self.snapshot_calls = 0
        self.watermarks: list[dict[str, int]] = []
        self.active_run_ids: list[tuple[int, ...]] = []
        self.lag_watermarks: list[int] = []
        self._deltas = deltas or [()]
        self._lag_deltas = lag_deltas or [IngestionLagDelta(bounds=BOUNDS)]

    async def metrics_snapshot(self, **kwargs) -> MetricsSnapshot:
        self.snapshot_calls += 1
        return _snapshot()

    async def worker_stat_observations(self, after, active_run_ids=()):
        self.watermarks.append(dict(after))
        self.active_run_ids.append(tuple(active_run_ids))
        index = min(len(self.watermarks) - 1, len(self._deltas) - 1)
        return self._deltas[index]

    async def ingestion_lag_since(self, after_article_id):
        self.lag_watermarks.append(after_article_id)
        index = min(len(self.lag_watermarks) - 1, len(self._lag_deltas) - 1)
        return self._lag_deltas[index]

    async def quality_snapshot(self, **kwargs) -> QualityMetrics:
        return _quality()


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
            (WorkerStatObservation("crawl", 7, {"articles_persisted": 10}, True),),
            (WorkerStatObservation("crawl", 9, {"articles_persisted": 4}, True),),
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
