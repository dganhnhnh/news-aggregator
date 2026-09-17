"""Prometheus exposition for the pipeline, derived entirely from PostgreSQL.

Every worker is an ephemeral container: Airflow starts `news-worker`, it runs
for a minute, and `auto_remove` deletes it. A scrape can never reach it, so
there are no per-process counters to collect and no Pushgateway to keep them
in. What survives a run is its row in `worker_runs` and the rows it wrote, so
this module reads those instead: the database *is* the metrics store, and the
collector turns it into an exposition on demand.

The module is split so the shaping is testable without a database:
`build_families` is a pure function from a snapshot to metric families, and
`MetricsCollector` owns the impure parts -- querying, caching, and carrying
counter state across scrapes.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable, Iterable, Mapping, Sequence

from prometheus_client import CONTENT_TYPE_LATEST, CollectorRegistry, generate_latest
from prometheus_client.core import CounterMetricFamily, GaugeMetricFamily, Metric

from adapters.storage import PostgresRawStorage
from models.storage_models import MetricsSnapshot, WorkerStatDelta


PREFIX = "news_radar"
JOB_STATES = ("pending", "running", "done", "dead_letter")

__all__ = [
    "CONTENT_TYPE_LATEST",
    "MetricsCollector",
    "build_families",
    "render",
]


def build_families(
    snapshot: MetricsSnapshot,
    stat_totals: Mapping[tuple[str, str], float] = {},
    collect_seconds: float = 0.0,
) -> list[Metric]:
    """Shape one snapshot into Prometheus metric families.

    Pure and total: given the same snapshot it emits the same families, which
    is what makes the naming and labelling testable without a database.

    Label values are all closed sets (stage, state, worker, text kind) or are
    capped upstream. Free text -- a feed URL, a disable reason, an error
    message -- never becomes a label: one bad day would otherwise leave
    thousands of dead series in Prometheus forever.
    """
    families: list[Metric] = []

    jobs = GaugeMetricFamily(
        f"{PREFIX}_jobs",
        "Processing jobs by stage and state.",
        labels=["stage", "state"],
    )
    oldest = GaugeMetricFamily(
        f"{PREFIX}_job_oldest_pending_age_seconds",
        "Age of the oldest job waiting to be claimed, by stage.",
        labels=["stage"],
    )
    expired = GaugeMetricFamily(
        f"{PREFIX}_job_expired_leases",
        "Running jobs whose lease has expired, by stage.",
        labels=["stage"],
    )
    for stage in snapshot.stages:
        counts = {
            "pending": stage.pending,
            "running": stage.running,
            "done": stage.done,
            "dead_letter": stage.dead_letter,
        }
        # Emit every state, including the zeros: a gauge that disappears when
        # a queue drains makes `dead_letter` alerts flap between "no data"
        # and a value instead of sitting flat at 0.
        for state in JOB_STATES:
            jobs.add_metric([stage.stage, state], float(counts[state]))
        expired.add_metric([stage.stage], float(stage.expired_leases))
        if stage.oldest_pending_age_seconds is not None:
            oldest.add_metric([stage.stage], stage.oldest_pending_age_seconds)
    families += [jobs, oldest, expired]

    runs = CounterMetricFamily(
        f"{PREFIX}_worker_runs",
        "Worker runs recorded in worker_runs, by terminal status.",
        labels=["worker", "status"],
    )
    for run in snapshot.runs:
        runs.add_metric([run.worker, run.status], float(run.count))
    families.append(runs)

    last_success = GaugeMetricFamily(
        f"{PREFIX}_worker_last_success_timestamp_seconds",
        "Unix time of each worker's most recent successful run.",
        labels=["worker"],
    )
    last_duration = GaugeMetricFamily(
        f"{PREFIX}_worker_last_run_duration_seconds",
        "Wall-clock duration of each worker's most recent finished run.",
        labels=["worker"],
    )
    running = GaugeMetricFamily(
        f"{PREFIX}_worker_running",
        "Runs currently marked running, by worker.",
        labels=["worker"],
    )
    heartbeat = GaugeMetricFamily(
        f"{PREFIX}_worker_running_heartbeat_age_seconds",
        "Age of the oldest heartbeat among running runs, by worker.",
        labels=["worker"],
    )
    for worker in snapshot.workers:
        running.add_metric([worker.worker], float(worker.running))
        if worker.last_success_at is not None:
            last_success.add_metric(
                [worker.worker], worker.last_success_at.timestamp()
            )
        if worker.last_run_duration_seconds is not None:
            last_duration.add_metric(
                [worker.worker], worker.last_run_duration_seconds
            )
        # Only present while something is running: a stale age for an idle
        # worker would read as a hung run.
        if worker.oldest_heartbeat_age_seconds is not None:
            heartbeat.add_metric(
                [worker.worker], worker.oldest_heartbeat_age_seconds
            )
    families += [last_success, last_duration, running, heartbeat]

    stats = CounterMetricFamily(
        f"{PREFIX}_worker_stat",
        "Cumulative numeric stats reported by finished worker runs.",
        labels=["worker", "stat"],
    )
    for (worker_name, stat), total in sorted(stat_totals.items()):
        stats.add_metric([worker_name, stat], total)
    families.append(stats)

    failures = GaugeMetricFamily(
        f"{PREFIX}_crawl_failures",
        "Rows in the crawl failure queue, by kind and state.",
        labels=["failure_kind", "state"],
    )
    due = GaugeMetricFamily(
        f"{PREFIX}_crawl_failures_due",
        "Open crawl failures whose retry is already due, by kind.",
        labels=["failure_kind"],
    )
    attempts = GaugeMetricFamily(
        f"{PREFIX}_crawl_failure_attempts_max",
        "Highest attempt count in the crawl failure queue, by kind.",
        labels=["failure_kind"],
    )
    for failure in snapshot.failures:
        open_rows = failure.total - failure.dead_lettered
        failures.add_metric([failure.failure_kind, "open"], float(open_rows))
        failures.add_metric(
            [failure.failure_kind, "dead_lettered"], float(failure.dead_lettered)
        )
        due.add_metric([failure.failure_kind], float(failure.due_now))
        attempts.add_metric([failure.failure_kind], float(failure.max_attempts))
    families += [failures, due, attempts]

    error_types = GaugeMetricFamily(
        f"{PREFIX}_crawl_failures_by_error_type",
        "Crawl failures by error type; the tail is folded into 'other'.",
        labels=["error_type", "state"],
    )
    for failure_type in snapshot.failure_types:
        error_types.add_metric(
            [failure_type.error_type, "open"],
            float(failure_type.total - failure_type.dead_lettered),
        )
        error_types.add_metric(
            [failure_type.error_type, "dead_lettered"],
            float(failure_type.dead_lettered),
        )
    families.append(error_types)

    feeds = GaugeMetricFamily(
        f"{PREFIX}_feeds",
        "Configured feeds by state.",
        labels=["state"],
    )
    feeds.add_metric(["active"], float(snapshot.feeds.active))
    feeds.add_metric(["disabled"], float(snapshot.feeds.disabled))
    families.append(feeds)
    families.append(
        GaugeMetricFamily(
            f"{PREFIX}_feeds_due",
            "Active feeds whose next poll is due.",
            value=float(snapshot.feeds.due),
        )
    )
    families.append(
        GaugeMetricFamily(
            f"{PREFIX}_feeds_overdue",
            "Active feeds past due by more than the configured grace period.",
            value=float(snapshot.feeds.overdue),
        )
    )
    families.append(
        GaugeMetricFamily(
            f"{PREFIX}_feed_consecutive_failures_max",
            "Highest consecutive failure count among active feeds.",
            value=float(snapshot.feeds.max_consecutive_failures),
        )
    )

    families.append(
        _labelled(
            f"{PREFIX}_articles",
            "Stored articles by where their content came from.",
            "content_source",
            snapshot.corpus.articles_by_content_source,
        )
    )
    families.append(
        _labelled(
            f"{PREFIX}_embeddings",
            "Article embeddings by which text was embedded.",
            "text_kind",
            snapshot.corpus.embeddings_by_text_kind,
        )
    )
    families.append(
        _labelled(
            f"{PREFIX}_clusters",
            "Clusters by status.",
            "status",
            snapshot.corpus.clusters_by_status,
        )
    )
    families.append(
        GaugeMetricFamily(
            f"{PREFIX}_cluster_members",
            "Articles attached to a cluster.",
            value=float(snapshot.corpus.cluster_members),
        )
    )
    families.append(
        _labelled(
            f"{PREFIX}_classified_articles",
            "Classified articles by outcome; 'uncategorized' scored below "
            "every topic threshold.",
            "outcome",
            snapshot.corpus.classified_articles,
        )
    )

    families.append(
        GaugeMetricFamily(
            f"{PREFIX}_recent_articles",
            "Articles whose event time falls in the recent window.",
            value=float(snapshot.corpus.recent_articles),
        )
    )
    families.append(
        _labelled(
            f"{PREFIX}_recent_articles_missing",
            "Recent articles that have not reached a pipeline stage yet.",
            "stage",
            snapshot.corpus.recent_missing,
        )
    )
    families.append(
        GaugeMetricFamily(
            f"{PREFIX}_recent_window_hours",
            "Width of the recent window, so dashboards can label it.",
            value=float(snapshot.recent_window_hours),
        )
    )

    families.append(
        GaugeMetricFamily(
            f"{PREFIX}_metrics_collect_duration_seconds",
            "Seconds spent querying PostgreSQL for the last collection.",
            value=collect_seconds,
        )
    )
    families.append(
        GaugeMetricFamily(
            f"{PREFIX}_metrics_snapshot_timestamp_seconds",
            "Unix time of the snapshot behind this response; it lags the "
            "scrape by up to the cache TTL.",
            value=snapshot.captured_at.timestamp(),
        )
    )
    return families


def _labelled(
    name: str,
    documentation: str,
    label: str,
    pairs: Sequence[tuple[str, int]],
) -> GaugeMetricFamily:
    family = GaugeMetricFamily(name, documentation, labels=[label])
    for value, count in pairs:
        family.add_metric([value], float(count))
    return family


class _Families:
    """Adapts a ready list of families to the registry's collector protocol."""

    def __init__(self, families: Iterable[Metric]) -> None:
        self._families = list(families)

    def collect(self) -> list[Metric]:
        return self._families


def render(families: Iterable[Metric]) -> bytes:
    """Serialise families in the Prometheus text format."""
    registry = CollectorRegistry()
    registry.register(_Families(families))
    return generate_latest(registry)


class MetricsCollector:
    """Turns database reads into an exposition, with counters that persist.

    Two pieces of state live here rather than in the database. The cache
    decouples the cost of a collection from how often anyone scrapes. The
    accumulated totals and per-worker watermarks are what let ephemeral
    workers' stats be exposed as real counters: each collection adds only the
    runs finished since the last one, so the numbers climb monotonically for
    as long as the process lives, and a restart re-reads history from zero and
    arrives back at the same totals instead of resetting them.
    """

    def __init__(
        self,
        storage: PostgresRawStorage,
        *,
        cache_seconds: float = 15.0,
        recent_window_hours: int = 24,
        max_error_types: int = 10,
        feed_overdue_grace_seconds: int = 300,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._storage = storage
        self._cache_seconds = cache_seconds
        self._recent_window_hours = recent_window_hours
        self._max_error_types = max_error_types
        self._feed_overdue_grace_seconds = feed_overdue_grace_seconds
        self._clock = clock
        # Serialises collection: two scrapes arriving together should cost one
        # trip to the database, not two.
        self._lock = asyncio.Lock()
        self._payload: bytes | None = None
        self._expires_at = 0.0
        self._totals: dict[tuple[str, str], float] = {}
        self._watermarks: dict[str, int] = {}

    async def expose(self) -> bytes:
        async with self._lock:
            if self._payload is not None and self._clock() < self._expires_at:
                return self._payload
            started = self._clock()
            snapshot = await self._storage.metrics_snapshot(
                recent_window_hours=self._recent_window_hours,
                feed_overdue_grace_seconds=self._feed_overdue_grace_seconds,
                max_error_types=self._max_error_types,
            )
            self._accumulate(await self._storage.worker_stat_totals(self._watermarks))
            elapsed = self._clock() - started
            self._payload = render(
                build_families(snapshot, dict(self._totals), elapsed)
            )
            self._expires_at = self._clock() + self._cache_seconds
            return self._payload

    def _accumulate(self, deltas: Sequence[WorkerStatDelta]) -> None:
        for delta in deltas:
            key = (delta.worker, delta.stat)
            self._totals[key] = self._totals.get(key, 0.0) + delta.total
            self._watermarks[delta.worker] = max(
                self._watermarks.get(delta.worker, 0), delta.through_run_id
            )
