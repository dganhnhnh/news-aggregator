"""The only module in the repo that knows which Airflow operator runs a worker.

Every task in every DAG is "one argv for `news-worker`". Keeping that shape in
a single factory is what makes the choice of `DockerOperator` reversible: a
move to `BashOperator` or `KubernetesPodOperator` rewrites `worker()` here and
leaves the DAG files untouched. So `DockerOperator` must not be imported
anywhere else under `orchestration/`.

See docs/orchestration-airflow.md §5 for the reasoning.
"""

from __future__ import annotations

import os
from datetime import timedelta

import pendulum
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.common.sql.operators.sql import SQLCheckOperator
from docker.types import Mount


# Both DAGs that produce work and the one that consumes it name these, so the
# strings live here rather than being retyped per file.
JOBS_ASSET_URI = "news-radar://processing_jobs"
STORIES_ASSET_URI = "news-radar://stories"

# The Airflow connection holding the application database URL. `worker()`
# renders it into DATABASE_URL, so no DAG ever contains a credential.
PG_CONN_ID = "news_radar_pg"

# Exit code the workers use for "another run already holds the advisory lock".
# It must not be 0 (a busy run is not work done) nor 1 (that is a failure);
# `news-worker --exit-busy` rejects 1 for exactly this reason.
EXIT_BUSY = 99

WORKER_IMAGE = os.environ.get("NEWS_RADAR_WORKER_IMAGE", "news-radar-worker:dev")
DOCKER_NETWORK = os.environ.get("NEWS_RADAR_DOCKER_NETWORK", "news-radar_default")
REPO = os.environ.get("NEWS_RADAR_REPO", "/opt/news-radar")

# Feed source and taxonomy seed, mounted read-only from the repo checkout.
FEED_PATH = "/data/engineering_blogs.opml"
TAXONOMY_SEED_PATH = "/data/taxonomy.seed.yaml"

DEFAULT_ARGS = {
    # Retrying a worker is almost never right: a run that failed has already
    # closed its `worker_runs` row, and its queue items keep their own backoff
    # in `processing_jobs`. Re-running immediately would just fail the same way.
    "retries": 0,
    "depends_on_past": False,
}

# Rendered by DockerOperator's Jinja pass, not at import time.
_DATABASE_URL = (
    "{{ conn."
    + PG_CONN_ID
    + ".get_uri()"
    + " | replace('postgres://', 'postgresql+asyncpg://')"
    + " | replace('postgresql://', 'postgresql+asyncpg://') }}"
)

COMMON = dict(
    image=WORKER_IMAGE,
    docker_url="unix://var/run/docker.sock",
    network_mode=DOCKER_NETWORK,
    # Preserve failed containers for diagnosis. A separate cleanup policy can
    # remove them after logs and the image revision have been captured.
    auto_remove="success",
    mount_tmp_dir=False,
    # Chromium dies at random on the 64MB default; crawl4ai is the reason this
    # is set for every task rather than only for crawl.
    shm_size=1024 * 1024 * 1024,
    mounts=[
        Mount(target="/models", source="news_radar_model_cache", type="volume"),
        Mount(target="/data", source=f"{REPO}/data", type="bind", read_only=True),
    ],
    environment={
        "DATABASE_URL": _DATABASE_URL,
        "FASTEMBED_CACHE_PATH": "/models",
        "LOG_LEVEL": "{{ var.value.get('news_radar_log_level', 'INFO') }}",
        "NEWS_RADAR_ORCHESTRATION_ID": "{{ dag.dag_id }}:{{ run_id }}",
        "NEWS_RADAR_AIRFLOW_DAG_ID": "{{ dag.dag_id }}",
        "NEWS_RADAR_AIRFLOW_TASK_ID": "{{ task.task_id }}",
        "NEWS_RADAR_IMAGE_REVISION": WORKER_IMAGE,
        "MAX_CONCURRENT_FEEDS": (
            "{{ var.value.get('news_radar_max_concurrent_feeds', '8') }}"
        ),
        "MAX_CONCURRENT_CRAWLS": (
            "{{ var.value.get('news_radar_max_concurrent_crawls', '6') }}"
        ),
        "FEED_FETCH_TIMEOUT_SECONDS": (
            "{{ var.value.get('news_radar_feed_fetch_timeout_seconds', '8') }}"
        ),
        "FEED_POLL_FAST_INTERVAL_SECONDS": (
            "{{ var.value.get('news_radar_feed_poll_fast_interval_seconds', '300') }}"
        ),
        "FEED_POLL_INTERVAL_SECONDS": (
            "{{ var.value.get('news_radar_feed_poll_interval_seconds', '1800') }}"
        ),
        "FEED_POLL_SLOW_INTERVAL_SECONDS": (
            "{{ var.value.get('news_radar_feed_poll_slow_interval_seconds', '21600') }}"
        ),
        "FEED_POLL_MIN_GAPS": (
            "{{ var.value.get('news_radar_feed_poll_min_gaps', '3') }}"
        ),
        "FEED_POLL_TRIM_FRACTION": (
            "{{ var.value.get('news_radar_feed_poll_trim_fraction', '0.1') }}"
        ),
        "FEED_POLL_FUTURE_TOLERANCE_SECONDS": (
            "{{ var.value.get('news_radar_feed_poll_future_tolerance_seconds', '300') }}"
        ),
    },
)


def _cooperative_budget_minutes(timeout: timedelta) -> int:
    hard_minutes = int(timeout.total_seconds() // 60)
    reserve_minutes = min(5, max(1, hard_minutes // 4))
    return max(1, hard_minutes - reserve_minutes)


def _container(
    task_id: str,
    command: list[str],
    *,
    timeout: timedelta,
    **kwargs,
) -> DockerOperator:
    # A single string, not a list: docker-py shlex-splits a string command, so
    # a Jinja expression that renders to nothing (`{{ '--prune' if ... }}`)
    # drops out of argv instead of arriving as an empty argument argparse
    # would reject. Every token here is a flag, a number or a path.
    return DockerOperator(
        task_id=task_id,
        command=" ".join(command),
        execution_timeout=timeout,
        **COMMON,
        **kwargs,
    )


def worker(
    task_id: str,
    *argv: str,
    pool: str,
    timeout: timedelta,
    trigger: str = "scheduled",
    busy_is_failure: bool = False,
    **kwargs,
) -> DockerOperator:
    """One run of one of the four workers (crawl, embed, classify, cluster).

    `--trigger` is always passed explicitly. Reserve 25% of short timeouts and
    at most five minutes of long ones for the active chunk to checkpoint and
    emit its final JSON summary before Airflow stops the container.
    """
    command = [
        *argv,
        "--once",
        "--trigger",
        trigger,
        "--exit-busy",
        str(EXIT_BUSY),
        # The worker writes this as the final stdout line, which is the line
        # DockerOperator pushes to XCom.
        "--summary-json",
        "/dev/stdout",
        "--max-runtime",
        f"{_cooperative_budget_minutes(timeout)}m",
    ]
    kwargs.setdefault("do_xcom_push", True)
    return _container(
        task_id,
        command,
        timeout=timeout,
        # Scheduled runs treat lock contention as a visible skip. Manual
        # consistency operations set busy_is_failure so the operator must
        # explicitly retry instead of receiving a deceptively green dagrun.
        skip_on_exit_code=None if busy_is_failure else EXIT_BUSY,
        pool=pool,
        **kwargs,
    )


def admin(
    task_id: str,
    *argv: str,
    timeout: timedelta,
    **kwargs,
) -> DockerOperator:
    """An admin `news-worker` subcommand: replay, cleanup, queue-status.

    These open no run window, take no advisory lock and write no `worker_runs`
    row, so none of the worker flags above apply to them -- and they stay out
    of the worker pools, because occupying the single embed slot to requeue a
    handful of dead-lettered rows would block real work.
    """
    kwargs.setdefault("do_xcom_push", False)
    return _container(task_id, list(argv), timeout=timeout, **kwargs)


def dag_defaults(**overrides):
    """Settings every DAG here shares.

    `catchup=False` and `max_active_runs=1` are not stylistic: a worker takes a
    PostgreSQL advisory lock, so a backlog of catch-up runs would serialise
    behind it and each one would skip on the busy exit code.
    """
    defaults = dict(
        start_date=pendulum.datetime(2026, 8, 1, tz="UTC"),
        catchup=False,
        max_active_runs=1,
        default_args=DEFAULT_ARGS,
        tags=["news-radar"],
    )
    defaults.update(overrides)
    return defaults


def topic_invariant_check(
    task_id: str = "validate_topic_invariants", *, outlets=None
) -> SQLCheckOperator:
    """Fail when materialized topic labels disagree with their source rows."""
    return SQLCheckOperator(
        task_id=task_id,
        conn_id=PG_CONN_ID,
        retries=1,
        execution_timeout=timedelta(minutes=5),
        outlets=outlets or [],
        sql="""
        SELECT
          NOT EXISTS (
            SELECT 1
            FROM article_classifications ac
            WHERE ac.assigned_count <> (
              SELECT count(*)
              FROM article_topics article_topic
              WHERE article_topic.article_id = ac.article_id
                AND article_topic.assigned
            )
          )
          AND NOT EXISTS (
            SELECT 1
            FROM cluster_topics ct
            WHERE ct.assigned
              AND NOT EXISTS (
                SELECT 1
                FROM cluster_members cm
                JOIN article_topics article_topic
                  ON article_topic.article_id = cm.article_id
                WHERE cm.cluster_id = ct.cluster_id
                  AND article_topic.topic_id = ct.topic_id
                  AND article_topic.assigned
              )
          )
        """,
    )
