"""Manual, strict end-to-end replay over one explicit time window.

Scheduled workers drain durable queues with relative windows. This DAG is the
only place Airflow maps an operator-provided `since`/`until` pair into every
stage, and labels every resulting `worker_run` as a backfill.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk import Asset, Param, dag, get_current_context, task

from _common import (
    FEED_PATH,
    PG_CONN_ID,
    STORIES_ASSET_URI,
    dag_defaults,
    topic_invariant_check,
    worker,
)


STORIES = Asset(STORIES_ASSET_URI)
SINCE = "{{ params.since }}"
UNTIL = "{{ params.until }}"


def _parse_bound(value: str, name: str) -> datetime:
    if not value:
        raise ValueError(f"{name} is required")
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


@dag(
    schedule=None,
    dagrun_timeout=timedelta(hours=8),
    params={
        "since": Param(
            None,
            type=["null", "string"],
            title="Window start",
            description="Required ISO-8601 date or datetime",
        ),
        "until": Param(
            None,
            type=["null", "string"],
            title="Window end",
            description="Required ISO-8601 date or datetime; must be after since",
        ),
    },
    doc_md=__doc__,
    **dag_defaults(),
)
def news_backfill():
    preflight = SQLExecuteQueryOperator(
        task_id="preflight_db",
        conn_id=PG_CONN_ID,
        sql="SELECT 1 FROM alembic_version",
        retries=1,
    )

    @task
    def validate_window() -> dict[str, str]:
        params = get_current_context()["params"]
        since = _parse_bound(params["since"], "since")
        until = _parse_bound(params["until"], "until")
        if since >= until:
            raise ValueError("since must be earlier than until")
        if until - since > timedelta(days=31):
            raise ValueError("a backfill window may not exceed 31 days")
        return {"since": since.isoformat(), "until": until.isoformat()}

    validated_window = validate_window()
    crawl = worker(
        "crawl",
        "crawl",
        "--strict",
        "--feed-path",
        FEED_PATH,
        "--since",
        SINCE,
        "--until",
        UNTIL,
        pool="news_crawl",
        timeout=timedelta(hours=1),
        trigger="backfill",
        busy_is_failure=True,
    )
    embed = worker(
        "embed",
        "embed",
        "--strict",
        "--since",
        SINCE,
        "--until",
        UNTIL,
        pool="news_embed",
        timeout=timedelta(hours=2),
        trigger="backfill",
        busy_is_failure=True,
    )
    classify = worker(
        "classify",
        "classify",
        "--strict",
        "--since",
        SINCE,
        "--until",
        UNTIL,
        pool="news_classify",
        timeout=timedelta(hours=2),
        trigger="backfill",
        busy_is_failure=True,
    )
    cluster = worker(
        "cluster",
        "cluster",
        "--strict",
        "--since",
        SINCE,
        "--until",
        UNTIL,
        pool="news_cluster",
        timeout=timedelta(hours=2),
        trigger="backfill",
        busy_is_failure=True,
    )
    cluster_merge = worker(
        "cluster_merge",
        "cluster",
        "--strict",
        "--merge-only",
        "--since",
        SINCE,
        "--until",
        UNTIL,
        pool="news_cluster",
        timeout=timedelta(minutes=45),
        trigger="backfill",
        busy_is_failure=True,
    )
    refresh_cluster_topics = worker(
        "refresh_cluster_topics",
        "classify",
        "--strict",
        "--refresh-clusters",
        "--since",
        SINCE,
        "--until",
        UNTIL,
        pool="news_classify",
        timeout=timedelta(minutes=45),
        trigger="backfill",
        busy_is_failure=True,
    )
    validate_topic_invariants = topic_invariant_check(outlets=[STORIES])

    preflight >> validated_window >> crawl >> embed >> [classify, cluster]
    cluster >> cluster_merge
    [classify, cluster_merge] >> refresh_cluster_topics
    refresh_cluster_topics >> validate_topic_invariants


news_backfill()
