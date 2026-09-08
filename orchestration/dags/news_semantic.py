"""Embed → classify/cluster → merge → refresh: everything after ingestion.

Scheduled independently because semantic freshness is not latency-critical.
The queue gate keeps empty runs cheap while allowing work from either the
instant or normal crawl tier to be processed in priority order.
"""

from __future__ import annotations

from datetime import timedelta

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import dag, task
from airflow.sdk.exceptions import AirflowSkipException

from _common import (
    PG_CONN_ID,
    dag_defaults,
    worker,
)


@dag(
    schedule="*/15 * * * *",
    dagrun_timeout=timedelta(minutes=60),
    doc_md=__doc__,
    **dag_defaults(),
)
def news_semantic():
    @task
    def queue_gate() -> int:
        """Skip model startup when no semantic stage has actionable work."""
        actionable = PostgresHook(postgres_conn_id=PG_CONN_ID).get_first(
            "SELECT count(*) FROM processing_jobs "
            "WHERE stage IN ('embed', 'classify', 'cluster') AND ("
            "(state = 'pending' AND available_at <= now()) OR "
            "(state = 'running' AND lease_until < now()))"
        )[0]
        if not actionable:
            raise AirflowSkipException("no semantic jobs are actionable")
        return int(actionable)

    gate = queue_gate()

    embed = worker(
        "embed",
        "embed",
        "--last",
        "24h",
        pool="news_embed",
        timeout=timedelta(minutes=30),
    )

    # Independent of each other: classify scores an article against the topic
    # set, cluster attaches it to a story. Separate pools so they overlap.
    classify = worker(
        "classify",
        "classify",
        "--last",
        "24h",
        pool="news_classify",
        timeout=timedelta(minutes=20),
    )
    cluster = worker(
        "cluster",
        "cluster",
        "--last",
        "24h",
        pool="news_cluster",
        timeout=timedelta(minutes=20),
    )

    cluster_merge = worker(
        "cluster_merge",
        "cluster",
        "--merge-only",
        "--last",
        "24h",
        pool="news_cluster",
        timeout=timedelta(minutes=15),
    )

    # Runs even when classify or cluster skipped on a busy lock.
    # `cluster_topics` is derived data that `_refresh_cluster_topics_locked`
    # replaces wholesale, so an extra refresh costs nothing while a missed
    # one leaves a story wearing a stale label.
    refresh_cluster_topics = worker(
        "refresh_cluster_topics",
        "classify",
        "--refresh-clusters",
        "--last",
        "24h",
        pool="news_classify",
        timeout=timedelta(minutes=15),
        trigger_rule="none_failed_min_one_success",
    )
    gate >> embed >> [classify, cluster]
    cluster >> cluster_merge
    [classify, cluster_merge] >> refresh_cluster_topics


news_semantic()
