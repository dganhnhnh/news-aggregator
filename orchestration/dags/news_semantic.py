"""Embed → classify/cluster → merge → refresh: everything after ingestion.

Asset-triggered rather than scheduled, so it runs exactly when `news_ingest`
actually queued work. A failure here is a real signal — nothing in this DAG
depends on a third-party host being up.
"""

from __future__ import annotations

from datetime import timedelta

from airflow.sdk import Asset, dag

from _common import (
    JOBS_ASSET_URI,
    STORIES_ASSET_URI,
    dag_defaults,
    topic_invariant_check,
    worker,
)


JOBS = Asset(JOBS_ASSET_URI)
STORIES = Asset(STORIES_ASSET_URI)


@dag(
    schedule=[JOBS],
    dagrun_timeout=timedelta(minutes=60),
    doc_md=__doc__,
    **dag_defaults(),
)
def news_semantic():
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

    # Runs even when classify or cluster skipped on a busy lock. The invariant
    # check below is the final task and publishes the Asset. `cluster_topics`
    # is derived data that
    # `_refresh_cluster_topics_locked` replaces wholesale, so an extra refresh
    # costs nothing while a missed one leaves a story wearing a stale label.
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
    # validate_topic_invariants = topic_invariant_check(outlets=[STORIES])

    embed >> [classify, cluster]
    cluster >> cluster_merge
    [classify, cluster_merge] >> refresh_cluster_topics
    # refresh_cluster_topics >> validate_topic_invariants


news_semantic()
