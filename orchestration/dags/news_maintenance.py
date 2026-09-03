"""Daily repair pass, deliberately off the ingest path.

Everything here either rewrites aggregates or requeues abandoned work, so it
runs at 03:00 rather than competing with a crawl for the same locks.
"""

from __future__ import annotations

from datetime import timedelta

from airflow.sdk import dag

from _common import admin, dag_defaults, worker


@dag(
    schedule="0 3 * * *",
    dagrun_timeout=timedelta(hours=2),
    doc_md=__doc__,
    **dag_defaults(),
)
def news_maintenance():
    # Repairs drift between the `clusters` aggregates and `cluster_members`.
    # Same pool as the other cluster modes: all three write the same table.
    cluster_repair = worker(
        "cluster_repair",
        "cluster",
        "--repair",
        "--last",
        "7d",
        pool="news_cluster",
        timeout=timedelta(minutes=30),
    )

    replay_dead_letter_jobs = admin(
        "replay_dead_letter_jobs",
        "replay",
        "--stage",
        "embed",
        "--limit",
        "200",
        timeout=timedelta(minutes=10),
    )
    replay_failures = admin(
        "replay_failures",
        "replay",
        "--failures",
        "--limit",
        "200",
        timeout=timedelta(minutes=10),
    )

    # Dry run on purpose. Quarantining rows is a judgement call an operator
    # makes after reading the log, not something a nightly DAG should decide;
    # `--apply` also needs the crawl and embed locks, which would make this
    # task fail whenever ingestion happens to be running.
    cleanup_block_pages = admin(
        "cleanup_block_pages",
        "cleanup-block-pages",
        "--limit",
        "200",
        timeout=timedelta(minutes=10),
    )

    # The task that turns the whole DAG red. `--fail-on-alert` exits 1 on a
    # dead-lettered job, a dead-lettered failure, or a failed/abandoned run in
    # the last day -- the conditions nobody would otherwise notice. It runs
    # even if a repair above failed, because that is when it matters most.
    run_health_report = admin(
        "run_health_report",
        "queue-status",
        "--json",
        "--runs-window",
        "24",
        "--fail-on-alert",
        timeout=timedelta(minutes=5),
        # `--json` already prints the snapshot as a single stdout line.
        do_xcom_push=True,
        trigger_rule="all_done",
    )

    (
        cluster_repair
        >> replay_dead_letter_jobs
        >> replay_failures
        >> cleanup_block_pages
        >> run_health_report
    )


news_maintenance()
