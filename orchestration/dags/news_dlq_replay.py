"""Inspect or manually replay dead-lettered work with an explicit limit.

The default `apply=false` is intentionally read-only. Dead letters have
already exhausted their durable retry budget, so a daily automatic replay
would create a poison loop and hide deterministic data or code failures.
"""

from __future__ import annotations

from datetime import timedelta

from airflow.sdk import Param, dag, get_current_context, task

from _common import admin, dag_defaults


@dag(
    schedule=None,
    dagrun_timeout=timedelta(minutes=30),
    params={
        "target": Param(
            "processing_jobs",
            enum=["processing_jobs", "crawl_failures"],
            title="Dead-letter queue",
        ),
        "stage": Param(
            "embed",
            enum=["embed", "classify", "cluster"],
            title="Processing stage",
            description="Used only when target=processing_jobs",
        ),
        "failure_kind": Param(
            "all",
            enum=["all", "article", "feed"],
            title="Failure kind",
            description="Used only when target=crawl_failures",
        ),
        "limit": Param(100, type="integer", minimum=1, maximum=1000),
        "apply": Param(
            False,
            type="boolean",
            title="Apply replay",
            description="False only prints the current queue snapshot",
        ),
    },
    doc_md=__doc__,
    **dag_defaults(),
)
def news_dlq_replay():
    @task.branch
    def choose_action() -> str:
        params = get_current_context()["params"]
        if not params["apply"]:
            return "preview_dead_letters"
        if params["target"] == "processing_jobs":
            return "replay_processing_jobs"
        return "replay_crawl_failures"

    preview_dead_letters = admin(
        "preview_dead_letters",
        "queue-status",
        "--json",
        "--runs-window",
        "24",
        timeout=timedelta(minutes=5),
        do_xcom_push=True,
    )
    replay_processing_jobs = admin(
        "replay_processing_jobs",
        "replay",
        "--stage",
        "{{ params.stage }}",
        "--limit",
        "{{ params.limit }}",
        timeout=timedelta(minutes=10),
    )
    replay_crawl_failures = admin(
        "replay_crawl_failures",
        "replay",
        "--failures",
        "{{ '--kind ' + params.failure_kind if params.failure_kind != 'all' else '' }}",
        "--limit",
        "{{ params.limit }}",
        timeout=timedelta(minutes=10),
    )
    verify_queue = admin(
        "verify_queue",
        "queue-status",
        "--json",
        "--runs-window",
        "24",
        timeout=timedelta(minutes=5),
        do_xcom_push=True,
        trigger_rule="none_failed_min_one_success",
    )

    choice = choose_action()
    choice >> [preview_dead_letters, replay_processing_jobs, replay_crawl_failures]
    [preview_dead_letters, replay_processing_jobs, replay_crawl_failures] >> verify_queue


news_dlq_replay()
