"""Reclassify the corpus after the taxonomy changed. Manual trigger only.

Changing a topic bumps its `classifier_revision`, which makes every existing
classification stale over a window the operator chooses. That is a decision a
person makes, not a schedule -- hence `schedule=None` and params in the UI.
"""

from __future__ import annotations

from datetime import timedelta

from airflow.sdk import Asset, Param, dag, get_current_context, task

from _common import (
    EXIT_BUSY,
    STORIES_ASSET_URI,
    TAXONOMY_SEED_PATH,
    admin,
    dag_defaults,
    topic_invariant_check,
    worker,
)


STORIES = Asset(STORIES_ASSET_URI)

# `--last` for every reclassification task. Wide by default because a taxonomy
# change is retroactive, and narrow enough that a mistrigger is not a week of
# compute.
WINDOW = "{{ params.window }}"


@dag(
    schedule=None,
    dagrun_timeout=timedelta(hours=6),
    params={
        "window": Param(
            "30d",
            type="string",
            title="Reclassification window",
            description="Relative window passed as --last, e.g. 7d, 30d, 12w",
        ),
        "seed_file": Param(
            TAXONOMY_SEED_PATH,
            type="string",
            title="Taxonomy seed file",
            description="Path inside the worker container (/data is the repo's data/)",
        ),
        "prune": Param(
            False,
            type="boolean",
            title="Deactivate topics missing from the seed",
            description="Maps to `taxonomy-import --prune`; logs what it will "
            "deactivate before writing",
        ),
        "reprocess_clusters": Param(
            False,
            type="boolean",
            title="Reprocess missing cluster memberships",
            description="Normally unnecessary for a taxonomy-only change; "
            "enable only when cluster membership also needs repair",
        ),
    },
    doc_md=__doc__,
    **dag_defaults(),
)
def news_taxonomy_refresh():
    taxonomy_import = admin(
        "taxonomy_import",
        "taxonomy-import",
        "--file",
        "{{ params.seed_file }}",
        # Renders to nothing when the param is false, so the flag disappears
        # from argv rather than being passed as an empty string.
        "{{ '--prune' if params.prune else '' }}",
        timeout=timedelta(minutes=10),
    )

    # Recompiles prototypes only for topics whose exemplars actually changed;
    # it takes its own advisory lock, so it gets the busy exit code too.
    taxonomy_rebuild = admin(
        "taxonomy_rebuild",
        "taxonomy-rebuild",
        "--exit-busy",
        str(EXIT_BUSY),
        timeout=timedelta(minutes=20),
    )

    # --stale, not --reprocess: it re-queues only articles never classified or
    # scored against a different classifier set, which is exactly what the
    # import just invalidated.
    classify_stale = worker(
        "classify_stale",
        "classify",
        "--strict",
        "--stale",
        "--last",
        WINDOW,
        pool="news_classify",
        timeout=timedelta(hours=2),
        trigger="manual",
        busy_is_failure=True,
    )

    @task.branch
    def choose_cluster_reprocess() -> str:
        return (
            "cluster_reprocess"
            if get_current_context()["params"]["reprocess_clusters"]
            else "skip_cluster_reprocess"
        )

    @task
    def skip_cluster_reprocess() -> None:
        """Explicitly record that taxonomy changes do not imply reclustering."""

    # Picks up embedded articles in the window that never landed in a cluster.
    cluster_reprocess = worker(
        "cluster_reprocess",
        "cluster",
        "--strict",
        "--reprocess",
        "--last",
        WINDOW,
        pool="news_cluster",
        timeout=timedelta(hours=2),
        trigger="manual",
        busy_is_failure=True,
    )

    cluster_merge = worker(
        "cluster_merge",
        "cluster",
        "--strict",
        "--merge-only",
        "--last",
        WINDOW,
        pool="news_cluster",
        timeout=timedelta(minutes=30),
        trigger="manual",
        busy_is_failure=True,
        trigger_rule="none_failed_min_one_success",
    )

    # Same aggregation path as news_semantic. Manual consistency work treats a
    # busy lock as failure; the invariant check below is the final task and the
    # only one that publishes the stories Asset.
    refresh_cluster_topics = worker(
        "refresh_cluster_topics",
        "classify",
        "--strict",
        "--refresh-clusters",
        "--last",
        WINDOW,
        pool="news_classify",
        timeout=timedelta(minutes=30),
        trigger="manual",
        trigger_rule="none_failed_min_one_success",
        busy_is_failure=True,
    )
    validate_topic_invariants = topic_invariant_check(outlets=[STORIES])

    choose_reprocess = choose_cluster_reprocess()
    skip_reprocess = skip_cluster_reprocess()
    taxonomy_import >> taxonomy_rebuild >> [classify_stale, choose_reprocess]
    choose_reprocess >> [cluster_reprocess, skip_reprocess]
    [cluster_reprocess, skip_reprocess] >> cluster_merge
    [classify_stale, cluster_merge] >> refresh_cluster_topics
    refresh_cluster_topics >> validate_topic_invariants


news_taxonomy_refresh()
