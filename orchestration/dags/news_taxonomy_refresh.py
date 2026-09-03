"""Reclassify the corpus after the taxonomy changed. Manual trigger only.

Changing a topic bumps its `classifier_revision`, which makes every existing
classification stale over a window the operator chooses. That is a decision a
person makes, not a schedule -- hence `schedule=None` and params in the UI.
"""

from __future__ import annotations

from datetime import timedelta

from airflow.sdk import Asset, Param, dag

from _common import (
    EXIT_BUSY,
    STORIES_ASSET_URI,
    TAXONOMY_SEED_PATH,
    admin,
    dag_defaults,
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
        skip_on_exit_code=EXIT_BUSY,
    )

    # --stale, not --reprocess: it re-queues only articles never classified or
    # scored against a different classifier set, which is exactly what the
    # import just invalidated.
    classify_stale = worker(
        "classify_stale",
        "classify",
        "--stale",
        "--last",
        WINDOW,
        pool="news_classify",
        timeout=timedelta(hours=2),
        trigger="manual",
    )

    # Picks up embedded articles in the window that never landed in a cluster.
    cluster_reprocess = worker(
        "cluster_reprocess",
        "cluster",
        "--reprocess",
        "--last",
        WINDOW,
        pool="news_cluster",
        timeout=timedelta(hours=2),
        trigger="manual",
    )

    cluster_merge = worker(
        "cluster_merge",
        "cluster",
        "--merge-only",
        "--last",
        WINDOW,
        pool="news_cluster",
        timeout=timedelta(minutes=30),
        trigger="manual",
    )

    # Same rule as in news_semantic: `cluster_topics` is fully replaced from
    # `article_topics`, so this must be the last task and must still run when
    # an upstream task skipped on a busy lock.
    refresh_cluster_topics = worker(
        "refresh_cluster_topics",
        "classify",
        "--refresh-clusters",
        "--last",
        WINDOW,
        pool="news_classify",
        timeout=timedelta(minutes=30),
        trigger="manual",
        trigger_rule="none_failed_min_one_success",
        outlets=[STORIES],
    )

    taxonomy_import >> taxonomy_rebuild >> [classify_stale, cluster_reprocess]
    cluster_reprocess >> cluster_merge
    [classify_stale, cluster_merge] >> refresh_cluster_topics


news_taxonomy_refresh()
