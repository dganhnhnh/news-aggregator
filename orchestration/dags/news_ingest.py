"""RSS/crawl ingestion: the only DAG that talks to the outside network.

It is kept separate from `news_semantic` so a broken feed host cannot mask a
classifier failure, and so the two can run at their own natural rates.
"""

from __future__ import annotations

from datetime import timedelta

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk import dag

from _common import PG_CONN_ID, dag_defaults, worker


@dag(
    schedule="*/5 * * * *",
    dagrun_timeout=timedelta(minutes=9),
    doc_md=__doc__,
    **dag_defaults(),
)
def news_ingest():
    preflight = SQLExecuteQueryOperator(
        task_id="preflight_db",
        conn_id=PG_CONN_ID,
        # A check, not a migration: Alembic is a deploy step. This fails fast
        # and cheaply when the database is unreachable or was never migrated,
        # instead of after a crawl has already spent twenty minutes.
        sql="SELECT 1 FROM alembic_version",
        retries=1,
    )

    crawl = worker(
        "crawl",
        "crawl",
        # `feed_state` is the source of truth for scheduled ingestion. Feed
        # files are only an explicit, one-off seeding mechanism; coupling this
        # task to one would make a deleted OPML file stop every crawl run.
        # Relative, not the DAG's data_interval: an explicit window would be
        # recorded as a backfill, and feeds only retain recent entries anyway.
        "--last",
        "12h",
        pool="news_crawl",
        timeout=timedelta(minutes=8),
    )

    preflight >> crawl


news_ingest()
