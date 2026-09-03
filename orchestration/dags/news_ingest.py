"""RSS/crawl ingestion: the only DAG that talks to the outside network.

It is kept separate from `news_semantic` so a broken feed host cannot mask a
classifier failure, and so the two can run at their own natural rates.
"""

from __future__ import annotations

from datetime import timedelta

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import Asset, dag, task
from airflow.sdk.exceptions import AirflowSkipException

from _common import FEED_PATH, JOBS_ASSET_URI, PG_CONN_ID, dag_defaults, worker


JOBS = Asset(JOBS_ASSET_URI)


@dag(
    schedule="*/30 * * * *",
    dagrun_timeout=timedelta(minutes=25),
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
        "--feed-path",
        FEED_PATH,
        # Relative, not the DAG's data_interval: an explicit window would be
        # recorded as a backfill, and feeds only retain recent entries anyway.
        "--last",
        "12h",
        pool="news_crawl",
        timeout=timedelta(minutes=20),
    )

    @task(outlets=[JOBS])
    def queue_gate() -> int:
        """Emit the asset only when there is actually work downstream.

        Skipping here is what keeps `news_semantic` from loading MiniLM to
        find an empty queue. A crawl run that wrote nothing new is the normal
        case at a 30-minute cadence, not an error.
        """
        pending = PostgresHook(postgres_conn_id=PG_CONN_ID).get_first(
            "SELECT count(*) FROM processing_jobs "
            "WHERE stage = 'embed' AND state = 'pending' AND available_at <= now()"
        )[0]
        if not pending:
            raise AirflowSkipException("no embed jobs are pending")
        return int(pending)

    preflight >> crawl >> queue_gate()


news_ingest()
