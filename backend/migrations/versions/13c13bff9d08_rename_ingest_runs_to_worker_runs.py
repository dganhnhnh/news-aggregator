"""rename ingest runs to worker runs

Revision ID: 13c13bff9d08
Revises: f28c6b33f838
Create Date: 2026-08-17 22:46:14.839576

Autogenerate proposed DROP TABLE ingest_runs + CREATE TABLE worker_runs because
Alembic cannot infer a rename. That would discard the operational history, so
the body below was rewritten as a rename plus a data migration.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision: str = '13c13bff9d08'
down_revision: Union[str, Sequence[str], None] = 'f28c6b33f838'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


_LEGACY_COUNTERS = (
    "feeds_processed",
    "articles_discovered",
    "crawl_succeeded",
    "crawl_failed",
    "retries_attempted",
    "retries_succeeded",
    "articles_written",
    "articles_unchanged",
)


def upgrade() -> None:
    op.rename_table('ingest_runs', 'worker_runs')

    # Added nullable first because the table already holds rows; the NOT NULL
    # constraints go on after the backfill below.
    op.add_column('worker_runs', sa.Column('worker', sa.Text(), nullable=True))
    op.add_column('worker_runs', sa.Column('trigger', sa.Text(), nullable=True))
    op.add_column('worker_runs', sa.Column('heartbeat_at', sa.DateTime(timezone=True), nullable=True))
    op.add_column('worker_runs', sa.Column('window_start', sa.DateTime(timezone=True), nullable=True))
    op.add_column('worker_runs', sa.Column('window_end', sa.DateTime(timezone=True), nullable=True))
    op.add_column('worker_runs', sa.Column('params', postgresql.JSONB(astext_type=sa.Text()), server_default=sa.text("'{}'::jsonb"), nullable=False))
    op.add_column('worker_runs', sa.Column('stats', postgresql.JSONB(astext_type=sa.Text()), server_default=sa.text("'{}'::jsonb"), nullable=False))

    # Every historic run was a crawl. heartbeat_at is seeded from the run's own
    # timestamps so the reaper never revisits rows that predate heartbeats.
    op.execute(
        """
        UPDATE worker_runs SET
          worker  = 'crawl',
          trigger = CASE WHEN job_type = 'service' THEN 'scheduled' ELSE 'manual' END,
          heartbeat_at = coalesce(finished_at, started_at),
          params = jsonb_build_object('legacy_job_type', job_type),
          stats  = jsonb_build_object(
            'feeds_processed',     feeds_processed,
            'articles_discovered', articles_discovered,
            'crawl_succeeded',     crawl_succeeded,
            'crawl_failed',        crawl_failed,
            'retries_attempted',   retries_attempted,
            'retries_succeeded',   retries_succeeded,
            'articles_written',    articles_written,
            'articles_unchanged',  articles_unchanged)
        """
    )

    for column in ('worker', 'trigger', 'heartbeat_at'):
        op.alter_column('worker_runs', column, nullable=False)

    op.drop_column('worker_runs', 'job_type')
    for column in _LEGACY_COUNTERS:
        op.drop_column('worker_runs', column)

    op.create_index('ix_worker_runs_running', 'worker_runs', ['heartbeat_at'], unique=False, postgresql_where=sa.text("status = 'running'"))
    op.create_index('ix_worker_runs_worker_started', 'worker_runs', ['worker', sa.literal_column('started_at DESC')], unique=False)


def downgrade() -> None:
    op.drop_index('ix_worker_runs_worker_started', table_name='worker_runs')
    op.drop_index('ix_worker_runs_running', table_name='worker_runs', postgresql_where=sa.text("status = 'running'"))

    op.add_column('worker_runs', sa.Column('job_type', sa.Text(), nullable=True))
    for column in _LEGACY_COUNTERS:
        op.add_column('worker_runs', sa.Column(column, sa.Integer(), server_default='0', nullable=False))

    counters = ",\n          ".join(
        f"{column} = coalesce((stats->>'{column}')::int, 0)"
        for column in _LEGACY_COUNTERS
    )
    op.execute(
        f"""
        UPDATE worker_runs SET
          job_type = coalesce(params->>'legacy_job_type', worker),
          {counters}
        """
    )
    # A run recorded after the upgrade may have been abandoned, a status the
    # old table never knew about; partial is its closest equivalent.
    op.execute("UPDATE worker_runs SET status = 'partial' WHERE status = 'abandoned'")
    op.alter_column('worker_runs', 'job_type', nullable=False)

    for column in ('stats', 'params', 'window_end', 'window_start', 'heartbeat_at', 'trigger', 'worker'):
        op.drop_column('worker_runs', column)

    op.rename_table('worker_runs', 'ingest_runs')
