"""SQLAlchemy Core schema shared by the repository and Alembic."""

from pgvector.sqlalchemy import Vector
from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    Computed,
    DateTime,
    ForeignKey,
    Identity,
    Index,
    Integer,
    LargeBinary,
    MetaData,
    PrimaryKeyConstraint,
    Table,
    Text,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB, TSVECTOR


metadata = MetaData()

sources = Table(
    "sources",
    metadata,
    Column("id", BigInteger, Identity(), primary_key=True),
    Column("slug", Text, nullable=False),
    Column("name", Text, nullable=False),
    Column("homepage", Text),
    Column("default_domain", Text),
    Column("active", Boolean, nullable=False, server_default="true"),
    Column(
        "first_seen_at",
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    ),
    UniqueConstraint("slug", name="uq_sources_slug"),
)

raw_articles = Table(
    "raw_articles",
    metadata,
    Column("id", BigInteger, Identity(), primary_key=True),
    Column(
        "source_id",
        BigInteger,
        ForeignKey("sources.id", name="fk_raw_articles_source_id_sources"),
    ),
    Column("url", Text, nullable=False),
    Column("url_original", Text),
    Column("title", Text),
    Column("source", Text),
    Column("content", Text, nullable=False),
    Column("content_hash", LargeBinary, nullable=False),
    Column("published_at", DateTime(timezone=True)),
    Column("crawled_at", DateTime(timezone=True), nullable=False),
    Column("updated_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    Column("lang", Text),
    Column(
        "event_at",
        DateTime(timezone=True),
        Computed("coalesce(published_at, crawled_at)", persisted=True),
        nullable=False,
    ),
    Column(
        "search_tsv",
        TSVECTOR,
        Computed(
            "to_tsvector('english', coalesce(title, '') || ' ' || left(content, 200000))",
            persisted=True,
        ),
    ),
    UniqueConstraint("url", name="uq_raw_articles_url"),
)
Index("ix_raw_articles_event_at_brin", raw_articles.c.event_at, postgresql_using="brin")
Index("ix_raw_articles_search_tsv", raw_articles.c.search_tsv, postgresql_using="gin")
Index("ix_raw_articles_source_event", raw_articles.c.source_id, raw_articles.c.event_at)
Index("ix_raw_articles_url_original", raw_articles.c.url_original)

crawl_failures = Table(
    "crawl_failures",
    metadata,
    Column("failure_kind", Text, nullable=False),
    Column("url", Text, nullable=False),
    Column("source", Text),
    Column("title", Text),
    Column("published_at", DateTime(timezone=True)),
    Column("error_type", Text, nullable=False),
    Column("error_message", Text, nullable=False),
    Column("attempt_count", Integer, nullable=False),
    Column("first_failed_at", DateTime(timezone=True), nullable=False),
    Column("last_failed_at", DateTime(timezone=True), nullable=False),
    Column("next_retry_at", DateTime(timezone=True), nullable=False),
    Column("dead_lettered_at", DateTime(timezone=True)),
    PrimaryKeyConstraint("failure_kind", "url", name="pk_crawl_failures"),
)
Index("ix_crawl_failures_next_retry_at", crawl_failures.c.next_retry_at)

worker_runs = Table(
    "worker_runs",
    metadata,
    Column("id", BigInteger, Identity(), primary_key=True),
    Column("worker", Text, nullable=False),
    Column("trigger", Text, nullable=False),
    Column("status", Text, nullable=False),
    Column("started_at", DateTime(timezone=True), nullable=False),
    Column("finished_at", DateTime(timezone=True)),
    Column("heartbeat_at", DateTime(timezone=True), nullable=False),
    Column("window_start", DateTime(timezone=True)),
    Column("window_end", DateTime(timezone=True)),
    Column("params", JSONB, nullable=False, server_default=text("'{}'::jsonb")),
    Column("stats", JSONB, nullable=False, server_default=text("'{}'::jsonb")),
    Column("error_message", Text),
)
Index(
    "ix_worker_runs_worker_started",
    worker_runs.c.worker,
    worker_runs.c.started_at.desc(),
)
# Partial index: the reaper only ever scans rows still marked running, and that
# set stays tiny while the table grows without bound.
Index(
    "ix_worker_runs_running",
    worker_runs.c.heartbeat_at,
    postgresql_where=text("status = 'running'"),
)

feed_state = Table(
    "feed_state",
    metadata,
    Column("feed_url", Text, primary_key=True),
    Column("source_label", Text),
    # Where a permanent (301/308) redirect actually lands. `feed_url` stays the
    # identity from the OPML/TSV file so re-seeding cannot resurrect a URL we
    # already know has moved; polls follow `resolved_url` when it is set.
    Column("resolved_url", Text),
    Column("etag", Text),
    Column("last_modified", Text),
    Column("last_polled_at", DateTime(timezone=True)),
    Column("last_success_at", DateTime(timezone=True)),
    Column("last_entry_published_at", DateTime(timezone=True)),
    Column("consecutive_failures", Integer, nullable=False, server_default="0"),
    Column(
        "next_poll_at",
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    ),
    Column("disabled_at", DateTime(timezone=True)),
    Column("disabled_reason", Text),
    Column(
        "updated_at",
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    ),
)
# Feeds with disabled_at set never appear in the "what's due" query, so they
# don't need to be in the index.
Index(
    "ix_feed_state_next_poll",
    feed_state.c.next_poll_at,
    postgresql_where=text("disabled_at IS NULL"),
)

processing_jobs = Table(
    "processing_jobs",
    metadata,
    Column(
        "article_id",
        BigInteger,
        ForeignKey("raw_articles.id", ondelete="CASCADE"),
        nullable=False,
    ),
    Column("stage", Text, nullable=False),
    Column("state", Text, nullable=False, server_default="pending"),
    Column("attempts", Integer, nullable=False, server_default="0"),
    Column(
        "available_at",
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    ),
    Column("lease_until", DateTime(timezone=True)),
    Column("locked_by", Text),
    Column("last_error", Text),
    Column(
        "updated_at",
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    ),
    PrimaryKeyConstraint("article_id", "stage", name="pk_processing_jobs"),
)
# Partial: only pending rows are ever scanned by claim(), so the index stays
# small no matter how many rows have already reached 'done'.
Index(
    "ix_processing_jobs_ready",
    processing_jobs.c.stage,
    processing_jobs.c.available_at,
    postgresql_where=text("state = 'pending'"),
)
Index(
    "ix_processing_jobs_leased",
    processing_jobs.c.lease_until,
    postgresql_where=text("state = 'running'"),
)

article_embeddings = Table(
    "article_embeddings",
    metadata,
    Column(
        "article_id",
        BigInteger,
        ForeignKey("raw_articles.id", ondelete="CASCADE"),
        primary_key=True,
    ),
    Column("model", Text, nullable=False),
    Column("dim", Integer, nullable=False),
    Column("content_hash", LargeBinary, nullable=False),
    Column("embedding", Vector(384), nullable=False),
    Column(
        "created_at",
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    ),
)
# The HNSW index is created by hand in the migration with `op.execute` --
# autogenerate cannot render pgvector's access method or operator class, see
# week1-foundation.md §4.6.
