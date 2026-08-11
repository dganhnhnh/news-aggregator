"""SQLAlchemy Core schema shared by the repository and Alembic."""

from sqlalchemy import (
    BigInteger,
    Column,
    DateTime,
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
)


metadata = MetaData()

raw_articles = Table(
    "raw_articles",
    metadata,
    Column("id", BigInteger, Identity(), primary_key=True),
    Column("url", Text, nullable=False),
    Column("title", Text),
    Column("source", Text),
    Column("content", Text, nullable=False),
    Column("content_hash", LargeBinary, nullable=False),
    Column("published_at", DateTime(timezone=True)),
    Column("crawled_at", DateTime(timezone=True), nullable=False),
    Column("updated_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    UniqueConstraint("url", name="uq_raw_articles_url"),
)

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
    PrimaryKeyConstraint("failure_kind", "url", name="pk_crawl_failures"),
)
Index("ix_crawl_failures_next_retry_at", crawl_failures.c.next_retry_at)

ingest_runs = Table(
    "ingest_runs",
    metadata,
    Column("id", BigInteger, Identity(), primary_key=True),
    Column("job_type", Text, nullable=False),
    Column("status", Text, nullable=False),
    Column("started_at", DateTime(timezone=True), nullable=False),
    Column("finished_at", DateTime(timezone=True)),
    Column("feeds_processed", Integer, nullable=False, server_default="0"),
    Column("articles_discovered", Integer, nullable=False, server_default="0"),
    Column("crawl_succeeded", Integer, nullable=False, server_default="0"),
    Column("crawl_failed", Integer, nullable=False, server_default="0"),
    Column("retries_attempted", Integer, nullable=False, server_default="0"),
    Column("retries_succeeded", Integer, nullable=False, server_default="0"),
    Column("articles_written", Integer, nullable=False, server_default="0"),
    Column("articles_unchanged", Integer, nullable=False, server_default="0"),
    Column("error_message", Text),
)
