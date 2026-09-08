"""SQLAlchemy Core schema shared by the repository and Alembic."""

from pgvector.sqlalchemy import Vector
from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    Computed,
    DateTime,
    Float,
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
    # The feed's own dek, rendered to Markdown. Stored for every article, not
    # just ones that failed to crawl: it is the preview text, and for sources
    # whose pages crawl into navigation chrome it is the better embedding
    # input. Empty when the feed shipped no usable summary.
    Column("feed_summary", Text, nullable=False, server_default=""),
    # Where `content` came from: 'feed_content' (the feed shipped the body),
    # 'crawl' (a page was fetched), or 'feed_summary' (neither — the row exists
    # only to carry the dek of an article whose page could not be crawled, and
    # `content` is empty).
    Column("content_source", Text, nullable=False, server_default="crawl"),
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
            # feed_summary is part of the document, not an afterthought: a
            # summary-only row has no content at all, and would otherwise be
            # invisible to search.
            "to_tsvector('english', coalesce(title, '') || ' ' "
            "|| left(content, 200000) || ' ' || coalesce(feed_summary, ''))",
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
    Column("priority", Integer, nullable=False, server_default="1"),
    Column("error_type", Text, nullable=False),
    Column("error_message", Text, nullable=False),
    Column("attempt_count", Integer, nullable=False),
    Column("first_failed_at", DateTime(timezone=True), nullable=False),
    Column("last_failed_at", DateTime(timezone=True), nullable=False),
    Column("next_retry_at", DateTime(timezone=True), nullable=False),
    Column("dead_lettered_at", DateTime(timezone=True)),
    PrimaryKeyConstraint("failure_kind", "url", name="pk_crawl_failures"),
)
Index(
    "ix_crawl_failures_next_retry_at",
    crawl_failures.c.priority,
    crawl_failures.c.next_retry_at,
)

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
    # Smaller values are processed first. 0 is the continuously-polled tier;
    # the existing scheduled feed set defaults to normal priority 1.
    Column("priority", Integer, nullable=False, server_default="1"),
    # Where a permanent (301/308) redirect actually lands. `feed_url` stays the
    # identity from the OPML/TSV file so re-seeding cannot resurrect a URL we
    # already know has moved; polls follow `resolved_url` when it is set.
    Column("resolved_url", Text),
    Column("etag", Text),
    Column("last_modified", Text),
    Column("last_polled_at", DateTime(timezone=True)),
    Column("last_success_at", DateTime(timezone=True)),
    Column("last_entry_published_at", DateTime(timezone=True)),
    # NULL means the feed has not yet had its one unconditional cadence
    # bootstrap fetch. Afterwards 304 responses reuse this persisted tier.
    Column("poll_interval_seconds", Integer),
    Column("observed_publish_gap_seconds", Float),
    Column(
        "observed_publish_gap_count", Integer, nullable=False, server_default="0"
    ),
    Column("cadence_checked_at", DateTime(timezone=True)),
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
    # Propagated from the feed that caused the write. Queue consumers sort
    # ascending so priority 0 work is claimed before priority 1.
    Column("priority", Integer, nullable=False, server_default="1"),
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
    processing_jobs.c.priority,
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
    # Hash of the text actually embedded, not of `raw_articles.content`. The
    # two differ whenever the fallback chain picks the feed summary, and only
    # this one answers the question the embed worker asks: has the input to the
    # model changed since last time?
    Column("content_hash", LargeBinary, nullable=False),
    # Which link in that chain won: 'content', 'feed_summary' or 'title'.
    # Vectors built from a 1200-character body and from a 100-character dek are
    # not equally placed, so cluster quality has to be readable per kind.
    Column("text_kind", Text, nullable=False, server_default="content"),
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

clusters = Table(
    "clusters",
    metadata,
    Column("id", BigInteger, Identity(), primary_key=True),
    # Store the exact arithmetic mean, not an L2-normalized running vector.
    # pgvector's cosine operator normalizes at comparison time, while keeping
    # the mean unnormalized lets `(centroid * n + article) / (n + 1)` remain
    # exact and independent of article arrival order.
    Column("centroid", Vector(384), nullable=False),
    Column("centroid_count", Integer, nullable=False),
    Column("first_seen_at", DateTime(timezone=True), nullable=False),
    Column("last_seen_at", DateTime(timezone=True), nullable=False),
    Column("n_articles", Integer, nullable=False),
    Column("n_distinct_sources", Integer, nullable=False),
    Column("headline", Text),
    Column("status", Text, nullable=False, server_default="open"),
    Column(
        "merged_into_id",
        BigInteger,
        ForeignKey("clusters.id", name="fk_clusters_merged_into_id_clusters"),
    ),
)
Index(
    "ix_clusters_open_last_seen",
    clusters.c.last_seen_at,
    postgresql_where=text("status = 'open'"),
)

cluster_members = Table(
    "cluster_members",
    metadata,
    Column(
        "cluster_id",
        BigInteger,
        ForeignKey("clusters.id", ondelete="CASCADE"),
        nullable=False,
    ),
    Column(
        "article_id",
        BigInteger,
        ForeignKey("raw_articles.id", ondelete="CASCADE"),
        nullable=False,
    ),
    # Similarity at assignment time. The cluster centroid may move later, so
    # this is audit data rather than a promise of current similarity.
    Column("similarity", Float, nullable=False),
    Column(
        "added_at",
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    ),
    PrimaryKeyConstraint("cluster_id", "article_id"),
    UniqueConstraint("article_id", name="uq_cluster_members_article"),
)

topics = Table(
    "topics",
    metadata,
    Column("id", BigInteger, Identity(), primary_key=True),
    Column("slug", Text, nullable=False),
    Column("display_name", Text, nullable=False),
    Column("description", Text),
    # 0.40, not the 0.55 this started at: measured against the 1015-article
    # corpus on 2026-08-28, 0.55 labelled 1.0% of articles while the highest
    # score any topic reached was 0.615. See docs/measurements.md "T6".
    Column("threshold", Float, nullable=False, server_default="0.40"),
    # Only exemplar/keyword/weight/threshold changes bump this; it is what
    # marks every existing article_topics/article_classifications row using
    # the old scoring inputs as stale. Presentation-only edits (display_name,
    # taxonomy placement) never touch it.
    Column("classifier_revision", Integer, nullable=False, server_default="1"),
    # A topic without at least three active exemplars cannot be activated --
    # there is nothing to average into a usable prototype.
    Column("active", Boolean, nullable=False, server_default="false"),
    Column(
        "updated_at",
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    ),
    UniqueConstraint("slug", name="uq_topics_slug"),
)

topic_signals = Table(
    "topic_signals",
    metadata,
    Column("id", BigInteger, Identity(), primary_key=True),
    Column(
        "topic_id",
        BigInteger,
        ForeignKey("topics.id", ondelete="CASCADE"),
        nullable=False,
    ),
    Column("kind", Text, nullable=False),  # exemplar | keyword
    Column("value", Text, nullable=False),
    Column("weight", Float, nullable=False, server_default="1"),
    Column("language", Text),
    Column("active", Boolean, nullable=False, server_default="true"),
    UniqueConstraint(
        "topic_id", "kind", "value", name="uq_topic_signals_topic_kind_value"
    ),
)

topic_prototypes = Table(
    "topic_prototypes",
    metadata,
    Column(
        "topic_id",
        BigInteger,
        ForeignKey("topics.id", ondelete="CASCADE"),
        primary_key=True,
    ),
    Column("model", Text, nullable=False),
    Column("dim", Integer, nullable=False),
    # Hash of the active exemplar set this vector was built from. A prototype
    # is only valid when this matches the topic's current exemplars; a
    # mismatch means `taxonomy-rebuild` has not caught up with the last edit.
    Column("definition_hash", LargeBinary, nullable=False),
    Column("embedding", Vector(384), nullable=False),
    Column(
        "generated_at",
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    ),
)

taxonomies = Table(
    "taxonomies",
    metadata,
    Column("id", BigInteger, Identity(), primary_key=True),
    Column("slug", Text, nullable=False),
    Column("display_name", Text, nullable=False),
    Column("revision", Integer, nullable=False, server_default="1"),
    Column("active", Boolean, nullable=False, server_default="true"),
    UniqueConstraint("slug", name="uq_taxonomies_slug"),
)

taxonomy_nodes = Table(
    "taxonomy_nodes",
    metadata,
    Column("id", BigInteger, Identity(), primary_key=True),
    Column(
        "taxonomy_id",
        BigInteger,
        ForeignKey("taxonomies.id", ondelete="CASCADE"),
        nullable=False,
    ),
    Column(
        "parent_id",
        BigInteger,
        ForeignKey("taxonomy_nodes.id", name="fk_taxonomy_nodes_parent_id_taxonomy_nodes"),
    ),
    Column("slug", Text, nullable=False),
    Column("display_name", Text, nullable=False),
    Column("sort_order", Integer, nullable=False, server_default="0"),
    UniqueConstraint(
        "taxonomy_id",
        "parent_id",
        "slug",
        name="uq_taxonomy_nodes_taxonomy_parent_slug",
    ),
)
# UniqueConstraint above cannot enforce root-level sibling uniqueness because
# Postgres treats NULL parent_id values as distinct from one another.
Index(
    "uq_taxonomy_nodes_root_slug",
    taxonomy_nodes.c.taxonomy_id,
    taxonomy_nodes.c.slug,
    unique=True,
    postgresql_where=taxonomy_nodes.c.parent_id.is_(None),
)

taxonomy_node_topics = Table(
    "taxonomy_node_topics",
    metadata,
    Column(
        "node_id",
        BigInteger,
        ForeignKey("taxonomy_nodes.id", ondelete="CASCADE"),
        nullable=False,
    ),
    Column(
        "topic_id",
        BigInteger,
        ForeignKey("topics.id", ondelete="CASCADE"),
        nullable=False,
    ),
    PrimaryKeyConstraint("node_id", "topic_id"),
)

article_classifications = Table(
    "article_classifications",
    metadata,
    Column(
        "article_id",
        BigInteger,
        ForeignKey("raw_articles.id", ondelete="CASCADE"),
        primary_key=True,
    ),
    Column("model", Text, nullable=False),
    # Fingerprint of the whole active classifier set (each topic's slug,
    # threshold, revision and prototype provenance). A row whose hash differs
    # from the current set was scored against a classifier that no longer
    # exists, which is exactly the stale-detection predicate `--stale` uses.
    Column("classifier_set_hash", LargeBinary, nullable=False),
    Column("max_score", Float, nullable=False),
    Column("assigned_count", Integer, nullable=False),
    Column(
        "classified_at",
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    ),
)

article_topics = Table(
    "article_topics",
    metadata,
    Column(
        "article_id",
        BigInteger,
        ForeignKey("raw_articles.id", ondelete="CASCADE"),
        nullable=False,
    ),
    Column(
        "topic_id",
        BigInteger,
        ForeignKey("topics.id", ondelete="CASCADE"),
        nullable=False,
    ),
    Column("score", Float, nullable=False),
    Column("prototype_score", Float, nullable=False),
    Column("keyword_score", Float, nullable=False),
    # Reserved for week 3+. Week 2 never learns a source prior -- training one
    # on the classifier's own predictions is self-reinforcing -- so the column
    # stays NULL and carries weight 0 in the score.
    Column("source_prior", Float),
    Column("assigned", Boolean, nullable=False),
    Column("classifier_revision", Integer, nullable=False),
    PrimaryKeyConstraint("article_id", "topic_id"),
)
# Week 2 stores a row per active topic, not just the ones over threshold, so
# aggregation and evaluation can see near-misses. Only assigned rows are ever
# queried by topic, so the index stays partial.
Index(
    "ix_article_topics_assigned",
    article_topics.c.topic_id,
    article_topics.c.article_id,
    postgresql_where=text("assigned"),
)

cluster_topics = Table(
    "cluster_topics",
    metadata,
    Column(
        "cluster_id",
        BigInteger,
        ForeignKey("clusters.id", ondelete="CASCADE"),
        nullable=False,
    ),
    Column(
        "topic_id",
        BigInteger,
        ForeignKey("topics.id", ondelete="CASCADE"),
        nullable=False,
    ),
    Column("score", Float, nullable=False),
    Column("assigned", Boolean, nullable=False),
    Column(
        "updated_at",
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    ),
    PrimaryKeyConstraint("cluster_id", "topic_id"),
)
Index(
    "ix_cluster_topics_assigned",
    cluster_topics.c.topic_id,
    cluster_topics.c.cluster_id,
    postgresql_where=text("assigned"),
)
