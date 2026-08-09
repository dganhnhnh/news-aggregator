# Project guide

## Purpose

News Radar searches and crawls web articles directly or through RSS/Atom feeds
from TSV/OPML. Persisted jobs keep only the latest normalized Markdown article
in PostgreSQL and record current failures plus per-run operational summaries.

## Structure and design

- `src/pipelines/news.py`: one-shot `news-pipeline` commands. `crawl` and `rss`
  persist only with `--persist`; Tavily `search` never persists.
- `src/services/news.py`: periodic `news-service`; always requires PostgreSQL,
  retries due failures before fresh feed ingestion, and emits JSONL only with
  `--emit-jsonl`.
- `src/adapters/retrieve_news_data/`: Tavily, crawl4ai, and RSS/OPML adapters.
- `src/services/raw_storage.py` and `ingestion.py`: normalization, hashing,
  batch deduplication, persistence orchestration, and retry handling.
- `src/adapters/storage/`: async SQLAlchemy/asyncpg repository and schema.
- PostgreSQL has three tables: `raw_articles` (latest version only),
  `crawl_failures` (current durable retry queue), and `ingest_runs` (monitoring).
  Raw HTML and historical article versions are intentionally not stored.
- PostgreSQL 16 runs through `compose.yaml` with named volume
  `news_radar_postgres_data`; host-side CLI jobs run Alembic explicitly.

## Development notes

- Keep domain models independent from I/O adapters. Preserve structured
  feed/article failures instead of only logging and dropping errors.
- Article identity is the normalized final URL. Upserts must preserve `id`,
  avoid updates when content/metadata are unchanged, and never erase existing
  metadata with empty incoming values.
- Retry assumes one `news-service` instance; multi-worker claiming is out of
  scope until explicitly implemented.
- Keep RSS resource usage bounded: fetch only `MAX_CONCURRENT_FEEDS` at once,
  crawl at most `MAX_CONCURRENT_CRAWLS` pages concurrently, and retain at most
  `CRAWL_BATCH_SIZE` crawl results before persistence. Do not create tasks for
  every feed/article in a run or open one browser per article.
- Persistence paths must consume `iter_batches()`, commit and checkpoint each
  chunk, and preserve completed chunks if a later chunk is interrupted.
- Keep limited-feed runs rotating through the source list; use
  `RSS_FEED_OFFSET`/`--feed-offset` when a deterministic starting point matters.
- Add focused unit tests and PostgreSQL integration tests for storage changes.
  Run `uv run --extra dev pytest`; integration tests use `TEST_DATABASE_URL`.

### Alembic migrations

- Treat `src/adapters/storage/schema.py` as the schema source of truth.
- Create revisions only with
  `uv run alembic revision --autogenerate -m "<short description>"` against a
  database already at head. Never hand-write the file or pass `--rev-id`; keep
  Alembic's generated hexadecimal prefix.
- Review generated operations, especially renames and data migrations. Never
  rewrite an already-applied migration; create a new revision.
- Validate with `uv run alembic upgrade head` and `uv run alembic check`.
