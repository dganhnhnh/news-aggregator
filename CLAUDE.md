# Project guide

## Purpose

News Radar searches and crawls web articles directly or through RSS/Atom feeds
from TSV/OPML. Persisted jobs keep only the latest normalized Markdown article
in PostgreSQL and record current failures plus per-run operational summaries.
A read-only FastAPI app exposes that data; `frontend/` is a placeholder for a
future Next.js UI.

## Structure and design

- `backend/`: single Python package (setuptools `where = ["backend"]`); all
  internal imports stay flat (`from core...`, `from adapters...`), matching
  `alembic.ini`'s `prepend_sys_path = backend`.
  - `pipelines/news.py`: one-shot `news-pipeline` commands. `crawl` and `rss`
    persist only with `--persist`; Tavily `search` never persists.
  - `workers/`: `news-worker`, the entry point for every worker. `base.py`
    owns the operational frame (advisory lock, `worker_runs` bookkeeping,
    heartbeats, shutdown signals, exit codes); a worker subclass implements
    only `execute()`. `window.py` parses `--last`/`--since`/`--until` into a
    shared `RunWindow`; `crawl.py` is the RSS/crawl worker.
  - `services/news.py`: deprecated `news-service` alias that warns, renames
    `--interval-seconds` to `--interval`, and delegates to `news-worker crawl`.
    Keep it thin; there must stay exactly one orchestration code path.
  - `adapters/retrieve_news_data/`: Tavily, crawl4ai, and RSS/OPML adapters.
  - `core/`: I/O-free policy shared by every layer — `urls.py`,
    `html_content.py` (feed HTML → Markdown), `content_quality.py`
    (block-page and nav-density verdicts).
  - `services/raw_storage.py` and `ingestion.py`: normalization, hashing,
    batch deduplication, persistence orchestration, and retry handling.
  - `adapters/storage/`: async SQLAlchemy/asyncpg repository and schema.
  - `api/`: `news-api` FastAPI app (`main.py`, `deps.py`, `routers/`); reuses
    `PostgresRawStorage` for read-only endpoints, never writes.
  - `migrations/`: Alembic environment and revisions.
  - `tests/`: unit and PostgreSQL integration tests.
- `frontend/`: empty placeholder for a future Next.js app; see
  `frontend/README.md` for the scaffold command.
- `orchestration/dags/`: Airflow DAGs, deliberately outside `backend/` so
  setuptools does not package them. `_common.py` is the only module that names
  an operator — every task is one `news-worker` argv, so swapping
  `DockerOperator` for something else must not touch a DAG file. See
  `docs/orchestration-airflow.md`.
- `deployments/`: `docker-compose.yml` (PostgreSQL only), `Dockerfile.api`,
  `Dockerfile.worker` (adds Chromium and makes `news-worker` PID 1 so SIGTERM
  reaches it) and `docker-compose.airflow.yml` (Airflow on its own metadata
  database, never the application one).
- PostgreSQL groups its tables in four layers: ingestion (`raw_articles`,
  latest version only; `sources`; `crawl_failures`, the current durable retry
  queue; `worker_runs`, one operational log for every worker with JSONB
  `params`/`stats` so a new worker needs no migration), embedding/clustering
  (`article_embeddings`, `processing_jobs`, `clusters`, `cluster_members`),
  the topic registry (`topics`, `topic_signals`, `topic_prototypes`,
  `taxonomies`, `taxonomy_nodes`, `taxonomy_node_topics`) and classification
  (`article_classifications`, `article_topics`, `cluster_topics`). Raw HTML
  and historical article versions are intentionally not stored.
- `cluster_topics` is derived data with no source of truth of its own: it is
  always fully replaced from `article_topics` by
  `_refresh_cluster_topics_locked`, which classify, cluster attach, merge and
  repair all route through so a story's label cannot depend on which worker
  touched it last. `news-worker classify --refresh-clusters` rebuilds it.
- The week-2 classification weights (`0.80` prototype / `0.20` keyword) go into
  `worker_runs.params` every run and have not been retuned. The default
  threshold has: `0.55` → `0.40` on 2026-08-28, because 0.55 labelled 1.0% of
  the corpus while no topic ever scored above 0.615. Any further change needs
  the same treatment — a sweep and a snapshot in `docs/measurements.md`, not a
  nudge to make a golden case pass. The full sweep belongs to week 4.
- PostgreSQL 16 runs through `deployments/docker-compose.yml` with named volume
  `news_radar_postgres_data`; host-side CLI jobs run Alembic explicitly. Always
  pass `--env-file .env` since the compose file no longer sits next to it.

## Development notes

- Keep domain models independent from I/O adapters. Preserve structured
  feed/article failures instead of only logging and dropping errors.
- All URL rules live in `core/urls.py`, which is I/O-free so any layer can
  import it. Keep the two concerns separate: `resolve_crawl_url` enforces
  structural validity (absolute http/https) and every path into the crawler
  applies it, while `is_probably_article` is discovery-only policy, so an
  explicit request for a PDF is still honoured. A `UnusableUrl` becomes a
  `permanent` failure — retrying a malformed URL can never change the outcome.
- Article identity is the normalized final URL. Upserts must preserve `id`,
  avoid updates when content/metadata are unchanged, and never erase existing
  metadata with empty incoming values. That last rule now covers `content`
  too: a summary-only row must never blank a body an earlier crawl won.
- An article has up to three texts, and `raw_articles.content_source`
  (`feed_content` | `crawl` | `feed_summary`) records where `content` came
  from. `feed_summary` is captured from every feed entry regardless of whether
  the crawl succeeded, because the entry drops out of the feed long before the
  retry queue gives up. A failed crawl with a dek is therefore a stored row
  with empty `content` — it does not resolve the crawl failure.
- `services/text.py:article_text` is the only place that decides which text
  represents an article; embed, cluster, and anything after them must call it
  instead of re-ranking the columns. List previews rank differently (dek
  first) and live in `PostgresRawStorage._excerpt_source`.
- `core/content_quality.py` is I/O-free page policy applied to rendered
  Markdown, complementing crawl4ai's HTML-level anti-bot detection. Keep its
  two verdicts distinct: a block page is *rejected* (never persisted, recorded
  as a retryable `BlockedPage` crawl failure), while nav-dense output is only
  *demoted* (the row is kept; `article_text` prefers the dek). Rejection rules
  must stay conservative enough that a real article *about* captchas survives.
- One run of a given worker at a time, enforced by `PostgresRawStorage.
  worker_lock`; a run that cannot take the lock exits 0 without doing work.
  Retry therefore assumes a single active crawl run; per-row claiming for
  parallel workers is out of scope until explicitly implemented.
- Worker plumbing belongs in `workers/base.py`, not in a worker subclass:
  check `ctx.shutdown` at chunk boundaries, report progress with
  `ctx.checkpoint()` (which doubles as the heartbeat), and return a
  `RunOutcome` instead of calling `finish_run` directly.
- Keep RSS resource usage bounded: fetch only `MAX_CONCURRENT_FEEDS` at once,
  crawl at most `MAX_CONCURRENT_CRAWLS` pages concurrently, and retain at most
  `CRAWL_BATCH_SIZE` crawl results before persistence. Do not create tasks for
  every feed/article in a run or open one browser per article.
- Persistence paths must consume `iter_batches()`, commit and checkpoint each
  chunk, and preserve completed chunks if a later chunk is interrupted.
- Keep limited-feed runs rotating through the source list; use
  `RSS_FEED_OFFSET`/`--feed-offset` when a deterministic starting point matters.
- The API is read-only: it must never write to `raw_articles`,
  `crawl_failures`, or `worker_runs`. Add new read methods to
  `PostgresRawStorage` rather than querying tables directly from routers.
- Add focused unit tests and PostgreSQL integration tests for storage changes.
  Run `uv run --extra dev pytest`; integration tests use `TEST_DATABASE_URL`.

### Alembic migrations

- Treat `backend/adapters/storage/schema.py` as the schema source of truth.
- Create revisions only with
  `uv run alembic revision --autogenerate -m "<short description>"` against a
  database already at head. Never hand-write the file or pass `--rev-id`; keep
  Alembic's generated hexadecimal prefix.
- Review generated operations, especially renames and data migrations. Never
  rewrite an already-applied migration; create a new revision.
- Validate with `uv run alembic upgrade head` and `uv run alembic check`.
