# News Radar

News Radar collects web articles, organizes them into topics and stories, and
makes them searchable through a FastAPI backend and Next.js frontend.

## Features

- Crawl pages directly or ingest RSS/Atom feeds from OPML and TSV files.
- Store the latest article as Markdown in PostgreSQL, deduplicated by URL.
- Generate embeddings, classify topics, and group related articles into stories.
- Browse articles and stories, manage feeds, and ask an AI research assistant
questions with streaming answers and article citations.
- Run continuous ingestion and semantic workers with retries, checkpoints, and
run history; poll high-priority feeds more frequently.
- Monitor pipeline health with queue reports, Prometheus metrics, and Grafana.

Demo snippets
- **Articles & Stories:** Search and filter articles, read full Markdown content, and explore related coverage from multiple sources.

https://github.com/user-attachments/assets/918fc1e4-fd25-4686-ac16-7f5612bc8154

- **Trends:** Track news coverage over time and compare article counts with percentage views.

https://github.com/user-attachments/assets/a7fa1152-a132-4cb3-a998-bd29bf0f4201


- **Ask AI:** Get streaming answers and summaries grounded in your article collection, with source citations.

https://github.com/user-attachments/assets/9a1e661b-3a9d-4cc9-b33b-c88811394684

- **Feeds:** Browse and manage RSS feeds, check polling activity, and add custom sources by keyword/topic.

https://github.com/user-attachments/assets/83f72197-a4ce-44d7-822c-8e3334098cae



## Quick start

Requires Python 3.12+, uv, Docker Compose, and Node.js with pnpm. Run commands
from the repository root unless shown otherwise.

### 1. Configure

```bash
cp .env_example .env
```

The default database settings work with the local Compose stack. Set
`GRAFANA_ADMIN_PASSWORD` for monitoring. Optional keys in `.env`:

- `TAVILY_API_KEY`: web search and feed discovery.
- `OPENROUTER_API_KEY`: AI research; choose a model with `OPENROUTER_MODEL`.



### 2. Set up the backend with one command

```bash
bash scripts/setup.sh
```

This installs Python dependencies, waits for PostgreSQL, runs
`[init_db.sh](scripts/init_db.sh)` to migrate and seed feeds from `data/Tech.opml`,
initializes taxonomy, then builds and starts the complete backend stack:
APIs, ingestion, semantic processing, maintenance, and monitoring.
Docker images include the crawler's browser dependencies. The first setup also
downloads the embedding model.

Monitoring uses the Linux configuration or automatically selects the Docker
Desktop override. Airflow remains suspended. Open [API docs](http://localhost:8000/docs),
[Grafana](http://localhost:3001), or [Prometheus](http://localhost:9090).

Feed seeding preserves existing feeds and their state. To run only migrations
and feed seeding against a running database, use `bash scripts/init_db.sh`.
PostgreSQL data persists in a named Docker volume.

### 3. Start the frontend

```bash
cd frontend
cp .env.example .env.local
pnpm install
pnpm dev
```

Open the [web app](http://localhost:3000) or
[API documentation](http://localhost:8000/docs). The public API reads article
and story data; the separate admin API manages feeds and is bound to localhost.

## Continuous processing

Setup starts the background services automatically. Check their work with:

```bash
uv run news-worker queue-status
```

Workers handle regular ingestion, semantic processing, nightly maintenance,
and priority-0 feeds. See the [worker operations guide](docs/orchestration-workers.md)
and [optional topic discovery](docs/dynamic-taxonomy.md).

## One-off commands

```bash
uv run news-pipeline search "technology news" --max-results 5
uv run news-pipeline crawl https://example.com --persist
uv run news-pipeline rss --feed-path data/Tech.opml --max-feeds 5 --max-articles-per-feed 5 --persist
```

Without `--persist`, crawl and RSS commands only print JSON Lines. Search
results are never persisted. Use `--help` on any command for more options.

## Development

```bash
uv run --extra dev pytest -m "not integration"
# Integration tests require a migrated test database:
TEST_DATABASE_URL=postgresql+asyncpg://user:password@localhost:5433/news_radar_test uv run --extra dev pytest
```

See the [frontend guide](frontend/README.md) for frontend configuration and checks.
Source code lives in `backend/` and `frontend/`; deployment files are in
`deployments/`.
