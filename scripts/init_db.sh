#!/usr/bin/env bash
# Apply the schema, then register only the feeds in data/Tech.opml.
set -euo pipefail

cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.."

uv run alembic upgrade head
uv run news-worker seed-feeds
