#!/usr/bin/env bash
# Initialize the backend, then start the complete active Compose stack.
set -euo pipefail

cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.."

for tool in uv docker; do
    if ! command -v "$tool" >/dev/null 2>&1; then
        printf 'Required command not found: %s\n' "$tool" >&2
        exit 1
    fi
done

if [[ ! -f .env ]]; then
    printf 'Create and configure .env before running backend setup.\n' >&2
    exit 1
fi

docker compose version >/dev/null
docker_os=$(docker info --format '{{.OperatingSystem}}')
compose=(docker compose --env-file .env
    -f deployments/docker-compose.yml
    -f deployments/docker-compose.monitoring.yml)
case "$docker_os" in
    *[Dd]esktop*)
        compose+=(-f deployments/docker-compose.monitoring.docker-desktop.yml)
        ;;
esac

# Validate all Compose configuration before installing or changing the database.
"${compose[@]}" config --quiet

printf '\n[1/5] Installing backend dependencies...\n'
uv sync --frozen

printf '\n[2/5] Starting PostgreSQL and waiting for readiness...\n'
"${compose[@]}" up -d --wait db

printf '\n[3/5] Applying migrations and seeding initial feeds...\n'
bash scripts/init_db.sh

printf '\n[4/5] Initializing taxonomy if empty and building topic prototypes...\n'
uv run news-worker taxonomy-import --file data/taxonomy.seed.yaml --if-empty
uv run news-worker taxonomy-rebuild

printf '\n[5/5] Building and starting APIs, workers, and monitoring...\n'
"${compose[@]}" up -d --build --wait

printf '\nBackend setup complete. Current services:\n'
"${compose[@]}" ps
