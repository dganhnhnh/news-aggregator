"""Remove DockerOperator workers left behind by an earlier scheduler process.

DockerOperator deliberately creates containers with Docker's daemon-side
AutoRemove disabled and removes them from an operator ``finally`` block. If
Docker Desktop or Airflow exits before that block runs, nobody owns the
container after the daemon starts again. The scheduler runs this script before
each start, before it is able to schedule replacement workers.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any


MANAGED_LABEL = "io.news-radar.airflow-worker"
LEGACY_ENV_PREFIX = "NEWS_RADAR_AIRFLOW_DAG_ID="


def is_airflow_worker(attrs: Mapping[str, Any]) -> bool:
    """Recognize both newly labelled workers and containers from older builds."""
    config = attrs.get("Config") or {}
    labels = config.get("Labels") or {}
    if labels.get(MANAGED_LABEL) == "true":
        return True

    # Older workers predate MANAGED_LABEL, but every one receives this
    # namespaced environment variable from orchestration/dags/_common.py.
    environment = config.get("Env") or []
    return any(value.startswith(LEGACY_ENV_PREFIX) for value in environment)


def main() -> int:
    import docker

    client = docker.from_env()
    removed: list[str] = []
    failures: list[str] = []
    try:
        for container in client.containers.list(all=True):
            try:
                container.reload()
                if not is_airflow_worker(container.attrs):
                    continue
                identity = container.name or container.short_id
                container.remove(force=True)
                removed.append(identity)
            except docker.errors.NotFound:
                # A concurrent Docker cleanup may have won the race.
                continue
            except docker.errors.APIError as exc:
                failures.append(f"{container.id[:12]}: {exc}")
    finally:
        client.close()

    print(
        "Airflow worker startup cleanup removed "
        f"{len(removed)} container(s): {', '.join(removed) or 'none'}"
    )
    if failures:
        message = "Could not remove stale Airflow workers: " + "; ".join(failures)
        raise RuntimeError(message)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
