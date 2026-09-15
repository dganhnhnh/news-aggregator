from __future__ import annotations

import importlib.util
from pathlib import Path


SCRIPT_PATH = (
    Path(__file__).resolve().parents[2]
    / "orchestration"
    / "scripts"
    / "cleanup_airflow_workers.py"
)
SPEC = importlib.util.spec_from_file_location("cleanup_airflow_workers", SCRIPT_PATH)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def test_recognizes_labelled_airflow_worker() -> None:
    attrs = {"Config": {"Labels": {MODULE.MANAGED_LABEL: "true"}, "Env": []}}

    assert MODULE.is_airflow_worker(attrs)


def test_recognizes_legacy_airflow_worker_from_environment() -> None:
    attrs = {
        "Config": {
            "Labels": {"com.docker.compose.service": "instant-crawl"},
            "Env": ["NEWS_RADAR_AIRFLOW_DAG_ID=news_ingest"],
        }
    }

    assert MODULE.is_airflow_worker(attrs)


def test_does_not_match_long_running_instant_crawl_service() -> None:
    attrs = {
        "Config": {
            "Labels": {"com.docker.compose.service": "instant-crawl"},
            "Env": ["INSTANT_FEED_POLL_INTERVAL_SECONDS=30"],
        }
    }

    assert not MODULE.is_airflow_worker(attrs)
