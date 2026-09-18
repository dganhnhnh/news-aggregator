from __future__ import annotations

import ast
import json
from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[2]
DEPLOYMENTS = ROOT / "deployments"
PROJECT_LABEL = "news-radar.project"
GROUP_LABEL = "news-radar.group"
SERVICE_LABEL = "news-radar.service"


def _yaml(path: Path) -> dict:
    return yaml.safe_load(path.read_text())


def test_compose_services_have_stable_monitoring_labels() -> None:
    expected = {
        "docker-compose.yml": {
            "db": ("app", "db"),
            "api": ("app", "api"),
            "admin-api": ("app", "admin-api"),
            "instant-crawl": ("app", "instant-crawl"),
        },
        "docker-compose.airflow.yml": {
            "airflow-db": ("airflow", "airflow-db"),
            "airflow-init": ("airflow", "airflow-init"),
            "airflow-scheduler": ("airflow", "airflow-scheduler"),
            "airflow-apiserver": ("airflow", "airflow-apiserver"),
            "airflow-dag-processor": ("airflow", "airflow-dag-processor"),
        },
        "docker-compose.monitoring.yml": {
            "node-exporter": ("monitoring", "node-exporter"),
            "cadvisor": ("monitoring", "cadvisor"),
            "postgres-exporter": ("monitoring", "postgres-exporter"),
            "prometheus": ("monitoring", "prometheus"),
            "grafana": ("monitoring", "grafana"),
        },
    }

    for compose_name, services in expected.items():
        compose = _yaml(DEPLOYMENTS / compose_name)
        for service_name, (group, stable_service_name) in services.items():
            labels = compose["services"][service_name]["labels"]
            assert labels == {
                PROJECT_LABEL: "news-radar",
                GROUP_LABEL: group,
                SERVICE_LABEL: stable_service_name,
            }


def test_airflow_workers_have_stable_monitoring_labels() -> None:
    module = ast.parse((ROOT / "orchestration" / "dags" / "_common.py").read_text())
    common = next(
        node.value
        for node in module.body
        if isinstance(node, ast.Assign)
        and any(isinstance(target, ast.Name) and target.id == "COMMON" for target in node.targets)
    )
    assert isinstance(common, ast.Call)
    labels_keyword = next(keyword for keyword in common.keywords if keyword.arg == "labels")
    labels = ast.literal_eval(labels_keyword.value)

    assert labels[PROJECT_LABEL] == "news-radar"
    assert labels[GROUP_LABEL] == "worker"
    assert labels[SERVICE_LABEL] == "airflow-worker"
    assert labels["io.news-radar.airflow-worker"] == "true"


def test_prometheus_retention_and_scrape_scope() -> None:
    for config_name in ("prometheus.yml", "prometheus.docker-desktop.yml"):
        config = _yaml(DEPLOYMENTS / "monitoring" / "prometheus" / config_name)

        assert config["global"]["scrape_interval"] == "15s"
        assert config["storage"]["tsdb"]["retention"] == {
            "time": "15d",
            "size": "5GB",
        }
        jobs = {job["job_name"]: job for job in config["scrape_configs"]}
        assert set(jobs) == {
            "prometheus",
            "node-exporter",
            "cadvisor",
            "news-radar",
            "news-radar-api",
            "postgres",
        }
        # Every collection reads the operational tables, so the application
        # job must stay slower than the infrastructure ones.
        assert jobs["news-radar"]["scrape_interval"] == "30s"
        assert jobs["cadvisor"]["metric_relabel_configs"] == [
            {
                "source_labels": ["container_label_news_radar_project"],
                "regex": "news-radar",
                "action": "keep",
            }
        ]


def test_news_radar_scrape_job_stays_on_the_admin_api() -> None:
    """The exporter is operator data, so it must never be scraped publicly.

    `admin-api` is the only service published on loopback alone; pointing the
    job at `api` instead would put queue depth and corpus size on 0.0.0.0.
    """
    targets = {
        "prometheus.yml": ["127.0.0.1:8001"],
        "prometheus.docker-desktop.yml": ["admin-api:8001"],
    }
    for config_name, expected in targets.items():
        config = _yaml(DEPLOYMENTS / "monitoring" / "prometheus" / config_name)
        job = next(
            job for job in config["scrape_configs"] if job["job_name"] == "news-radar"
        )
        assert job["static_configs"][0]["targets"] == expected

    compose = _yaml(DEPLOYMENTS / "docker-compose.yml")
    assert compose["services"]["admin-api"]["ports"] == ["127.0.0.1:${ADMIN_API_PORT:-8001}:8001"]


def test_api_exposes_its_own_metrics_on_a_loopback_port() -> None:
    """The API is public; what it reveals about itself is not.

    `news-api` serves its registry on a second listener precisely so the
    scrape target can be private while 8000 stays open, which only holds as
    long as that port is published to 127.0.0.1.
    """
    targets = {
        "prometheus.yml": ["127.0.0.1:8002"],
        "prometheus.docker-desktop.yml": ["api:8002"],
    }
    for config_name, expected in targets.items():
        config = _yaml(DEPLOYMENTS / "monitoring" / "prometheus" / config_name)
        job = next(
            job
            for job in config["scrape_configs"]
            if job["job_name"] == "news-radar-api"
        )
        assert job["static_configs"][0]["targets"] == expected

    api = _yaml(DEPLOYMENTS / "docker-compose.yml")["services"]["api"]
    assert api["ports"] == [
        "${API_PORT:-8000}:8000",
        "127.0.0.1:${API_METRICS_PORT:-8002}:8002",
    ]
    assert api["environment"]["API_METRICS_PORT"] == 8002


def test_postgres_internals_come_from_the_exporter_image() -> None:
    exporter = _yaml(DEPLOYMENTS / "docker-compose.monitoring.yml")["services"][
        "postgres-exporter"
    ]
    assert exporter["image"].startswith("quay.io/prometheuscommunity/postgres-exporter:")
    assert "--web.listen-address=127.0.0.1:9187" in exporter["command"]

    for config_name, expected in {
        "prometheus.yml": ["127.0.0.1:9187"],
        "prometheus.docker-desktop.yml": ["postgres-exporter:9187"],
    }.items():
        config = _yaml(DEPLOYMENTS / "monitoring" / "prometheus" / config_name)
        job = next(
            job for job in config["scrape_configs"] if job["job_name"] == "postgres"
        )
        assert job["static_configs"][0]["targets"] == expected


def test_grafana_provisions_all_dashboards() -> None:
    dashboards_dir = DEPLOYMENTS / "monitoring" / "grafana" / "dashboards"
    dashboards = [json.loads(path.read_text()) for path in dashboards_dir.glob("*.json")]

    assert {dashboard["uid"] for dashboard in dashboards} == {
        "news-radar-host",
        "news-radar-containers",
        "news-radar-pipeline",
        "news-radar-quality",
        "news-radar-services",
    }
    for dashboard in dashboards:
        for panel in dashboard["panels"]:
            assert panel["datasource"]["uid"] == "news-radar-prometheus"

    containers = next(
        dashboard
        for dashboard in dashboards
        if dashboard["uid"] == "news-radar-containers"
    )
    table_panels = {
        panel["id"]: panel
        for panel in containers["panels"]
        if panel["id"] >= 9
    }
    assert set(table_panels) == {9, 10, 11}
    assert all(panel["type"] == "table" for panel in table_panels.values())
    assert all(panel["targets"][0]["instant"] for panel in table_panels.values())
    assert all(not panel["targets"][0]["range"] for panel in table_panels.values())

    datasource = _yaml(
        DEPLOYMENTS
        / "monitoring"
        / "grafana"
        / "provisioning"
        / "datasources"
        / "prometheus.yml"
    )["datasources"][0]
    assert datasource["url"] == "$PROMETHEUS_URL"
