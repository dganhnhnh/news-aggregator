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
        assert set(jobs) == {"prometheus", "node-exporter", "cadvisor"}
        assert jobs["cadvisor"]["metric_relabel_configs"] == [
            {
                "source_labels": ["container_label_news_radar_project"],
                "regex": "news-radar",
                "action": "keep",
            }
        ]


def test_grafana_provisions_both_dashboards() -> None:
    dashboards_dir = DEPLOYMENTS / "monitoring" / "grafana" / "dashboards"
    dashboards = [json.loads(path.read_text()) for path in dashboards_dir.glob("*.json")]

    assert {dashboard["uid"] for dashboard in dashboards} == {
        "news-radar-host",
        "news-radar-containers",
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
