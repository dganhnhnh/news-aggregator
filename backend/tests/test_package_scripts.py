from pathlib import Path
import tomllib


def test_public_uv_commands_are_declared() -> None:
    pyproject = Path(__file__).parents[2] / "pyproject.toml"
    project = tomllib.loads(pyproject.read_text(encoding="utf-8"))["project"]

    assert project["scripts"] == {
        "news-pipeline": "pipelines.news:main",
        "news-worker": "workers.cli:main",
        # Deprecated alias for `news-worker crawl`.
        "news-service": "services.news:main",
        "news-api": "api.main:run",
        "news-admin-api": "admin_api.main:run",
    }
