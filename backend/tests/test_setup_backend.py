import os
from pathlib import Path
import shutil
import subprocess

import pytest


ROOT = Path(__file__).resolve().parents[2]


def _run_setup(tmp_path: Path, *, docker_os: str = "Ubuntu", fail: str = ""):
    project = tmp_path / "project with spaces"
    scripts = project / "scripts"
    scripts.mkdir(parents=True)
    for name in ("setup.sh", "init_db.sh"):
        shutil.copyfile(ROOT / "scripts" / name, scripts / name)
    (project / ".env").touch()
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    log = tmp_path / "commands.log"
    for name in ("uv", "docker"):
        stub = bin_dir / name
        stub.write_text(
            '#!/usr/bin/env bash\n'
            'command="${0##*/} $*"\n'
            'printf "%s\\t%s\\n" "$PWD" "$command" >> "$SETUP_TEST_LOG"\n'
            'if [[ "$command" == "$SETUP_TEST_FAIL" ]]; then exit 17; fi\n'
            'if [[ "$command" == docker\\ info* ]]; then\n'
            '    printf "%s\\n" "$SETUP_TEST_DOCKER_OS"\n'
            'fi\n'
        )
        stub.chmod(0o755)
    result = subprocess.run(
        ["bash", str(scripts / "setup.sh")],
        cwd=tmp_path,
        env={
            **os.environ,
            "PATH": f"{bin_dir}{os.pathsep}{os.environ['PATH']}",
            "SETUP_TEST_LOG": str(log),
            "SETUP_TEST_FAIL": fail,
            "SETUP_TEST_DOCKER_OS": docker_os,
        },
        capture_output=True,
        text=True,
    )
    entries = [line.split("\t", 1) for line in log.read_text().splitlines()]
    assert all(cwd == str(project) for cwd, _ in entries)
    return result, [command for _, command in entries]


@pytest.mark.parametrize("docker_os", ["Ubuntu", "Docker Desktop"])
def test_setup_initializes_before_starting_full_stack(
    tmp_path: Path, docker_os: str
) -> None:
    result, commands = _run_setup(tmp_path, docker_os=docker_os)
    assert result.returncode == 0, result.stderr
    database_start = next(
        i for i, cmd in enumerate(commands) if cmd.endswith("up -d --wait db")
    )
    stack_start = next(
        i for i, cmd in enumerate(commands) if cmd.endswith("up -d --build --wait")
    )
    initialization = [
        "uv run alembic upgrade head",
        "uv run news-worker seed-feeds",
        "uv run news-worker taxonomy-import --file data/taxonomy.seed.yaml --if-empty",
        "uv run news-worker taxonomy-rebuild",
    ]
    positions = [commands.index(cmd) for cmd in initialization]
    assert (
        database_start
        < positions[0] < positions[1] < positions[2] < positions[3]
        < stack_start
    )
    for command in commands:
        if command.startswith("docker compose --env-file"):
            assert "-f deployments/docker-compose.monitoring.yml" in command
            assert ("docker-compose.monitoring.docker-desktop.yml" in command) == (
                docker_os == "Docker Desktop"
            )
            assert "airflow" not in command


@pytest.mark.parametrize(
    "fail",
    [
        "uv sync --frozen",
        "uv run alembic upgrade head",
        "uv run news-worker seed-feeds",
        "uv run news-worker taxonomy-rebuild",
    ],
)
def test_setup_does_not_start_workers_after_initialization_failure(
    tmp_path: Path, fail: str
) -> None:
    result, commands = _run_setup(tmp_path, fail=fail)
    assert result.returncode == 17, result.stderr
    assert commands[-1] == fail
    assert not any(cmd.endswith("up -d --build --wait") for cmd in commands)
