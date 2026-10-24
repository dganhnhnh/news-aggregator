import os
from pathlib import Path
import subprocess

import pytest


ROOT = Path(__file__).resolve().parents[2]


@pytest.mark.parametrize("migration_exit", [0, 1])
def test_init_db_seeds_only_after_successful_migration(
    tmp_path: Path, migration_exit: int
) -> None:
    uv = tmp_path / "uv"
    uv.write_text(
        '#!/bin/sh\n'
        'printf "%s:%s\\n" "$PWD" "$*" >> "$INIT_TEST_LOG"\n'
        'if [ "$2" = "alembic" ]; then exit "$INIT_TEST_MIGRATION_EXIT"; fi\n'
    )
    uv.chmod(0o755)
    log = tmp_path / "commands.log"
    result = subprocess.run(
        ["bash", str(ROOT / "scripts" / "init_db.sh")],
        cwd=tmp_path,
        env={
            **os.environ,
            "PATH": f"{tmp_path}{os.pathsep}{os.environ['PATH']}",
            "INIT_TEST_LOG": str(log),
            "INIT_TEST_MIGRATION_EXIT": str(migration_exit),
        },
        capture_output=True,
        text=True,
    )
    expected = [f"{ROOT}:run alembic upgrade head"]
    if migration_exit == 0:
        expected.append(f"{ROOT}:run news-worker seed-feeds")
    assert result.returncode == migration_exit, result.stderr
    assert log.read_text().splitlines() == expected
