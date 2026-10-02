import argparse
import asyncio
import os
import signal
import sys
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest

import json
from pathlib import Path

from models.storage_models import IngestRunStats
from workers.base import (
    EXIT_FAILED,
    EXIT_OK,
    RunContext,
    RunOutcome,
    Worker,
    exit_busy_for,
    shutdown_signals,
    stale_after_for,
)
from workers.window import RunWindow


NOW = datetime(2026, 8, 17, 12, 0, tzinfo=timezone.utc)
STALE_AFTER = timedelta(seconds=900)


def _window(explicit: bool = False, days: int = 3) -> RunWindow:
    return RunWindow(start=NOW - timedelta(days=days), end=NOW, explicit=explicit)


def _args(**overrides: Any) -> argparse.Namespace:
    defaults: dict[str, Any] = {
        "worker": "crawl",
        "once": True,
        "interval": None,
        "strict": False,
        "trigger": None,
        "max_runtime": None,
    }
    defaults.update(overrides)
    return argparse.Namespace(**defaults)


class FakeStorage:
    """Records the order of the bookkeeping calls a run makes."""

    def __init__(self, *, lock_acquired: bool = True) -> None:
        self.calls: list[Any] = []
        self._lock_acquired = lock_acquired
        self._next_run_id = 1
        self.started_params: list[dict[str, Any]] = []
        self.lock_modes: list[str] = []

    async def reap_abandoned(self, worker: str, stale_after: timedelta) -> int:
        self.calls.append(("reap", worker, stale_after))
        return 0

    @asynccontextmanager
    async def worker_lock(self, name: str, *, shared: bool = False):
        self.calls.append(("lock", name, self._lock_acquired))
        self.lock_modes.append("shared" if shared else "exclusive")
        yield self._lock_acquired

    async def start_run(self, **kwargs: Any) -> int:
        run_id = self._next_run_id
        self._next_run_id += 1
        self.calls.append(("start", run_id, kwargs["worker"], kwargs["trigger"]))
        self.started_params.append(dict(kwargs["params"]))
        return run_id

    async def update_run(self, run_id: int, stats: Any) -> None:
        self.calls.append(("checkpoint", run_id, stats.articles_written))

    async def finish_run(
        self,
        run_id: int,
        status: str,
        stats: Any,
        error_message: str | None = None,
    ) -> None:
        self.calls.append(("finish", run_id, status, error_message))


class ScriptedWorker(Worker):
    name = "crawl"

    def __init__(self, outcome: RunOutcome | BaseException) -> None:
        self.outcome = outcome
        self.executed = 0

    async def execute(self, ctx: RunContext) -> RunOutcome:
        self.executed += 1
        # CancelledError is a BaseException, so this cannot narrow to Exception.
        if isinstance(self.outcome, BaseException):
            raise self.outcome
        return self.outcome


def _run(worker: Worker, storage: FakeStorage, args: argparse.Namespace) -> int:
    return asyncio.run(
        worker.run(storage, args, _window(), stale_after=STALE_AFTER)
    )


def test_lock_held_elsewhere_skips_the_run_without_bookkeeping() -> None:
    storage = FakeStorage(lock_acquired=False)
    worker = ScriptedWorker(RunOutcome("succeeded", IngestRunStats()))

    exit_code = _run(worker, storage, _args())

    assert exit_code == EXIT_OK
    assert worker.executed == 0
    assert [call[0] for call in storage.calls] == ["reap", "lock"]


def test_stale_runs_are_reaped_before_the_lock_is_taken() -> None:
    # A run worth reaping belongs to a dead process that no longer holds the
    # lock, so reaping has to happen first or it would never happen at all.
    storage = FakeStorage()
    worker = ScriptedWorker(RunOutcome("succeeded", IngestRunStats()))

    _run(worker, storage, _args())

    assert [call[0] for call in storage.calls] == [
        "reap",
        "lock",
        "start",
        "finish",
    ]
    assert storage.calls[0] == ("reap", "crawl", STALE_AFTER)


def test_failing_execute_closes_the_run_and_exits_one() -> None:
    storage = FakeStorage()
    worker = ScriptedWorker(RuntimeError("feed host is down"))

    exit_code = _run(worker, storage, _args())

    assert exit_code == EXIT_FAILED
    assert ("finish", 1, "failed", "feed host is down") in storage.calls


def test_partial_exits_zero_unless_strict() -> None:
    lenient = FakeStorage()
    strict = FakeStorage()
    outcome = RunOutcome("partial", IngestRunStats(), "3 feeds failed")

    assert _run(ScriptedWorker(outcome), lenient, _args()) == EXIT_OK
    assert _run(ScriptedWorker(outcome), strict, _args(strict=True)) == EXIT_FAILED
    assert ("finish", 1, "partial", "3 feeds failed") in strict.calls


def test_explicit_window_is_recorded_as_a_backfill() -> None:
    storage = FakeStorage()
    worker = ScriptedWorker(RunOutcome("succeeded", IngestRunStats()))

    asyncio.run(
        worker.run(
            storage,
            _args(),
            _window(explicit=True),
            stale_after=STALE_AFTER,
        )
    )

    assert ("start", 1, "crawl", "backfill") in storage.calls


def test_trigger_can_be_overridden_on_the_command_line() -> None:
    storage = FakeStorage()
    worker = ScriptedWorker(RunOutcome("succeeded", IngestRunStats()))

    _run(worker, storage, _args(trigger="manual"))

    assert ("start", 1, "crawl", "manual") in storage.calls


def test_shutdown_between_chunks_keeps_the_first_checkpoint() -> None:
    storage = FakeStorage()

    class ChunkedWorker(Worker):
        name = "crawl"

        async def execute(self, ctx: RunContext) -> RunOutcome:
            stats = IngestRunStats()
            for chunk in range(3):
                stats.articles_written += 1
                await ctx.checkpoint(stats)
                if chunk == 0:
                    # Stands in for a SIGTERM arriving mid-run.
                    ctx.shutdown.set()
                if ctx.shutdown.is_set():
                    return RunOutcome("partial", stats, "Stopped on a shutdown signal")
            return RunOutcome("succeeded", stats)

    exit_code = _run(ChunkedWorker(), storage, _args())

    assert exit_code == EXIT_OK
    assert ("checkpoint", 1, 1) in storage.calls
    assert ("checkpoint", 1, 2) not in storage.calls
    assert ("finish", 1, "partial", "Stopped on a shutdown signal") in storage.calls


def test_max_runtime_requests_a_cooperative_stop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    storage = FakeStorage()

    class WaitingWorker(Worker):
        name = "crawl"

        async def execute(self, ctx: RunContext) -> RunOutcome:
            await asyncio.wait_for(ctx.shutdown.wait(), timeout=1)
            return RunOutcome("partial", IngestRunStats(), "Runtime budget reached")

    monkeypatch.setattr(
        "workers.base.parse_duration", lambda value: timedelta(milliseconds=1)
    )

    exit_code = _run(WaitingWorker(), storage, _args(max_runtime="25m"))

    assert exit_code == EXIT_OK
    assert ("finish", 1, "partial", "Runtime budget reached") in storage.calls
    assert storage.started_params == [{"max_runtime": "25m"}]


def test_orchestration_metadata_is_recorded_and_emitted(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("NEWS_RADAR_ORCHESTRATION_ID", "news_semantic:run-42")
    monkeypatch.setenv("NEWS_RADAR_AIRFLOW_DAG_ID", "news_semantic")
    monkeypatch.setenv("NEWS_RADAR_AIRFLOW_TASK_ID", "classify")
    monkeypatch.setenv("NEWS_RADAR_IMAGE_REVISION", "news-radar-worker:abc123")
    target = tmp_path / "summary.jsonl"
    storage = FakeStorage()

    _run(
        ScriptedWorker(RunOutcome("succeeded", IngestRunStats())),
        storage,
        _args(summary_json=str(target)),
    )

    expected = {
        "orchestration_id": "news_semantic:run-42",
        "dag_id": "news_semantic",
        "task_id": "classify",
        "image_revision": "news-radar-worker:abc123",
    }
    assert storage.started_params == [{"orchestration": expected}]
    assert json.loads(target.read_text())["orchestration"] == expected


def test_interval_loops_with_a_refreshed_implicit_window_until_shutdown(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    storage = FakeStorage()
    initial_window = _window()
    refreshed_window = RunWindow(
        start=initial_window.start + timedelta(hours=1),
        end=initial_window.end + timedelta(hours=1),
        explicit=False,
    )
    monkeypatch.setattr(
        RunWindow,
        "refreshed",
        lambda self: refreshed_window,
    )

    class LoopingWorker(Worker):
        name = "crawl"
        runs = 0
        windows: list[RunWindow] = []

        async def execute(self, ctx: RunContext) -> RunOutcome:
            self.runs += 1
            self.windows.append(ctx.window)
            if self.runs == 2:
                ctx.shutdown.set()
            return RunOutcome("succeeded", IngestRunStats())

    worker = LoopingWorker()
    exit_code = asyncio.run(
        worker.run(
            storage,
            _args(once=False, interval=1),
            initial_window,
            stale_after=STALE_AFTER,
        )
    )

    assert exit_code == EXIT_OK
    assert worker.windows == [initial_window, refreshed_window]
    assert [call for call in storage.calls if call[0] == "start"] == [
        ("start", 1, "crawl", "scheduled"),
        ("start", 2, "crawl", "scheduled"),
    ]


def test_cancellation_closes_the_run_as_partial() -> None:
    storage = FakeStorage()
    worker = ScriptedWorker(asyncio.CancelledError())

    with pytest.raises(asyncio.CancelledError):
        _run(worker, storage, _args())

    assert storage.calls[-1] == (
        "finish",
        1,
        "partial",
        "Cancelled before the run completed",
    )


@pytest.mark.skipif(
    sys.platform == "win32", reason="POSIX signal handling only"
)
def test_sigterm_sets_the_shutdown_event_without_killing_the_process() -> None:
    async def scenario() -> bool:
        event = asyncio.Event()
        with shutdown_signals(event):
            os.kill(os.getpid(), signal.SIGTERM)
            await asyncio.wait_for(event.wait(), timeout=5)
        return event.is_set()

    assert asyncio.run(scenario()) is True


def test_a_busy_run_stays_a_success_unless_a_code_is_asked_for() -> None:
    # Overlapping cron schedules have always been a non-event, so the default
    # must not change; a scheduler that needs "skipped" opts in explicitly.
    assert exit_busy_for(_args()) == EXIT_OK
    assert exit_busy_for(_args(exit_busy=0)) == EXIT_OK
    assert exit_busy_for(_args(exit_busy=99)) == 99


def test_lock_held_elsewhere_uses_the_requested_busy_exit_code() -> None:
    storage = FakeStorage(lock_acquired=False)
    worker = ScriptedWorker(RunOutcome("succeeded", IngestRunStats()))

    exit_code = _run(worker, storage, _args(exit_busy=99))

    assert exit_code == 99
    assert worker.executed == 0
    assert [call[0] for call in storage.calls] == ["reap", "lock"]


def test_summary_json_writes_one_line_per_finished_run(tmp_path: Path) -> None:
    target = tmp_path / "summary.jsonl"
    storage = FakeStorage()
    stats = IngestRunStats()
    stats.articles_written = 7

    _run(
        ScriptedWorker(RunOutcome("partial", stats, "1 feed failed")),
        storage,
        _args(summary_json=str(target)),
    )

    lines = target.read_text().splitlines()
    assert len(lines) == 1
    payload = json.loads(lines[0])
    assert payload["run_id"] == 1
    assert payload["worker"] == "crawl"
    assert payload["status"] == "partial"
    assert payload["trigger"] == "scheduled"
    assert payload["error_message"] == "1 feed failed"
    assert payload["stats"]["articles_written"] == 7


def test_summary_json_is_written_even_when_the_run_failed(tmp_path: Path) -> None:
    # The failure path is exactly when a scheduler most wants the summary.
    target = tmp_path / "summary.jsonl"
    storage = FakeStorage()

    exit_code = _run(
        ScriptedWorker(RuntimeError("feed host is down")),
        storage,
        _args(summary_json=str(target)),
    )

    assert exit_code == EXIT_FAILED
    payload = json.loads(target.read_text().splitlines()[0])
    assert payload["status"] == "failed"
    assert payload["error_message"] == "feed host is down"
    assert payload["stats"] is None


def test_a_summary_that_cannot_be_written_does_not_fail_the_run(
    tmp_path: Path,
) -> None:
    storage = FakeStorage()

    exit_code = _run(
        ScriptedWorker(RunOutcome("succeeded", IngestRunStats())),
        storage,
        _args(summary_json=str(tmp_path / "missing-dir" / "summary.jsonl")),
    )

    assert exit_code == EXIT_OK
    assert ("finish", 1, "succeeded", None) in storage.calls


def test_stale_after_never_dips_below_the_configured_floor() -> None:
    assert stale_after_for(_args(), 900) == timedelta(seconds=900)
    assert stale_after_for(_args(interval=60), 900) == timedelta(seconds=900)
    assert stale_after_for(_args(interval=1800), 900) == timedelta(seconds=5400)


def test_max_runtime_with_interval_ends_the_run_not_the_loop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The `ingest` service: a slow crawl hits its budget, closes as partial,
    # and the process sleeps and runs again instead of exiting.
    storage = FakeStorage()
    monkeypatch.setattr(
        "workers.base.parse_duration", lambda value: timedelta(milliseconds=1)
    )
    monkeypatch.setattr("workers.base.sleep_or_shutdown", _no_sleep)

    class BudgetedWorker(Worker):
        name = "crawl"
        runs = 0

        async def execute(self, ctx: RunContext) -> RunOutcome:
            self.runs += 1
            if self.runs == 1:
                await asyncio.wait_for(ctx.shutdown.wait(), timeout=1)
                return RunOutcome("partial", IngestRunStats(), "Runtime budget reached")
            ctx.shutdown.set()
            return RunOutcome("succeeded", IngestRunStats())

    worker = BudgetedWorker()
    exit_code = _run(worker, storage, _args(once=False, interval=1, max_runtime="8m"))

    assert exit_code == EXIT_OK
    assert worker.runs == 2
    assert ("finish", 1, "partial", "Runtime budget reached") in storage.calls
    assert ("finish", 2, "succeeded", None) in storage.calls


async def _no_sleep(seconds: float, event: asyncio.Event) -> None:
    return None


def test_process_shutdown_still_reaches_a_budgeted_run() -> None:
    storage = FakeStorage()
    seen: list[bool] = []

    class WaitingWorker(Worker):
        name = "crawl"

        async def execute(self, ctx: RunContext) -> RunOutcome:
            await asyncio.wait_for(ctx.shutdown.wait(), timeout=1)
            seen.append(True)
            return RunOutcome("partial", IngestRunStats(), "Stopped")

    async def scenario() -> int:
        shutdown = asyncio.Event()
        asyncio.get_running_loop().call_later(0.01, shutdown.set)
        return await WaitingWorker().run_once(
            storage, _args(max_runtime="1h"), _window(), shutdown,
            stale_after=STALE_AFTER,
        )

    assert asyncio.run(scenario()) == EXIT_OK
    assert seen == [True]


def test_lock_mode_is_passed_to_the_worker_lock() -> None:
    class SharedWorker(ScriptedWorker):
        def lock_mode(self, args: argparse.Namespace):
            return "shared"

    storage = FakeStorage()
    _run(SharedWorker(RunOutcome("succeeded", IngestRunStats())), storage, _args())
    _run(ScriptedWorker(RunOutcome("succeeded", IngestRunStats())), storage, _args())

    assert storage.lock_modes == ["shared", "exclusive"]


def test_last_outcome_distinguishes_busy_from_ran() -> None:
    outcome = RunOutcome("succeeded", IngestRunStats())
    worker = ScriptedWorker(outcome)

    _run(worker, FakeStorage(), _args())
    assert worker.last_outcome == outcome

    _run(worker, FakeStorage(lock_acquired=False), _args())
    assert worker.last_outcome is None

    failing = ScriptedWorker(RuntimeError("boom"))
    _run(failing, FakeStorage(), _args())
    assert failing.last_outcome is not None
    assert failing.last_outcome.status == "failed"


def test_embed_and_classify_share_the_lock_only_when_draining() -> None:
    from workers.classify import ClassifyWorker
    from workers.embed import EmbedWorker

    embed = EmbedWorker()
    assert embed.lock_mode(_args(reprocess=False, model_changed=False)) == "shared"
    assert embed.lock_mode(_args(reprocess=True, model_changed=False)) == "exclusive"
    assert embed.lock_mode(_args(reprocess=False, model_changed=True)) == "exclusive"

    classify = ClassifyWorker()
    drain = {"reprocess": False, "stale": False, "refresh_clusters": False}
    assert classify.lock_mode(_args(**drain)) == "shared"
    for flag in drain:
        assert classify.lock_mode(_args(**(drain | {flag: True}))) == "exclusive"
