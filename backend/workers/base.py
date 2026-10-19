"""Operational frame shared by every worker.

A worker subclass implements `execute()` and nothing else: the advisory lock,
the `worker_runs` bookkeeping, heartbeats, graceful shutdown and the exit-code
contract all live here so they cannot drift between workers.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import signal
import sys
from copy import deepcopy
from abc import ABC, abstractmethod
from collections.abc import Iterator, Mapping
from contextlib import contextmanager, suppress
from dataclasses import asdict, dataclass, field, is_dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Literal, NamedTuple

from adapters.storage import PostgresRawStorage
from core.logger import get_logger
from models.storage_models import RunStatus, WorkerTrigger
from workers.window import RunWindow, parse_duration


logger = get_logger(__name__)

EXIT_OK = 0
EXIT_FAILED = 1
# Paths that mean "write the summary to this process's stdout" rather than to
# a file. `/dev/stdout` is accepted because that is what a Docker command line
# reads naturally, but opening it in append mode is not portable.
STDOUT_TARGETS = frozenset({"-", "/dev/stdout"})


LockMode = Literal["exclusive", "shared"]


class RunOutcome(NamedTuple):
    status: RunStatus
    stats: Any = None
    error_message: str | None = None


@dataclass
class RunContext:
    """What a worker body is handed: identity, scope and a stop signal."""

    run_id: int
    """Also the correlation id for every log line belonging to this run."""

    window: RunWindow
    shutdown: asyncio.Event
    args: argparse.Namespace
    storage: PostgresRawStorage = field(repr=False)
    last_stats: Any = field(default=None, init=False, repr=False)

    async def checkpoint(self, stats: Any) -> None:
        """Persist progress and prove the run is still alive."""
        await self.storage.update_run(self.run_id, stats)
        self.last_stats = deepcopy(stats)


@contextmanager
def shutdown_signals(event: asyncio.Event) -> Iterator[asyncio.Event]:
    """Turn SIGTERM/SIGINT into a cooperative stop request."""
    loop = asyncio.get_running_loop()
    installed: list[signal.Signals] = []
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, event.set)
        except NotImplementedError:  # pragma: no cover - non-POSIX loops
            continue
        installed.append(sig)
    try:
        yield event
    finally:
        for sig in installed:
            loop.remove_signal_handler(sig)


async def sleep_or_shutdown(seconds: float, event: asyncio.Event) -> None:
    """Wait out the interval but wake immediately on a stop request."""
    try:
        await asyncio.wait_for(event.wait(), timeout=seconds)
    except asyncio.TimeoutError:
        return


async def request_shutdown_after(
    duration: timedelta, event: asyncio.Event, worker_name: str
) -> bool:
    """Request a cooperative stop before the orchestrator's hard timeout.

    Returns whether the budget, rather than something else, set the event.
    """
    await sleep_or_shutdown(duration.total_seconds(), event)
    if event.is_set():
        return False
    logger.warning(
        "%s reached its %s max runtime; stopping after the current chunk",
        worker_name,
        duration,
    )
    event.set()
    return True


async def _forward(source: asyncio.Event, target: asyncio.Event) -> None:
    await source.wait()
    target.set()


def orchestration_context() -> dict[str, str]:
    """Read optional scheduler correlation metadata injected by the runtime."""
    fields = {
        "orchestration_id": "NEWS_RADAR_ORCHESTRATION_ID",
        "dag_id": "NEWS_RADAR_AIRFLOW_DAG_ID",
        "task_id": "NEWS_RADAR_AIRFLOW_TASK_ID",
        "image_revision": "NEWS_RADAR_IMAGE_REVISION",
    }
    return {
        key: value
        for key, env_name in fields.items()
        if (value := os.getenv(env_name, "").strip())
    }


def _json_safe(value: Any) -> Any:
    """Flatten a stats dataclass the same way `worker_runs.stats` stores it."""
    if value is None:
        return None
    if is_dataclass(value) and not isinstance(value, type):
        return asdict(value)
    if isinstance(value, Mapping):
        return dict(value)
    return value


def emit_summary(target: str, payload: Mapping[str, Any]) -> None:
    """Write one JSON line describing a finished run.

    It has to be the *last* line on stdout: `DockerOperator(do_xcom_push=True)`
    pushes `log_lines[-1]` and nothing else, so anything logged afterwards
    would silently become the XCom value instead.
    """
    line = json.dumps(payload, default=str, ensure_ascii=False)
    try:
        if target in STDOUT_TARGETS:
            sys.stdout.write(line + "\n")
            sys.stdout.flush()
            return
        with Path(target).open("a", encoding="utf-8") as handle:
            handle.write(line + "\n")
    except OSError:
        # A summary is a reporting convenience; losing it must never turn a
        # successful run into a failed one.
        logger.exception("Could not write the run summary to %s", target)


class Worker(ABC):
    name: str

    last_outcome: RunOutcome | None = None
    """What the latest `run_once` produced; `None` when the lock was busy."""

    @abstractmethod
    async def execute(self, ctx: RunContext) -> RunOutcome:
        """Do the work, checking `ctx.shutdown` between chunks."""

    def run_params(self, args: argparse.Namespace) -> Mapping[str, Any]:
        """Flags worth recording on the run for later diagnosis."""
        return {}

    def lock_mode(self, args: argparse.Namespace) -> LockMode:
        """How this run shares the worker lock with other runs of the same name.

        Exclusive by default. A worker whose run only drains `processing_jobs`
        -- where `SKIP LOCKED` already keeps two claimers apart -- may return
        "shared" so replicas run side by side, while anything that must see
        the stage quiet (a bulk re-queue, `cleanup-block-pages --apply`) still
        takes the exclusive lock and waits for all of them.
        """
        return "exclusive"

    async def run(
        self,
        storage: PostgresRawStorage,
        args: argparse.Namespace,
        window: RunWindow,
        *,
        stale_after: timedelta,
    ) -> int:
        """Run once, or loop on `--interval`, and return a process exit code."""
        shutdown = asyncio.Event()
        # Installed for the whole lifetime rather than per run: with
        # --interval, a SIGTERM arriving during the sleep between runs would
        # otherwise hit the default handler and kill the process outright.
        with shutdown_signals(shutdown):
            while True:
                exit_code = await self.run_once(
                    storage, args, window, shutdown, stale_after=stale_after
                )
                interval = getattr(args, "interval", None)
                if not interval or exit_code != EXIT_OK or shutdown.is_set():
                    return exit_code
                logger.info("Next %s run in %d seconds", self.name, interval)
                await sleep_or_shutdown(interval, shutdown)
                if shutdown.is_set():
                    logger.info("Shutdown requested; not starting another run")
                    return EXIT_OK
                # Relative windows such as the default --last 3d describe
                # every scheduled run, not only the instant when this process
                # started. Explicit backfill bounds deliberately stay fixed.
                window = window.refreshed()

    async def run_once(
        self,
        storage: PostgresRawStorage,
        args: argparse.Namespace,
        window: RunWindow,
        shutdown: asyncio.Event,
        *,
        stale_after: timedelta,
    ) -> int:
        # Reap before taking the lock: the runs worth cleaning up belong to
        # processes that already died and no longer hold it.
        reaped = await storage.reap_abandoned(self.name, stale_after)
        if reaped:
            logger.info("Marked %d stale %s run(s) as abandoned", reaped, self.name)

        self.last_outcome = None
        mode = self.lock_mode(args)
        lock = (
            storage.worker_lock(self.name, shared=True)
            if mode == "shared"
            else storage.worker_lock(self.name)
        )
        async with lock as acquired:
            if not acquired:
                logger.info(
                    "Another %s run holds the lock; exiting without doing work",
                    self.name,
                )
                if target := getattr(args, "summary_json", None):
                    emit_summary(target, {
                        "run_id": None,
                        "worker": self.name,
                        "status": "skipped",
                        "reason": "lock_busy",
                        "trigger": _trigger_for(args, window),
                        "window_start": window.start.isoformat(),
                        "window_end": window.end.isoformat(),
                        "stats": None,
                        "error_message": None,
                        "orchestration": orchestration_context() or None,
                        "emitted_at": datetime.now(timezone.utc).isoformat(),
                    })
                return exit_busy_for(args)

            if window.is_deep_backfill:
                logger.warning(
                    "Window starts %s, further back than RSS feeds retain; "
                    "discovery will return few or no entries for that period",
                    window.start.isoformat(),
                )

            run_id = await storage.start_run(
                worker=self.name,
                trigger=_trigger_for(args, window),
                window_start=window.start,
                window_end=window.end,
                params=self._recorded_params(args),
            )
            logger.info(
                "Started %s run %d over %s..%s",
                self.name,
                run_id,
                window.start.isoformat(),
                window.end.isoformat(),
            )

            # The body stops on `run_stop`, not on the process-wide event: a
            # runtime budget ends this run only, so a looping worker (or the
            # semantic service driving several workers) keeps going. A process
            # shutdown still reaches the body through `forward`.
            run_stop = asyncio.Event()
            forward = asyncio.create_task(_forward(shutdown, run_stop))
            ctx = RunContext(
                run_id=run_id,
                window=window,
                shutdown=run_stop,
                args=args,
                storage=storage,
            )
            max_runtime = getattr(args, "max_runtime", None)
            runtime_guard = (
                asyncio.create_task(
                    request_shutdown_after(
                        parse_duration(max_runtime), run_stop, self.name
                    )
                )
                if max_runtime
                else None
            )
            try:
                try:
                    outcome = await self.execute(ctx)
                finally:
                    budget_reached = False
                    if runtime_guard is not None:
                        if runtime_guard.done() and not runtime_guard.cancelled():
                            budget_reached = runtime_guard.result()
                        runtime_guard.cancel()
                        with suppress(asyncio.CancelledError):
                            await runtime_guard
                    forward.cancel()
                    with suppress(asyncio.CancelledError):
                        await forward
                    # A body that asked to stop for its own reasons still
                    # means "stop the process"; only the budget is run-local.
                    if run_stop.is_set() and not budget_reached:
                        shutdown.set()
            except asyncio.CancelledError:
                await self._finish(
                    storage,
                    run_id,
                    RunOutcome("partial", ctx.last_stats, "Cancelled before the run completed"),
                    args=args,
                    window=window,
                )
                raise
            except Exception as exc:
                logger.exception("%s run %d failed", self.name, run_id)
                self.last_outcome = RunOutcome("failed", ctx.last_stats, str(exc))
                await self._finish(
                    storage, run_id, self.last_outcome, args=args, window=window
                )
                return EXIT_FAILED

            recorded = await self._finish(storage, run_id, outcome, args=args, window=window)
            if not recorded:
                self.last_outcome = RunOutcome(
                    "failed", outcome.stats, "Could not persist terminal run state"
                )
                return EXIT_FAILED
            self.last_outcome = outcome
            if outcome.status == "failed":
                return EXIT_FAILED
            if outcome.status == "partial" and getattr(args, "strict", False):
                return EXIT_FAILED
            return EXIT_OK

    def _recorded_params(self, args: argparse.Namespace) -> dict[str, Any]:
        """Combine business flags with scheduler metadata without a migration."""
        params = dict(self.run_params(args))
        if max_runtime := getattr(args, "max_runtime", None):
            params["max_runtime"] = max_runtime
        if context := orchestration_context():
            params["orchestration"] = context
        return params

    async def _finish(
        self,
        storage: PostgresRawStorage,
        run_id: int,
        outcome: RunOutcome,
        *,
        args: argparse.Namespace,
        window: RunWindow,
    ) -> bool:
        recorded = True
        try:
            await storage.finish_run(
                run_id, outcome.status, outcome.stats, outcome.error_message
            )
        except Exception:
            logger.exception("Could not close out %s run %d", self.name, run_id)
            recorded = False
            outcome = RunOutcome("failed", outcome.stats, "Could not persist terminal run state")
        else:
            logger.info("Finished %s run %d as %s", self.name, run_id, outcome.status)

        target = getattr(args, "summary_json", None)
        if target:
            context = orchestration_context()
            emit_summary(
                target,
                {
                    "run_id": run_id,
                    "worker": self.name,
                    "status": outcome.status,
                    "trigger": _trigger_for(args, window),
                    "window_start": window.start.isoformat(),
                    "window_end": window.end.isoformat(),
                    "stats": _json_safe(outcome.stats),
                    "error_message": outcome.error_message,
                    "orchestration": context or None,
                    "emitted_at": datetime.now(timezone.utc).isoformat(),
                },
            )
        return recorded


def exit_busy_for(args: argparse.Namespace) -> int:
    """Exit code to use when another run already holds the worker's lock.

    Defaults to `EXIT_OK`, because overlapping cron schedules are a non-event
    and always have been. A scheduler that needs to tell "skipped" apart from
    "did the work" -- Airflow's `skip_on_exit_code` -- passes its own code, so
    a busy run stops showing up as a success that quietly did nothing.
    """
    return int(getattr(args, "exit_busy", None) or EXIT_OK)


def _trigger_for(args: argparse.Namespace, window: RunWindow) -> WorkerTrigger:
    trigger = getattr(args, "trigger", None)
    if trigger:
        return trigger
    return "backfill" if window.explicit else "scheduled"


def stale_after_for(args: argparse.Namespace, floor_seconds: int) -> timedelta:
    """How long a run may go without a heartbeat before it is reaped.

    Three intervals of slack for a looping worker, never less than the
    configured floor -- which is what a one-shot cron run falls back to.
    """
    interval = getattr(args, "interval", None) or 0
    return timedelta(seconds=max(3 * interval, floor_seconds))
