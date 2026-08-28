"""Operational frame shared by every worker.

A worker subclass implements `execute()` and nothing else: the advisory lock,
the `worker_runs` bookkeeping, heartbeats, graceful shutdown and the exit-code
contract all live here so they cannot drift between workers.
"""

from __future__ import annotations

import argparse
import asyncio
import signal
from abc import ABC, abstractmethod
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any, NamedTuple

from adapters.storage import PostgresRawStorage
from core.logger import get_logger
from models.storage_models import RunStatus, WorkerTrigger
from workers.window import RunWindow


logger = get_logger(__name__)

EXIT_OK = 0
EXIT_FAILED = 1


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

    async def checkpoint(self, stats: Any) -> None:
        """Persist progress and prove the run is still alive."""
        await self.storage.update_run(self.run_id, stats)


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


class Worker(ABC):
    name: str

    @abstractmethod
    async def execute(self, ctx: RunContext) -> RunOutcome:
        """Do the work, checking `ctx.shutdown` between chunks."""

    def run_params(self, args: argparse.Namespace) -> Mapping[str, Any]:
        """Flags worth recording on the run for later diagnosis."""
        return {}

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

        async with storage.worker_lock(self.name) as acquired:
            if not acquired:
                logger.info(
                    "Another %s run holds the lock; exiting without doing work",
                    self.name,
                )
                return EXIT_OK

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
                params=self.run_params(args),
            )
            logger.info(
                "Started %s run %d over %s..%s",
                self.name,
                run_id,
                window.start.isoformat(),
                window.end.isoformat(),
            )

            ctx = RunContext(
                run_id=run_id,
                window=window,
                shutdown=shutdown,
                args=args,
                storage=storage,
            )
            try:
                outcome = await self.execute(ctx)
            except asyncio.CancelledError:
                await self._finish(
                    storage,
                    run_id,
                    RunOutcome("partial", None, "Cancelled before the run completed"),
                )
                raise
            except Exception as exc:
                logger.exception("%s run %d failed", self.name, run_id)
                await self._finish(
                    storage, run_id, RunOutcome("failed", None, str(exc))
                )
                return EXIT_FAILED

            await self._finish(storage, run_id, outcome)
            if outcome.status == "failed":
                return EXIT_FAILED
            if outcome.status == "partial" and getattr(args, "strict", False):
                return EXIT_FAILED
            return EXIT_OK

    async def _finish(
        self,
        storage: PostgresRawStorage,
        run_id: int,
        outcome: RunOutcome,
    ) -> None:
        try:
            await storage.finish_run(
                run_id, outcome.status, outcome.stats, outcome.error_message
            )
        except Exception:  # pragma: no cover - best effort bookkeeping
            logger.exception("Could not close out %s run %d", self.name, run_id)
            return
        logger.info("Finished %s run %d as %s", self.name, run_id, outcome.status)


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
