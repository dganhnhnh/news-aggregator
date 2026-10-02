"""`news-worker semantic`: embed → (classify ∥ cluster → merge) → refresh.

Replaces the `news_semantic` DAG with one long-lived process that is woken by
the `pg_notify` every enqueue into `processing_jobs` sends, instead of by a
fifteen-minute cron. That removes two costs the DAG paid on every run:
reloading the embedding model and waiting out the rest of the schedule.

This is not a `Worker`. It drives the existing embed, classify and cluster
workers through `Worker.run_once`, so each stage keeps its own lock, its own
`worker_runs` row and its own metrics exactly as before. What lives here is
only the pacing: when a pass starts, which stages it runs, and in what order.

Several replicas may run at once. Embed and classify drains take their lock
shared and split the queue through `SKIP LOCKED`; cluster, merge and the
label refresh take it exclusively, so a replica that finds one busy skips it
and the replica holding it finishes the job.
"""

from __future__ import annotations

import argparse
import asyncio
import time
from collections.abc import Callable
from contextlib import suppress
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

from adapters.storage import PostgresRawStorage
from core.logger import get_logger
from workers.base import (
    EXIT_FAILED,
    EXIT_OK,
    Worker,
    shutdown_signals,
    sleep_or_shutdown,
)
from workers.classify import ClassifyWorker
from workers.cluster import ClusterWorker
from workers.embed import EmbedWorker
from workers.window import RunWindow


logger = get_logger(__name__)

STAGES = ("embed", "classify", "cluster")


@dataclass(frozen=True)
class StageArgs:
    """One parsed `news-worker` namespace per stage invocation."""

    embed: argparse.Namespace
    classify: argparse.Namespace
    cluster: argparse.Namespace
    merge: argparse.Namespace
    refresh: argparse.Namespace


@dataclass(frozen=True)
class SemanticConfig:
    once: bool = False
    fallback_interval: float = 300.0
    """Longest idle wait. Covers a missed notification and retried jobs,
    whose `available_at` comes due without anyone enqueuing anything."""

    min_pass_interval: float = 60.0
    """Minimum gap between pass starts, so instant-crawl's 30-second writes
    fold into one pass instead of one pass each."""

    merge_every: timedelta = timedelta(minutes=15)
    refresh_every: timedelta = timedelta(minutes=15)
    stale_after: timedelta = timedelta(seconds=900)


@dataclass
class _Workers:
    embed: Worker
    classify: Worker
    cluster: Worker
    merge: Worker
    refresh: Worker


def _did_work(worker: Worker) -> bool:
    outcome = worker.last_outcome
    if outcome is None:
        return False
    return bool(getattr(outcome.stats, "claimed", 0))


def _ran(worker: Worker) -> bool:
    """Whether the latest run took its lock, as opposed to finding it busy."""
    return worker.last_outcome is not None


def _failed(worker: Worker) -> bool:
    outcome = worker.last_outcome
    return outcome is not None and outcome.status == "failed"


async def _wait_any(events: list[asyncio.Event], timeout: float) -> None:
    waiters = [asyncio.create_task(event.wait()) for event in events]
    try:
        await asyncio.wait(
            waiters, timeout=timeout, return_when=asyncio.FIRST_COMPLETED
        )
    finally:
        for waiter in waiters:
            waiter.cancel()
        for waiter in waiters:
            with suppress(asyncio.CancelledError):
                await waiter


class SemanticService:
    def __init__(
        self,
        storage: PostgresRawStorage,
        stage_args: StageArgs,
        window: RunWindow,
        config: SemanticConfig,
        *,
        workers: _Workers | None = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._storage = storage
        self._args = stage_args
        self._window = window
        self._config = config
        self._workers = workers
        self._clock = clock
        self._merge_pending = False
        self._refresh_pending = False
        self._last_merge: float | None = None
        self._last_refresh: float | None = None
        self._any_failed = False

    def _build_workers(self) -> _Workers:
        # Loaded once for the life of the process: this, not the queue, is
        # the saving over one container per DAG run.
        from adapters.embedding.fastembed_embedder import FastEmbedEmbedder

        return _Workers(
            embed=EmbedWorker(embedder=FastEmbedEmbedder()),
            classify=ClassifyWorker(),
            cluster=ClusterWorker(),
            merge=ClusterWorker(),
            refresh=ClassifyWorker(),
        )

    async def run(self) -> int:
        if self._workers is None:
            self._workers = self._build_workers()
        shutdown = asyncio.Event()
        with shutdown_signals(shutdown):
            if self._config.once:
                return await self._drain_once(shutdown)
            await self._serve(shutdown)
        logger.info("Semantic service stopped")
        return EXIT_OK

    async def _drain_once(self, shutdown: asyncio.Event) -> int:
        while not shutdown.is_set():
            if not await self.run_pass(shutdown):
                break
        if not shutdown.is_set():
            await self._flush(shutdown, force=True)
        return EXIT_FAILED if self._any_failed else EXIT_OK

    async def _serve(self, shutdown: asyncio.Event) -> None:
        last_start: float | None = None
        while not shutdown.is_set():
            try:
                async with self._storage.listen() as listener:
                    logger.info("Semantic service listening for queued jobs")
                    while not shutdown.is_set() and listener.alive:
                        if last_start is not None:
                            gap = self._config.min_pass_interval - (
                                self._clock() - last_start
                            )
                            if gap > 0:
                                await sleep_or_shutdown(gap, shutdown)
                                if shutdown.is_set():
                                    return
                        # Cleared before the pass, so anything committed while
                        # it runs leaves the event set and earns one more pass.
                        listener.event.clear()
                        last_start = self._clock()
                        if await self.run_pass(shutdown):
                            continue
                        await _wait_any(
                            [listener.event, shutdown],
                            self._config.fallback_interval,
                        )
                        if shutdown.is_set():
                            return
                        if not listener.alive:
                            logger.warning("Lost the LISTEN connection; reconnecting")
                        elif listener.event.is_set():
                            logger.info("Woken by notify")
                        else:
                            logger.info("Woken by the fallback timer")
            except Exception:
                # A database blip must not cost a model reload: log, wait out
                # the fallback interval and listen again.
                logger.exception("Semantic service loop failed; retrying")
                await sleep_or_shutdown(self._config.fallback_interval, shutdown)

    async def run_pass(self, shutdown: asyncio.Event) -> bool:
        """One pass over every stage; returns whether any stage claimed work."""
        assert self._workers is not None
        workers = self._workers
        window = self._window.refreshed()
        counts = await self._storage.actionable_jobs(STAGES)
        worked = False

        if counts["embed"]:
            worked |= await self._stage(workers.embed, self._args.embed, window, shutdown)
            if shutdown.is_set():
                return worked
            # Embedding is what queues classify and cluster, so count again.
            counts |= await self._storage.actionable_jobs(("classify", "cluster"))

        async def classify_branch() -> bool:
            if not counts["classify"]:
                return False
            return await self._stage(
                workers.classify, self._args.classify, window, shutdown
            )

        async def cluster_branch() -> bool:
            did = False
            if counts["cluster"]:
                did = await self._stage(
                    workers.cluster, self._args.cluster, window, shutdown
                )
            if did:
                self._merge_pending = True
            if not shutdown.is_set():
                await self._maybe_merge(window, shutdown, force=False)
            return did

        classified, clustered = await asyncio.gather(
            classify_branch(), cluster_branch()
        )
        worked |= classified or clustered
        if classified or clustered:
            self._refresh_pending = True
        if not shutdown.is_set():
            await self._maybe_refresh(window, shutdown, force=False)
        return worked

    async def _flush(self, shutdown: asyncio.Event, *, force: bool) -> None:
        window = self._window.refreshed()
        await self._maybe_merge(window, shutdown, force=force)
        if not shutdown.is_set():
            await self._maybe_refresh(window, shutdown, force=force)

    async def _maybe_merge(
        self, window: RunWindow, shutdown: asyncio.Event, *, force: bool
    ) -> None:
        if not self._merge_pending or not (
            force or self._due(self._last_merge, self._config.merge_every)
        ):
            return
        assert self._workers is not None
        await self._stage(self._workers.merge, self._args.merge, window, shutdown)
        # A busy lock leaves the merge owed; another replica is merging, and
        # this one will try again on a later pass.
        if _ran(self._workers.merge):
            self._merge_pending = False
            self._last_merge = self._clock()
            self._refresh_pending = True

    async def _maybe_refresh(
        self, window: RunWindow, shutdown: asyncio.Event, *, force: bool
    ) -> None:
        if not self._refresh_pending or not (
            force or self._due(self._last_refresh, self._config.refresh_every)
        ):
            return
        assert self._workers is not None
        await self._stage(self._workers.refresh, self._args.refresh, window, shutdown)
        if _ran(self._workers.refresh):
            self._refresh_pending = False
            self._last_refresh = self._clock()

    def _due(self, last: float | None, every: timedelta) -> bool:
        return last is None or self._clock() - last >= every.total_seconds()

    async def _stage(
        self,
        worker: Worker,
        args: argparse.Namespace,
        window: RunWindow,
        shutdown: asyncio.Event,
    ) -> bool:
        await worker.run_once(
            self._storage,
            args,
            window,
            shutdown,
            stale_after=self._config.stale_after,
        )
        if _failed(worker):
            self._any_failed = True
        return _did_work(worker)


def describe(config: SemanticConfig) -> dict[str, Any]:
    """The pacing knobs, for the startup log line."""
    return {
        "once": config.once,
        "fallback_interval": config.fallback_interval,
        "min_pass_interval": config.min_pass_interval,
        "merge_every": str(config.merge_every),
        "refresh_every": str(config.refresh_every),
    }
