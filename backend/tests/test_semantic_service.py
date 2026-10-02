import argparse
import asyncio
import os
import signal
import sys
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest

from adapters.storage import NotificationListener
from workers.base import EXIT_FAILED, EXIT_OK, RunContext, RunOutcome, Worker
from workers.semantic import SemanticConfig, SemanticService, StageArgs, _Workers
from workers.window import RunWindow


NOW = datetime(2026, 9, 29, 12, 0, tzinfo=timezone.utc)
WINDOW = RunWindow(start=NOW - timedelta(hours=24), end=NOW, explicit=False)


@dataclass
class Stats:
    claimed: int = 0


class FakeStorage:
    """A queue of per-stage counts that the fake workers drain."""

    def __init__(self, **counts: int) -> None:
        self.counts = {"embed": 0, "classify": 0, "cluster": 0} | counts
        self.listener = NotificationListener()
        self.listens = 0

    async def actionable_jobs(self, stages) -> dict[str, int]:
        return {stage: self.counts.get(stage, 0) for stage in stages}

    @asynccontextmanager
    async def listen(self):
        self.listens += 1
        self.listener = NotificationListener()
        yield self.listener


class FakeStage(Worker):
    """Records each run and drains its stage; `feeds` queues follow-on work."""

    def __init__(
        self,
        label: str,
        log: list[str],
        storage: FakeStorage,
        *,
        stage: str | None = None,
        feeds: tuple[str, ...] = (),
        busy: bool = False,
        fail: bool = False,
        delay: float = 0.0,
    ) -> None:
        self.name = label
        self.label = label
        self.log = log
        self.storage = storage
        self.stage = stage
        self.feeds = feeds
        self.busy = busy
        self.fail = fail
        self.delay = delay

    async def execute(self, ctx: RunContext) -> RunOutcome:  # pragma: no cover
        raise NotImplementedError

    async def run_once(self, storage, args, window, shutdown, *, stale_after) -> int:
        self.log.append(f"{self.label}:start")
        if self.delay:
            await asyncio.sleep(self.delay)
        self.log.append(f"{self.label}:end")
        if self.busy:
            self.last_outcome = None
            return EXIT_OK
        if self.fail:
            self.last_outcome = RunOutcome("failed", None, "boom")
            return EXIT_FAILED
        claimed = 0
        if self.stage is not None:
            claimed = self.storage.counts[self.stage]
            self.storage.counts[self.stage] = 0
            if claimed:
                for stage in self.feeds:
                    self.storage.counts[stage] += claimed
        self.last_outcome = RunOutcome("succeeded", Stats(claimed=claimed))
        return EXIT_OK


def _ns(**kwargs: Any) -> argparse.Namespace:
    return argparse.Namespace(**kwargs)


STAGE_ARGS = StageArgs(
    embed=_ns(stage="embed"),
    classify=_ns(stage="classify"),
    cluster=_ns(stage="cluster"),
    merge=_ns(stage="merge"),
    refresh=_ns(stage="refresh"),
)


class Clock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now


def _service(
    storage: FakeStorage,
    log: list[str],
    config: SemanticConfig | None = None,
    *,
    clock=None,
    **overrides: Any,
) -> tuple[SemanticService, _Workers]:
    workers = _Workers(
        embed=overrides.get("embed")
        or FakeStage("embed", log, storage, stage="embed", feeds=("classify", "cluster")),
        classify=overrides.get("classify")
        or FakeStage("classify", log, storage, stage="classify"),
        cluster=overrides.get("cluster")
        or FakeStage("cluster", log, storage, stage="cluster"),
        merge=overrides.get("merge") or FakeStage("merge", log, storage),
        refresh=overrides.get("refresh") or FakeStage("refresh", log, storage),
    )
    service = SemanticService(
        storage,  # type: ignore[arg-type]
        STAGE_ARGS,
        WINDOW,
        config or SemanticConfig(),
        workers=workers,
        clock=clock or Clock(),
    )
    return service, workers


def _starts(log: list[str]) -> list[str]:
    return [entry.split(":")[0] for entry in log if entry.endswith(":start")]


def test_a_pass_runs_embed_then_classify_and_cluster_then_merge_and_refresh() -> None:
    storage = FakeStorage(embed=3)
    log: list[str] = []
    service, _ = _service(storage, log)

    worked = asyncio.run(service.run_pass(asyncio.Event()))

    assert worked
    starts = _starts(log)
    assert starts[0] == "embed"
    assert starts[-1] == "refresh"
    assert set(starts[1:3]) == {"classify", "cluster"}
    assert starts.index("merge") > starts.index("cluster")
    # Embedding is what queued the downstream stages: they were re-counted.
    assert storage.counts == {"embed": 0, "classify": 0, "cluster": 0}


def test_classify_and_cluster_overlap() -> None:
    storage = FakeStorage(classify=1, cluster=1)
    log: list[str] = []
    service, _ = _service(
        storage,
        log,
        classify=FakeStage("classify", log, storage, stage="classify", delay=0.02),
        cluster=FakeStage("cluster", log, storage, stage="cluster", delay=0.02),
    )

    asyncio.run(service.run_pass(asyncio.Event()))

    # Both started before either finished.
    assert log[:2] in (
        ["classify:start", "cluster:start"],
        ["cluster:start", "classify:start"],
    )


def test_stages_without_actionable_jobs_are_skipped() -> None:
    storage = FakeStorage()
    log: list[str] = []
    service, _ = _service(storage, log)

    worked = asyncio.run(service.run_pass(asyncio.Event()))

    assert not worked
    # Nothing ran, so no worker_runs row would have been written either.
    assert log == []


def test_only_the_stage_with_work_runs() -> None:
    storage = FakeStorage(classify=2)
    log: list[str] = []
    service, _ = _service(storage, log)

    asyncio.run(service.run_pass(asyncio.Event()))

    assert _starts(log) == ["classify", "refresh"]


def test_merge_and_refresh_are_throttled() -> None:
    storage = FakeStorage(cluster=1)
    log: list[str] = []
    clock = Clock()
    service, _ = _service(storage, log, clock=clock)

    async def scenario() -> None:
        shutdown = asyncio.Event()
        await service.run_pass(shutdown)
        storage.counts["cluster"] = 1
        clock.now += 60
        await service.run_pass(shutdown)
        storage.counts["cluster"] = 1
        clock.now += 15 * 60
        await service.run_pass(shutdown)

    asyncio.run(scenario())

    starts = _starts(log)
    assert starts.count("cluster") == 3
    assert starts.count("merge") == 2
    assert starts.count("refresh") == 2


def test_a_busy_merge_stays_owed() -> None:
    storage = FakeStorage(cluster=1)
    log: list[str] = []
    clock = Clock()
    merge = FakeStage("merge", log, storage, busy=True)
    service, _ = _service(storage, log, clock=clock, merge=merge)

    async def scenario() -> None:
        shutdown = asyncio.Event()
        await service.run_pass(shutdown)
        merge.busy = False
        # No new clustering, and still inside merge_every: a merge that never
        # ran is still due.
        await service.run_pass(shutdown)
        await service.run_pass(shutdown)

    asyncio.run(scenario())

    assert _starts(log).count("merge") == 2


def test_once_drains_then_forces_merge_and_refresh() -> None:
    storage = FakeStorage(embed=2)
    log: list[str] = []
    clock = Clock()
    service, _ = _service(storage, log, SemanticConfig(once=True), clock=clock)

    async def scenario() -> int:
        return await service._drain_once(asyncio.Event())

    assert asyncio.run(scenario()) == EXIT_OK
    starts = _starts(log)
    assert starts[0] == "embed"
    assert "merge" in starts and starts[-1] == "refresh"
    assert storage.counts == {"embed": 0, "classify": 0, "cluster": 0}


def test_once_exits_one_when_a_stage_failed() -> None:
    storage = FakeStorage(classify=1)
    log: list[str] = []
    service, _ = _service(
        storage,
        log,
        SemanticConfig(once=True),
        classify=FakeStage("classify", log, storage, fail=True),
    )

    assert asyncio.run(service._drain_once(asyncio.Event())) == EXIT_FAILED


def _serve_until(
    service: SemanticService,
    storage: FakeStorage,
    log: list[str],
    *,
    stop_when,
    during=None,
    timeout: float = 2.0,
) -> None:
    async def scenario() -> None:
        shutdown = asyncio.Event()
        task = asyncio.create_task(service._serve(shutdown))
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        while not stop_when() and loop.time() < deadline:
            if during is not None:
                during()
            await asyncio.sleep(0.005)
        shutdown.set()
        await asyncio.wait_for(task, timeout=1)

    asyncio.run(scenario())


def test_notify_wakes_an_idle_service() -> None:
    storage = FakeStorage()
    log: list[str] = []
    config = SemanticConfig(fallback_interval=60, min_pass_interval=0)
    service, _ = _service(storage, log, config)
    rang = []

    def ring() -> None:
        if not rang and storage.listens:
            storage.counts["classify"] = 1
            storage.listener.event.set()
            rang.append(True)

    _serve_until(
        service, storage, log, stop_when=lambda: "classify:end" in log, during=ring
    )

    assert "classify:end" in log


def test_fallback_timer_wakes_without_a_notification() -> None:
    # A job whose backoff expired is due, but nobody enqueued anything.
    storage = FakeStorage()
    log: list[str] = []
    config = SemanticConfig(fallback_interval=0.05, min_pass_interval=0)
    service, _ = _service(storage, log, config)
    loop_started = []

    def make_due() -> None:
        if not loop_started and storage.listens:
            storage.counts["cluster"] = 1
            loop_started.append(True)

    _serve_until(
        service, storage, log, stop_when=lambda: "cluster:end" in log, during=make_due
    )

    assert "cluster:end" in log


def test_passes_are_spaced_by_min_pass_interval() -> None:
    # instant-crawl commits every 30 seconds; each commit must not cost a pass.
    storage = FakeStorage(classify=1)
    log: list[str] = []
    config = SemanticConfig(fallback_interval=60, min_pass_interval=0.2)
    service, _ = _service(storage, log, config)
    stamps: list[float] = []
    original = service.run_pass

    async def timed_pass(shutdown: asyncio.Event) -> bool:
        stamps.append(asyncio.get_running_loop().time())
        # Keep the queue busy so the service never idles.
        storage.counts["classify"] = 1
        return await original(shutdown)

    service.run_pass = timed_pass  # type: ignore[method-assign]
    _serve_until(service, storage, log, stop_when=lambda: len(stamps) >= 3)

    gaps = [b - a for a, b in zip(stamps, stamps[1:])]
    assert len(gaps) >= 2
    assert all(gap >= 0.18 for gap in gaps)


def test_notifications_during_a_pass_coalesce_into_one_more_pass() -> None:
    storage = FakeStorage(classify=1)
    log: list[str] = []
    config = SemanticConfig(fallback_interval=60, min_pass_interval=0)
    passes: list[int] = []
    service, _ = _service(storage, log, config)
    original = service.run_pass

    async def pass_with_bursts(shutdown: asyncio.Event) -> bool:
        passes.append(1)
        did = await original(shutdown)
        if len(passes) == 1:
            for _ in range(5):
                storage.listener.event.set()
        return did

    service.run_pass = pass_with_bursts  # type: ignore[method-assign]
    _serve_until(service, storage, log, stop_when=lambda: False, timeout=0.2)

    # Pass 1 did work -> pass 2; five rings during pass 1 fold into pass 2,
    # which found nothing, so the service is idle waiting for the next ring.
    assert len(passes) == 2


def test_a_dead_listen_connection_is_reopened() -> None:
    storage = FakeStorage()
    log: list[str] = []
    config = SemanticConfig(fallback_interval=60, min_pass_interval=0)
    service, _ = _service(storage, log, config)
    killed = []

    def kill() -> None:
        if not killed and storage.listens == 1:
            storage.listener.alive = False
            storage.listener.event.set()
            killed.append(True)

    _serve_until(service, storage, log, stop_when=lambda: storage.listens >= 2, during=kill)

    assert storage.listens >= 2


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX signals")
def test_sigterm_stops_the_service_cleanly() -> None:
    storage = FakeStorage()
    log: list[str] = []
    service, _ = _service(storage, log, SemanticConfig(fallback_interval=60))

    async def scenario() -> int:
        loop = asyncio.get_running_loop()
        loop.call_later(0.05, os.kill, os.getpid(), signal.SIGTERM)
        return await asyncio.wait_for(service.run(), timeout=2)

    assert asyncio.run(scenario()) == EXIT_OK
