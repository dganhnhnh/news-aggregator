import asyncio
from datetime import datetime, timedelta, timezone
import os

import pytest
from sqlalchemy import delete, select, update
from sqlalchemy.ext.asyncio import create_async_engine

from adapters.storage.postgres import PostgresRawStorage
from adapters.storage.schema import worker_runs


DATABASE_URL = os.getenv("TEST_DATABASE_URL")
pytestmark = pytest.mark.integration


def test_worker_lock_admits_one_holder_at_a_time() -> None:
    if not DATABASE_URL:
        pytest.skip("TEST_DATABASE_URL is not set")
    asyncio.run(_test_worker_lock_admits_one_holder_at_a_time())


async def _test_worker_lock_admits_one_holder_at_a_time() -> None:
    # Two storages stand in for two processes: advisory locks are per session,
    # so a single engine would happily hand the same lock to itself.
    first = PostgresRawStorage(DATABASE_URL)
    second = PostgresRawStorage(DATABASE_URL)
    try:
        await first.check_ready()
        async with first.worker_lock("test-lock") as held:
            assert held is True
            async with second.worker_lock("test-lock") as blocked:
                assert blocked is False
            # A different worker name is a different lock.
            async with second.worker_lock("test-lock-other") as unrelated:
                assert unrelated is True

        async with second.worker_lock("test-lock") as reacquired:
            assert reacquired is True
    finally:
        await first.close()
        await second.close()


def test_reaper_only_touches_stale_runs_of_its_own_worker() -> None:
    if not DATABASE_URL:
        pytest.skip("TEST_DATABASE_URL is not set")
    asyncio.run(_test_reaper_only_touches_stale_runs_of_its_own_worker())


async def _test_reaper_only_touches_stale_runs_of_its_own_worker() -> None:
    storage = PostgresRawStorage(DATABASE_URL)
    engine = create_async_engine(DATABASE_URL)
    run_ids: list[int] = []
    ready = False
    try:
        await storage.check_ready()
        ready = True
        stale_id = await storage.start_run(worker="reaper-test", trigger="scheduled")
        fresh_id = await storage.start_run(worker="reaper-test", trigger="scheduled")
        other_id = await storage.start_run(
            worker="reaper-test-other", trigger="scheduled"
        )
        run_ids = [stale_id, fresh_id, other_id]

        long_ago = datetime.now(timezone.utc) - timedelta(hours=2)
        async with engine.begin() as connection:
            await connection.execute(
                update(worker_runs)
                .where(worker_runs.c.id.in_([stale_id, other_id]))
                .values(heartbeat_at=long_ago)
            )

        reaped = await storage.reap_abandoned("reaper-test", timedelta(seconds=900))
        assert reaped == 1

        async with engine.connect() as connection:
            rows = await connection.execute(
                select(
                    worker_runs.c.id,
                    worker_runs.c.status,
                    worker_runs.c.finished_at,
                    worker_runs.c.error_message,
                ).where(worker_runs.c.id.in_(run_ids))
            )
            by_id = {row.id: row for row in rows}

        assert by_id[stale_id].status == "abandoned"
        assert by_id[stale_id].finished_at is not None
        assert "Heartbeat" in by_id[stale_id].error_message
        assert by_id[fresh_id].status == "running"
        # Stale, but belongs to another worker: not this reaper's business.
        assert by_id[other_id].status == "running"

        # A second pass has nothing left to do.
        assert await storage.reap_abandoned("reaper-test", timedelta(seconds=900)) == 0
    finally:
        if ready and run_ids:
            async with engine.begin() as connection:
                await connection.execute(
                    delete(worker_runs).where(worker_runs.c.id.in_(run_ids))
                )
        await engine.dispose()
        await storage.close()
