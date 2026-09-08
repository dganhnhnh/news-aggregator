"""`JobQueue` port over `processing_jobs`, so a Redis-backed queue can
implement the same interface later (spec.md §4, week4) without touching the
stage workers.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncEngine

from adapters.storage.postgres import retry_delay
from adapters.storage.schema import processing_jobs


DEFAULT_MAX_ATTEMPTS = 5


@dataclass(frozen=True)
class Job:
    article_id: int
    stage: str
    attempts: int
    priority: int = 1


class JobQueue(ABC):
    @abstractmethod
    async def claim(
        self, stage: str, n: int, *, locked_by: str, lease: timedelta
    ) -> list[Job]:
        """Atomically move up to `n` pending, due jobs to 'running' and return them."""

    @abstractmethod
    async def complete(self, article_id: int, stage: str) -> None: ...

    @abstractmethod
    async def fail(
        self,
        article_id: int,
        stage: str,
        error: str,
        *,
        max_attempts: int = DEFAULT_MAX_ATTEMPTS,
    ) -> None:
        """Return the job to 'pending' with backoff, or 'dead_letter' past `max_attempts`."""

    @abstractmethod
    async def reclaim_expired(self, stage: str) -> int:
        """Return jobs whose lease expired -- the worker holding it died -- to 'pending'."""


class PostgresJobQueue(JobQueue):
    def __init__(self, engine: AsyncEngine) -> None:
        self._engine = engine

    async def reclaim_expired(self, stage: str) -> int:
        now = datetime.now(timezone.utc)
        async with self._engine.begin() as connection:
            result = await connection.execute(
                update(processing_jobs)
                .where(
                    processing_jobs.c.stage == stage,
                    processing_jobs.c.state == "running",
                    processing_jobs.c.lease_until < now,
                )
                .values(
                    state="pending", lease_until=None, locked_by=None, updated_at=now
                )
            )
            return int(result.rowcount or 0)

    async def claim(
        self, stage: str, n: int, *, locked_by: str, lease: timedelta
    ) -> list[Job]:
        if n <= 0:
            return []
        now = datetime.now(timezone.utc)
        # Claim commits immediately rather than holding the row lock through
        # the embed call: a batch takes seconds, and FOR UPDATE for that long
        # would block every other worker trying to claim.
        async with self._engine.begin() as connection:
            subquery = (
                select(processing_jobs.c.article_id)
                .where(
                    processing_jobs.c.stage == stage,
                    processing_jobs.c.state == "pending",
                    processing_jobs.c.available_at <= now,
                )
                .order_by(
                    processing_jobs.c.priority,
                    processing_jobs.c.available_at,
                )
                .limit(n)
                .with_for_update(skip_locked=True)
            )
            result = await connection.execute(
                update(processing_jobs)
                .where(
                    processing_jobs.c.stage == stage,
                    processing_jobs.c.article_id.in_(subquery),
                )
                .values(
                    state="running",
                    lease_until=now + lease,
                    locked_by=locked_by,
                    updated_at=now,
                )
                .returning(
                    processing_jobs.c.article_id,
                    processing_jobs.c.attempts,
                    processing_jobs.c.priority,
                )
            )
            return [
                Job(
                    article_id=row.article_id,
                    stage=stage,
                    attempts=row.attempts,
                    priority=row.priority,
                )
                for row in result
            ]

    async def complete(self, article_id: int, stage: str) -> None:
        async with self._engine.begin() as connection:
            await connection.execute(
                update(processing_jobs)
                .where(
                    processing_jobs.c.article_id == article_id,
                    processing_jobs.c.stage == stage,
                )
                .values(
                    state="done",
                    lease_until=None,
                    locked_by=None,
                    updated_at=datetime.now(timezone.utc),
                )
            )

    async def fail(
        self,
        article_id: int,
        stage: str,
        error: str,
        *,
        max_attempts: int = DEFAULT_MAX_ATTEMPTS,
    ) -> None:
        now = datetime.now(timezone.utc)
        async with self._engine.begin() as connection:
            row = (
                await connection.execute(
                    select(processing_jobs.c.attempts)
                    .where(
                        processing_jobs.c.article_id == article_id,
                        processing_jobs.c.stage == stage,
                    )
                    .with_for_update()
                )
            ).one()
            attempts = int(row.attempts) + 1
            dead = attempts >= max_attempts
            await connection.execute(
                update(processing_jobs)
                .where(
                    processing_jobs.c.article_id == article_id,
                    processing_jobs.c.stage == stage,
                )
                .values(
                    state="dead_letter" if dead else "pending",
                    attempts=attempts,
                    available_at=now if dead else now + retry_delay(attempts),
                    lease_until=None,
                    locked_by=None,
                    last_error=error[:2000],
                    updated_at=now,
                )
            )
