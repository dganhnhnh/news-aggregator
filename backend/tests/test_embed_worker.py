import argparse
import asyncio
from collections.abc import Sequence
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest

from models.storage_models import EmbeddingCandidate, EmbeddingRecord
from services.raw_storage import content_hash
from services.text import article_embed_text
from workers.base import RunContext
from workers.embed import EmbedWorker
from workers.queue import Job
from workers.window import RunWindow


NOW = datetime(2026, 8, 21, 12, 0, tzinfo=timezone.utc)


def _window(*, explicit: bool = False, days: int = 3) -> RunWindow:
    return RunWindow(start=NOW - timedelta(days=days), end=NOW, explicit=explicit)


def _args(**overrides: Any) -> argparse.Namespace:
    defaults: dict[str, Any] = {
        "batch_size": 10,
        "limit": 0,
        "reprocess": False,
        "model_changed": False,
    }
    defaults.update(overrides)
    return argparse.Namespace(**defaults)


def _candidate(
    article_id: int,
    *,
    source_id: int | None = 7,
    content: str = "Body text.",
    feed_summary: str = "",
    content_source: str = "crawl",
    existing_model: str | None = None,
    existing_content_hash: bytes | None = None,
) -> EmbeddingCandidate:
    return EmbeddingCandidate(
        article_id=article_id,
        source_id=source_id,
        title="Title",
        content=content,
        feed_summary=feed_summary,
        content_source=content_source,
        existing_model=existing_model,
        existing_content_hash=existing_content_hash,
    )


def _embedded_hash(
    candidate: EmbeddingCandidate, boilerplate: frozenset[str] = frozenset()
) -> bytes:
    """The hash the worker will compute for this candidate's chosen text."""
    text, _ = article_embed_text(
        title=candidate.title,
        content=candidate.content,
        feed_summary=candidate.feed_summary,
        content_source=candidate.content_source,
        boilerplate=boilerplate,
    )
    return content_hash(text)


class FakeQueue:
    """In-memory JobQueue; only implements what EmbedWorker.execute touches."""

    def __init__(self, pending: list[int], *, reclaimed: int = 0) -> None:
        self._pending = list(pending)
        self.reclaimed = reclaimed
        self.completed: list[int] = []
        self.failed: list[tuple[int, str]] = []
        self.claim_calls = 0

    async def reclaim_expired(self, stage: str) -> int:
        return self.reclaimed

    async def claim(
        self, stage: str, n: int, *, locked_by: str, lease: timedelta
    ) -> list[Job]:
        self.claim_calls += 1
        batch, self._pending = self._pending[:n], self._pending[n:]
        return [Job(article_id=article_id, stage=stage, attempts=0) for article_id in batch]

    async def complete(self, article_id: int, stage: str) -> None:
        self.completed.append(article_id)

    async def fail(
        self, article_id: int, stage: str, error: str, *, max_attempts: int = 5
    ) -> None:
        self.failed.append((article_id, error))


class FakeStorage:
    def __init__(
        self,
        candidates: dict[int, EmbeddingCandidate],
        profiles: dict[int, frozenset[str]] | None = None,
    ) -> None:
        self._candidates = candidates
        self._profiles = profiles or {}
        self.upserted: list[EmbeddingRecord] = []
        self.reenqueue_calls: list[dict[str, Any]] = []
        self.reenqueue_result = 0
        self.ensure_calls: list[tuple[list[int], tuple[str, ...], int]] = []
        self.profile_loads = 0

    async def boilerplate_profiles(self) -> dict[int, frozenset[str]]:
        self.profile_loads += 1
        return dict(self._profiles)

    async def embeddable_articles(
        self, article_ids: Sequence[int]
    ) -> dict[int, EmbeddingCandidate]:
        return {aid: self._candidates[aid] for aid in article_ids if aid in self._candidates}

    async def upsert_embeddings(self, records: Sequence[EmbeddingRecord]) -> None:
        self.upserted.extend(records)

    async def ensure_processing_jobs(
        self,
        article_ids: Sequence[int],
        stages: Sequence[str],
        *,
        priority: int = 1,
    ) -> None:
        self.ensure_calls.append((list(article_ids), tuple(stages), priority))

    async def update_run(self, run_id: int, stats: Any) -> None:
        pass

    async def reenqueue_embeddings(
        self, *, since: datetime, until: datetime, stage: str = "embed", model: str | None = None
    ) -> int:
        self.reenqueue_calls.append(
            {"since": since, "until": until, "stage": stage, "model": model}
        )
        return self.reenqueue_result


class FakeEmbedder:
    model_name = "fake-model"
    dim = 3

    def __init__(self, *, fail_texts: set[str] | None = None) -> None:
        self._fail_texts = fail_texts or set()
        self.batches: list[list[str]] = []

    def embed(self, texts: Sequence[str]) -> list[list[float]]:
        self.batches.append(list(texts))
        if any(text in self._fail_texts for text in texts):
            raise RuntimeError("embedding backend exploded")
        return [[0.1, 0.2, 0.3] for _ in texts]


def _run(
    storage: FakeStorage,
    queue: FakeQueue,
    embedder: FakeEmbedder,
    args: argparse.Namespace,
    *,
    shutdown: asyncio.Event | None = None,
):
    ctx = RunContext(
        run_id=1,
        window=_window(),
        shutdown=shutdown or asyncio.Event(),
        args=args,
        storage=storage,
    )
    worker = EmbedWorker(embedder=embedder, queue=queue)
    return asyncio.run(worker.execute(ctx)), ctx


def test_new_articles_are_embedded_and_completed() -> None:
    storage = FakeStorage({1: _candidate(1), 2: _candidate(2)})
    queue = FakeQueue([1, 2])
    embedder = FakeEmbedder()

    outcome, _ = _run(storage, queue, embedder, _args())

    assert outcome.status == "succeeded"
    assert outcome.stats.embedded == 2
    assert {record.article_id for record in storage.upserted} == {1, 2}
    assert sorted(queue.completed) == [1, 2]


def test_unchanged_content_is_skipped_without_calling_the_embedder() -> None:
    unchanged = _candidate(1, existing_model="fake-model")
    unchanged = _candidate(
        1,
        existing_model="fake-model",
        existing_content_hash=_embedded_hash(unchanged),
    )
    storage = FakeStorage({1: unchanged})
    queue = FakeQueue([1])
    embedder = FakeEmbedder()

    outcome, _ = _run(storage, queue, embedder, _args())

    assert outcome.stats.skipped_unchanged == 1
    assert outcome.stats.embedded == 0
    assert embedder.batches == []
    assert storage.upserted == []
    assert storage.ensure_calls == [([1], ("classify", "cluster"), 1)]
    assert queue.completed == [1]


def test_changed_content_is_reembedded_despite_a_prior_embedding() -> None:
    changed = _candidate(
        1, existing_model="fake-model", existing_content_hash=b"a stale hash"
    )
    storage = FakeStorage({1: changed})
    queue = FakeQueue([1])
    embedder = FakeEmbedder()

    outcome, _ = _run(storage, queue, embedder, _args())

    assert outcome.stats.embedded == 1
    assert outcome.stats.skipped_unchanged == 0


def test_batch_failure_isolates_only_the_poison_pill_article() -> None:
    queue = FakeQueue([1, 2])

    class SelectiveEmbedder(FakeEmbedder):
        def embed(self, texts: Sequence[str]) -> list[list[float]]:
            self.batches.append(list(texts))
            if len(texts) > 1:
                raise RuntimeError("batch exploded")
            if texts[0] == "poison":
                raise RuntimeError("this article is bad")
            return [[0.1, 0.2, 0.3] for _ in texts]

    storage = FakeStorage(
        {
            1: _candidate(1, content="good body"),
            2: EmbeddingCandidate(
                article_id=2,
                source_id=7,
                title=None,
                content="poison",
                feed_summary="",
                content_source="crawl",
                existing_model=None,
                existing_content_hash=None,
            ),
        }
    )
    embedder = SelectiveEmbedder()

    outcome, _ = _run(storage, queue, embedder, _args())

    assert outcome.status == "partial"
    assert outcome.stats.embedded == 1
    assert outcome.stats.failed == 1
    assert queue.completed == [1]
    assert [article_id for article_id, _ in queue.failed] == [2]
    assert {record.article_id for record in storage.upserted} == {1}


def test_limit_bounds_total_articles_processed() -> None:
    storage = FakeStorage({i: _candidate(i) for i in range(1, 6)})
    queue = FakeQueue([1, 2, 3, 4, 5])
    embedder = FakeEmbedder()

    outcome, _ = _run(storage, queue, embedder, _args(batch_size=2, limit=3))

    assert outcome.stats.claimed == 3


def test_shutdown_between_chunks_stops_but_keeps_completed_work() -> None:
    storage = FakeStorage({i: _candidate(i) for i in range(1, 5)})
    queue = FakeQueue([1, 2, 3, 4])
    embedder = FakeEmbedder()
    shutdown = asyncio.Event()

    real_claim = queue.claim

    async def claim_then_shutdown(*args: Any, **kwargs: Any) -> list[Job]:
        jobs = await real_claim(*args, **kwargs)
        if queue.claim_calls == 1:
            shutdown.set()
        return jobs

    queue.claim = claim_then_shutdown  # type: ignore[method-assign]

    outcome, _ = _run(
        storage, queue, embedder, _args(batch_size=2), shutdown=shutdown
    )

    assert outcome.status == "partial"
    assert outcome.stats.claimed == 2
    assert len(queue.completed) == 2


def test_reclaimed_leases_are_reported_in_stats() -> None:
    storage = FakeStorage({})
    queue = FakeQueue([], reclaimed=3)
    embedder = FakeEmbedder()

    outcome, _ = _run(storage, queue, embedder, _args())

    assert outcome.stats.reclaimed == 3
    assert outcome.stats.claimed == 0


def test_reprocess_triggers_reenqueue_without_a_model_filter() -> None:
    storage = FakeStorage({})
    queue = FakeQueue([])
    embedder = FakeEmbedder()

    outcome, ctx = _run(storage, queue, embedder, _args(reprocess=True))

    assert len(storage.reenqueue_calls) == 1
    call = storage.reenqueue_calls[0]
    assert call["model"] is None
    assert call["since"] == ctx.window.start
    assert call["until"] == ctx.window.end


def test_model_changed_triggers_reenqueue_with_the_configured_model() -> None:
    storage = FakeStorage({})
    queue = FakeQueue([])
    embedder = FakeEmbedder()

    _run(storage, queue, embedder, _args(model_changed=True))

    assert storage.reenqueue_calls[0]["model"] == "fake-model"


def test_neither_reprocess_nor_model_changed_skips_reenqueue() -> None:
    storage = FakeStorage({})
    queue = FakeQueue([])
    embedder = FakeEmbedder()

    _run(storage, queue, embedder, _args())

    assert storage.reenqueue_calls == []
