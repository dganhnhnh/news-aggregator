import argparse
import asyncio
from datetime import datetime, timedelta, timezone
from typing import Any

from models.storage_models import (
    ClusterArticle,
    ClusterAttachResult,
    ClusterCandidate,
    ClusterMembership,
    ClusterMergeResult,
)
from workers.base import RunContext
from workers.cluster import ClusterWorker
from workers.queue import Job
from workers.window import RunWindow


NOW = datetime(2026, 8, 26, 12, 0, tzinfo=timezone.utc)


def _args(**overrides: Any) -> argparse.Namespace:
    values = {
        "batch_size": 10,
        "candidate_limit": 20,
        "cluster_threshold": 0.75,
        "cluster_window_hours": 72,
        "merge_threshold": 0.85,
        "merge_only": False,
        "repair": False,
        "reprocess": False,
        "limit": 0,
    }
    values.update(overrides)
    return argparse.Namespace(**values)


def _article(article_id: int) -> ClusterArticle:
    return ClusterArticle(
        article_id=article_id,
        source_id=article_id,
        title=f"Article {article_id}",
        event_at=NOW,
        model="test",
        dim=2,
        embedding=[1.0, 0.0],
    )


class FakeQueue:
    def __init__(self, pending: list[int]) -> None:
        self.pending = pending
        self.completed: list[int] = []
        self.failed: list[int] = []

    async def reclaim_expired(self, stage: str) -> int:
        return 0

    async def claim(self, stage, n, *, locked_by, lease):
        batch, self.pending = self.pending[:n], self.pending[n:]
        return [Job(article_id=item, stage=stage, attempts=0) for item in batch]

    async def complete(self, article_id: int, stage: str) -> None:
        self.completed.append(article_id)

    async def fail(self, article_id, stage, error, *, max_attempts=5) -> None:
        self.failed.append(article_id)


class FakeStorage:
    def __init__(self) -> None:
        self.articles = {1: _article(1), 2: _article(2)}
        self.memberships: dict[int, ClusterMembership] = {}
        self.checkpoints: list[Any] = []
        self.requeued = 0
        self.removed_members: list[int] = []
        self.merge_pairs: list[tuple[int, int]] = []
        self.merge_calls: list[tuple[int, int]] = []
        self.merge_results: list[ClusterMergeResult] = []
        self.repair_fixed = 0

    async def clusterable_articles(self, ids):
        return {item: self.articles[item] for item in ids if item in self.articles}

    async def cluster_membership(self, article_id):
        return self.memberships.get(article_id)

    async def remove_cluster_member(self, article_id):
        membership = self.memberships.pop(article_id, None)
        if membership is None:
            return None
        self.removed_members.append(article_id)
        return membership.cluster_id

    async def active_cluster_candidates(self, article, *, window, limit):
        if article.article_id == 2:
            return [ClusterCandidate(10, [1.0, 0.0], 1, NOW, NOW, 0.9)]
        return []

    async def create_cluster(self, article):
        self.memberships[article.article_id] = ClusterMembership(10, False)
        return ClusterAttachResult(10, True, 1)

    async def attach_cluster_member(self, cluster_id, article, *, similarity):
        self.memberships[article.article_id] = ClusterMembership(cluster_id, False)
        return ClusterAttachResult(cluster_id, True, 2)

    async def close_stale_clusters(self, *, before):
        return 0

    async def reenqueue_clusters(self, *, since, until):
        return self.requeued

    async def merge_candidates(self, *, threshold, limit):
        pairs, self.merge_pairs = self.merge_pairs[:limit], self.merge_pairs[limit:]
        return pairs

    async def merge_clusters(self, cluster_a_id, cluster_b_id):
        self.merge_calls.append((cluster_a_id, cluster_b_id))
        if self.merge_results:
            return self.merge_results.pop(0)
        lower, higher = sorted((cluster_a_id, cluster_b_id))
        return ClusterMergeResult(lower, higher, True)

    async def repair_clusters(self, *, limit):
        fixed = min(self.repair_fixed, limit)
        self.repair_fixed -= fixed
        return fixed

    async def update_run(self, run_id, stats):
        self.checkpoints.append(stats)


def _run(storage: FakeStorage, queue: FakeQueue, args: argparse.Namespace):
    ctx = RunContext(
        run_id=1,
        window=RunWindow(NOW - timedelta(days=3), NOW, explicit=False),
        shutdown=asyncio.Event(),
        args=args,
        storage=storage,
    )
    return asyncio.run(ClusterWorker(queue=queue).execute(ctx))


def test_worker_creates_attaches_and_completes_missing_embedding() -> None:
    storage = FakeStorage()
    queue = FakeQueue([1, 2, 3])

    outcome = _run(storage, queue, _args())

    assert outcome.status == "succeeded"
    assert outcome.stats.created == 1
    assert outcome.stats.attached == 1
    assert outcome.stats.missing_embedding == 1
    assert queue.completed == [1, 2, 3]


def test_worker_retry_detects_membership_without_updating_centroid_again() -> None:
    storage = FakeStorage()
    storage.memberships[1] = ClusterMembership(cluster_id=10, stale=False)
    queue = FakeQueue([1])

    outcome = _run(storage, queue, _args())

    assert outcome.stats.already_member == 1
    assert outcome.stats.created == 0
    assert outcome.stats.attached == 0
    assert outcome.stats.reevaluated == 0
    assert queue.completed == [1]


def test_worker_reevaluates_stale_membership_after_reembed() -> None:
    storage = FakeStorage()
    storage.memberships[1] = ClusterMembership(cluster_id=10, stale=True)
    queue = FakeQueue([1])

    outcome = _run(storage, queue, _args())

    assert outcome.stats.reevaluated == 1
    assert outcome.stats.already_member == 0
    assert storage.removed_members == [1]
    # Article 1 has no configured candidates, so it re-attaches into a new
    # cluster rather than completing on stale membership alone.
    assert outcome.stats.created == 1
    assert queue.completed == [1]


def test_reprocess_uses_worker_window() -> None:
    storage = FakeStorage()
    storage.requeued = 4

    outcome = _run(storage, FakeQueue([]), _args(reprocess=True))

    assert outcome.stats.requeued == 4


def test_worker_merge_only_mode_skips_matching() -> None:
    storage = FakeStorage()
    storage.merge_pairs = [(10, 11)]
    queue = FakeQueue([1, 2])

    outcome = _run(storage, queue, _args(merge_only=True))

    assert outcome.stats.merged == 1
    assert storage.merge_calls == [(10, 11)]
    assert outcome.stats.claimed == 0
    assert queue.completed == []


def test_worker_repair_mode_skips_matching() -> None:
    storage = FakeStorage()
    storage.repair_fixed = 3
    queue = FakeQueue([1, 2])

    outcome = _run(storage, queue, _args(repair=True))

    assert outcome.stats.repaired == 3
    assert outcome.stats.claimed == 0
    assert queue.completed == []

