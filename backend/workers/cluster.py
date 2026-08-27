"""Cluster worker: attach embedded articles to active story centroids."""

from __future__ import annotations

import argparse
import os
from collections.abc import Mapping
from datetime import timedelta
from typing import Any

from core.logger import get_logger
from models.storage_models import ClusterRunStats
from services.clustering import choose_cluster
from workers.base import RunContext, RunOutcome, Worker
from workers.queue import DEFAULT_MAX_ATTEMPTS, JobQueue, PostgresJobQueue


logger = get_logger(__name__)

STAGE = "cluster"
LEASE = timedelta(minutes=10)
MAX_ATTEMPTS = DEFAULT_MAX_ATTEMPTS
MERGE_BATCH_SIZE = 20
REPAIR_BATCH_SIZE = 50


class ClusterWorker(Worker):
    name = STAGE

    def __init__(self, queue: JobQueue | None = None) -> None:
        self._queue = queue

    def run_params(self, args: argparse.Namespace) -> Mapping[str, Any]:
        return {
            "batch_size": args.batch_size,
            "candidate_limit": args.candidate_limit,
            "cluster_threshold": args.cluster_threshold,
            "cluster_window_hours": args.cluster_window_hours,
            "merge_threshold": args.merge_threshold,
            "merge_only": args.merge_only,
            "repair": args.repair,
            "reprocess": args.reprocess,
            "limit": args.limit,
        }

    async def execute(self, ctx: RunContext) -> RunOutcome:
        args = ctx.args
        storage = ctx.storage
        queue = self._queue or PostgresJobQueue(storage.engine)
        locked_by = f"cluster-{ctx.run_id}-{os.getpid()}"
        cluster_window = timedelta(hours=args.cluster_window_hours)
        stats = ClusterRunStats()

        if args.merge_only:
            interrupted = await self._run_merge(ctx, storage, args, stats)
        elif args.repair:
            interrupted = await self._run_repair(ctx, storage, args, stats)
        else:
            stats.reclaimed = await queue.reclaim_expired(STAGE)
            if args.reprocess:
                stats.requeued = await storage.reenqueue_clusters(
                    since=ctx.window.start, until=ctx.window.end
                )
            interrupted = await self._run_matching(
                ctx, storage, queue, locked_by, cluster_window, args, stats
            )

        # Always closes stale clusters, regardless of mode, so a merge-only or
        # repair-only run still keeps the active candidate set current.
        stats.closed = await storage.close_stale_clusters(
            before=ctx.window.end - cluster_window
        )
        logger.info(
            "Cluster run %d: reclaimed=%d requeued=%d claimed=%d created=%d "
            "attached=%d already_member=%d reevaluated=%d missing_embedding=%d "
            "merged=%d repaired=%d closed=%d failed=%d",
            ctx.run_id,
            stats.reclaimed,
            stats.requeued,
            stats.claimed,
            stats.created,
            stats.attached,
            stats.already_member,
            stats.reevaluated,
            stats.missing_embedding,
            stats.merged,
            stats.repaired,
            stats.closed,
            stats.failed,
        )
        if interrupted:
            return RunOutcome("partial", stats, "Stopped early on a shutdown signal")
        return RunOutcome("partial" if stats.failed else "succeeded", stats)

    async def _run_matching(
        self,
        ctx: RunContext,
        storage: Any,
        queue: JobQueue,
        locked_by: str,
        cluster_window: timedelta,
        args: argparse.Namespace,
        stats: ClusterRunStats,
    ) -> bool:
        limit = args.limit or None
        while not ctx.shutdown.is_set():
            remaining = (
                limit - stats.claimed if limit is not None else args.batch_size
            )
            if remaining <= 0:
                break
            jobs = await queue.claim(
                STAGE,
                min(args.batch_size, remaining),
                locked_by=locked_by,
                lease=LEASE,
            )
            if not jobs:
                break
            stats.claimed += len(jobs)
            articles = await storage.clusterable_articles(
                [job.article_id for job in jobs]
            )

            for job in jobs:
                article = articles.get(job.article_id)
                if article is None:
                    stats.missing_embedding += 1
                    await queue.complete(job.article_id, STAGE)
                    continue
                try:
                    membership = await storage.cluster_membership(
                        article.article_id
                    )
                    if membership is not None and not membership.stale:
                        stats.already_member += 1
                        await queue.complete(article.article_id, STAGE)
                        continue
                    if membership is not None and membership.stale:
                        # The embedding was rewritten after clustering; the
                        # old centroid no longer reflects this article, so
                        # detach and rebuild before re-evaluating it fresh.
                        await storage.remove_cluster_member(article.article_id)
                        stats.reevaluated += 1

                    candidates = await storage.active_cluster_candidates(
                        article,
                        window=cluster_window,
                        limit=args.candidate_limit,
                    )
                    best = choose_cluster(
                        candidates,
                        article_at=article.event_at,
                        threshold=args.cluster_threshold,
                    )
                    if best is None:
                        result = await storage.create_cluster(article)
                        if result.attached:
                            stats.created += 1
                        else:
                            stats.already_member += 1
                    else:
                        result = await storage.attach_cluster_member(
                            best.cluster_id,
                            article,
                            similarity=best.similarity,
                        )
                        if result.attached:
                            stats.attached += 1
                        else:
                            stats.already_member += 1
                    await queue.complete(article.article_id, STAGE)
                except Exception as exc:
                    logger.warning(
                        "Clustering failed for article %d: %s",
                        article.article_id,
                        exc,
                    )
                    await queue.fail(
                        article.article_id,
                        STAGE,
                        str(exc),
                        max_attempts=MAX_ATTEMPTS,
                    )
                    stats.failed += 1

            await ctx.checkpoint(stats)
            if ctx.shutdown.is_set():
                return True
        return False

    async def _run_merge(
        self,
        ctx: RunContext,
        storage: Any,
        args: argparse.Namespace,
        stats: ClusterRunStats,
    ) -> bool:
        """Pair-scan and merge open clusters until no more pairs clear the
        merge threshold. `--limit` bounds the total pairs evaluated, not just
        successful merges, so a run of races that fail to merge still
        terminates."""
        limit = args.limit or None
        processed = 0
        while not ctx.shutdown.is_set():
            remaining = (
                limit - processed if limit is not None else MERGE_BATCH_SIZE
            )
            if remaining <= 0:
                break
            pairs = await storage.merge_candidates(
                threshold=args.merge_threshold,
                limit=min(MERGE_BATCH_SIZE, remaining),
            )
            if not pairs:
                break
            for cluster_a_id, cluster_b_id in pairs:
                result = await storage.merge_clusters(cluster_a_id, cluster_b_id)
                if result.merged:
                    stats.merged += 1
            processed += len(pairs)
            await ctx.checkpoint(stats)
        return ctx.shutdown.is_set()

    async def _run_repair(
        self,
        ctx: RunContext,
        storage: Any,
        args: argparse.Namespace,
        stats: ClusterRunStats,
    ) -> bool:
        limit = args.limit or None
        processed = 0
        while not ctx.shutdown.is_set():
            remaining = (
                limit - processed if limit is not None else REPAIR_BATCH_SIZE
            )
            if remaining <= 0:
                break
            fixed = await storage.repair_clusters(
                limit=min(REPAIR_BATCH_SIZE, remaining)
            )
            stats.repaired += fixed
            processed += fixed
            await ctx.checkpoint(stats)
            if fixed < REPAIR_BATCH_SIZE:
                break
        return ctx.shutdown.is_set()
