"""Classify worker: score embedded articles against the active topic set and
keep the affected cluster's aggregate label in step.
"""

from __future__ import annotations

import argparse
import os
from collections.abc import Mapping
from datetime import timedelta
from typing import Any

from core.logger import get_logger
from models.storage_models import ClassifyRunStats
from services.classification import (
    KEYWORD_WEIGHT,
    PROTOTYPE_WEIGHT,
    ClassifierMismatch,
    classify_article,
)
from services.text import article_embed_text
from workers.base import RunContext, RunOutcome, Worker
from workers.queue import DEFAULT_MAX_ATTEMPTS, JobQueue, PostgresJobQueue


logger = get_logger(__name__)

STAGE = "classify"
LEASE = timedelta(minutes=10)
MAX_ATTEMPTS = DEFAULT_MAX_ATTEMPTS


class ClassifyWorker(Worker):
    name = STAGE

    def __init__(self, queue: JobQueue | None = None) -> None:
        self._queue = queue

    def run_params(self, args: argparse.Namespace) -> Mapping[str, Any]:
        # The weights are recorded per run because they are a starting point
        # rather than a conclusion (spec §2.4): a later threshold sweep has to
        # be able to tell which run used which coefficients.
        return {
            "batch_size": args.batch_size,
            "reprocess": args.reprocess,
            "stale": args.stale,
            "refresh_clusters": args.refresh_clusters,
            "limit": args.limit,
            "prototype_weight": PROTOTYPE_WEIGHT,
            "keyword_weight": KEYWORD_WEIGHT,
        }

    async def execute(self, ctx: RunContext) -> RunOutcome:
        args = ctx.args
        storage = ctx.storage
        queue = self._queue or PostgresJobQueue(storage.engine)
        locked_by = f"classify-{ctx.run_id}-{os.getpid()}"
        stats = ClassifyRunStats()

        snapshot = await storage.classifier_snapshot()
        if snapshot.topics_without_prototype:
            logger.warning(
                "Skipping %d active topic(s) with no compiled prototype: %s; "
                "run `news-worker taxonomy-rebuild`",
                len(snapshot.topics_without_prototype),
                ", ".join(snapshot.topics_without_prototype),
            )
        if args.refresh_clusters:
            stats.clusters_refreshed = await storage.reenqueue_cluster_topic_refresh(
                since=ctx.window.start, until=ctx.window.end
            )
            logger.info(
                "Classify run %d: refreshed %d cluster label set(s)",
                ctx.run_id,
                stats.clusters_refreshed,
            )
            return RunOutcome("succeeded", stats)

        if not snapshot.classifiers:
            # An empty registry is a setup problem, not a data problem: draining
            # the queue here would mark every article "classified against
            # nothing" and hide it behind a done job.
            logger.warning(
                "No active topic has a prototype; nothing to classify. "
                "Run `news-worker taxonomy-import` then `taxonomy-rebuild`"
            )
            return RunOutcome("succeeded", stats)

        stats.reclaimed = await queue.reclaim_expired(STAGE)
        if args.reprocess or args.stale:
            stats.requeued = await storage.reenqueue_classifications(
                since=ctx.window.start,
                until=ctx.window.end,
                set_hash=snapshot.set_hash if args.stale else None,
            )

        interrupted = await self._run_classification(
            ctx, storage, queue, locked_by, snapshot, args, stats
        )

        logger.info(
            "Classify run %d: reclaimed=%d requeued=%d claimed=%d classified=%d "
            "assigned=%d uncategorized=%d clusters_refreshed=%d "
            "missing_embedding=%d failed=%d",
            ctx.run_id,
            stats.reclaimed,
            stats.requeued,
            stats.claimed,
            stats.classified,
            stats.assigned,
            stats.uncategorized,
            stats.clusters_refreshed,
            stats.missing_embedding,
            stats.failed,
        )
        if interrupted:
            return RunOutcome("partial", stats, "Stopped early on a shutdown signal")
        return RunOutcome("partial" if stats.failed else "succeeded", stats)

    async def _run_classification(
        self,
        ctx: RunContext,
        storage: Any,
        queue: JobQueue,
        locked_by: str,
        snapshot: Any,
        args: argparse.Namespace,
        stats: ClassifyRunStats,
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
            articles = await storage.classifiable_articles(
                [job.article_id for job in jobs]
            )

            for job in jobs:
                article = articles.get(job.article_id)
                if article is None:
                    # Not yet embedded (or the article is gone). Classification
                    # is defined against the vector, so complete and let the
                    # embed worker re-enqueue this stage when it lands.
                    stats.missing_embedding += 1
                    await queue.complete(job.article_id, STAGE)
                    continue
                try:
                    text, _ = article_embed_text(
                        title=article.title,
                        content=article.content,
                        feed_summary=article.feed_summary,
                        content_source=article.content_source,
                    )
                    classification = classify_article(
                        embedding=article.embedding,
                        model=article.model,
                        dim=article.dim,
                        text=text,
                        classifiers=snapshot.classifiers,
                    )
                    cluster_id = await storage.replace_article_classification(
                        article.article_id,
                        classification,
                        model=article.model,
                        set_hash=snapshot.set_hash,
                    )
                    stats.classified += 1
                    if classification.assigned_count:
                        stats.assigned += 1
                    else:
                        stats.uncategorized += 1
                    if cluster_id is not None:
                        stats.clusters_refreshed += 1
                    await queue.complete(article.article_id, STAGE)
                except ClassifierMismatch as exc:
                    # Actionable and not retryable by waiting: the operator has
                    # to rebuild prototypes or re-embed. Fail loudly rather than
                    # storing a cosine between incomparable vectors.
                    logger.error(
                        "Classifier mismatch for article %d: %s",
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
                except Exception as exc:
                    logger.warning(
                        "Classification failed for article %d: %s",
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
