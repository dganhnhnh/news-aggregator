"""Embed worker: claim due `processing_jobs(stage='embed')` rows, embed the
article's leading text, and upsert into `article_embeddings`.
"""

from __future__ import annotations

import argparse
import os
from asyncio import to_thread
from collections.abc import Mapping
from datetime import timedelta
from typing import Any

from adapters.embedding.embedder import Embedder
from adapters.embedding.fastembed_embedder import FastEmbedEmbedder
from adapters.storage import PostgresRawStorage
from core.logger import get_logger
from models.storage_models import EmbedRunStats, EmbeddingRecord
from services.text import embed_text
from workers.base import RunContext, RunOutcome, Worker
from workers.queue import DEFAULT_MAX_ATTEMPTS, JobQueue, PostgresJobQueue


logger = get_logger(__name__)

STAGE = "embed"
LEASE = timedelta(minutes=10)
MAX_ATTEMPTS = DEFAULT_MAX_ATTEMPTS


class EmbedWorker(Worker):
    name = "embed"

    def __init__(
        self,
        embedder: Embedder | None = None,
        queue: JobQueue | None = None,
    ) -> None:
        self._embedder = embedder
        self._queue = queue

    def run_params(self, args: argparse.Namespace) -> Mapping[str, Any]:
        return {
            "batch_size": args.batch_size,
            "reprocess": args.reprocess,
            "model_changed": args.model_changed,
            "limit": args.limit,
        }

    async def execute(self, ctx: RunContext) -> RunOutcome:
        args = ctx.args
        storage = ctx.storage
        embedder = self._embedder or FastEmbedEmbedder()
        queue = self._queue or PostgresJobQueue(storage.engine)
        locked_by = f"embed-{ctx.run_id}-{os.getpid()}"
        stats = EmbedRunStats()
        interrupted = False

        stats.reclaimed = await queue.reclaim_expired(STAGE)

        if args.reprocess or args.model_changed:
            stats.requeued = await storage.reenqueue_embeddings(
                since=ctx.window.start,
                until=ctx.window.end,
                stage=STAGE,
                model=embedder.model_name if args.model_changed else None,
            )

        limit = args.limit or None
        while not ctx.shutdown.is_set():
            remaining = limit - stats.claimed if limit is not None else args.batch_size
            if remaining <= 0:
                break
            n = min(args.batch_size, remaining)
            jobs = await queue.claim(STAGE, n, locked_by=locked_by, lease=LEASE)
            if not jobs:
                break
            stats.claimed += len(jobs)

            candidates = await storage.embeddable_articles(
                [job.article_id for job in jobs]
            )
            to_embed: list[tuple[int, str, bytes]] = []
            for job in jobs:
                candidate = candidates.get(job.article_id)
                if candidate is None:
                    # Article vanished since it was claimed; nothing to embed.
                    await queue.complete(job.article_id, STAGE)
                    continue
                if (
                    candidate.existing_model == embedder.model_name
                    and candidate.existing_content_hash == candidate.content_hash
                ):
                    stats.skipped_unchanged += 1
                    await queue.complete(job.article_id, STAGE)
                    continue
                text = embed_text(candidate.title, candidate.content)
                to_embed.append((job.article_id, text, candidate.content_hash))

            if to_embed:
                await self._embed_and_store(
                    to_embed, embedder, storage, queue, stats
                )

            await ctx.checkpoint(stats)
            if ctx.shutdown.is_set():
                interrupted = True
                break

        logger.info(
            "Embed run %d: reclaimed=%d requeued=%d claimed=%d embedded=%d "
            "skipped_unchanged=%d failed=%d",
            ctx.run_id,
            stats.reclaimed,
            stats.requeued,
            stats.claimed,
            stats.embedded,
            stats.skipped_unchanged,
            stats.failed,
        )

        if interrupted:
            return RunOutcome("partial", stats, "Stopped early on a shutdown signal")
        return RunOutcome("partial" if stats.failed else "succeeded", stats)

    async def _embed_and_store(
        self,
        items: list[tuple[int, str, bytes]],
        embedder: Embedder,
        storage: PostgresRawStorage,
        queue: JobQueue,
        stats: EmbedRunStats,
    ) -> None:
        texts = [text for _, text, _ in items]
        try:
            vectors = await to_thread(embedder.embed, texts)
        except Exception as exc:
            # Poison-pill isolation: one bad article must not fail the whole
            # batch, so retry each item alone and let only the culprit's
            # attempt count advance.
            logger.warning(
                "Batch embed failed (%d items); retrying individually: %s",
                len(items),
                exc,
            )
            for article_id, text, content_hash in items:
                await self._embed_one(
                    article_id, text, content_hash, embedder, storage, queue, stats
                )
            return

        records = [
            EmbeddingRecord(
                article_id=article_id,
                model=embedder.model_name,
                dim=embedder.dim,
                content_hash=content_hash,
                embedding=vector,
            )
            for (article_id, _, content_hash), vector in zip(items, vectors)
        ]
        await storage.upsert_embeddings(records)
        for article_id, _, _ in items:
            await queue.complete(article_id, STAGE)
        stats.embedded += len(items)

    async def _embed_one(
        self,
        article_id: int,
        text: str,
        content_hash: bytes,
        embedder: Embedder,
        storage: PostgresRawStorage,
        queue: JobQueue,
        stats: EmbedRunStats,
    ) -> None:
        try:
            vectors = await to_thread(embedder.embed, [text])
        except Exception as exc:
            logger.warning("Embed failed for article %d: %s", article_id, exc)
            await queue.fail(
                article_id, STAGE, str(exc), max_attempts=MAX_ATTEMPTS
            )
            stats.failed += 1
            return

        await storage.upsert_embeddings(
            [
                EmbeddingRecord(
                    article_id=article_id,
                    model=embedder.model_name,
                    dim=embedder.dim,
                    content_hash=content_hash,
                    embedding=vectors[0],
                )
            ]
        )
        await queue.complete(article_id, STAGE)
        stats.embedded += 1
