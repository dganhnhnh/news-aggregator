"""Embed worker: claim due `processing_jobs(stage='embed')` rows, embed the
article's leading text, and upsert into `article_embeddings`.
"""

from __future__ import annotations

import argparse
import os
from asyncio import to_thread
from collections.abc import Mapping
from datetime import timedelta
from typing import Any, NamedTuple

from adapters.embedding.embedder import Embedder
from adapters.storage import PostgresRawStorage
from core.logger import get_logger
from models.storage_models import EmbedRunStats, EmbeddingRecord, TextKind
from services.raw_storage import content_hash
from services.text import article_embed_text
from workers.base import RunContext, RunOutcome, Worker
from workers.queue import DEFAULT_MAX_ATTEMPTS, JobQueue, PostgresJobQueue


logger = get_logger(__name__)

STAGE = "embed"
LEASE = timedelta(minutes=10)
MAX_ATTEMPTS = DEFAULT_MAX_ATTEMPTS


class _EmbedItem(NamedTuple):
    """One article's embedding input, resolved and hashed."""

    article_id: int
    text: str
    text_hash: bytes
    text_kind: TextKind
    priority: int


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
        if self._embedder is None:
            # Import the ONNX runtime only for an actual embed run. Every CLI
            # command imports this module to register its worker, and eager
            # loading made crawl containers emit irrelevant CPU warnings.
            from adapters.embedding.fastembed_embedder import FastEmbedEmbedder

            embedder: Embedder = FastEmbedEmbedder()
        else:
            embedder = self._embedder
        queue = self._queue or PostgresJobQueue(storage.engine)
        locked_by = f"embed-{ctx.run_id}-{os.getpid()}"
        stats = EmbedRunStats()
        interrupted = False

        stats.reclaimed = await queue.reclaim_expired(STAGE)

        # One load for the whole run. A profile that changes mid-run would make
        # two articles from the same source embed under different rules, and
        # `news-worker boilerplate-refresh` is a separate command precisely so
        # that cannot happen while a run is in flight.
        profiles = await storage.boilerplate_profiles()

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
            to_embed: list[_EmbedItem] = []
            for job in jobs:
                candidate = candidates.get(job.article_id)
                if candidate is None:
                    # Article vanished since it was claimed; nothing to embed.
                    await queue.complete(job.article_id, STAGE)
                    continue
                text, text_kind = article_embed_text(
                    title=candidate.title,
                    content=candidate.content,
                    feed_summary=candidate.feed_summary,
                    content_source=candidate.content_source,
                    boilerplate=profiles.get(candidate.source_id, frozenset()),
                )
                if not text.strip():
                    # No title, no body, no dek: there is nothing to place in
                    # the vector space, and a zero vector would be worse than
                    # an absent one.
                    await queue.complete(job.article_id, STAGE)
                    continue
                # Hashing the chosen text, not the article's content, is what
                # makes this skip correct when the fallback chain changes which
                # text wins without `content` itself changing.
                text_hash = content_hash(text)
                if (
                    candidate.existing_model == embedder.model_name
                    and candidate.existing_content_hash == text_hash
                ):
                    stats.skipped_unchanged += 1
                    # A prior process may have stored the vector before the
                    # downstream queues existed. Ensure missing jobs even on
                    # the unchanged fast path, without replaying done jobs.
                    await storage.ensure_processing_jobs(
                        [job.article_id],
                        ("classify", "cluster"),
                        priority=job.priority,
                    )
                    await queue.complete(job.article_id, STAGE)
                    continue
                to_embed.append(
                    _EmbedItem(
                        job.article_id,
                        text,
                        text_hash,
                        text_kind,
                        job.priority,
                    )
                )

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
        items: list[_EmbedItem],
        embedder: Embedder,
        storage: PostgresRawStorage,
        queue: JobQueue,
        stats: EmbedRunStats,
    ) -> None:
        try:
            vectors = await to_thread(embedder.embed, [item.text for item in items])
        except Exception as exc:
            # Poison-pill isolation: one bad article must not fail the whole
            # batch, so retry each item alone and let only the culprit's
            # attempt count advance.
            logger.warning(
                "Batch embed failed (%d items); retrying individually: %s",
                len(items),
                exc,
            )
            for item in items:
                await self._embed_one(item, embedder, storage, queue, stats)
            return

        records = [
            self._record(item, embedder, vector)
            for item, vector in zip(items, vectors)
        ]
        await storage.upsert_embeddings(records)
        for item in items:
            await queue.complete(item.article_id, STAGE)
        stats.embedded += len(items)

    async def _embed_one(
        self,
        item: _EmbedItem,
        embedder: Embedder,
        storage: PostgresRawStorage,
        queue: JobQueue,
        stats: EmbedRunStats,
    ) -> None:
        try:
            vectors = await to_thread(embedder.embed, [item.text])
        except Exception as exc:
            logger.warning("Embed failed for article %d: %s", item.article_id, exc)
            await queue.fail(
                item.article_id, STAGE, str(exc), max_attempts=MAX_ATTEMPTS
            )
            stats.failed += 1
            return

        await storage.upsert_embeddings(
            [self._record(item, embedder, vectors[0])]
        )
        await queue.complete(item.article_id, STAGE)
        stats.embedded += 1

    @staticmethod
    def _record(
        item: _EmbedItem, embedder: Embedder, vector: list[float]
    ) -> EmbeddingRecord:
        return EmbeddingRecord(
            article_id=item.article_id,
            model=embedder.model_name,
            dim=embedder.dim,
            content_hash=item.text_hash,
            embedding=vector,
            text_kind=item.text_kind,
            priority=item.priority,
        )
