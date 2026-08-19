"""Crawl worker: retry due failures, then ingest fresh feed entries."""

from __future__ import annotations

import argparse
from collections.abc import Mapping
from typing import Any

from adapters.retrieve_news_data.google_news_rss_searcher import RssFeedSearcher
from core.logger import get_logger
from models.storage_models import IngestRunStats
from pipelines.news import emit_results
from services.ingestion import iter_retry_batches, persist_batch
from workers.base import RunContext, RunOutcome, Worker


logger = get_logger(__name__)


class CrawlWorker(Worker):
    name = "crawl"

    def run_params(self, args: argparse.Namespace) -> Mapping[str, Any]:
        return {
            "feed_path": str(args.feed_path),
            "max_feeds": args.max_feeds,
            "max_articles_per_feed": args.max_articles_per_feed,
            "max_concurrent_feeds": args.max_concurrent_feeds,
            "max_concurrent_crawls": args.max_concurrent_crawls,
            "crawl_batch_size": args.crawl_batch_size,
            "feed_offset": args.feed_offset,
            "max_retries_per_run": args.max_retries_per_run,
        }

    async def execute(self, ctx: RunContext) -> RunOutcome:
        args = ctx.args
        storage = ctx.storage
        searcher = RssFeedSearcher(
            feed_path=args.feed_path,
            max_concurrent_crawls=args.max_concurrent_crawls,
            max_concurrent_feeds=args.max_concurrent_feeds,
            crawl_batch_size=args.crawl_batch_size,
        )
        stats = IngestRunStats()
        has_failures = False
        interrupted = False

        async for (
            retry_batch,
            retries_attempted,
            retries_succeeded,
        ) in iter_retry_batches(
            storage,
            searcher,
            limit=args.max_retries_per_run,
            max_articles_per_feed=args.max_articles_per_feed,
            max_concurrent_crawls=args.max_concurrent_crawls,
            crawl_batch_size=args.crawl_batch_size,
        ):
            retry_summary = await persist_batch(storage, retry_batch)
            stats.add_batch(retry_batch)
            stats.retries_attempted += retries_attempted
            stats.retries_succeeded += retries_succeeded
            stats.articles_written += retry_summary.written
            stats.articles_unchanged += retry_summary.unchanged
            has_failures = has_failures or bool(retry_batch.failures)
            await ctx.checkpoint(stats)
            if args.emit_jsonl:
                emit_results(retry_batch.results)
            # Chunk boundaries are commit boundaries: stopping here keeps
            # everything already written, and the recorded stats match it.
            if ctx.shutdown.is_set():
                interrupted = True
                break

        if not interrupted:
            async for fresh_batch in searcher.iter_batches(
                max_articles_per_feed=args.max_articles_per_feed,
                max_feeds=args.max_feeds,
                feed_offset=args.feed_offset,
            ):
                fresh_summary = await persist_batch(storage, fresh_batch)
                stats.add_batch(fresh_batch)
                stats.articles_written += fresh_summary.written
                stats.articles_unchanged += fresh_summary.unchanged
                has_failures = has_failures or bool(fresh_batch.failures)
                await ctx.checkpoint(stats)
                if args.emit_jsonl:
                    emit_results(fresh_batch.results)
                if ctx.shutdown.is_set():
                    interrupted = True
                    break
            # Keep limited runs rotating through the feed list across
            # iterations of an --interval loop.
            args.feed_offset = searcher.next_feed_offset

        logger.info(
            "Crawl run %d: feeds=%d discovered=%d crawled=%d failed=%d "
            "retries=%d/%d written=%d unchanged=%d",
            ctx.run_id,
            stats.feeds_processed,
            stats.articles_discovered,
            stats.crawl_succeeded,
            stats.crawl_failed,
            stats.retries_succeeded,
            stats.retries_attempted,
            stats.articles_written,
            stats.articles_unchanged,
        )

        if interrupted:
            return RunOutcome("partial", stats, "Stopped early on a shutdown signal")
        return RunOutcome("partial" if has_failures else "succeeded", stats)
