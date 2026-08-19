"""Crawl worker: retry due article failures, then poll due feeds."""

from __future__ import annotations

import argparse
from collections.abc import Iterator, Mapping
from datetime import datetime, timedelta, timezone
from typing import Any

from adapters.retrieve_news_data.google_news_rss_searcher import RssFeedSearcher
from adapters.retrieve_news_data.rss_discovery import RssDiscovery
from core.logger import get_logger
from models.discovery_models import DiscoveredEntry
from models.storage_models import CrawlBatch, FailureTarget, FeedSeed, IngestRunStats
from pipelines.news import emit_results
from services.ingestion import iter_retry_batches, persist_batch
from services.raw_storage import normalize_url
from workers.base import RunContext, RunOutcome, Worker


logger = get_logger(__name__)


def _chunks(items: list, size: int) -> Iterator[list]:
    for start in range(0, len(items), size):
        yield items[start : start + size]


class CrawlWorker(Worker):
    name = "crawl"

    def run_params(self, args: argparse.Namespace) -> Mapping[str, Any]:
        return {
            "feed_path": str(args.feed_path) if args.feed_path else None,
            "feeds": args.feeds,
            "ignore_schedule": args.ignore_schedule,
            "max_feeds": args.max_feeds,
            "max_articles_per_feed": args.max_articles_per_feed,
            "max_concurrent_feeds": args.max_concurrent_feeds,
            "max_concurrent_crawls": args.max_concurrent_crawls,
            "crawl_batch_size": args.crawl_batch_size,
            "poll_interval": args.poll_interval,
            "disable_after_failures": args.disable_after_failures,
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
        discovery = RssDiscovery(searcher)
        stats = IngestRunStats()
        has_failures = False
        interrupted = False

        if args.feed_path is not None:
            seeded = await storage.seed_feeds(
                [
                    FeedSeed(feed_url=source.rss_url, source_label=source.query_label)
                    for source in searcher._load_queries()
                ]
            )
            logger.info("Seeded %d new feed(s) from %s", seeded, args.feed_path)

        async for (
            retry_batch,
            retries_attempted,
            retries_succeeded,
        ) in iter_retry_batches(
            storage,
            searcher,
            limit=args.max_retries_per_run,
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
            targets = await storage.due_feeds(
                limit=args.max_feeds,
                feed_urls=args.feeds or None,
                ignore_schedule=args.ignore_schedule,
            )
            if not targets and args.feed_path is None:
                probe = await storage.due_feeds(limit=1, ignore_schedule=True)
                if not probe:
                    logger.warning(
                        "feed_state is empty; seed it once with "
                        "`news-worker crawl --feed-path <file.opml/.tsv>`"
                    )

            async for poll in discovery.poll(targets):
                if ctx.shutdown.is_set():
                    interrupted = True
                    break

                stats.feeds_processed += 1
                next_poll_at = datetime.now(timezone.utc) + timedelta(
                    seconds=args.poll_interval
                )

                if poll.status == "not_modified":
                    stats.feeds_not_modified += 1
                    await persist_batch(
                        storage,
                        CrawlBatch(
                            resolved_failures=[
                                FailureTarget(
                                    failure_kind="feed", url=poll.target.feed_url
                                )
                            ]
                        ),
                    )
                    await storage.record_feed_poll(
                        poll.target.feed_url,
                        next_poll_at=next_poll_at,
                        not_modified=True,
                    )
                    continue

                if poll.status == "failed":
                    assert poll.error is not None
                    stats.feeds_failed += 1
                    has_failures = True
                    outcome = await storage.record_feed_failure(
                        poll.target.feed_url,
                        error_message=poll.error.error_message,
                        disable_after=args.disable_after_failures,
                    )
                    await persist_batch(storage, CrawlBatch(failures=[poll.error]))
                    if outcome.disabled:
                        stats.feeds_disabled += 1
                        logger.warning(
                            "Feed disabled after %d consecutive failures: %s",
                            outcome.consecutive_failures,
                            poll.target.feed_url,
                        )
                    continue

                # poll.status == "fetched"
                stats.articles_discovered += len(poll.entries)
                await persist_batch(
                    storage,
                    CrawlBatch(
                        resolved_failures=[
                            FailureTarget(
                                failure_kind="feed", url=poll.target.feed_url
                            )
                        ]
                    ),
                )

                bounded = (
                    poll.entries[: args.max_articles_per_feed]
                    if args.max_articles_per_feed > 0
                    else poll.entries
                )
                selected: list[DiscoveredEntry] = []
                for entry in bounded:
                    if entry.published_at is None:
                        stats.entries_skipped_undated += 1
                    if not ctx.window.contains(entry.published_at):
                        stats.entries_skipped_out_of_window += 1
                        continue
                    selected.append(entry)

                if selected:
                    candidates: set[str] = set()
                    for entry in selected:
                        candidates.add(entry.entry_url)
                        try:
                            candidates.add(normalize_url(entry.entry_url))
                        except ValueError:
                            pass
                    known = await storage.known_urls(list(candidates))
                    still_new = []
                    for entry in selected:
                        try:
                            normalized = normalize_url(entry.entry_url)
                        except ValueError:
                            normalized = entry.entry_url
                        if entry.entry_url in known or normalized in known:
                            stats.entries_skipped_known += 1
                            continue
                        still_new.append(entry)
                    selected = still_new

                latest_published = max(
                    (
                        entry.published_at
                        for entry in poll.entries
                        if entry.published_at is not None
                    ),
                    default=None,
                )

                for chunk in _chunks(selected, args.crawl_batch_size):
                    batch = await discovery.crawl_entries(chunk)
                    summary = await persist_batch(storage, batch)
                    stats.add_batch(batch)
                    stats.articles_written += summary.written
                    stats.articles_unchanged += summary.unchanged
                    has_failures = has_failures or bool(batch.failures)
                    await ctx.checkpoint(stats)
                    if args.emit_jsonl:
                        emit_results(batch.results)
                    if ctx.shutdown.is_set():
                        interrupted = True
                        break

                if interrupted:
                    # Don't record the poll: a fresh fetch next run is
                    # cheaper than silently losing entries that never made
                    # it past this cut-off crawl.
                    break

                await storage.record_feed_poll(
                    poll.target.feed_url,
                    next_poll_at=next_poll_at,
                    etag=poll.etag,
                    last_modified=poll.last_modified,
                    last_entry_published_at=latest_published,
                )

        logger.info(
            "Crawl run %d: feeds=%d not_modified=%d failed=%d disabled=%d "
            "discovered=%d crawled=%d failed_crawls=%d retries=%d/%d "
            "written=%d unchanged=%d skipped(window=%d undated=%d known=%d)",
            ctx.run_id,
            stats.feeds_processed,
            stats.feeds_not_modified,
            stats.feeds_failed,
            stats.feeds_disabled,
            stats.articles_discovered,
            stats.crawl_succeeded,
            stats.crawl_failed,
            stats.retries_succeeded,
            stats.retries_attempted,
            stats.articles_written,
            stats.articles_unchanged,
            stats.entries_skipped_out_of_window,
            stats.entries_skipped_undated,
            stats.entries_skipped_known,
        )

        if interrupted:
            return RunOutcome("partial", stats, "Stopped early on a shutdown signal")
        return RunOutcome("partial" if has_failures else "succeeded", stats)
