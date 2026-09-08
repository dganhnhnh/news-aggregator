"""Crawl worker: retry due article failures, then poll due feeds."""

from __future__ import annotations

import argparse
from collections.abc import Iterator, Mapping
from datetime import datetime, timedelta, timezone
from typing import Any

from adapters.retrieve_news_data.google_news_rss_searcher import RssFeedSearcher
from adapters.retrieve_news_data.rss_discovery import RssDiscovery
from core.feed_schedule import observe_feed_cadence, poll_interval_for_gap
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
    feed_priority = 1
    adaptive_polling = True

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
            "user_agent": args.user_agent,
            "feed_fetch_timeout": args.feed_fetch_timeout,
            "feed_priority": self.feed_priority,
            "poll_tiers": (
                [
                    args.poll_fast_interval,
                    args.poll_interval,
                    args.poll_slow_interval,
                ]
                if self.adaptive_polling
                else [args.poll_interval]
            ),
            "poll_min_gaps": args.poll_min_gaps,
            "poll_trim_fraction": args.poll_trim_fraction,
            "poll_future_tolerance": args.poll_future_tolerance,
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
            feed_content_min_chars=args.feed_content_min_chars,
            max_crawls_per_host=args.max_crawls_per_host,
            crawl_delay_per_host=args.crawl_delay_per_host,
            user_agent=args.user_agent,
            feed_fetch_timeout_seconds=args.feed_fetch_timeout,
        )
        discovery = RssDiscovery(searcher)
        stats = IngestRunStats()
        has_failures = False
        interrupted = False

        stats.stale_failures_purged = await storage.purge_stale_feed_failures()
        if stats.stale_failures_purged:
            logger.info(
                "Purged %d feed failure(s) already disproved by a later "
                "successful poll",
                stats.stale_failures_purged,
            )

        if args.feed_path is not None:
            seeded = await storage.seed_feeds(
                [
                    FeedSeed(
                        feed_url=source.rss_url,
                        source_label=source.query_label,
                        priority=self.feed_priority,
                    )
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
            priority=self.feed_priority,
        ):
            retry_summary = await persist_batch(
                storage, retry_batch, priority=self.feed_priority
            )
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
                priority=self.feed_priority,
            )
            if not targets and args.feed_path is None:
                probe = await storage.due_feeds(
                    limit=1,
                    ignore_schedule=True,
                    priority=self.feed_priority,
                )
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
                stats.entries_skipped_non_article += poll.entries_skipped_non_article
                if poll.resolved_url:
                    stats.feeds_redirected += 1
                    logger.info(
                        "Feed permanently moved: %s -> %s",
                        poll.target.feed_url,
                        poll.resolved_url,
                    )
                if poll.status == "not_modified":
                    stats.feeds_not_modified += 1
                    interval = (
                        (
                            poll.target.poll_interval_seconds
                            or args.poll_fast_interval
                        )
                        if self.adaptive_polling
                        else args.poll_interval
                    )
                    next_poll_at = datetime.now(timezone.utc) + timedelta(
                        seconds=interval
                    )
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
                        resolved_url=poll.resolved_url,
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
                        permanent=poll.error.permanent,
                        resolved_url=poll.resolved_url,
                    )
                    await persist_batch(
                        storage,
                        CrawlBatch(failures=[poll.error]),
                        priority=poll.target.priority,
                    )
                    if outcome.disabled:
                        stats.feeds_disabled += 1
                        if poll.error.permanent:
                            logger.warning(
                                "Feed disabled — permanently unreachable: %s (%s)",
                                poll.target.feed_url,
                                poll.error.error_message,
                            )
                        else:
                            logger.warning(
                                "Feed disabled after %d consecutive failures: %s",
                                outcome.consecutive_failures,
                                poll.target.feed_url,
                            )
                    continue

                # poll.status == "fetched"
                stats.articles_discovered += len(poll.entries)
                cadence_observed_at = datetime.now(timezone.utc)
                cadence = observe_feed_cadence(
                    [
                        (entry.entry_url, entry.published_at)
                        for entry in poll.entries
                    ],
                    observed_at=cadence_observed_at,
                    trim_fraction=args.poll_trim_fraction,
                    future_tolerance_seconds=args.poll_future_tolerance,
                )
                interval = (
                    poll_interval_for_gap(
                        cadence.trimmed_mean_gap_seconds,
                        gap_count=cadence.gap_count,
                        min_gaps=args.poll_min_gaps,
                        tiers_seconds=(
                            args.poll_fast_interval,
                            args.poll_interval,
                            args.poll_slow_interval,
                        ),
                    )
                    if self.adaptive_polling
                    else args.poll_interval
                )
                next_poll_at = cadence_observed_at + timedelta(seconds=interval)
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

                for chunk in _chunks(selected, args.crawl_batch_size):
                    batch = await discovery.crawl_entries(chunk)
                    summary = await persist_batch(
                        storage, batch, priority=poll.target.priority
                    )
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
                    last_entry_published_at=cadence.latest_published_at,
                    poll_interval_seconds=interval,
                    observed_publish_gap_seconds=(
                        cadence.trimmed_mean_gap_seconds
                    ),
                    observed_publish_gap_count=cadence.gap_count,
                    resolved_url=poll.resolved_url,
                )

        logger.info(
            "Crawl run %d: feeds=%d not_modified=%d failed=%d disabled=%d "
            "redirected=%d purged=%d "
            "discovered=%d crawled=%d from_feed=%d failed_crawls=%d retries=%d/%d "
            "written=%d unchanged=%d "
            "skipped(window=%d undated=%d known=%d non_article=%d)",
            ctx.run_id,
            stats.feeds_processed,
            stats.feeds_not_modified,
            stats.feeds_failed,
            stats.feeds_disabled,
            stats.feeds_redirected,
            stats.stale_failures_purged,
            stats.articles_discovered,
            stats.crawl_succeeded,
            stats.articles_from_feed_content,
            stats.crawl_failed,
            stats.retries_succeeded,
            stats.retries_attempted,
            stats.articles_written,
            stats.articles_unchanged,
            stats.entries_skipped_out_of_window,
            stats.entries_skipped_undated,
            stats.entries_skipped_known,
            stats.entries_skipped_non_article,
        )

        if interrupted:
            return RunOutcome("partial", stats, "Stopped early on a shutdown signal")
        return RunOutcome("partial" if has_failures else "succeeded", stats)
