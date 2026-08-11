"""Periodic RSS/Atom crawler service backed by TSV or OPML input."""

from __future__ import annotations

import argparse
import asyncio
from pathlib import Path

from adapters.retrieve_news_data.google_news_rss_searcher import RssFeedSearcher
from adapters.storage import PostgresRawStorage, StorageError
from core.config import Settings
from core.logger import get_logger, setup_logging
from models.storage_models import IngestRunStats
from pipelines.news import emit_results
from services.ingestion import iter_retry_batches, persist_batch


logger = get_logger(__name__)


def build_parser(settings: Settings) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--feed-path",
        "--tsv-path",
        dest="feed_path",
        type=Path,
        default=settings.rss_feed_path,
        help=(
            "TSV or OPML feed source path; defaults to RSS_FEED_PATH "
            "(or legacy GOOGLE_NEWS_TSV_PATH)"
        ),
    )
    parser.add_argument(
        "--interval-seconds",
        type=int,
        default=settings.service_interval_seconds,
    )
    parser.add_argument(
        "--max-feeds",
        type=int,
        default=settings.max_feeds,
        help="Maximum feeds to process per iteration; 0 means all feeds",
    )
    parser.add_argument(
        "--max-articles-per-feed",
        type=int,
        default=settings.max_articles_per_feed,
    )
    parser.add_argument(
        "--max-concurrent-feeds",
        type=int,
        default=settings.max_concurrent_feeds,
    )
    parser.add_argument(
        "--max-concurrent-crawls",
        type=int,
        default=settings.max_concurrent_crawls,
    )
    parser.add_argument(
        "--crawl-batch-size",
        type=int,
        default=settings.crawl_batch_size,
        help="Maximum crawled articles retained before each DB checkpoint",
    )
    parser.add_argument(
        "--feed-offset",
        type=int,
        default=settings.feed_offset,
        help="Starting feed index; periodic runs rotate from the next group",
    )
    parser.add_argument(
        "--max-retries-per-run",
        type=int,
        default=settings.max_retries_per_run,
    )
    parser.add_argument("--emit-jsonl", action="store_true")
    parser.add_argument("--once", action="store_true")
    return parser


async def run_service(args: argparse.Namespace, settings: Settings) -> None:
    if not settings.database_url:
        raise StorageError("DATABASE_URL is required for news-service")
    storage = PostgresRawStorage(settings.database_url)
    try:
        await storage.check_ready()
        searcher = RssFeedSearcher(
            feed_path=args.feed_path,
            max_concurrent_crawls=args.max_concurrent_crawls,
            max_concurrent_feeds=args.max_concurrent_feeds,
            crawl_batch_size=args.crawl_batch_size,
        )
        feed_offset = args.feed_offset
        while True:
            stats = IngestRunStats()
            run_id: int | None = None
            try:
                run_id = await storage.start_run("service")
                has_failures = False
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
                    await storage.update_run(run_id, stats)
                    if args.emit_jsonl:
                        emit_results(retry_batch.results)
                async for fresh_batch in searcher.iter_batches(
                    max_articles_per_feed=args.max_articles_per_feed,
                    max_feeds=args.max_feeds,
                    feed_offset=feed_offset,
                ):
                    fresh_summary = await persist_batch(storage, fresh_batch)
                    stats.add_batch(fresh_batch)
                    stats.articles_written += fresh_summary.written
                    stats.articles_unchanged += fresh_summary.unchanged
                    has_failures = has_failures or bool(fresh_batch.failures)
                    await storage.update_run(run_id, stats)
                    if args.emit_jsonl:
                        emit_results(fresh_batch.results)
                feed_offset = searcher.next_feed_offset

                await storage.finish_run(
                    run_id,
                    "partial" if has_failures else "succeeded",
                    stats,
                )
                logger.info(
                    "Ingest run %d: feeds=%d discovered=%d crawled=%d failed=%d "
                    "retries=%d/%d written=%d unchanged=%d",
                    run_id,
                    stats.feeds_processed,
                    stats.articles_discovered,
                    stats.crawl_succeeded,
                    stats.crawl_failed,
                    stats.retries_succeeded,
                    stats.retries_attempted,
                    stats.articles_written,
                    stats.articles_unchanged,
                )
            except asyncio.CancelledError:
                if run_id is not None:
                    await storage.finish_run(
                        run_id,
                        "partial",
                        stats,
                        "Interrupted before the iteration completed",
                    )
                raise
            except Exception as exc:
                logger.exception("RSS pipeline iteration failed")
                if run_id is not None:
                    try:
                        await storage.finish_run(run_id, "failed", stats, str(exc))
                    except Exception:
                        logger.exception(
                            "Could not mark ingest run %d as failed", run_id
                        )
                if args.once:
                    raise

            if args.once:
                return
            logger.info("Next RSS run in %d seconds", args.interval_seconds)
            await asyncio.sleep(args.interval_seconds)
    finally:
        await storage.close()


def main() -> None:
    settings = Settings.from_env()
    setup_logging(settings.log_level)
    parser = build_parser(settings)
    args = parser.parse_args()
    if args.feed_path is None:
        parser.error(
            "--feed-path/--tsv-path or "
            "RSS_FEED_PATH/GOOGLE_NEWS_TSV_PATH is required"
        )
    if not args.feed_path.is_file():
        parser.error(f"RSS feed source file not found: {args.feed_path}")
    if args.interval_seconds <= 0:
        parser.error("--interval-seconds must be positive")
    if args.max_feeds < 0:
        parser.error("--max-feeds must not be negative")
    if args.max_articles_per_feed < 0:
        parser.error("--max-articles-per-feed must not be negative")
    if args.max_concurrent_feeds <= 0:
        parser.error("--max-concurrent-feeds must be positive")
    if args.max_concurrent_crawls <= 0:
        parser.error("--max-concurrent-crawls must be positive")
    if args.crawl_batch_size <= 0:
        parser.error("--crawl-batch-size must be positive")
    if args.feed_offset < 0:
        parser.error("--feed-offset must not be negative")
    if args.max_retries_per_run < 0:
        parser.error("--max-retries-per-run must not be negative")
    if not settings.database_url:
        parser.error("DATABASE_URL is required for news-service")
    try:
        asyncio.run(run_service(args, settings))
    except StorageError as exc:
        parser.error(str(exc))
    except KeyboardInterrupt:
        logger.info("Service stopped")


if __name__ == "__main__":
    main()
