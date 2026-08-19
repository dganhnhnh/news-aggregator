"""`news-worker`: one entry point for every worker."""

from __future__ import annotations

import argparse
import asyncio
from pathlib import Path

from adapters.storage import PostgresRawStorage, StorageError
from core.config import Settings
from core.logger import get_logger, setup_logging
from workers.base import EXIT_OK, Worker, stale_after_for
from workers.crawl import CrawlWorker
from workers.window import RunWindow, build_window


logger = get_logger(__name__)

WORKERS: dict[str, type[Worker]] = {CrawlWorker.name: CrawlWorker}


def _add_common_arguments(parser: argparse.ArgumentParser) -> None:
    window = parser.add_mutually_exclusive_group()
    window.add_argument(
        "--last",
        metavar="DURATION",
        help="Relative window such as 30m, 12h, 3d or 2w (default: 3d)",
    )
    window.add_argument(
        "--since",
        metavar="TIMESTAMP",
        help="Explicit window start as an ISO date or datetime",
    )
    parser.add_argument(
        "--until",
        metavar="TIMESTAMP",
        help="Explicit window end; requires --since (default: now)",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run a single time and exit; this is the default",
    )
    parser.add_argument(
        "--interval",
        type=int,
        metavar="SECONDS",
        help="Keep running, waiting this long between runs",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Exit 1 on a partial run; for CI, not for cron",
    )
    parser.add_argument(
        "--trigger",
        choices=("scheduled", "manual", "backfill"),
        help="Override how this run is labelled in worker_runs",
    )


def build_parser(settings: Settings) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="news-worker", description=__doc__)
    subparsers = parser.add_subparsers(dest="worker", required=True)

    crawl = subparsers.add_parser("crawl", help="Ingest RSS/Atom feeds")
    _add_common_arguments(crawl)
    crawl.add_argument(
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
    crawl.add_argument(
        "--max-feeds",
        type=int,
        default=settings.max_feeds,
        help="Maximum feeds to process per run; 0 means all feeds",
    )
    crawl.add_argument(
        "--max-articles-per-feed",
        type=int,
        default=settings.max_articles_per_feed,
    )
    crawl.add_argument(
        "--max-concurrent-feeds",
        type=int,
        default=settings.max_concurrent_feeds,
    )
    crawl.add_argument(
        "--max-concurrent-crawls",
        type=int,
        default=settings.max_concurrent_crawls,
    )
    crawl.add_argument(
        "--crawl-batch-size",
        type=int,
        default=settings.crawl_batch_size,
        help="Maximum crawled articles retained before each DB checkpoint",
    )
    crawl.add_argument(
        "--feed-offset",
        type=int,
        default=settings.feed_offset,
        help="Starting feed index; looping runs rotate from the next group",
    )
    crawl.add_argument(
        "--max-retries-per-run",
        type=int,
        default=settings.max_retries_per_run,
    )
    crawl.add_argument("--emit-jsonl", action="store_true")
    return parser


def resolve_window(
    args: argparse.Namespace,
    parser: argparse.ArgumentParser,
) -> RunWindow:
    try:
        return build_window(last=args.last, since=args.since, until=args.until)
    except ValueError as exc:
        parser.error(str(exc))


def _validate_crawl(args: argparse.Namespace, parser: argparse.ArgumentParser) -> None:
    if args.feed_path is None:
        parser.error(
            "--feed-path/--tsv-path or RSS_FEED_PATH/GOOGLE_NEWS_TSV_PATH is required"
        )
    if not args.feed_path.is_file():
        parser.error(f"RSS feed source file not found: {args.feed_path}")
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


def validate(args: argparse.Namespace, parser: argparse.ArgumentParser) -> None:
    if args.interval is not None and args.interval <= 0:
        parser.error("--interval must be positive")
    if args.once and args.interval is not None:
        parser.error("--once and --interval are mutually exclusive")
    if args.worker == "crawl":
        _validate_crawl(args, parser)


async def _dispatch(
    args: argparse.Namespace,
    settings: Settings,
    window: RunWindow,
) -> int:
    storage = PostgresRawStorage(settings.database_url)
    try:
        await storage.check_ready()
        worker = WORKERS[args.worker]()
        return await worker.run(
            storage,
            args,
            window,
            stale_after=stale_after_for(
                args, settings.worker_heartbeat_timeout_seconds
            ),
        )
    finally:
        await storage.close()


def main(argv: list[str] | None = None) -> None:
    settings = Settings.from_env()
    setup_logging(settings.log_level)
    parser = build_parser(settings)
    args = parser.parse_args(argv)
    validate(args, parser)
    window = resolve_window(args, parser)
    if not settings.database_url:
        parser.error("DATABASE_URL is required for news-worker")
    try:
        exit_code = asyncio.run(_dispatch(args, settings, window))
    except StorageError as exc:
        parser.error(str(exc))
    except KeyboardInterrupt:
        # Interrupts during the run itself are handled cooperatively; reaching
        # here means the operator stopped us before any run was recorded.
        logger.info("Worker stopped")
        exit_code = EXIT_OK
    raise SystemExit(exit_code)


if __name__ == "__main__":
    main()
