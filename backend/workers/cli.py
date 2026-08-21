"""`news-worker`: one entry point for every worker."""

from __future__ import annotations

import argparse
import asyncio
from pathlib import Path

from adapters.storage import PostgresRawStorage, StorageError
from core.config import Settings
from core.logger import get_logger, setup_logging
from workers.base import EXIT_FAILED, EXIT_OK, Worker, stale_after_for
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
    subparsers = parser.add_subparsers(dest="command", required=True)

    crawl = subparsers.add_parser("crawl", help="Ingest RSS/Atom feeds")
    _add_common_arguments(crawl)
    crawl.add_argument(
        "--feed-path",
        "--tsv-path",
        dest="feed_path",
        type=Path,
        default=settings.rss_feed_path,
        help=(
            "TSV or OPML feed source path to seed feed_state from; optional "
            "once feeds are seeded. Defaults to RSS_FEED_PATH "
            "(or legacy GOOGLE_NEWS_TSV_PATH)"
        ),
    )
    crawl.add_argument(
        "--feeds",
        metavar="URL[,URL...]",
        help=(
            "Poll only these feed URLs, ignoring both the schedule and "
            "disabled_at; for inspecting a specific feed"
        ),
    )
    crawl.add_argument(
        "--ignore-schedule",
        action="store_true",
        help="Poll every non-disabled feed regardless of next_poll_at",
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
        "--poll-interval",
        type=int,
        metavar="SECONDS",
        default=settings.feed_poll_interval_seconds,
        help="How long before a successfully polled feed is due again",
    )
    crawl.add_argument(
        "--disable-after-failures",
        type=int,
        default=settings.feed_disable_after_failures,
        help="Consecutive feed failures before a feed is auto-disabled",
    )
    crawl.add_argument(
        "--max-retries-per-run",
        type=int,
        default=settings.max_retries_per_run,
    )
    crawl.add_argument("--emit-jsonl", action="store_true")

    replay = subparsers.add_parser(
        "replay", help="Requeue dead-lettered crawl_failures rows"
    )
    replay_target = replay.add_mutually_exclusive_group(required=True)
    replay_target.add_argument(
        "--failures",
        action="store_true",
        help="Replay dead-lettered crawl_failures rows",
    )
    replay_target.add_argument(
        "--stage",
        metavar="STAGE",
        help="Replay processing_jobs for STAGE (not yet implemented)",
    )
    replay.add_argument(
        "--kind",
        choices=("article", "feed"),
        help="Restrict --failures replay to this failure_kind",
    )
    replay.add_argument(
        "--limit",
        type=int,
        default=100,
        help="Maximum rows to requeue",
    )

    enable_feed = subparsers.add_parser(
        "enable-feed", help="Clear a feed's disabled_at"
    )
    enable_feed.add_argument("url", help="Feed URL, matching feed_state.feed_url")

    disable_feed = subparsers.add_parser(
        "disable-feed", help="Manually disable a feed"
    )
    disable_feed.add_argument("url", help="Feed URL, matching feed_state.feed_url")
    disable_feed.add_argument(
        "--reason", required=True, help="Why the feed is being disabled"
    )

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
    if args.feed_path is not None and not args.feed_path.is_file():
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
    if args.poll_interval <= 0:
        parser.error("--poll-interval must be positive")
    if args.disable_after_failures <= 0:
        parser.error("--disable-after-failures must be positive")
    if args.max_retries_per_run < 0:
        parser.error("--max-retries-per-run must not be negative")
    args.feeds = (
        [feed.strip() for feed in args.feeds.split(",") if feed.strip()]
        if args.feeds
        else []
    )


def validate(args: argparse.Namespace, parser: argparse.ArgumentParser) -> None:
    if args.command in WORKERS:
        if args.interval is not None and args.interval <= 0:
            parser.error("--interval must be positive")
        if args.once and args.interval is not None:
            parser.error("--once and --interval are mutually exclusive")
        if args.command == "crawl":
            _validate_crawl(args, parser)
    elif args.command == "replay":
        if args.limit <= 0:
            parser.error("--limit must be positive")
        if args.stage is not None:
            parser.error("--stage replay is not implemented yet; use --failures")


async def _dispatch(
    args: argparse.Namespace,
    settings: Settings,
    window: RunWindow | None,
) -> int:
    storage = PostgresRawStorage(settings.database_url)
    try:
        await storage.check_ready()
        if args.command in WORKERS:
            worker = WORKERS[args.command]()
            return await worker.run(
                storage,
                args,
                window,
                stale_after=stale_after_for(
                    args, settings.worker_heartbeat_timeout_seconds
                ),
            )
        return await _dispatch_admin(storage, args)
    finally:
        await storage.close()


async def _dispatch_admin(
    storage: PostgresRawStorage, args: argparse.Namespace
) -> int:
    if args.command == "replay":
        count = await storage.replay_failures(kind=args.kind, limit=args.limit)
        logger.info("Requeued %d dead-lettered failure(s)", count)
        return EXIT_OK
    if args.command == "enable-feed":
        found = await storage.enable_feed(args.url)
        if not found:
            logger.error("Feed not found: %s", args.url)
            return EXIT_FAILED
        logger.info("Enabled feed: %s", args.url)
        return EXIT_OK
    if args.command == "disable-feed":
        found = await storage.disable_feed(args.url, reason=args.reason)
        if not found:
            logger.error("Feed not found: %s", args.url)
            return EXIT_FAILED
        logger.info("Disabled feed: %s", args.url)
        return EXIT_OK
    raise AssertionError(f"unhandled admin command: {args.command}")


def main(argv: list[str] | None = None) -> None:
    settings = Settings.from_env()
    setup_logging(settings.log_level)
    parser = build_parser(settings)
    args = parser.parse_args(argv)
    validate(args, parser)
    if not settings.database_url:
        parser.error("DATABASE_URL is required for news-worker")
    window = resolve_window(args, parser) if args.command in WORKERS else None
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
