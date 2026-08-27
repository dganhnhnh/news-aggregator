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
from workers.cluster import ClusterWorker
from workers.embed import EmbedWorker
from workers.window import RunWindow, build_window


logger = get_logger(__name__)

WORKERS: dict[str, type[Worker]] = {
    CrawlWorker.name: CrawlWorker,
    EmbedWorker.name: EmbedWorker,
    ClusterWorker.name: ClusterWorker,
}


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
        "--max-crawls-per-host",
        type=int,
        default=settings.max_crawls_per_host,
        help="Concurrent crawls allowed against any single host",
    )
    crawl.add_argument(
        "--crawl-delay-per-host",
        type=float,
        metavar="SECONDS",
        default=settings.crawl_delay_per_host_seconds,
        help="Pause between consecutive crawls of the same host",
    )
    crawl.add_argument(
        "--feed-content-min-chars",
        type=int,
        default=settings.feed_content_min_chars,
        help=(
            "Store the feed's own article body when it renders to at least "
            "this many characters instead of crawling the page; 0 disables it"
        ),
    )
    crawl.add_argument(
        "--user-agent",
        default=settings.feed_user_agent,
        help="User-Agent sent when fetching feeds",
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

    embed = subparsers.add_parser(
        "embed", help="Embed articles queued in processing_jobs(stage='embed')"
    )
    _add_common_arguments(embed)
    embed.add_argument(
        "--batch-size",
        type=int,
        default=settings.embed_batch_size,
        help="Articles embedded per claim/checkpoint chunk",
    )
    embed.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Maximum articles to process this run; 0 means unbounded",
    )
    reprocess_group = embed.add_mutually_exclusive_group()
    reprocess_group.add_argument(
        "--reprocess",
        action="store_true",
        help="Re-queue already-embedded articles in the window, regardless of model",
    )
    reprocess_group.add_argument(
        "--model-changed",
        action="store_true",
        help="Re-queue articles in the window whose stored embedding used a "
        "different model than the one configured now",
    )

    cluster = subparsers.add_parser(
        "cluster",
        help="Cluster articles queued in processing_jobs(stage='cluster')",
    )
    _add_common_arguments(cluster)
    cluster.add_argument(
        "--batch-size",
        type=int,
        default=settings.cluster_batch_size,
        help="Articles claimed per checkpoint chunk",
    )
    cluster.add_argument(
        "--candidate-limit",
        type=int,
        default=settings.cluster_candidate_limit,
        help="Maximum active cluster centroids compared per article",
    )
    cluster.add_argument(
        "--cluster-threshold",
        type=float,
        default=settings.cluster_threshold,
        help="Minimum cosine similarity for attaching to an existing cluster",
    )
    cluster.add_argument(
        "--cluster-window-hours",
        type=int,
        default=settings.cluster_window_hours,
        help="Only compare clusters whose event window overlaps this many hours",
    )
    cluster.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Maximum articles to process this run; 0 means unbounded",
    )
    cluster.add_argument(
        "--reprocess",
        action="store_true",
        help="Queue embedded articles in the window that have no cluster membership",
    )
    cluster.add_argument(
        "--merge-threshold",
        type=float,
        default=settings.cluster_merge_threshold,
        help="Minimum cosine similarity between two open clusters to merge them",
    )
    cluster_mode = cluster.add_mutually_exclusive_group()
    cluster_mode.add_argument(
        "--merge-only",
        action="store_true",
        help="Pair-scan and merge open clusters instead of matching new articles",
    )
    cluster_mode.add_argument(
        "--repair",
        action="store_true",
        help="Detect and fix drift between cluster aggregates and cluster_members "
        "instead of matching new articles",
    )
    replay = subparsers.add_parser(
        "replay", help="Requeue dead-lettered crawl_failures or processing_jobs rows"
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
        help="Replay dead-lettered processing_jobs rows for STAGE (e.g. embed)",
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

    cleanup_block_pages = subparsers.add_parser(
        "cleanup-block-pages",
        help="Quarantine legacy summary-only rows with block-page titles",
    )
    cleanup_block_pages.add_argument(
        "--limit",
        type=int,
        default=100,
        help="Maximum rows to inspect/apply in one run",
    )
    cleanup_block_pages.add_argument(
        "--apply",
        action="store_true",
        help="Apply the cleanup; without this flag the command is a dry run",
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
    if args.max_crawls_per_host <= 0:
        parser.error("--max-crawls-per-host must be positive")
    if args.crawl_delay_per_host < 0:
        parser.error("--crawl-delay-per-host must not be negative")
    if args.feed_content_min_chars < 0:
        parser.error("--feed-content-min-chars must not be negative")
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


def _validate_embed(args: argparse.Namespace, parser: argparse.ArgumentParser) -> None:
    if args.batch_size <= 0:
        parser.error("--batch-size must be positive")
    if args.limit < 0:
        parser.error("--limit must not be negative")


def _validate_cluster(args: argparse.Namespace, parser: argparse.ArgumentParser) -> None:
    if args.batch_size <= 0:
        parser.error("--batch-size must be positive")
    if args.candidate_limit <= 0:
        parser.error("--candidate-limit must be positive")
    if not 0 <= args.cluster_threshold <= 1:
        parser.error("--cluster-threshold must be between 0 and 1")
    if args.cluster_window_hours <= 0:
        parser.error("--cluster-window-hours must be positive")
    if not 0 <= args.merge_threshold <= 1:
        parser.error("--merge-threshold must be between 0 and 1")
    if args.limit < 0:
        parser.error("--limit must not be negative")


def validate(args: argparse.Namespace, parser: argparse.ArgumentParser) -> None:
    if args.command in WORKERS:
        if args.interval is not None and args.interval <= 0:
            parser.error("--interval must be positive")
        if args.once and args.interval is not None:
            parser.error("--once and --interval are mutually exclusive")
        if args.command == "crawl":
            _validate_crawl(args, parser)
        elif args.command == "embed":
            _validate_embed(args, parser)
        elif args.command == "cluster":
            _validate_cluster(args, parser)
    elif args.command in {"replay", "cleanup-block-pages"}:
        if args.limit <= 0:
            parser.error("--limit must be positive")


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
    if args.command == "cleanup-block-pages":
        if args.apply:
            # A claimed embed job may already hold the stale title in memory,
            # while crawl could resolve the same failure during cleanup. Take
            # both worker locks so the transaction has a clean boundary.
            async with storage.worker_lock("crawl") as crawl_locked:
                if not crawl_locked:
                    logger.error("Crawl worker is active; cleanup was not applied")
                    return EXIT_FAILED
                async with storage.worker_lock("embed") as embed_locked:
                    if not embed_locked:
                        logger.error("Embed worker is active; cleanup was not applied")
                        return EXIT_FAILED
                    findings = await storage.historical_block_pages(limit=args.limit)
                    for item in findings:
                        logger.info(
                            "Legacy block-page title: article=%d url=%s reason=%s",
                            item.article_id,
                            item.url,
                            item.reason,
                        )
                    count = await storage.quarantine_historical_block_pages(findings)
            logger.info(
                "Quarantined %d legacy block-page title(s); summaries remain "
                "embeddable and article URLs are due for re-crawl",
                count,
            )
            return EXIT_OK

        findings = await storage.historical_block_pages(limit=args.limit)
        for item in findings:
            logger.info(
                "Legacy block-page title: article=%d url=%s reason=%s",
                item.article_id,
                item.url,
                item.reason,
            )
        logger.info(
            "Dry run found %d row(s); pass --apply to quarantine them",
            len(findings),
        )
        return EXIT_OK

    if args.command == "replay":
        if args.stage is not None:
            count = await storage.replay_stage_jobs(args.stage, limit=args.limit)
            logger.info("Requeued %d dead-lettered %s job(s)", count, args.stage)
            return EXIT_OK
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
