"""`news-worker`: one entry point for every worker."""

from __future__ import annotations

import argparse
import asyncio
import copy
import json
from asyncio import to_thread
from dataclasses import asdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import yaml

from adapters.storage import PostgresRawStorage, StorageError
from core.boilerplate import MIN_DOCS_PER_SOURCE, PROFILE_SAMPLE_SIZE
from core.config import Settings
from core.logger import get_logger, setup_logging
from models.storage_models import PrototypeRebuildStats, TopicPrototypeRecord
from services.taxonomy import (
    MIN_EXEMPLARS_TO_ACTIVATE,
    TaxonomyValidationError,
    compile_prototype,
    definition_hash,
    parse_seed,
    to_seed_dict,
    validate_seed,
)
from workers.base import (
    EXIT_FAILED,
    EXIT_OK,
    Worker,
    exit_busy_for,
    stale_after_for,
)
from workers.classify import ClassifyWorker
from workers.crawl import CrawlWorker
from workers.cluster import ClusterWorker
from workers.embed import EmbedWorker
from workers.instant_crawl import InstantCrawlWorker
from workers.semantic import SemanticConfig, SemanticService, StageArgs, describe
from workers.window import RunWindow, build_window, parse_duration


logger = get_logger(__name__)

WORKERS: dict[str, type[Worker]] = {
    CrawlWorker.name: CrawlWorker,
    InstantCrawlWorker.name: InstantCrawlWorker,
    EmbedWorker.name: EmbedWorker,
    ClassifyWorker.name: ClassifyWorker,
    ClusterWorker.name: ClusterWorker,
}

# What `news-worker semantic` runs for each stage. The budgets are the old
# `news_semantic` DAG timeouts: with no orchestrator left to kill a stage they
# only keep a long backlog in one stage from starving the others.
SEMANTIC_STAGE_ARGV: dict[str, list[str]] = {
    "embed": ["embed", "--max-runtime", "30m"],
    "classify": ["classify", "--max-runtime", "20m"],
    "cluster": ["cluster", "--max-runtime", "20m"],
    "merge": ["cluster", "--merge-only", "--max-runtime", "15m"],
    "refresh": ["classify", "--refresh-clusters", "--max-runtime", "15m"],
}

# The nightly `news_maintenance` DAG, one argv per step. The health report is
# last and always runs, so a failed repair still leaves a snapshot behind.
MAINTENANCE_STEPS: tuple[list[str], ...] = (
    ["cluster", "--repair", "--last", "7d", "--max-runtime", "30m"],
    ["boilerplate-refresh"],
    ["cleanup-block-pages", "--limit", "200"],
    ["queue-status", "--json", "--runs-window", "24", "--fail-on-alert"],
)


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
    parser.add_argument(
        "--exit-busy",
        type=int,
        default=0,
        metavar="CODE",
        help=(
            "Exit with CODE instead of 0 when another run holds the lock, so a "
            "scheduler can report the run as skipped rather than successful"
        ),
    )
    parser.add_argument(
        "--summary-json",
        metavar="PATH",
        help=(
            "Append one JSON line describing the finished run; '-' or "
            "/dev/stdout writes it as the last line of stdout"
        ),
    )
    parser.add_argument(
        "--max-runtime",
        metavar="DURATION",
        help=(
            "Request a cooperative stop after a duration such as 25m or 2h; "
            "the current chunk is checkpointed before the worker exits"
        ),
    )


def build_parser(settings: Settings) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="news-worker", description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    crawl = argparse.ArgumentParser(add_help=False)
    _add_common_arguments(crawl)
    crawl.add_argument(
        "--feed-path",
        "--tsv-path",
        dest="feed_path",
        type=Path,
        default=settings.rss_feed_path,
        help=(
            "TSV or OPML feed source path to seed feed_state from; optional "
            "once feeds are seeded. Normal crawl defaults to RSS_FEED_PATH "
            "(or legacy GOOGLE_NEWS_TSV_PATH); instant-crawl defaults to no file"
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
        "--feed-fetch-timeout",
        type=float,
        metavar="SECONDS",
        default=settings.feed_fetch_timeout_seconds,
        help="Timeout for one RSS/Atom fetch",
    )
    crawl.add_argument(
        "--poll-fast-interval",
        type=int,
        metavar="SECONDS",
        default=settings.feed_poll_fast_interval_seconds,
        help="Fast adaptive feed poll tier",
    )
    crawl.add_argument(
        "--poll-interval",
        type=int,
        metavar="SECONDS",
        default=settings.feed_poll_interval_seconds,
        help="Medium adaptive crawl tier; fixed interval for instant-crawl",
    )
    crawl.add_argument(
        "--poll-slow-interval",
        type=int,
        metavar="SECONDS",
        default=settings.feed_poll_slow_interval_seconds,
        help="Slow adaptive feed poll tier",
    )
    crawl.add_argument(
        "--poll-min-gaps",
        type=int,
        default=settings.feed_poll_min_gaps,
        help="Minimum publication gaps required before using a slower tier",
    )
    crawl.add_argument(
        "--poll-trim-fraction",
        type=float,
        default=settings.feed_poll_trim_fraction,
        help="Fraction trimmed from each end of publication-gap samples",
    )
    crawl.add_argument(
        "--poll-future-tolerance",
        type=float,
        metavar="SECONDS",
        default=settings.feed_poll_future_tolerance_seconds,
        help="Future timestamp tolerance when measuring feed cadence",
    )
    crawl.add_argument(
        "--disable-after-failures",
        type=int,
        default=settings.feed_disable_after_failures,
        help="Consecutive feed failures before a feed is auto-disabled",
    )
    crawl.add_argument(
        "--feed-stale-after-days",
        type=int,
        default=settings.feed_stale_after_days,
        help=(
            "Retire (disable) a feed whose newest entry is older than this "
            "many days, instead of only tiering it by inter-entry gaps"
        ),
    )
    crawl.add_argument(
        "--max-retries-per-run",
        type=int,
        default=settings.max_retries_per_run,
    )
    crawl.add_argument("--emit-jsonl", action="store_true")

    subparsers.add_parser(
        "crawl",
        help="Ingest normal priority-1 RSS/Atom feeds",
        parents=[crawl],
    )
    instant_crawl = subparsers.add_parser(
        "instant-crawl",
        help="Continuously ingest only instant priority-0 feeds",
        parents=[copy.deepcopy(crawl)],
    )
    instant_crawl.set_defaults(
        feed_path=None,
        poll_interval=settings.instant_feed_poll_interval_seconds,
    )

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

    classify = subparsers.add_parser(
        "classify",
        help="Score articles queued in processing_jobs(stage='classify') "
        "against the active topic set",
    )
    _add_common_arguments(classify)
    classify.add_argument(
        "--batch-size",
        type=int,
        default=settings.classify_batch_size,
        help="Articles claimed per checkpoint chunk",
    )
    classify.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Maximum articles to process this run; 0 means unbounded",
    )
    classify_mode = classify.add_mutually_exclusive_group()
    classify_mode.add_argument(
        "--reprocess",
        action="store_true",
        help="Re-queue every embedded article in the window, even if its "
        "classification is already current",
    )
    classify_mode.add_argument(
        "--stale",
        action="store_true",
        help="Re-queue only articles in the window never classified or scored "
        "against a different classifier set (e.g. after a topic changed)",
    )
    classify_mode.add_argument(
        "--refresh-clusters",
        action="store_true",
        help="Rebuild cluster_topics from article_topics for clusters in the "
        "window, without reclassifying any article",
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

    taxonomy_import = subparsers.add_parser(
        "taxonomy-import", help="Import topics and taxonomy trees from a YAML seed file"
    )
    taxonomy_import.add_argument("--file", type=Path, required=True, help="Seed YAML path")
    taxonomy_import.add_argument(
        "--prune",
        action="store_true",
        help="Deactivate topics and remove taxonomy nodes absent from the file",
    )

    taxonomy_export = subparsers.add_parser(
        "taxonomy-export", help="Export the current topic registry and taxonomy trees"
    )
    taxonomy_export.add_argument(
        "--file", type=Path, required=True, help="Where to write the seed YAML"
    )

    taxonomy_rebuild = subparsers.add_parser(
        "taxonomy-rebuild",
        help="Recompile topic prototypes from their active exemplars",
    )
    taxonomy_rebuild.add_argument(
        "--topic", metavar="SLUG", help="Rebuild only this topic; default is every active topic"
    )
    taxonomy_rebuild.add_argument(
        "--exit-busy",
        type=int,
        default=0,
        metavar="CODE",
        help="Exit with CODE when another rebuild holds the lock",
    )

    boilerplate_refresh = subparsers.add_parser(
        "boilerplate-refresh",
        help="Rebuild each source's repeated-chrome profile from its corpus",
    )
    boilerplate_refresh.add_argument(
        "--source",
        metavar="SLUG",
        help="Refresh only this source; default is every source",
    )
    boilerplate_refresh.add_argument(
        "--sample",
        type=int,
        default=PROFILE_SAMPLE_SIZE,
        help="Articles per source the profile is built from, newest first",
    )
    boilerplate_refresh.add_argument(
        "--exit-busy",
        type=int,
        default=0,
        metavar="CODE",
        help="Exit with CODE when another refresh holds the lock",
    )

    queue_status = subparsers.add_parser(
        "queue-status",
        help="Report processing_jobs, crawl_failures and worker_runs state",
    )
    queue_status.add_argument(
        "--json",
        action="store_true",
        dest="as_json",
        help="Emit one JSON object on stdout instead of human-readable lines",
    )
    queue_status.add_argument(
        "--runs-window",
        type=int,
        default=24,
        metavar="HOURS",
        help="How far back to summarise worker_runs",
    )
    queue_status.add_argument(
        "--fail-on-alert",
        action="store_true",
        help=(
            "Exit 1 when the snapshot contains a dead-lettered job, a "
            "dead-lettered failure, or a failed/abandoned run in the window"
        ),
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

    set_feed_priority = subparsers.add_parser(
        "set-feed-priority",
        help="Set a feed's crawl tier (0=instant, 1=normal)",
    )
    set_feed_priority.add_argument(
        "url", help="Feed URL, matching feed_state.feed_url"
    )
    set_feed_priority.add_argument(
        "priority",
        type=int,
        choices=(0, 1),
        help="0 for instant crawl, 1 for normal scheduled crawl",
    )

    semantic = subparsers.add_parser(
        "semantic",
        help="Keep running embed -> (classify | cluster -> merge) -> refresh, "
        "woken whenever work is queued",
    )
    semantic.add_argument(
        "--last",
        default="24h",
        metavar="DURATION",
        help="Relative window every stage runs over (default: 24h)",
    )
    semantic.add_argument(
        "--once",
        action="store_true",
        help="Drain until nothing is actionable, merge and refresh, then exit",
    )
    semantic.add_argument(
        "--fallback-interval",
        type=float,
        default=300.0,
        metavar="SECONDS",
        help="Longest idle wait without a notification (default: 300)",
    )
    semantic.add_argument(
        "--min-pass-interval",
        type=float,
        default=60.0,
        metavar="SECONDS",
        help="Minimum gap between pass starts, folding bursts of "
        "notifications into one pass (default: 60)",
    )
    semantic.add_argument(
        "--merge-every",
        default="15m",
        metavar="DURATION",
        help="Minimum gap between cluster merges (default: 15m)",
    )
    semantic.add_argument(
        "--refresh-every",
        default="15m",
        metavar="DURATION",
        help="Minimum gap between cluster_topics refreshes (default: 15m)",
    )
    # Relative windows only: a fixed backfill range belongs to a one-shot
    # stage command, not to a service that runs for days.
    semantic.set_defaults(since=None, until=None)

    subparsers.add_parser(
        "maintenance",
        help="Run the nightly steps: cluster repair, boilerplate refresh, "
        "block-page dry run and the queue health report",
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


def _validate_crawl(
    args: argparse.Namespace,
    parser: argparse.ArgumentParser,
    *,
    adaptive: bool,
) -> None:
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
    if args.feed_fetch_timeout <= 0:
        parser.error("--feed-fetch-timeout must be positive")
    if adaptive:
        if not 0 < args.poll_fast_interval < args.poll_interval < args.poll_slow_interval:
            parser.error("adaptive poll intervals must be positive and increasing")
    elif args.poll_interval <= 0:
        parser.error("--poll-interval must be positive")
    if args.poll_min_gaps <= 0:
        parser.error("--poll-min-gaps must be positive")
    if not 0 <= args.poll_trim_fraction < 0.5:
        parser.error("--poll-trim-fraction must be in [0, 0.5)")
    if args.poll_future_tolerance < 0:
        parser.error("--poll-future-tolerance must not be negative")
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


def _validate_exit_busy(
    args: argparse.Namespace, parser: argparse.ArgumentParser
) -> None:
    code = getattr(args, "exit_busy", 0)
    if not 0 <= code <= 255:
        parser.error("--exit-busy must be between 0 and 255")
    if code == EXIT_FAILED:
        # Reusing the failure code would make "another run holds the lock"
        # indistinguishable from "this run failed", which is the whole point
        # of the flag.
        parser.error(f"--exit-busy must not be {EXIT_FAILED}, which means failure")


def validate(args: argparse.Namespace, parser: argparse.ArgumentParser) -> None:
    if args.command in WORKERS or args.command in {
        "taxonomy-rebuild",
        "boilerplate-refresh",
    }:
        _validate_exit_busy(args, parser)
    if args.command in WORKERS:
        if args.interval is not None and args.interval <= 0:
            parser.error("--interval must be positive")
        if args.once and args.interval is not None:
            parser.error("--once and --interval are mutually exclusive")
        if args.max_runtime is not None:
            try:
                parse_duration(args.max_runtime)
            except ValueError as exc:
                parser.error(str(exc))
        if args.command in {"crawl", "instant-crawl"}:
            _validate_crawl(args, parser, adaptive=args.command == "crawl")
        elif args.command in {"embed", "classify"}:
            _validate_embed(args, parser)
        elif args.command == "cluster":
            _validate_cluster(args, parser)
    elif args.command in {"replay", "cleanup-block-pages"}:
        if args.limit <= 0:
            parser.error("--limit must be positive")
    elif args.command == "semantic":
        if args.fallback_interval <= 0:
            parser.error("--fallback-interval must be positive")
        if args.min_pass_interval < 0:
            parser.error("--min-pass-interval must not be negative")
        for flag in ("last", "merge_every", "refresh_every"):
            try:
                parse_duration(getattr(args, flag))
            except ValueError as exc:
                parser.error(str(exc))
    elif args.command == "queue-status":
        if args.runs_window <= 0:
            parser.error("--runs-window must be positive")
    elif args.command == "boilerplate-refresh":
        if args.sample < MIN_DOCS_PER_SOURCE:
            parser.error(
                f"--sample must be at least {MIN_DOCS_PER_SOURCE}, the smallest "
                "corpus a profile is ever built from"
            )
    elif args.command in {"taxonomy-import", "taxonomy-export"}:
        if args.command == "taxonomy-import" and not args.file.is_file():
            parser.error(f"Taxonomy seed file not found: {args.file}")


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


def semantic_stage_args(
    args: argparse.Namespace,
    settings: Settings,
) -> StageArgs:
    """Parse each stage's argv through the real parser, so a stage gets the
    same defaults and validation it would get on its own command line."""
    parser = build_parser(settings)
    parsed: dict[str, argparse.Namespace] = {}
    for stage, argv in SEMANTIC_STAGE_ARGV.items():
        stage_args = parser.parse_args([*argv, "--last", args.last])
        validate(stage_args, parser)
        parsed[stage] = stage_args
    return StageArgs(**parsed)


async def _run_semantic(
    args: argparse.Namespace,
    settings: Settings,
    window: RunWindow,
) -> int:
    config = SemanticConfig(
        once=args.once,
        fallback_interval=args.fallback_interval,
        min_pass_interval=args.min_pass_interval,
        merge_every=parse_duration(args.merge_every),
        refresh_every=parse_duration(args.refresh_every),
        stale_after=timedelta(seconds=settings.worker_heartbeat_timeout_seconds),
    )
    stage_args = semantic_stage_args(args, settings)
    logger.info("Starting semantic service: %s", describe(config))
    storage = PostgresRawStorage(settings.database_url)
    try:
        await storage.check_ready()
        return await SemanticService(storage, stage_args, window, config).run()
    finally:
        await storage.close()


async def _run_maintenance(settings: Settings) -> int:
    """Run every step even when an earlier one fails; exit 1 if any did."""
    parser = build_parser(settings)
    failed: list[str] = []
    for argv in MAINTENANCE_STEPS:
        step = " ".join(argv)
        step_args = parser.parse_args(argv)
        validate(step_args, parser)
        window = (
            resolve_window(step_args, parser)
            if step_args.command in WORKERS
            else None
        )
        logger.info("Maintenance step: %s", step)
        try:
            code = await _dispatch(step_args, settings, window)
        except Exception:
            logger.exception("Maintenance step failed: %s", step)
            code = EXIT_FAILED
        if code != EXIT_OK:
            logger.error("Maintenance step exited %d: %s", code, step)
            failed.append(step)
    if failed:
        logger.error("Maintenance finished with %d failed step(s)", len(failed))
        return EXIT_FAILED
    logger.info("Maintenance finished")
    return EXIT_OK


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
    if args.command == "set-feed-priority":
        found = await storage.set_feed_priority(args.url, args.priority)
        if not found:
            logger.error("Feed not found: %s", args.url)
            return EXIT_FAILED
        logger.info("Set feed priority=%d: %s", args.priority, args.url)
        return EXIT_OK

    if args.command == "taxonomy-import":
        raw = yaml.safe_load(args.file.read_text()) or {}
        try:
            seed = parse_seed(raw)
            validate_seed(seed)
        except TaxonomyValidationError as exc:
            logger.error("Taxonomy seed is invalid:")
            for message in exc.errors:
                logger.error("  - %s", message)
            return EXIT_FAILED

        if args.prune:
            # Print what --prune is about to affect before the write commits,
            # per the spec's "log before ghi" rule; a short read/write race
            # with a concurrent import is acceptable for a single-operator CLI.
            current_topics, _ = await storage.export_taxonomy()
            seed_slugs = {topic.slug for topic in seed.topics}
            at_risk = sorted(
                topic.slug
                for topic in current_topics
                if topic.active and topic.slug not in seed_slugs
            )
            if at_risk:
                logger.info(
                    "--prune will deactivate %d topic(s): %s",
                    len(at_risk),
                    ", ".join(at_risk),
                )

        summary = await storage.import_taxonomy(seed, prune=args.prune)
        for outcome in summary.topics:
            if outcome.created:
                logger.info(
                    "Created topic %s (revision %d)", outcome.slug, outcome.new_revision
                )
            elif outcome.revision_bumped:
                logger.info(
                    "Updated topic %s: classifier_revision %d -> %d",
                    outcome.slug,
                    outcome.old_revision,
                    outcome.new_revision,
                )
        logger.info(
            "Imported taxonomy: %d topic(s); taxonomies created=%s updated=%s; "
            "nodes created=%d updated=%d; pruned topics=%s pruned nodes=%s",
            len(summary.topics),
            summary.taxonomies_created,
            summary.taxonomies_updated,
            summary.nodes_created,
            summary.nodes_updated,
            summary.pruned_topics,
            summary.pruned_nodes,
        )
        return EXIT_OK

    if args.command == "taxonomy-export":
        topic_snapshots, taxonomy_snapshots = await storage.export_taxonomy()
        payload = to_seed_dict(topic_snapshots, taxonomy_snapshots)
        args.file.write_text(yaml.safe_dump(payload, sort_keys=False, allow_unicode=True))
        logger.info(
            "Exported %d topic(s) and %d taxonomy(ies) to %s",
            len(topic_snapshots),
            len(taxonomy_snapshots),
            args.file,
        )
        return EXIT_OK

    if args.command == "queue-status":
        return await _report_queue_status(storage, args)

    if args.command == "taxonomy-rebuild":
        async with storage.worker_lock("taxonomy-rebuild") as acquired:
            if not acquired:
                logger.info(
                    "Another taxonomy-rebuild run holds the lock; exiting without doing work"
                )
                return exit_busy_for(args)
            run_id = await storage.start_run(
                worker="taxonomy-rebuild",
                trigger="manual",
                params={"topic": args.topic},
            )
            try:
                stats = await _rebuild_taxonomy_prototypes(storage, topic_slug=args.topic)
            except Exception as exc:
                logger.exception("taxonomy-rebuild run %d failed", run_id)
                await storage.finish_run(run_id, "failed", None, str(exc))
                return EXIT_FAILED
            await storage.finish_run(run_id, "succeeded", stats)
        logger.info(
            "Prototype rebuild: considered=%d rebuilt=%d skipped_unchanged=%d "
            "skipped_insufficient_exemplars=%d",
            stats.considered,
            stats.rebuilt,
            stats.skipped_unchanged,
            stats.skipped_insufficient_exemplars,
        )
        return EXIT_OK

    if args.command == "boilerplate-refresh":
        # Same lock discipline as taxonomy-rebuild: this rewrites a registry
        # every embed and classify run reads, so two concurrent refreshes must
        # not interleave their writes.
        async with storage.worker_lock("boilerplate-refresh") as acquired:
            if not acquired:
                logger.info(
                    "Another boilerplate-refresh run holds the lock; "
                    "exiting without doing work"
                )
                return exit_busy_for(args)
            run_id = await storage.start_run(
                worker="boilerplate-refresh",
                trigger="manual",
                params={"source": args.source, "sample": args.sample},
            )
            try:
                stats = await storage.refresh_boilerplate_profiles(
                    source_slug=args.source, sample_size=args.sample
                )
            except Exception as exc:
                logger.exception("boilerplate-refresh run %d failed", run_id)
                await storage.finish_run(run_id, "failed", None, str(exc))
                return EXIT_FAILED
            await storage.finish_run(run_id, "succeeded", stats)
        logger.info(
            "Boilerplate refresh: considered=%d refreshed=%d unchanged=%d "
            "skipped_too_few_articles=%d empty_profiles=%d lines=%d",
            stats.considered,
            stats.refreshed,
            stats.unchanged,
            stats.skipped_too_few_articles,
            stats.empty_profiles,
            stats.lines,
        )
        if stats.refreshed:
            logger.info(
                "Profiles changed; articles already embedded keep their old "
                "vectors until `news-worker embed --reprocess` re-queues them"
            )
        return EXIT_OK

    raise AssertionError(f"unhandled admin command: {args.command}")


def _json_default(value: Any) -> str:
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


async def _report_queue_status(
    storage: PostgresRawStorage, args: argparse.Namespace
) -> int:
    """Print one read-only snapshot of every operational queue.

    The JSON form is what a scheduler consumes; the default form is what
    replaces the ad-hoc `psql` one-liners in the README.
    """
    snapshot = await storage.queue_status(runs_window_hours=args.runs_window)
    alerts = snapshot.alerts()

    if args.as_json:
        payload = asdict(snapshot) | {"alerts": alerts}
        # Single line, on stdout, so `--json` composes with XCom and with jq.
        # Timestamps go out as ISO-8601, matching `--summary-json`; `str()` on
        # a datetime would emit a space where the 'T' belongs.
        print(json.dumps(payload, default=_json_default, ensure_ascii=False))
    else:
        for stage in snapshot.stages:
            logger.info(
                "stage %-9s pending=%-6d running=%-4d done=%-7d dead_letter=%-4d "
                "oldest_pending=%s",
                stage.stage,
                stage.pending,
                stage.running,
                stage.done,
                stage.dead_letter,
                stage.oldest_pending_at.isoformat() if stage.oldest_pending_at else "-",
            )
        for failure in snapshot.failures:
            logger.info(
                "failures %-8s total=%-6d due_now=%-5d dead_lettered=%-4d "
                "max_attempts=%d",
                failure.failure_kind,
                failure.total,
                failure.due_now,
                failure.dead_lettered,
                failure.max_attempts,
            )
        for run in snapshot.runs:
            logger.info(
                "runs(%dh) %-16s %-10s %d",
                snapshot.runs_window_hours,
                run.worker,
                run.status,
                run.count,
            )
        for alert in alerts:
            logger.warning("ALERT: %s", alert)

    if alerts and args.fail_on_alert:
        return EXIT_FAILED
    return EXIT_OK


async def _rebuild_taxonomy_prototypes(
    storage: PostgresRawStorage, *, topic_slug: str | None
) -> PrototypeRebuildStats:
    """Recompile prototypes for active topics whose exemplars changed.

    One embed call for every exemplar across every topic being rebuilt,
    rather than one call per topic: the workload here is a handful of topics
    with a few dozen exemplars total, so poison-pill isolation (see
    `EmbedWorker._embed_and_store`) is not worth the extra code.
    """
    # Keep ONNX/FastEmbed out of crawl and administrative CLI startup. Some
    # container CPUs make onnxruntime print a harmless vendor warning merely
    # on import, even when no embedding operation is requested.
    from adapters.embedding.fastembed_embedder import FastEmbedEmbedder

    embedder = FastEmbedEmbedder()
    stats = PrototypeRebuildStats()
    exemplar_sets = await storage.topics_for_prototype_rebuild(topic_slug)

    to_compile: list[tuple[Any, bytes]] = []
    for item in exemplar_sets:
        stats.considered += 1
        if len(item.exemplars) < MIN_EXEMPLARS_TO_ACTIVATE:
            stats.skipped_insufficient_exemplars += 1
            continue
        new_hash = definition_hash(item.exemplars)
        if (
            item.current_definition_hash == new_hash
            and item.current_model == embedder.model_name
            and item.current_dim == embedder.dim
        ):
            stats.skipped_unchanged += 1
            continue
        to_compile.append((item, new_hash))

    if not to_compile:
        return stats

    flat_texts = [exemplar for item, _ in to_compile for exemplar in item.exemplars]
    vectors = await to_thread(embedder.embed, flat_texts)

    records: list[TopicPrototypeRecord] = []
    offset = 0
    for item, new_hash in to_compile:
        n = len(item.exemplars)
        prototype = compile_prototype(vectors[offset : offset + n])
        offset += n
        records.append(
            TopicPrototypeRecord(
                topic_id=item.topic_id,
                model=embedder.model_name,
                dim=embedder.dim,
                definition_hash=new_hash,
                embedding=prototype,
            )
        )
    await storage.upsert_topic_prototypes(records)
    stats.rebuilt = len(records)
    return stats


def main(argv: list[str] | None = None) -> None:
    settings = Settings.from_env()
    setup_logging(settings.log_level)
    parser = build_parser(settings)
    args = parser.parse_args(argv)
    validate(args, parser)
    if not settings.database_url:
        parser.error("DATABASE_URL is required for news-worker")
    window = (
        resolve_window(args, parser)
        if args.command in WORKERS or args.command == "semantic"
        else None
    )
    try:
        if args.command == "semantic":
            exit_code = asyncio.run(_run_semantic(args, settings, window))
        elif args.command == "maintenance":
            exit_code = asyncio.run(_run_maintenance(settings))
        else:
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
