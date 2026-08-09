"""Periodic RSS/Atom crawler service backed by TSV or OPML input."""

from __future__ import annotations

import argparse
import asyncio
from pathlib import Path

from core.config import Settings
from core.logger import get_logger, setup_logging
from pipelines.news import emit_results, run_rss


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
        "--max-concurrent-crawls",
        type=int,
        default=settings.max_concurrent_crawls,
    )
    parser.add_argument("--once", action="store_true")
    return parser


async def run_service(args: argparse.Namespace) -> None:
    while True:
        try:
            results = await run_rss(
                args.feed_path,
                max_feeds=args.max_feeds,
                max_articles_per_feed=args.max_articles_per_feed,
                max_concurrent_crawls=args.max_concurrent_crawls,
            )
            emit_results(results)
        except Exception:
            logger.exception("RSS pipeline iteration failed")
            if args.once:
                raise

        if args.once:
            return
        logger.info("Next RSS run in %d seconds", args.interval_seconds)
        await asyncio.sleep(args.interval_seconds)


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
    try:
        asyncio.run(run_service(args))
    except KeyboardInterrupt:
        logger.info("Service stopped")


if __name__ == "__main__":
    main()
