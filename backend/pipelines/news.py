"""Command-line pipeline for search, direct crawling, and RSS/Atom feeds."""

from __future__ import annotations

import argparse
import asyncio
import json
from dataclasses import asdict, is_dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Sequence

from adapters.retrieve_news_data.crawler import Crawl4aiProvider
from adapters.retrieve_news_data.google_news_rss_searcher import (
    RssFeedSearcher,
    RssFeedSource,
)
from adapters.retrieve_news_data.searcher import SearchFetcher, TavilySearcher
from adapters.storage import PostgresRawStorage, StorageError
from core.config import DEFAULT_USER_AGENT, Settings
from core.logger import get_logger, setup_logging
from core.urls import SCHEME_RELATIVE_BASE, UnusableUrl, resolve_crawl_url
from models.search_and_crawl_models import (
    CrawlError,
    CrawlResult,
    SearchProviderError,
    SearchResult,
)
from models.storage_models import CrawlBatch, CrawlFailureOccurrence, IngestRunStats
from services.ingestion import direct_crawl_batch, persist_batch


logger = get_logger(__name__)


def _json_default(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    if is_dataclass(value) and not isinstance(value, type):
        return asdict(value)
    raise TypeError(f"Cannot serialize {type(value).__name__}")


def emit_results(results: Sequence[SearchResult | CrawlResult]) -> None:
    """Write one JSON object per line so downstream consumers can stream it."""
    for result in results:
        print(json.dumps(asdict(result), default=_json_default, ensure_ascii=False))


async def run_search(
    query: str,
    *,
    max_results: int,
    extract: bool,
    api_key: str | None,
) -> list[SearchResult]:
    fetcher = SearchFetcher(sources=[TavilySearcher(api_key=api_key)])
    return await fetcher.fetch(query, max_results=max_results, extract=extract)


async def run_crawl_batch(urls: list[str]) -> CrawlBatch:
    # Command-line URLs have no containing document to resolve against, so a
    # protocol-relative argument (`//host/path`) defaults to https instead of
    # reaching the crawler without a scheme. Anything still unresolvable is a
    # permanent failure: retrying it can never produce a different outcome.
    crawlable: list[str] = []
    rejected: list[CrawlFailureOccurrence] = []
    for url in urls:
        try:
            crawlable.append(resolve_crawl_url(url, base_url=SCHEME_RELATIVE_BASE))
        except UnusableUrl as exc:
            logger.error("Not crawlable, skipping: %s", exc)
            rejected.append(
                CrawlFailureOccurrence(
                    failure_kind="article",
                    url=url,
                    error_type=f"UnusableUrl:{exc.reason}",
                    error_message=str(exc),
                    permanent=True,
                )
            )

    if not crawlable:
        return CrawlBatch(failures=rejected, articles_discovered=len(urls))

    provider = Crawl4aiProvider()
    try:
        if len(crawlable) == 1:
            results = [await provider.crawl(crawlable[0])]
        else:
            results = await provider.crawl_batch(crawlable)
    except Exception as exc:
        rejected.extend(
            CrawlFailureOccurrence(
                failure_kind="article",
                url=url,
                error_type=type(exc).__name__,
                error_message=str(exc),
            )
            for url in crawlable
        )
        return CrawlBatch(failures=rejected, articles_discovered=len(urls))

    batch = direct_crawl_batch(results)
    batch.failures.extend(rejected)
    batch.articles_discovered += len(rejected)
    return batch


async def run_crawl(urls: list[str]) -> list[CrawlResult]:
    return (await run_crawl_batch(urls)).results


async def run_rss_batch(
    feed_path: Path,
    *,
    max_feeds: int,
    max_articles_per_feed: int,
    max_concurrent_crawls: int,
    max_concurrent_feeds: int = 2,
    crawl_batch_size: int = 10,
    feed_offset: int = 0,
    user_agent: str = DEFAULT_USER_AGENT,
) -> CrawlBatch:
    searcher = RssFeedSearcher(
        feed_path=feed_path,
        max_concurrent_crawls=max_concurrent_crawls,
        max_concurrent_feeds=max_concurrent_feeds,
        crawl_batch_size=crawl_batch_size,
        user_agent=user_agent,
    )
    return await searcher.fetch_batch(
        max_articles_per_feed=max_articles_per_feed,
        max_feeds=max_feeds,
        feed_offset=feed_offset,
    )


async def run_rss(
    feed_path: Path,
    *,
    max_feeds: int,
    max_articles_per_feed: int,
    max_concurrent_crawls: int,
    max_concurrent_feeds: int = 2,
    crawl_batch_size: int = 10,
    feed_offset: int = 0,
) -> list[CrawlResult]:
    return (
        await run_rss_batch(
            feed_path,
            max_feeds=max_feeds,
            max_articles_per_feed=max_articles_per_feed,
            max_concurrent_crawls=max_concurrent_crawls,
            max_concurrent_feeds=max_concurrent_feeds,
            crawl_batch_size=crawl_batch_size,
            feed_offset=feed_offset,
        )
    ).results


def build_parser(settings: Settings) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    search = subparsers.add_parser("search", help="Search with Tavily")
    search.add_argument("query")
    search.add_argument("--max-results", type=int, default=10)
    search.add_argument("--extract", action="store_true")

    crawl = subparsers.add_parser("crawl", help="Crawl one or more URLs")
    crawl.add_argument("urls", nargs="+")
    crawl.add_argument("--persist", action="store_true", help="Upsert into PostgreSQL")

    rss = subparsers.add_parser("rss", help="Fetch and crawl RSS/Atom feeds")
    feed_source = rss.add_mutually_exclusive_group()
    feed_source.add_argument(
        "feed_urls",
        nargs="*",
        metavar="URL",
        help=(
            "One or more RSS/Atom URLs to process directly; use --feed-path "
            "instead to load feeds from TSV/OPML"
        ),
    )
    feed_source.add_argument(
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
    rss.add_argument(
        "--max-feeds",
        type=int,
        default=settings.max_feeds,
        help="Maximum feeds to process; 0 means all feeds",
    )
    rss.add_argument(
        "--max-articles-per-feed",
        type=int,
        default=settings.max_articles_per_feed,
    )
    rss.add_argument(
        "--max-concurrent-feeds",
        type=int,
        default=settings.max_concurrent_feeds,
    )
    rss.add_argument(
        "--max-concurrent-crawls",
        type=int,
        default=settings.max_concurrent_crawls,
    )
    rss.add_argument(
        "--crawl-batch-size",
        type=int,
        default=settings.crawl_batch_size,
        help="Maximum crawled articles retained before each DB checkpoint",
    )
    rss.add_argument(
        "--feed-offset",
        type=int,
        default=settings.feed_offset,
    )
    rss.add_argument("--persist", action="store_true", help="Upsert into PostgreSQL")
    return parser


async def _dispatch(args: argparse.Namespace, settings: Settings) -> None:
    if args.command == "search":
        results = await run_search(
            args.query,
            max_results=args.max_results,
            extract=args.extract,
            api_key=settings.tavily_api_key,
        )
        emit_results(results)
        return

    direct_feed_urls: list[str] = []
    if args.command != "crawl":
        direct_feed_urls = [
            resolve_crawl_url(url, base_url=SCHEME_RELATIVE_BASE)
            for url in args.feed_urls
        ]
        if args.feed_path is None and not direct_feed_urls:
            raise ValueError(
                "rss requires one or more feed URLs, --feed-path/--tsv-path, "
                "or RSS_FEED_PATH/GOOGLE_NEWS_TSV_PATH"
            )

    storage: PostgresRawStorage | None = None
    run_id: int | None = None
    stats = IngestRunStats()
    try:
        if args.persist:
            if not settings.database_url:
                raise StorageError("DATABASE_URL is required when --persist is used")
            storage = PostgresRawStorage(settings.database_url)
            await storage.check_ready()
            # news-pipeline is a hand-run debugging entry point, so it books a
            # manual crawl run; the exact subcommand stays in params rather
            # than widening the worker vocabulary.
            run_id = await storage.start_run(
                worker="crawl",
                trigger="manual",
                params={"entrypoint": "news-pipeline", "command": args.command},
            )

        if args.command == "crawl":
            batch = await run_crawl_batch(args.urls)
        else:
            sources = (
                [RssFeedSource(url, url) for url in direct_feed_urls]
                if direct_feed_urls
                else None
            )
            searcher = RssFeedSearcher(
                feed_path=None if direct_feed_urls else args.feed_path,
                max_concurrent_crawls=args.max_concurrent_crawls,
                max_concurrent_feeds=args.max_concurrent_feeds,
                crawl_batch_size=args.crawl_batch_size,
                user_agent=settings.feed_user_agent,
            )
            has_failures = False
            total_received = 0
            total_deduplicated = 0
            total_written = 0
            total_unchanged = 0
            total_rejected = 0
            async for batch in searcher.iter_batches(
                max_articles_per_feed=args.max_articles_per_feed,
                max_feeds=args.max_feeds,
                sources=sources,
                feed_offset=args.feed_offset,
            ):
                if storage is not None:
                    summary = await persist_batch(storage, batch)
                    stats.add_batch(batch)
                    stats.articles_written += summary.written
                    stats.articles_unchanged += summary.unchanged
                    total_received += summary.received
                    total_deduplicated += summary.deduplicated
                    total_written += summary.written
                    total_unchanged += summary.unchanged
                    total_rejected += summary.rejected
                    await storage.update_run(run_id, stats)
                else:
                    stats.add_batch(batch)
                has_failures = has_failures or bool(batch.failures)
                emit_results(batch.results)
            if storage is not None:
                await storage.finish_run(
                    run_id,
                    "partial" if has_failures else "succeeded",
                    stats,
                )
                logger.info(
                    "Storage: received=%d deduplicated=%d written=%d "
                    "unchanged=%d rejected=%d",
                    total_received,
                    total_deduplicated,
                    total_written,
                    total_unchanged,
                    total_rejected,
                )
            return
        stats.add_batch(batch)
        if storage is not None:
            summary = await persist_batch(storage, batch)
            stats.articles_written += summary.written
            stats.articles_unchanged += summary.unchanged
            await storage.finish_run(
                run_id,
                "partial" if batch.failures else "succeeded",
                stats,
            )
            logger.info(
                "Storage: received=%d deduplicated=%d written=%d unchanged=%d rejected=%d",
                summary.received,
                summary.deduplicated,
                summary.written,
                summary.unchanged,
                summary.rejected,
            )
        emit_results(batch.results)
    except asyncio.CancelledError:
        if storage is not None and run_id is not None:
            await storage.finish_run(
                run_id,
                "partial",
                stats,
                "Interrupted before the pipeline completed",
            )
        raise
    except Exception as exc:
        if storage is not None and run_id is not None:
            await storage.finish_run(run_id, "failed", stats, str(exc))
        raise
    finally:
        if storage is not None:
            await storage.close()


def main() -> None:
    settings = Settings.from_env()
    setup_logging(settings.log_level)
    parser = build_parser(settings)
    args = parser.parse_args()
    if args.command == "rss":
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
    try:
        asyncio.run(_dispatch(args, settings))
    except (
        CrawlError,
        FileNotFoundError,
        SearchProviderError,
        StorageError,
        ValueError,
    ) as exc:
        parser.error(str(exc))
    except KeyboardInterrupt:
        logger.info("Pipeline interrupted")


if __name__ == "__main__":
    main()
