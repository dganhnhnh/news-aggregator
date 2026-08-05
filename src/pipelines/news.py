"""Command-line pipeline for search, direct crawling, and Google News RSS."""

from __future__ import annotations

import argparse
import asyncio
import json
from dataclasses import asdict, is_dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Sequence

from adapters.retrieve_news_data.crawler import CrawlFetcher
from adapters.retrieve_news_data.google_news_rss_searcher import GoogleNewsRssSearcher
from adapters.retrieve_news_data.searcher import SearchFetcher, TavilySearcher
from core.config import Settings
from core.logger import get_logger, setup_logging
from models.search_and_crawl_models import (
    CrawlError,
    CrawlResult,
    SearchProviderError,
    SearchResult,
)


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


async def run_crawl(urls: list[str]) -> list[CrawlResult]:
    fetcher = CrawlFetcher()
    if len(urls) == 1:
        return [await fetcher.fetch(urls[0])]
    return await fetcher.fetch_batch(urls)


async def run_rss(
    tsv_path: Path,
    *,
    max_articles_per_feed: int,
    max_concurrent_crawls: int,
) -> list[CrawlResult]:
    searcher = GoogleNewsRssSearcher(
        tsv_path=tsv_path,
        max_concurrent_crawls=max_concurrent_crawls,
    )
    return await searcher.fetch_all(max_articles_per_feed=max_articles_per_feed)


def build_parser(settings: Settings) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    search = subparsers.add_parser("search", help="Search with Tavily")
    search.add_argument("query")
    search.add_argument("--max-results", type=int, default=10)
    search.add_argument("--extract", action="store_true")

    crawl = subparsers.add_parser("crawl", help="Crawl one or more URLs")
    crawl.add_argument("urls", nargs="+")

    rss = subparsers.add_parser("rss", help="Fetch and crawl Google News RSS feeds")
    rss.add_argument(
        "--tsv-path",
        type=Path,
        default=settings.google_news_tsv_path,
        help="TSV config path; defaults to GOOGLE_NEWS_TSV_PATH",
    )
    rss.add_argument(
        "--max-articles-per-feed",
        type=int,
        default=settings.max_articles_per_feed,
    )
    rss.add_argument(
        "--max-concurrent-crawls",
        type=int,
        default=settings.max_concurrent_crawls,
    )
    return parser


async def _dispatch(args: argparse.Namespace, settings: Settings) -> None:
    if args.command == "search":
        results = await run_search(
            args.query,
            max_results=args.max_results,
            extract=args.extract,
            api_key=settings.tavily_api_key,
        )
    elif args.command == "crawl":
        results = await run_crawl(args.urls)
    else:
        if args.tsv_path is None:
            raise ValueError("rss requires --tsv-path or GOOGLE_NEWS_TSV_PATH")
        results = await run_rss(
            args.tsv_path,
            max_articles_per_feed=args.max_articles_per_feed,
            max_concurrent_crawls=args.max_concurrent_crawls,
        )
    emit_results(results)


def main() -> None:
    settings = Settings.from_env()
    setup_logging(settings.log_level)
    parser = build_parser(settings)
    args = parser.parse_args()
    try:
        asyncio.run(_dispatch(args, settings))
    except (CrawlError, FileNotFoundError, SearchProviderError, ValueError) as exc:
        parser.error(str(exc))
    except KeyboardInterrupt:
        logger.info("Pipeline interrupted")


if __name__ == "__main__":
    main()
