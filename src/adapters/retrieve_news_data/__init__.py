"""News discovery and content retrieval adapters."""

from .crawler import Crawl4aiProvider, CrawlFetcher
from .google_news_rss_searcher import (
    GoogleNewsRssSearcher,
    RssFeedSearcher,
    RssFeedSource,
)
from .searcher import SearchFetcher, TavilySearcher

__all__ = [
    "Crawl4aiProvider",
    "CrawlFetcher",
    "GoogleNewsRssSearcher",
    "RssFeedSearcher",
    "RssFeedSource",
    "SearchFetcher",
    "TavilySearcher",
]
