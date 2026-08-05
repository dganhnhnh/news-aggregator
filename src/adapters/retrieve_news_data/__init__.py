"""News discovery and content retrieval adapters."""

from .crawler import Crawl4aiProvider, CrawlFetcher
from .google_news_rss_searcher import GoogleNewsRssSearcher
from .searcher import SearchFetcher, TavilySearcher

__all__ = [
    "Crawl4aiProvider",
    "CrawlFetcher",
    "GoogleNewsRssSearcher",
    "SearchFetcher",
    "TavilySearcher",
]
