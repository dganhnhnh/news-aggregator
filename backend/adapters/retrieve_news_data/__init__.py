"""News discovery and content retrieval adapters."""

from .crawler import Crawl4aiProvider, CrawlFetcher
from .google_news_feed_discovery import FeedDiscoveryError, GoogleNewsFeedDiscovery
from .google_news_rss_searcher import (
    GoogleNewsRssSearcher,
    RssFeedSearcher,
    RssFeedSource,
)
from .searcher import SearchFetcher, TavilySearcher
from .topic_feed_discovery import TopicFeedDiscovery

__all__ = [
    "Crawl4aiProvider",
    "CrawlFetcher",
    "FeedDiscoveryError",
    "GoogleNewsRssSearcher",
    "GoogleNewsFeedDiscovery",
    "RssFeedSearcher",
    "RssFeedSource",
    "SearchFetcher",
    "TavilySearcher",
    "TopicFeedDiscovery",
]
