"""Thin `DiscoverySource` adapter over `RssFeedSearcher`.

All fetch/decode/crawl logic lives on the searcher; this class exists only
so `CrawlWorker` can depend on the `DiscoverySource` port instead of the
concrete RSS implementation.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Sequence

from adapters.retrieve_news_data.google_news_rss_searcher import RssFeedSearcher
from models.discovery_models import DiscoveredEntry, FeedPoll
from models.storage_models import CrawlBatch, FeedPollTarget


class RssDiscovery:
    def __init__(self, searcher: RssFeedSearcher) -> None:
        self._searcher = searcher

    def poll(self, targets: Sequence[FeedPollTarget]) -> AsyncIterator[FeedPoll]:
        return self._searcher.poll_feeds(targets)

    async def crawl_entries(self, entries: Sequence[DiscoveredEntry]) -> CrawlBatch:
        return await self._searcher.crawl_entries(entries)
