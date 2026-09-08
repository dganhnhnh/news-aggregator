"""Continuously poll only priority-0 feeds with a fixed short interval."""

from workers.crawl import CrawlWorker


class InstantCrawlWorker(CrawlWorker):
    """The latency-sensitive crawl tier, isolated by its own worker lock."""

    name = "instant-crawl"
    feed_priority = 0
    adaptive_polling = False
