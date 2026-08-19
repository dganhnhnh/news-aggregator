"""Discovery domain models — pure data plus the DiscoverySource port.

Separating "where entries come from" from "how they're filtered and
persisted" is what lets a filtering step be inserted between the two, and
lets a future deep-backfill adapter (sitemap/CDX/GDELT) plug into the same
port without CrawlWorker changing at all.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from typing import Literal, Protocol

from models.storage_models import CrawlBatch, CrawlFailureOccurrence, FeedPollTarget


@dataclass(frozen=True)
class DiscoveredEntry:
    """One feed entry, not yet decoded/normalized/crawled."""

    feed_url: str
    query_label: str
    feed_title: str
    entry_url: str
    title: str
    source: str
    published_at: datetime | None


FeedPollStatus = Literal["fetched", "not_modified", "failed"]


@dataclass(frozen=True)
class FeedPoll:
    """Outcome of polling a single feed."""

    target: FeedPollTarget
    status: FeedPollStatus
    entries: list[DiscoveredEntry] = field(default_factory=list)
    feed_title: str = ""
    etag: str | None = None
    last_modified: str | None = None
    error: CrawlFailureOccurrence | None = None


class DiscoverySource(Protocol):
    def poll(
        self, targets: Sequence[FeedPollTarget]
    ) -> AsyncIterator[FeedPoll]:
        """Poll each target, yielding one FeedPoll per feed."""
        ...

    async def crawl_entries(
        self, entries: Sequence[DiscoveredEntry]
    ) -> CrawlBatch:
        """Crawl a bounded chunk of already-filtered entries."""
        ...
