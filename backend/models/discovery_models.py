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
    # Full article HTML when the feed shipped one (RSS content:encoded / Atom
    # <content>). Carried through discovery so the crawl step can render it
    # instead of fetching a page the feed already gave us.
    content_html: str = ""
    # The entry's <description>/<summary> HTML, kept separately from
    # `content_html` even when the two are the same string: this one is stored
    # for every article as a preview and as a fallback for embedding, and it is
    # unrecoverable once the entry scrolls out of the feed.
    summary_html: str = ""
    # Publisher website advertised by an RSS item (Google News exposes this
    # on its ``<source url=\"...\">`` element). Kept last so existing positional
    # constructors retain their meaning.
    source_url: str = ""


@dataclass(frozen=True)
class FeedDiscoveryCandidate:
    """A validated feed with explainable topic-discovery evidence."""

    feed_url: str
    source_label: str
    website_url: str
    feed_scope: Literal["category", "tag", "sitewide"]
    discovery_source: Literal[
        "tavily_direct", "tavily_publisher", "google_fallback"
    ]
    relevance_reasons: tuple[str, ...]
    sample_titles: tuple[str, ...] = ()
    last_published_at: datetime | None = None


@dataclass(frozen=True)
class FeedDiscoveryStats:
    """Bounded operational counts for one topic-discovery request."""

    search_results: int = 0
    publishers_inspected: int = 0
    pages_inspected: int = 0
    feeds_validated: int = 0
    google_fallback_used: bool = False
    feeds_attempted: int = 0
    feeds_relevant: int = 0


@dataclass(frozen=True)
class FeedDiscoveryResult:
    """Bounded, ranked feed candidates for one topic query."""

    query: str
    candidates: tuple[FeedDiscoveryCandidate, ...]
    stats: FeedDiscoveryStats = field(default_factory=FeedDiscoveryStats)
    warnings: tuple[str, ...] = ()


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
    # Set only when the feed answered through a permanent redirect chain, so a
    # caller can record the new location instead of re-following it every poll.
    resolved_url: str | None = None
    # Entries dropped because they point at something that is not an article
    # (podcast enclosures, other media). Reported rather than silently lost.
    entries_skipped_non_article: int = 0


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
