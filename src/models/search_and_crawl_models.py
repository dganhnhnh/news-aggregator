"""
Search and crawl domain models — pure data, zero external dependencies.

This module contains only dataclasses, enums, and exceptions.
No imports from crawl4ai, tavily, or any other I/O library.
"""
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional


# ═══════════════════════════════════════════════════════════════════════════════
#  SEARCH MODELS
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass(frozen=True)
class SearchResult:
    """
    Immutable snapshot of a single web-search result.

    Represents the raw output from a search engine before any
    downstream processing (e.g. feeding into an LLM).
    """

    id: str
    url: str
    title: str
    content_snippet: str
    published_date: Optional[datetime] = None
    score: Optional[float] = None
    source: str = ""
    metadata: Optional[dict] = field(default_factory=dict)

    @property
    def content_length(self) -> int:
        return len(self.content_snippet)

    @property
    def is_recent(self) -> bool:
        """True if the result was published within the last 24 hours."""
        if self.published_date is None:
            return False
        delta = datetime.now(timezone.utc) - self.published_date
        return delta.total_seconds() < 86_400

    def __str__(self) -> str:
        date_str = (
            self.published_date.strftime("%Y-%m-%d")
            if self.published_date
            else "no-date"
        )
        snippet = (self.content_snippet[:80] + "…") if len(self.content_snippet) > 80 else self.content_snippet
        return f"[{date_str}] {self.title} — {self.url}\n  {snippet}"


class SearchError(Exception):
    """Raised when all configured search providers fail."""


class SearchProviderError(Exception):
    """Raised when a single search provider fails."""


class Searcher(ABC):
    """
    Abstract port for web-search providers.

    Concrete implementations (adapters) translate the provider's
    API response into the canonical SearchResult format.
    """

    name: str

    @abstractmethod
    async def search(self, query: str, max_results: int = 10) -> list[SearchResult]:
        """
        Run a keyword / semantic search query.

        Args:
            query: Free-text search query.
            max_results: Maximum number of results to return.

        Returns:
            List of SearchResult objects, ordered by relevance.
        """
        ...

    @abstractmethod
    async def search_and_extract(
        self, query: str, max_results: int = 5
    ) -> list[SearchResult]:
        """
        Run a search query and immediately fetch the full content of
        each returned URL (search → extract pipeline).

        Args:
            query: Free-text search query.
            max_results: How many top results to pull full content for.

        Returns:
            List of SearchResult objects enriched with full page content.
        """
        ...


# ═══════════════════════════════════════════════════════════════════════════════
#  CRAWL MODELS
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass(frozen=True)
class CrawlMetadata:
    """Structured metadata extracted from a crawled page."""

    title: Optional[str] = None
    description: Optional[str] = None
    language: Optional[str] = None
    author: Optional[str] = None
    provider: str = ""

    def __str__(self) -> str:
        return f"title={self.title!r}, description={self.description!r}"


@dataclass
class CrawlResult:
    """
    Immutable snapshot of a crawled web page.

    url            — the canonical URL that was fetched
    markdown       — main content rendered as plain text/markdown
    html           — cleaned HTML (may be empty)
    metadata       — structured page metadata
    success        — True iff the crawl completed without critical errors
    error_message  — human-readable error (empty when success=True)
    fetched_at     — UTC timestamp of the crawl
    """

    url: str
    markdown: str
    html: str = ""
    metadata: CrawlMetadata = field(default_factory=CrawlMetadata)
    success: bool = True
    error_message: str = ""
    fetched_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    context: dict = field(default_factory=dict)

    @property
    def content_length(self) -> int:
        """Approximate character count of the markdown content."""
        return len(self.markdown)

    def __str__(self) -> str:
        status = "OK" if self.success else f"FAIL: {self.error_message}"
        preview = self.markdown[:100].replace("\n", " ")
        return f"[{status}] {self.url}\n  {preview}…"


class CrawlError(Exception):
    """Raised when all configured crawl providers fail."""


class CrawlProviderError(Exception):
    """Raised when a single crawl provider fails."""


class CrawlerProvider(ABC):
    """
    Abstract port for web-crawling providers.

    Concrete implementations wrap a specific library (e.g. crawl4ai)
    and translate its response into the canonical CrawlResult format.
    """

    name: str

    @abstractmethod
    async def crawl(self, url: str) -> CrawlResult:
        """
        Fetch and extract content from a single URL.

        Args:
            url: The fully-qualified URL to crawl.

        Returns:
            CrawlResult with markdown content and metadata.
        """
        ...

    @abstractmethod
    async def crawl_batch(
        self,
        urls: list[str],
        *,
        max_concurrency: int | None = None,
    ) -> list[CrawlResult]:
        """
        Fetch and extract content from multiple URLs.

        Args:
            urls: List of fully-qualified URLs.
            max_concurrency: Optional upper bound for active page crawls.

        Returns:
            List of CrawlResult objects (one per URL, in the same order).
        """
        ...
