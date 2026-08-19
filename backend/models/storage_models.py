"""Domain objects for raw storage and ingest operations."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Literal

from models.search_and_crawl_models import CrawlResult


FailureKind = Literal["feed", "article"]
RunStatus = Literal["running", "succeeded", "partial", "failed", "abandoned"]
WorkerTrigger = Literal["scheduled", "manual", "backfill"]


@dataclass(frozen=True)
class FailureTarget:
    failure_kind: FailureKind
    url: str


@dataclass(frozen=True)
class CrawlFailureOccurrence:
    failure_kind: FailureKind
    url: str
    error_type: str
    error_message: str
    source: str | None = None
    title: str | None = None
    published_at: datetime | None = None


@dataclass(frozen=True)
class StoredCrawlFailure(CrawlFailureOccurrence):
    attempt_count: int = 1
    first_failed_at: datetime | None = None
    last_failed_at: datetime | None = None
    next_retry_at: datetime | None = None


@dataclass(frozen=True)
class RawArticle:
    url: str
    url_original: str
    title: str | None
    source: str | None
    lang: str | None
    content: str
    content_hash: bytes
    published_at: datetime | None
    crawled_at: datetime


@dataclass(frozen=True)
class ArticleListItem:
    id: int
    url: str
    title: str | None
    source: str | None
    published_at: datetime | None
    crawled_at: datetime


@dataclass
class CrawlBatch:
    results: list[CrawlResult] = field(default_factory=list)
    failures: list[CrawlFailureOccurrence] = field(default_factory=list)
    resolved_failures: list[FailureTarget] = field(default_factory=list)
    feeds_processed: int = 0
    articles_discovered: int = 0

    @property
    def crawl_succeeded(self) -> int:
        return sum(result.success for result in self.results)

    @property
    def crawl_failed(self) -> int:
        return len(self.failures)


@dataclass
class IngestRunStats:
    feeds_processed: int = 0
    articles_discovered: int = 0
    crawl_succeeded: int = 0
    crawl_failed: int = 0
    retries_attempted: int = 0
    retries_succeeded: int = 0
    articles_written: int = 0
    articles_unchanged: int = 0

    def add_batch(self, batch: CrawlBatch, *, retry: bool = False) -> None:
        self.feeds_processed += batch.feeds_processed
        self.articles_discovered += batch.articles_discovered
        self.crawl_succeeded += batch.crawl_succeeded
        self.crawl_failed += batch.crawl_failed
        if retry:
            self.retries_attempted += batch.crawl_succeeded + batch.crawl_failed
            self.retries_succeeded += batch.crawl_succeeded


@dataclass(frozen=True)
class StorageWriteSummary:
    received: int
    deduplicated: int
    written: int
    unchanged: int
    rejected: int
