"""Shared ingest orchestration for CLI pipelines and the periodic service."""

from __future__ import annotations

from collections.abc import AsyncIterator
from dataclasses import replace

from adapters.retrieve_news_data.google_news_rss_searcher import RssFeedSearcher
from adapters.storage.postgres import PostgresRawStorage
from core.content_quality import crawl_failure_error_type
from core.crawl_errors import error_context
from core.urls import URL_ERROR_CONTEXT_KEY
from models.search_and_crawl_models import CrawlMetadata, CrawlResult
from models.storage_models import (
    CrawlBatch,
    CrawlFailureOccurrence,
    FailureTarget,
    StoredCrawlFailure,
    StorageWriteSummary,
)
from services.raw_storage import prepare_articles


def _url_rejection_reason(result: CrawlResult) -> str:
    """The reason the crawler refused the URL, or "" if it actually tried."""
    return result.context.get(URL_ERROR_CONTEXT_KEY, "")


def direct_crawl_batch(results: list[CrawlResult]) -> CrawlBatch:
    batch = CrawlBatch(
        results=results,
        articles_discovered=len(results),
    )
    for result in results:
        if result.success:
            resolved_urls = {result.url, result.context.get("requested_url", "")}
            batch.resolved_failures.extend(
                FailureTarget(failure_kind="article", url=url)
                for url in resolved_urls
                if url
            )
        else:
            rejection = _url_rejection_reason(result)
            batch.failures.append(
                CrawlFailureOccurrence(
                    failure_kind="article",
                    url=result.url,
                    error_type=crawl_failure_error_type(result.context),
                    error_message=result.error_message or "Unknown crawl failure",
                    # Only a malformed URL is hopeless. A block page is not: the
                    # same URL may well serve the article on a later attempt.
                    permanent=bool(rejection),
                )
            )
    return batch


async def persist_batch(
    storage: PostgresRawStorage,
    batch: CrawlBatch,
    *,
    priority: int = 1,
) -> StorageWriteSummary:
    articles, rejected = prepare_articles(batch.results)
    return await storage.apply_batch(
        articles,
        batch.failures,
        batch.resolved_failures,
        received=len(batch.results),
        rejected=rejected,
        priority=priority,
    )


def _enrich_retry_result(
    result: CrawlResult,
    failure: StoredCrawlFailure,
) -> CrawlResult:
    metadata = result.metadata or CrawlMetadata()
    result.metadata = replace(
        metadata,
        title=metadata.title or failure.title,
        provider="rss_feed",
    )
    result.context.update(
        {
            "feed_title": failure.source or "",
            "query_label": failure.source or "",
            "feed_url": failure.feed_url or "",
            "published_date": failure.published_at,
            "feed_entry_url": failure.url,
        }
    )
    return result


async def retry_due_failures(
    storage: PostgresRawStorage,
    searcher: RssFeedSearcher,
    *,
    limit: int,
    max_concurrent_crawls: int,
    crawl_batch_size: int = 10,
    priority: int | None = None,
) -> tuple[CrawlBatch, int, int]:
    """Compatibility wrapper combining bounded retry batches."""
    combined = CrawlBatch()
    attempted = 0
    succeeded = 0
    async for batch, attempted_delta, succeeded_delta in iter_retry_batches(
        storage,
        searcher,
        limit=limit,
        max_concurrent_crawls=max_concurrent_crawls,
        crawl_batch_size=crawl_batch_size,
        priority=priority,
    ):
        _merge_batch(combined, batch)
        attempted += attempted_delta
        succeeded += succeeded_delta
    return combined, attempted, succeeded


async def iter_retry_batches(
    storage: PostgresRawStorage,
    searcher: RssFeedSearcher,
    *,
    limit: int,
    max_concurrent_crawls: int,
    crawl_batch_size: int,
    priority: int | None = None,
) -> AsyncIterator[tuple[CrawlBatch, int, int]]:
    """Yield retry work in bounded batches with per-batch target counters.

    Only article failures are retried here: feed_state is the sole retry
    schedule for feeds (see CrawlWorker), so a feed-kind row in
    crawl_failures is a log entry, not a queue to drain.
    """
    due = (
        await storage.due_failures(limit)
        if priority is None
        else await storage.due_failures(limit, priority=priority)
    )
    if not due:
        return

    article_failures = [item for item in due if item.failure_kind == "article"]

    if article_failures:
        for start in range(0, len(article_failures), crawl_batch_size):
            failures = article_failures[start : start + crawl_batch_size]
            try:
                outcomes = await searcher.crawl_batch(
                    [failure.url for failure in failures],
                    max_concurrency=max_concurrent_crawls,
                )
            except Exception as exc:
                outcomes = [
                    CrawlResult(
                        url=failure.url,
                        markdown="",
                        success=False,
                        error_message=str(exc),
                        context=error_context(exc, default="BatchError"),
                    )
                    for failure in failures
                ]
            batch = CrawlBatch()
            succeeded = 0
            for failure, outcome in zip(failures, outcomes, strict=True):
                outcome = _enrich_retry_result(outcome, failure)
                batch.results.append(outcome)
                if outcome.success:
                    succeeded += 1
                    batch.resolved_failures.append(
                        FailureTarget(failure_kind="article", url=failure.url)
                    )
                else:
                    rejection = _url_rejection_reason(outcome)
                    batch.failures.append(
                        CrawlFailureOccurrence(
                            failure_kind="article",
                            url=failure.url,
                            source=failure.source,
                            title=failure.title,
                            published_at=failure.published_at,
                            feed_url=failure.feed_url,
                            error_type=crawl_failure_error_type(outcome.context),
                            error_message=(
                                outcome.error_message or "Unknown crawl failure"
                            ),
                            # A queued row whose URL is malformed can never
                            # succeed; dead-letter it instead of re-crawling.
                            permanent=bool(rejection),
                        )
                    )
            yield batch, len(failures), succeeded


def _merge_batch(destination: CrawlBatch, source: CrawlBatch) -> None:
    destination.results.extend(source.results)
    destination.failures.extend(source.failures)
    destination.resolved_failures.extend(source.resolved_failures)
    destination.feeds_processed += source.feeds_processed
    destination.articles_discovered += source.articles_discovered
