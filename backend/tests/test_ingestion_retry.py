import asyncio
from datetime import datetime, timezone

from models.search_and_crawl_models import CrawlResult
from models.storage_models import StoredCrawlFailure
from services.ingestion import retry_due_failures


def test_article_retry_preserves_context_and_resolves_original_failure() -> None:
    published_at = datetime(2026, 8, 1, tzinfo=timezone.utc)
    failure = StoredCrawlFailure(
        failure_kind="article",
        url="https://example.com/requested",
        source="Example Engineering",
        title="RSS title",
        published_at=published_at,
        error_type="TimeoutError",
        error_message="timeout",
    )

    class FakeStorage:
        async def due_failures(self, limit: int):
            assert limit == 50
            return [failure]

    class FakeSearcher:
        async def crawl_batch(self, urls, *, max_concurrency=None):
            assert urls == [failure.url]
            assert max_concurrency == 1
            return [
                CrawlResult(
                    url="https://example.com/final",
                    markdown="# Retried article",
                )
            ]

    batch, attempted, succeeded = asyncio.run(
        retry_due_failures(
            FakeStorage(),
            FakeSearcher(),
            limit=50,
            max_concurrent_crawls=1,
        )
    )

    assert attempted == succeeded == 1
    assert batch.results[0].metadata.title == "RSS title"
    assert batch.results[0].context["feed_title"] == "Example Engineering"
    assert batch.results[0].context["published_date"] == published_at
    assert [(item.failure_kind, item.url) for item in batch.resolved_failures] == [
        ("article", failure.url)
    ]


def test_article_retries_are_chunked() -> None:
    failures = [
        StoredCrawlFailure(
            failure_kind="article",
            url=f"https://example.com/{index}",
            error_type="TimeoutError",
            error_message="timeout",
        )
        for index in range(5)
    ]

    class FakeStorage:
        async def due_failures(self, limit: int):
            return failures

    class FakeSearcher:
        calls: list[list[str]] = []

        async def crawl_batch(self, urls, *, max_concurrency=None):
            self.calls.append(list(urls))
            return [CrawlResult(url=url, markdown="# Retried") for url in urls]

    searcher = FakeSearcher()
    batch, attempted, succeeded = asyncio.run(
        retry_due_failures(
            FakeStorage(),
            searcher,
            limit=50,
            max_concurrent_crawls=2,
            crawl_batch_size=2,
        )
    )

    assert [len(call) for call in searcher.calls] == [2, 2, 1]
    assert len(batch.results) == 5
    assert attempted == succeeded == 5
