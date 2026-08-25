import asyncio
from datetime import datetime, timezone

from core.urls import URL_ERROR_CONTEXT_KEY
from models.search_and_crawl_models import CrawlMetadata, CrawlResult
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


def test_retrying_a_malformed_queued_url_dead_letters_it_instead_of_looping() -> None:
    # Rows written before entry links were resolved against the feed URL hold
    # hrefs no crawler can fetch. Retrying one can never change the outcome, so
    # it is recorded as permanent rather than burning five attempts.
    failure = StoredCrawlFailure(
        failure_kind="article",
        url="/2020/06/22/post.html",
        error_type="CrawlError",
        error_message="crawler runtime error",
    )

    class FakeStorage:
        async def due_failures(self, limit: int):
            return [failure]

    class FakeSearcher:
        async def crawl_batch(self, urls, *, max_concurrency=None):
            return [
                CrawlResult(
                    url=url,
                    markdown="",
                    success=False,
                    error_message="URL is not absolute",
                    context={URL_ERROR_CONTEXT_KEY: "relative"},
                )
                for url in urls
            ]

    batch, attempted, succeeded = asyncio.run(
        retry_due_failures(
            FakeStorage(),
            FakeSearcher(),
            limit=50,
            max_concurrent_crawls=1,
        )
    )

    assert (attempted, succeeded) == (1, 0)
    assert len(batch.failures) == 1
    assert batch.failures[0].permanent is True
    assert batch.failures[0].error_type == "UnusableUrl:relative"


def test_a_retry_keeps_the_stored_feed_title_over_the_block_pages() -> None:
    # crawl_failures.title is the last copy of the headline once the entry has
    # dropped out of its feed, so a retry that hits the wall again must not
    # trade it for the wall's own <title>.
    failure = StoredCrawlFailure(
        failure_kind="article",
        url="https://www.engadget.com/gamecube-panasonic-q",
        title="How Xbox controllers have changed in the last 25 years",
        error_type="CrawlError",
        error_message="Blocked by anti-bot protection: HTTP 403",
    )

    class FakeStorage:
        async def due_failures(self, limit: int):
            return [failure]

    class FakeSearcher:
        async def crawl_batch(self, urls, *, max_concurrency=None):
            # What the adapter hands back for a blocked page: no title, because
            # the only one available described the interstitial.
            return [
                CrawlResult(
                    url=url,
                    markdown="",
                    success=False,
                    error_message="Blocked by anti-bot protection: HTTP 403",
                    metadata=CrawlMetadata(title=""),
                )
                for url in urls
            ]

    batch, _, _ = asyncio.run(
        retry_due_failures(
            FakeStorage(),
            FakeSearcher(),
            limit=50,
            max_concurrent_crawls=1,
        )
    )

    assert batch.results[0].metadata.title == failure.title


def test_a_genuine_crawl_failure_stays_retryable() -> None:
    failure = StoredCrawlFailure(
        failure_kind="article",
        url="https://example.com/article",
        error_type="TimeoutError",
        error_message="timeout",
    )

    class FakeStorage:
        async def due_failures(self, limit: int):
            return [failure]

    class FakeSearcher:
        async def crawl_batch(self, urls, *, max_concurrency=None):
            return [
                CrawlResult(
                    url=url,
                    markdown="",
                    success=False,
                    error_message="ERR_CONNECTION_RESET",
                )
                for url in urls
            ]

    batch, _, _ = asyncio.run(
        retry_due_failures(
            FakeStorage(),
            FakeSearcher(),
            limit=50,
            max_concurrent_crawls=1,
        )
    )

    assert batch.failures[0].permanent is False
    assert batch.failures[0].error_type == "CrawlError"
