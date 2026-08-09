from datetime import datetime, timedelta, timezone

from adapters.storage.postgres import retry_delay
from models.search_and_crawl_models import CrawlMetadata, CrawlResult
from services.raw_storage import (
    content_hash,
    crawl_result_to_article,
    normalize_content,
    normalize_url,
    prepare_articles,
)


def test_normalize_url_is_conservative_and_removes_tracking() -> None:
    assert normalize_url(
        "HTTPS://Example.COM:443/news?id=7&utm_source=rss&fbclid=x#section"
    ) == "https://example.com/news?id=7"


def test_normalize_content_and_hash_are_stable() -> None:
    first = normalize_content("  # Article\r\nBody\r\n")
    second = normalize_content("# Article\nBody")

    assert first == second == "# Article\nBody"
    assert content_hash(first) == content_hash(second)
    assert len(content_hash(first)) == 32


def test_crawl_result_mapping_uses_feed_source_and_published_date() -> None:
    published_at = datetime(2026, 8, 1, tzinfo=timezone.utc)
    result = CrawlResult(
        url="https://example.com/article?utm_medium=rss",
        markdown="# Article",
        metadata=CrawlMetadata(title="Article"),
        context={
            "feed_title": "Example Engineering",
            "query_label": "Fallback",
            "published_date": published_at,
        },
    )

    article = crawl_result_to_article(result)

    assert article.url == "https://example.com/article"
    assert article.source == "Example Engineering"
    assert article.published_at == published_at
    assert article.content == "# Article"


def test_prepare_articles_keeps_newest_result_per_url() -> None:
    now = datetime.now(timezone.utc)
    old = CrawlResult(
        url="https://example.com/article?utm_source=old",
        markdown="old",
        fetched_at=now - timedelta(minutes=1),
    )
    new = CrawlResult(
        url="https://example.com/article",
        markdown="new",
        fetched_at=now,
    )

    articles, rejected = prepare_articles([old, new])

    assert rejected == 0
    assert len(articles) == 1
    assert articles[0].content == "new"


def test_retry_delay_uses_exponential_backoff_with_one_day_cap() -> None:
    assert retry_delay(1) == timedelta(minutes=5)
    assert retry_delay(2) == timedelta(minutes=10)
    assert retry_delay(20) == timedelta(days=1)
