from datetime import datetime, timedelta, timezone

import pytest

from adapters.storage.postgres import retry_delay
from models.search_and_crawl_models import CrawlMetadata, CrawlResult
from services.raw_storage import (
    content_hash,
    crawl_result_to_article,
    normalize_content,
    normalize_url,
    prepare_articles,
    source_slug,
)


NORMALIZE_URL_CASES = [
    # (label, input, expected)
    (
        "scheme/host lowercased, default https port dropped, utm+fbclid stripped",
        "HTTPS://Example.COM:443/news?id=7&utm_source=rss&fbclid=x#section",
        "https://example.com/news?id=7",
    ),
    (
        "query params sorted regardless of original order",
        "https://example.com/a?b=2&a=1",
        "https://example.com/a?a=1&b=2",
    ),
    (
        "trailing slash stripped on a non-root path",
        "https://example.com/path/",
        "https://example.com/path",
    ),
    (
        "root path keeps its single slash",
        "https://example.com",
        "https://example.com/",
    ),
    (
        "mc_cid and mc_eid stripped",
        "https://example.com/p?mc_cid=abc&mc_eid=def&x=1",
        "https://example.com/p?x=1",
    ),
    (
        "igshid stripped",
        "https://example.com/p?igshid=xyz",
        "https://example.com/p",
    ),
    (
        "ref and ref_src stripped",
        "https://example.com/p?ref=fb&ref_src=twitter",
        "https://example.com/p",
    ),
    (
        "_hsenc stripped",
        "https://example.com/p?_hsenc=abc",
        "https://example.com/p",
    ),
    (
        "/amp path suffix stripped",
        "https://example.com/article/amp",
        "https://example.com/article",
    ),
    (
        "/amp/ path suffix stripped",
        "https://example.com/article/amp/",
        "https://example.com/article",
    ),
    (
        ".amp path suffix stripped",
        "https://example.com/article.amp",
        "https://example.com/article",
    ),
    (
        "bare /amp path collapses to root",
        "https://example.com/amp",
        "https://example.com/",
    ),
    (
        "m. host prefix collapsed",
        "https://m.example.com/article",
        "https://example.com/article",
    ),
    (
        "amp. host prefix collapsed",
        "https://amp.example.com/article",
        "https://example.com/article",
    ),
    (
        "www. host prefix is NOT collapsed (only m./amp. are)",
        "https://www.example.com/article",
        "https://www.example.com/article",
    ),
    (
        "non-tracking query params are NOT merged away",
        "https://example.com/list?page=2",
        "https://example.com/list?page=2",
    ),
    (
        "blank-value query params are NOT merged away",
        "https://example.com/a?id=",
        "https://example.com/a?id=",
    ),
    (
        "non-default port is preserved",
        "https://example.com:8443/a",
        "https://example.com:8443/a",
    ),
    (
        "userinfo is preserved",
        "https://user:pass@example.com/a",
        "https://user:pass@example.com/a",
    ),
    (
        "path merely containing 'amp' is not mistaken for the AMP suffix",
        "https://example.com/example-amp",
        "https://example.com/example-amp",
    ),
]


@pytest.mark.parametrize(
    "label,url,expected",
    NORMALIZE_URL_CASES,
    ids=[case[0] for case in NORMALIZE_URL_CASES],
)
def test_normalize_url(label: str, url: str, expected: str) -> None:
    assert normalize_url(url) == expected


def test_source_slug_normalizes_only_non_publisher_host_variants() -> None:
    assert source_slug("https://www.example.com/article") == "example.com"
    assert source_slug("https://m.example.com/article") == "example.com"
    assert source_slug("https://amp.example.com/article") == "example.com"
    assert source_slug("https://example.com:8443/article") == "example.com"
    assert source_slug("https://news.example.com/article") == "news.example.com"


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
        metadata=CrawlMetadata(title="Article", language="en"),
        context={
            "feed_title": "Example Engineering",
            "query_label": "Fallback",
            "published_date": published_at,
        },
    )

    article = crawl_result_to_article(result)

    assert article.url == "https://example.com/article"
    assert article.url_original == "https://example.com/article?utm_medium=rss"
    assert article.source == "Example Engineering"
    assert article.lang == "en"
    assert article.published_at == published_at
    assert article.content == "# Article"


def test_crawl_result_mapping_lang_is_none_when_undetected() -> None:
    result = CrawlResult(url="https://example.com/article", markdown="# Article")

    article = crawl_result_to_article(result)

    assert article.lang is None


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


def _upper_bound(_lo: float, hi: float) -> float:
    return hi


def test_retry_delay_uses_exponential_backoff_with_one_day_cap() -> None:
    assert retry_delay(1, random_uniform=_upper_bound) == timedelta(minutes=5)
    assert retry_delay(2, random_uniform=_upper_bound) == timedelta(minutes=10)
    assert retry_delay(20, random_uniform=_upper_bound) == timedelta(days=1)


def test_retry_delay_applies_full_jitter_between_zero_and_the_cap() -> None:
    assert retry_delay(2, random_uniform=lambda lo, hi: lo) == timedelta(0)
    assert retry_delay(2, random_uniform=lambda lo, hi: hi) == timedelta(minutes=10)

    seen = []
    retry_delay(3, random_uniform=lambda lo, hi: seen.append((lo, hi)) or lo)
    assert seen == [(0, 1200)]
