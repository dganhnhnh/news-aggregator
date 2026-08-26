from datetime import datetime, timedelta, timezone

import pytest

from adapters.storage.postgres import retry_delay
from core.html_content import FEED_CONTENT_KEY, FEED_SUMMARY_KEY
from core.urls import URL_ERROR_CONTEXT_KEY
from models.search_and_crawl_models import CrawlMetadata, CrawlResult
from services.raw_storage import (
    content_hash,
    crawl_result_to_article,
    normalize_content,
    normalize_url,
    prepare_articles,
    source_slug,
)


SUMMARY = (
    "Hugging Face is using the breach to push for openness in AI development, "
    "arguing that closed models make incidents like this harder to audit."
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
    (
        "Reddit challenge state is not article identity",
        "https://www.reddit.com/r/python/comments/abc/story/"
        "?token=second&solution=new&js_challenge=1&sort=new",
        "https://www.reddit.com/r/python/comments/abc/story?sort=new",
    ),
    (
        "generic token parameters remain meaningful outside Reddit",
        "https://example.com/article?token=identity",
        "https://example.com/article?token=identity",
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


def test_a_failed_crawl_still_becomes_a_row_when_the_feed_gave_a_dek() -> None:
    # The point of the whole feed_summary column: an entry drops out of its
    # feed after ~10-20 newer items, so a dek not stored on the first failure
    # is gone for good, long before the retry queue gives up.
    result = CrawlResult(
        url="https://www.nytimes.com/article",
        markdown="",
        success=False,
        error_message="HTTP 403 anti-bot",
        metadata=CrawlMetadata(title="Hugging Face breached by rogue bots"),
        context={FEED_SUMMARY_KEY: SUMMARY, "feed_title": "NYT Technology"},
    )

    article = crawl_result_to_article(result)

    assert article.content == ""
    assert article.feed_summary == SUMMARY
    assert article.content_source == "feed_summary"
    assert article.title == "Hugging Face breached by rogue bots"


def test_a_successful_crawl_records_where_its_content_came_from() -> None:
    crawled = crawl_result_to_article(
        CrawlResult(url="https://example.com/a", markdown="# Body")
    )
    from_feed = crawl_result_to_article(
        CrawlResult(
            url="https://example.com/b",
            markdown="# Body",
            context={FEED_CONTENT_KEY: True},
        )
    )

    assert crawled.content_source == "crawl"
    assert from_feed.content_source == "feed_content"


def test_a_failed_crawl_with_no_dek_is_still_rejected() -> None:
    result = CrawlResult(url="https://example.com/a", markdown="", success=False)

    with pytest.raises(ValueError):
        crawl_result_to_article(result)


def test_an_unusable_url_is_rejected_even_when_a_dek_exists() -> None:
    # Storage identity is the normalized URL; a URL that never resolved has no
    # identity to key a row on, however much text came with it.
    result = CrawlResult(
        url="/relative/path",
        markdown="",
        success=False,
        context={URL_ERROR_CONTEXT_KEY: "relative_url", FEED_SUMMARY_KEY: SUMMARY},
    )

    with pytest.raises(ValueError):
        crawl_result_to_article(result)


def test_a_summary_only_result_never_displaces_real_content_in_a_batch() -> None:
    now = datetime.now(timezone.utc)
    crawled = CrawlResult(
        url="https://example.com/article",
        markdown="The real body.",
        fetched_at=now - timedelta(minutes=1),
    )
    later_failure = CrawlResult(
        url="https://example.com/article",
        markdown="",
        success=False,
        fetched_at=now,
        context={FEED_SUMMARY_KEY: SUMMARY},
    )

    articles, rejected = prepare_articles([crawled, later_failure])

    assert rejected == 0
    assert len(articles) == 1
    assert articles[0].content == "The real body."


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
