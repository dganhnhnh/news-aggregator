from __future__ import annotations

import pytest

from core.feed_urls import infer_feed_source_label, validate_feed_url


@pytest.mark.parametrize(
    ("url", "expected"),
    [
        ("https://feeds.bbc.co.uk/news/rss.xml", "bbc"),
        ("https://rss.nytimes.com/services/xml/rss/nyt/HomePage.xml", "nytimes"),
        ("http://localhost:8080/feed", "localhost"),
        ("http://127.0.0.1/feed", "127.0.0.1"),
    ],
)
def test_infer_feed_source_label(url: str, expected: str) -> None:
    assert infer_feed_source_label(url) == expected


def test_validate_feed_url_trims_absolute_http_url() -> None:
    assert validate_feed_url("  https://example.com/feed.xml  ") == (
        "https://example.com/feed.xml"
    )


@pytest.mark.parametrize("url", ["", "/feed.xml", "ftp://example.com/feed.xml"])
def test_validate_feed_url_rejects_uncrawlable_url(url: str) -> None:
    with pytest.raises(ValueError):
        validate_feed_url(url)
