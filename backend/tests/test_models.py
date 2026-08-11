from datetime import datetime, timedelta, timezone

from models.search_and_crawl_models import CrawlResult, SearchResult


def test_search_result_recency() -> None:
    recent = SearchResult(
        id="1",
        url="https://example.com/news",
        title="News",
        content_snippet="content",
        published_date=datetime.now(timezone.utc) - timedelta(minutes=5),
    )
    assert recent.is_recent


def test_crawl_result_has_isolated_context() -> None:
    first = CrawlResult(url="https://example.com/1", markdown="one")
    second = CrawlResult(url="https://example.com/2", markdown="two")

    first.context["feed"] = "gold"

    assert second.context == {}
    assert first.content_length == 3
