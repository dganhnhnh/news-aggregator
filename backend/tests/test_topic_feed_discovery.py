from __future__ import annotations

import asyncio
from datetime import datetime, timezone

import httpx

from adapters.retrieve_news_data.topic_feed_discovery import TopicFeedDiscovery
from models.discovery_models import (
    FeedDiscoveryCandidate,
    FeedDiscoveryResult,
    FeedDiscoveryStats,
)
from models.search_and_crawl_models import SearchResult


def _result(url: str, title: str, snippet: str, score: float = 0.8) -> SearchResult:
    return SearchResult(
        id=url,
        url=url,
        title=title,
        content_snippet=snippet,
        score=score,
        source="tavily",
    )


def _feed(title: str, *, link: str, category: str = "") -> str:
    tag = f"<category>{category}</category>" if category else ""
    return f"""<rss version="2.0"><channel><title>{title}</title>
      <link>{link}</link>{tag}
      <item><title>{title} latest analysis</title><link>{link}story</link>
      <pubDate>Mon, 14 Sep 2026 10:00:00 GMT</pubDate></item>
    </channel></rss>"""


class FakeSearchProvider:
    def __init__(
        self,
        *,
        direct: list[SearchResult] | None = None,
        publishers: list[SearchResult] | None = None,
    ) -> None:
        self.direct = direct or []
        self.publishers = publishers or []
        self.calls: list[dict] = []

    async def search(self, query, max_results=10, **options):
        self.calls.append(
            {"query": query, "max_results": max_results, **options}
        )
        if query.startswith("RSS and Atom"):
            return self.direct
        return self.publishers


class FakeGoogleFallback:
    def __init__(self) -> None:
        self.calls = 0

    async def discover(self, query, *, language, country, limit):
        self.calls += 1
        return FeedDiscoveryResult(
            query=query,
            candidates=(
                FeedDiscoveryCandidate(
                    feed_url="https://fallback.com/feed.xml",
                    source_label="Fallback Finance",
                    website_url="https://fallback.com/",
                    feed_scope="sitewide",
                    discovery_source="google_fallback",
                    relevance_reasons=("Google News publisher evidence",),
                    sample_titles=("Finance fallback",),
                    last_published_at=datetime(2026, 9, 14, tzinfo=timezone.utc),
                ),
            ),
            stats=FeedDiscoveryStats(
                search_results=30,
                publishers_inspected=12,
                pages_inspected=1,
                feeds_validated=1,
                google_fallback_used=True,
            ),
        )


def _run(discovery: TopicFeedDiscovery, query: str = "finance", limit: int = 12):
    return asyncio.run(discovery.discover(query, limit=limit))


def test_direct_feed_search_uses_two_basic_tavily_intents() -> None:
    provider = FakeSearchProvider(
        direct=[
            _result(
                "https://finance.example.com/rss.xml",
                "Finance Daily RSS",
                "A finance news feed",
            )
        ]
    )

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            request=request,
            text=_feed("Finance Daily", link="https://finance.example.com/"),
            headers={"content-type": "application/rss+xml"},
        )

    async def execute():
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            return await TopicFeedDiscovery(
                provider, http_client=client, google_fallback=None
            ).discover("finance")

    result = asyncio.run(execute())

    assert len(provider.calls) == 2
    assert all(call["search_depth"] == "basic" for call in provider.calls)
    assert all(call["topic"] == "general" for call in provider.calls)
    assert result.candidates[0].discovery_source == "tavily_direct"
    assert result.candidates[0].feed_url == "https://finance.example.com/rss.xml"
    assert result.stats.search_results == 1


def test_directory_page_returns_relevant_category_feed_only() -> None:
    provider = FakeSearchProvider(
        direct=[
            _result(
                "https://publisher.com/rss-directory",
                "Publisher finance RSS feeds",
                "Choose a finance or market feed",
            )
        ]
    )

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/rss-directory":
            return httpx.Response(
                200,
                request=request,
                text=(
                    '<a href="/category/finance/feed/">Finance RSS</a>'
                    '<a href="/category/sports/feed/">Sports RSS</a>'
                ),
                headers={"content-type": "text/html"},
            )
        title = "Finance Markets" if "finance" in request.url.path else "Sports"
        return httpx.Response(
            200,
            request=request,
            text=_feed(title, link="https://publisher.com/", category=title),
            headers={"content-type": "application/rss+xml"},
        )

    async def execute():
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            return await TopicFeedDiscovery(
                provider, http_client=client, google_fallback=None
            ).discover("finance")

    result = asyncio.run(execute())

    assert [item.feed_url for item in result.candidates] == [
        "https://publisher.com/category/finance/feed/"
    ]
    assert result.candidates[0].feed_scope == "category"


def test_specialist_publisher_main_feed_is_kept_but_general_publisher_is_not() -> None:
    provider = FakeSearchProvider(
        publishers=[
            _result(
                "https://specialist.com/",
                "Specialist Finance Review",
                "A publication focused on finance and markets",
                0.9,
            ),
            _result(
                "https://general.com/",
                "General Daily",
                "Latest headlines from around the world",
                0.7,
            ),
        ]
    )

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/":
            title = "Finance Review" if request.url.host == "specialist.com" else "General Daily"
            return httpx.Response(
                200,
                request=request,
                text=(
                    f"<title>{title}</title>"
                    '<link rel="alternate" type="application/rss+xml" href="/feed.xml">'
                ),
                headers={"content-type": "text/html"},
            )
        title = "Finance Review" if request.url.host == "specialist.com" else "General Daily"
        return httpx.Response(
            200,
            request=request,
            text=_feed(title, link=f"https://{request.url.host}/"),
            headers={"content-type": "application/rss+xml"},
        )

    async def execute():
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            return await TopicFeedDiscovery(
                provider, http_client=client, google_fallback=None
            ).discover("finance")

    result = asyncio.run(execute())

    assert [item.website_url for item in result.candidates] == [
        "https://specialist.com/"
    ]
    assert result.candidates[0].feed_scope == "sitewide"
    assert result.candidates[0].discovery_source == "tavily_publisher"


def test_google_fallback_augments_small_feed_first_result() -> None:
    provider = FakeSearchProvider(
        direct=[
            _result(
                "https://finance.example.com/rss.xml",
                "Finance Daily RSS",
                "Finance feed",
            )
        ]
    )
    fallback = FakeGoogleFallback()

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            request=request,
            text=_feed("Finance Daily", link="https://finance.example.com/"),
        )

    async def execute():
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            return await TopicFeedDiscovery(
                provider, http_client=client, google_fallback=fallback
            ).discover("finance")

    result = asyncio.run(execute())

    assert fallback.calls == 1
    assert [item.discovery_source for item in result.candidates] == [
        "tavily_direct",
        "google_fallback",
    ]
    assert result.stats.google_fallback_used is True


def test_google_fallback_is_not_used_after_six_candidates() -> None:
    provider = FakeSearchProvider(
        direct=[
            _result(
                f"https://finance{i}.com/rss.xml",
                f"Finance {i} RSS",
                "Finance feed",
                0.9 - i / 100,
            )
            for i in range(6)
        ]
    )
    fallback = FakeGoogleFallback()

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            request=request,
            text=_feed("Finance", link=f"https://{request.url.host}/"),
        )

    async def execute():
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            return await TopicFeedDiscovery(
                provider, http_client=client, google_fallback=fallback
            ).discover("finance")

    result = asyncio.run(execute())

    assert len(result.candidates) == 6
    assert fallback.calls == 0
    assert result.stats.google_fallback_used is False


def test_results_are_limited_to_two_feeds_per_publisher() -> None:
    provider = FakeSearchProvider(
        direct=[
            _result(
                "https://publisher.com/rss-directory",
                "Finance topic feeds",
                "Finance category feeds",
            )
        ]
    )

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/rss-directory":
            return httpx.Response(
                200,
                request=request,
                text="".join(
                    f'<a href="/category/finance-{index}/feed/">Finance {index} RSS</a>'
                    for index in range(3)
                ),
            )
        index = request.url.path.split("finance-", 1)[1].split("/", 1)[0]
        return httpx.Response(
            200,
            request=request,
            text=_feed(
                f"Finance {index}",
                link=f"https://publisher.com/category/finance-{index}/",
            ),
        )

    async def execute():
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            return await TopicFeedDiscovery(
                provider, http_client=client, google_fallback=None
            ).discover("finance")

    result = asyncio.run(execute())

    assert len(result.candidates) == 2
    assert all(item.feed_scope == "category" for item in result.candidates)


def test_tavily_failure_returns_labeled_google_fallback() -> None:
    class FailingProvider:
        async def search(self, query, max_results=10, **options):
            raise RuntimeError("quota unavailable")

    fallback = FakeGoogleFallback()
    result = _run(
        TopicFeedDiscovery(FailingProvider(), google_fallback=fallback),
        query="finance",
    )

    assert fallback.calls == 1
    assert result.candidates[0].discovery_source == "google_fallback"
    assert result.stats.google_fallback_used is True
    assert any("quota unavailable" in warning for warning in result.warnings)


def test_redirect_to_private_network_is_rejected() -> None:
    provider = FakeSearchProvider(
        direct=[_result("https://safe.example/feed", "Finance RSS", "Finance feed")]
    )
    requested: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requested.append(str(request.url))
        return httpx.Response(
            302,
            request=request,
            headers={"location": "http://127.0.0.1:8000/private"},
        )

    async def execute():
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            return await TopicFeedDiscovery(
                provider, http_client=client, google_fallback=None
            ).discover("finance")

    result = asyncio.run(execute())

    assert result.candidates == ()
    assert requested == ["https://safe.example/feed"]


def test_feed_older_than_one_year_is_not_returned() -> None:
    provider = FakeSearchProvider(
        direct=[_result("https://old.example/rss.xml", "Finance RSS", "Finance feed")]
    )
    old_feed = """<rss version="2.0"><channel><title>Finance Archive</title>
      <item><title>Old finance</title><link>https://old.example/story</link>
      <pubDate>Mon, 01 Jan 2024 10:00:00 GMT</pubDate></item>
    </channel></rss>"""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, request=request, text=old_feed)

    async def execute():
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            return await TopicFeedDiscovery(
                provider, http_client=client, google_fallback=None
            ).discover("finance")

    assert asyncio.run(execute()).candidates == ()
