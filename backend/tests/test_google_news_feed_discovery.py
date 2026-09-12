from __future__ import annotations

import asyncio
from urllib.parse import parse_qs

import httpx
import pytest

from adapters.retrieve_news_data.google_news_feed_discovery import (
    FeedDiscoveryError,
    GoogleNewsFeedDiscovery,
)


def _response(request: httpx.Request, body: str, content_type: str) -> httpx.Response:
    return httpx.Response(
        200,
        request=request,
        text=body,
        headers={"content-type": content_type},
    )


def test_search_feed_url_encodes_query_and_locale() -> None:
    url = GoogleNewsFeedDiscovery.build_search_feed_url(
        "generative AI & media", language="vi", country="vn"
    )
    parsed = httpx.URL(url)

    assert parsed.host == "news.google.com"
    assert parsed.path == "/rss/search"
    assert parse_qs(parsed.query.decode()) == {
        "q": ["generative AI & media"],
        "hl": ["vi-VN"],
        "gl": ["VN"],
        "ceid": ["VN:vi"],
    }


def test_discover_finds_validated_publisher_feed_without_tsv() -> None:
    requested: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requested.append(str(request.url))
        if request.url.host == "news.google.com":
            return _response(
                request,
                """<rss version="2.0"><channel><title>Google News</title>
                <item><title>First AI story</title>
                  <link>https://publisher.example/articles/1</link>
                  <source url="https://publisher.example/">Publisher</source>
                </item>
                <item><title>Second AI story</title>
                  <link>https://publisher.example/articles/2</link>
                  <source url="https://publisher.example/">Publisher</source>
                </item>
                </channel></rss>""",
                "application/rss+xml",
            )
        if request.url.path == "/":
            return _response(
                request,
                '<html><head><link rel="alternate" type="application/rss+xml" '
                'href="/news.xml"></head></html>',
                "text/html",
            )
        if request.url.path.startswith("/articles/"):
            return _response(request, "<html><head></head></html>", "text/html")
        if request.url.path == "/news.xml":
            return _response(
                request,
                """<rss version="2.0"><channel><title>Publisher News</title>
                <item><title>Story</title>
                <link>https://publisher.example/story</link></item>
                </channel></rss>""",
                "application/rss+xml",
            )
        return httpx.Response(404, request=request)

    async def run():
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(handler), follow_redirects=True
        ) as client:
            return await GoogleNewsFeedDiscovery(
                http_client=client,
                max_publishers=3,
                max_feeds_per_publisher=1,
            ).discover("artificial intelligence")

    result = asyncio.run(run())

    assert result.query == "artificial intelligence"
    assert result.articles_considered == 2
    assert result.publishers_considered == 1
    assert len(result.candidates) == 1
    candidate = result.candidates[0]
    assert candidate.feed_url == "https://publisher.example/news.xml"
    assert candidate.source_label == "Publisher News"
    assert candidate.website_url == "https://publisher.example/"
    assert candidate.matched_articles == 2
    assert candidate.sample_titles == ("First AI story", "Second AI story")
    assert not any(path.endswith(".tsv") for path in requested)


def test_discover_decodes_article_when_google_omits_source_url(monkeypatch) -> None:
    async def fake_decode(_url: str) -> str:
        return "https://decoded.example/posts/story"

    monkeypatch.setattr(
        "adapters.retrieve_news_data.google_news_feed_discovery.decode_google_news_url",
        fake_decode,
    )

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.host == "news.google.com":
            return _response(
                request,
                """<rss version="2.0"><channel><title>Google News</title>
                <item><title>Decoded story</title>
                  <link>https://news.google.com/rss/articles/token</link>
                  <source>Decoded Publisher</source>
                </item></channel></rss>""",
                "application/rss+xml",
            )
        if request.url.path == "/":
            return _response(
                request,
                '<link rel="alternate" type="application/atom+xml" href="/atom.xml">',
                "text/html",
            )
        if request.url.path == "/posts/story":
            return _response(request, "<html></html>", "text/html")
        if request.url.path == "/atom.xml":
            return _response(
                request,
                """<feed xmlns="http://www.w3.org/2005/Atom">
                <title>Decoded Feed</title><entry><title>Story</title>
                <id>one</id></entry></feed>""",
                "application/atom+xml",
            )
        return httpx.Response(404, request=request)

    async def run():
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(handler), follow_redirects=True
        ) as client:
            return await GoogleNewsFeedDiscovery(
                http_client=client, max_feeds_per_publisher=1
            ).discover("decoded")

    result = asyncio.run(run())
    assert result.candidates[0].feed_url == "https://decoded.example/atom.xml"
    assert result.candidates[0].matched_articles == 1


def test_discover_uses_bounded_conventional_path_fallback() -> None:
    requested_paths: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.host == "news.google.com":
            return _response(
                request,
                """<rss version="2.0"><channel><title>Google News</title>
                <item><title>Fallback story</title>
                  <link>https://fallback.example/story</link>
                  <source url="https://fallback.example/blog">Fallback</source>
                </item></channel></rss>""",
                "application/rss+xml",
            )
        requested_paths.append(request.url.path)
        if request.url.path in {"/blog", "/story"}:
            return _response(request, "<html></html>", "text/html")
        if request.url.path == "/feed":
            return _response(
                request,
                """<rss version="2.0"><channel><title>Fallback Feed</title>
                <item><title>Story</title><link>https://fallback.example/story</link>
                </item></channel></rss>""",
                "application/rss+xml",
            )
        return httpx.Response(404, request=request)

    async def run():
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(handler), follow_redirects=True
        ) as client:
            return await GoogleNewsFeedDiscovery(
                http_client=client, max_feeds_per_publisher=1
            ).discover("fallback")

    result = asyncio.run(run())
    assert result.candidates[0].feed_url == "https://fallback.example/feed"
    assert requested_paths == ["/blog", "/story", "/feed"]


def test_discover_skips_publisher_serving_empty_bodies() -> None:
    """An empty ``200`` is a non-feed, not a crash.

    ``feedparser`` omits ``version`` entirely for a body it cannot sniff, so
    attribute access on it used to raise ``AttributeError`` and fail the whole
    discovery request.
    """

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.host == "news.google.com":
            return _response(
                request,
                """<rss version="2.0"><channel><title>Google News</title>
                <item><title>Empty story</title>
                  <link>https://empty.example/story</link>
                  <source url="https://empty.example/">Empty</source>
                </item></channel></rss>""",
                "application/rss+xml",
            )
        if request.url.path == "/rss.xml":
            return _response(
                request,
                """<rss version="2.0"><channel><title>Empty Feed</title>
                <item><title>Story</title><link>https://empty.example/story</link>
                </item></channel></rss>""",
                "application/rss+xml",
            )
        return _response(request, "", "text/html")

    async def run():
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(handler), follow_redirects=True
        ) as client:
            return await GoogleNewsFeedDiscovery(
                http_client=client, max_feeds_per_publisher=1
            ).discover("empty")

    result = asyncio.run(run())
    assert [item.feed_url for item in result.candidates] == [
        "https://empty.example/rss.xml"
    ]


def test_discover_reports_google_news_fetch_failure() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(503, request=request)

    async def run():
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(handler), follow_redirects=True
        ) as client:
            await GoogleNewsFeedDiscovery(http_client=client).discover("outage")

    with pytest.raises(FeedDiscoveryError):
        asyncio.run(run())


@pytest.mark.parametrize("query", ["", "   "])
def test_blank_query_is_rejected(query: str) -> None:
    with pytest.raises(ValueError, match="query"):
        GoogleNewsFeedDiscovery.build_search_feed_url(query)
