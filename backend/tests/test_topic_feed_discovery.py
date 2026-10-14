from __future__ import annotations

import asyncio
from datetime import datetime, timezone

import httpx
import pytest

from adapters.retrieve_news_data.topic_feed_discovery import TopicFeedDiscovery
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
        if query.startswith("RSS and Atom") or query.endswith(" RSS feed"):
            return self.direct
        return self.publishers


class FakeGoogleFallback:
    def __init__(self) -> None:
        self.calls = 0

    async def search_publishers(self, query, *, language, country):
        self.calls += 1
        return [_result("https://fallback.com/feed.xml", "Fallback Finance", "")]



def _run(discovery: TopicFeedDiscovery, query: str = "finance", limit: int = 12):
    return asyncio.run(discovery.discover(query, limit=limit))


def test_direct_feed_search_uses_four_bounded_basic_queries() -> None:
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

    assert len(provider.calls) == 4
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
            text=_feed("Finance Daily", link=f"https://{request.url.host}/"),
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
    async def execute():
        async with httpx.AsyncClient(transport=httpx.MockTransport(
            lambda request: httpx.Response(200, text=_feed("Finance", link="https://fallback.com/"))
        )) as client:
            return await TopicFeedDiscovery(
                FailingProvider(), google_fallback=fallback, http_client=client
            ).discover("finance")
    result = asyncio.run(execute())

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


@pytest.mark.parametrize(("query", "text", "expected"), [
    ("artificial intelligence", "AI research", True),
    ("AI news", "Machine learning research", True),
    ("artificial intelligence", "Military intelligence", False),
    ("artificial intelligence", "Artificial grass", False),
    ("AI", "https://gardening.ai/feed", False),
    ("cybersecurity", "Cybersecurity report https://[redacted]/payload", True),
    ("finance", "Monetary policy and stock markets", True),
    ("finance", "Supermarket opening hours", False),
    ("climate technology", "Climate and weather", False),
    ("climate technology", "New technology for climate science", True),
])
def test_topic_matching_requires_concept_or_all_query_words(query, text, expected):
    assert TopicFeedDiscovery._has_query_overlap(query, text) is expected


def _discover_fixture(pages, *, query="finance", seeds=None, **options):
    requested = []
    def handler(request):
        url = str(request.url)
        requested.append(url)
        return httpx.Response(200 if url in pages else 404, text=pages.get(url, ""))
    async def execute():
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            provider = FakeSearchProvider(direct=[
                _result(url, "Finance and artificial intelligence", "Topic-focused publisher")
                for url in seeds or [next(iter(pages))]
            ])
            return await TopicFeedDiscovery(provider, http_client=client, **options).discover(query)
    return asyncio.run(execute()), requested


def test_directory_does_not_cap_distinct_publishers_at_three():
    pages = {"https://directory.example/list": "".join(
        f'<a href="https://publisher{i}.com/feed">Finance RSS</a>' for i in range(7)
    )}
    pages.update({f"https://publisher{i}.com/feed": _feed("Finance", link=f"https://publisher{i}.com/") for i in range(7)})
    result, _ = _discover_fixture(pages)
    assert len(result.candidates) == 7
    assert result.stats.feeds_attempted == result.stats.feeds_validated == 7


def test_fragment_links_are_not_fetched_and_direct_feed_is_not_fetched_twice():
    pages = {
        "https://directory.example/rss": '<a href="#finance">RSS feed</a><a href="https://directory.example/rss#topic">RSS feed</a><a href="https://publisher.com/feed#one">Finance RSS</a>',
        "https://publisher.com/feed": _feed("Finance", link="https://publisher.com/"),
    }
    result, requested = _discover_fixture(pages)
    assert len(result.candidates) == 1
    assert requested == list(pages)
    _, requested = _discover_fixture({"https://publisher.com/feed": pages["https://publisher.com/feed"]})
    assert requested == ["https://publisher.com/feed"]


def test_validation_is_shared_between_pages_and_attempts_are_bounded():
    pages = {f"https://directory{i}.com/list": "".join(
        f'<a href="https://publisher.com/feed{j}.xml">Finance RSS</a>' for j in range(20)
    ) for i in range(2)}
    seeds = list(pages)
    pages.update({f"https://publisher.com/feed{j}.xml": _feed(f"Finance {j}", link="https://publisher.com/") for j in range(20)})
    result, requested = _discover_fixture(pages, seeds=seeds, max_feed_attempts_per_page=4)
    assert len(requested) == 6
    assert result.stats.feeds_attempted == result.stats.feeds_validated == 4


def _articles_feed(title, titles, *, category="", enclosure=""):
    return f'<rss version="2.0"><channel><title>{title}</title><link>https://publisher.com/</link>' + "".join(
        f'<item><title>{text}</title><description>Analysis</description><category>{category if i == 0 else ""}</category>{enclosure}</item>'
        for i, text in enumerate(titles)
    ) + '</channel></rss>'


def test_recent_article_majority_can_establish_relevance():
    result, _ = _discover_fixture({"https://publisher.com/feed": _articles_feed(
        "Research Weekly", ["AI progress", "Machine learning study", "LLM evaluation", "Local news", "Sports"]
    )}, query="artificial intelligence")
    assert len(result.candidates) == 1
    assert any("3/5" in reason for reason in result.candidates[0].relevance_reasons)


def test_one_topical_article_or_search_snippet_does_not_make_specialist_feed():
    result, _ = _discover_fixture({"https://publisher.com/feed": _articles_feed(
        "General Weekly", ["AI progress", "Local news", "Sports", "Arts", "Travel"], category="Artificial Intelligence"
    )}, query="artificial intelligence")
    assert not result.candidates
    assert result.stats.feeds_validated == 1
    assert result.stats.feeds_relevant == 0


@pytest.mark.parametrize("title,titles,enclosure", [
    ("Comments on: Finance", ["Finance discussion"] * 4, ""),
    ("Finance", ["Finance weekly"] * 4, '<enclosure url="https://publisher.com/a.mp3" type="audio/mpeg"/>'),
    ("Finance links", ["Finance RSS feeds to follow"] * 4, ""),
])
def test_non_article_feeds_are_rejected(title, titles, enclosure):
    result, _ = _discover_fixture({"https://publisher.com/feed": _articles_feed(title, titles, enclosure=enclosure)})
    assert not result.candidates


def test_google_fallback_must_pass_topic_evidence():
    result, _ = _discover_fixture({"https://fallback.com/feed.xml": _feed("Gardening", link="https://fallback.com/")}, google_fallback=FakeGoogleFallback())
    assert result.stats.google_fallback_used
    assert not result.candidates


def test_global_budget_counts_failed_probes_across_directory_pages():
    seeds = [f"https://directory{i}.com/list" for i in range(3)]
    pages = {url: "".join(
        f'<a href="https://publisher{i}-{j}.com/feed.xml">Finance RSS</a>'
        for j in range(24)
    ) for i, url in enumerate(seeds)}
    result, requested = _discover_fixture(pages, seeds=seeds)
    probes = [url for url in requested if url not in seeds]
    assert result.stats.feeds_attempted == len(probes) == len(set(probes)) == 50
    assert result.stats.feeds_validated == 0
    assert any("limit of 50" in warning for warning in result.warnings)


@pytest.mark.parametrize("path", ["feed.xml", "latest"])
def test_direct_feed_seeds_cannot_bypass_budget_even_without_feed_url_hint(path):
    pages = {f"https://publisher{i}.com/{path}": _articles_feed(
        "Finance", ["Finance analysis"] * 3
    ) for i in range(10)}
    result, requested = _discover_fixture(
        pages, seeds=list(pages), max_feed_urls_checked=5
    )
    assert result.stats.feeds_attempted == result.stats.feeds_validated == 5
    assert len(requested) == 5


def test_budget_retains_results_and_still_applies_result_limit():
    seeds = [f"https://directory{i}.com/list" for i in range(3)]
    pages = {url: "".join(
        f'<a href="https://publisher{i}-{j}.com/feed.xml">Finance RSS</a>'
        for j in range(24)
    ) for i, url in enumerate(seeds)}
    pages.update({f"https://publisher{i}-{j}.com/feed.xml": _articles_feed(
        f"Finance {i}-{j}", ["Finance analysis"] * 3
    ).replace("https://publisher.com/", f"https://publisher{i}-{j}.com/")
        for i in range(3) for j in range(24)})
    result, _ = _discover_fixture(pages, seeds=seeds)
    assert result.stats.feeds_attempted == result.stats.feeds_validated == 50
    assert result.stats.feeds_relevant == 50
    assert len(result.candidates) == 25


def test_budget_is_shared_with_real_google_fallback_and_includes_its_probes():
    from adapters.retrieve_news_data.google_news_feed_discovery import GoogleNewsFeedDiscovery

    requested = []
    def handler(request):
        url = str(request.url)
        requested.append(url)
        if request.url.host == "news.google.com":
            return httpx.Response(200, text='''<rss version="2.0"><channel><title>Google News</title>
                <item><title>Finance news</title><link>https://fallback.com/story</link>
                <source url="https://fallback.com/">Finance</source></item>
                </channel></rss>''')
        if url == "https://directory.com/list":
            return httpx.Response(200, text="".join(
                f'<a href="https://broken.com/feed{i}.xml">Finance RSS</a>' for i in range(4)
            ))
        if url == "https://fallback.com/":
            return httpx.Response(200, text="".join(
                f'<link rel="alternate" type="application/rss+xml" title="Finance" href="/{name}.xml">'
                for name in ("one", "two")
            ))
        if request.url.path in {"/one.xml", "/two.xml"}:
            return httpx.Response(200, text=_articles_feed(
                str(request.url), ["Finance analysis"] * 3
            ).replace("https://publisher.com/", "https://fallback.com/"))
        return httpx.Response(404)

    async def execute():
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            return await TopicFeedDiscovery(
                FakeSearchProvider(direct=[_result("https://directory.com/list", "Finance", "")]),
                http_client=client, google_fallback=GoogleNewsFeedDiscovery(client),
                max_feed_urls_checked=6,
            ).discover("finance")
    result = asyncio.run(execute())
    probes = [url for url in requested if url.startswith("https://broken.com/")
              or url.endswith(("/one.xml", "/two.xml"))]
    assert result.stats.google_fallback_used
    assert result.stats.feeds_attempted == len(probes) == 6
    assert result.stats.feeds_validated == 2
    assert len(result.candidates) == 2
    assert all(item.discovery_source == "google_fallback" for item in result.candidates)
    assert len(requested) == len(set(requested))


def test_google_fallback_is_skipped_when_budget_is_exhausted():
    fallback = FakeGoogleFallback()
    pages = {"https://directory.com/list": "".join(
        f'<a href="https://broken.com/feed{i}.xml">Finance RSS</a>' for i in range(5)
    )}
    result, _ = _discover_fixture(
        pages, google_fallback=fallback, max_feed_urls_checked=4
    )
    assert result.stats.feeds_attempted == 4
    assert not result.stats.google_fallback_used
    assert fallback.calls == 0


def test_google_fallback_reuses_primary_feed_validation():
    fallback = FakeGoogleFallback()
    result, requested = _discover_fixture({
        "https://fallback.com/feed.xml": _articles_feed("Gardening", ["Garden plants"] * 3)
    }, google_fallback=fallback)
    assert result.stats.google_fallback_used
    assert result.stats.feeds_attempted == result.stats.feeds_validated == 1
    assert requested == ["https://fallback.com/feed.xml"]
    assert not result.candidates


@pytest.mark.parametrize("path,title,category", [
    ("/feed", "Artificial Intelligence Weekly", ""),
    ("/topic/AI/feed", "General Weekly", ""),
    ("/feed", "General Weekly", "Artificial Intelligence"),
])
def test_metadata_path_and_entry_tags_cannot_override_unrelated_articles(path, title, category):
    feed = _articles_feed(title, ["Sports", "Travel", "Cooking", "Gardening", "Local news"])
    if category:
        feed = feed.replace("<category></category>", f"<category>{category}</category>")
    result, _ = _discover_fixture({"https://publisher.com" + path: feed}, query="AI")
    assert not result.candidates
    assert result.stats.feeds_validated == 1


def test_sparse_sample_requires_both_metadata_and_article_support():
    result, _ = _discover_fixture({"https://publisher.com/feed": _articles_feed(
        "Artificial Intelligence Weekly", ["Sports results"]
    )}, query="AI")
    assert not result.candidates
    result, _ = _discover_fixture({"https://publisher.com/feed": _articles_feed(
        "Artificial Intelligence Weekly", ["Neural network research"]
    )}, query="AI")
    assert len(result.candidates) == 1
    assert any("less certain" in reason for reason in result.candidates[0].relevance_reasons)


def test_ai_domain_terminology_can_establish_article_coverage():
    result, _ = _discover_fixture({"https://publisher.com/feed": _articles_feed(
        "Research Weekly", ["Transformer architecture evaluation", "Text generation methods",
                            "Retrieval augmented generation benchmarks", "Diffusion model research",
                            "GPU inference optimization"]
    )}, query="artificial intelligence")
    assert len(result.candidates) == 1
    assert any("5/5" in reason for reason in result.candidates[0].relevance_reasons)


def test_article_coverage_ranks_before_feed_scope():
    strong = "https://specialist.com/feed"
    weak = "https://general.com/category/AI/feed"
    pages = {
        strong: _articles_feed("Research Weekly", ["AI research"] * 5).replace(
            "https://publisher.com/", "https://specialist.com/"),
        weak: _articles_feed("AI Category", ["AI research"] * 3 + ["Sports", "Travel"]).replace(
            "https://publisher.com/", "https://general.com/"),
    }
    result, _ = _discover_fixture(pages, seeds=list(pages), query="AI")
    assert [item.feed_url for item in result.candidates] == [strong, weak]


def test_hosted_publications_have_independent_publisher_quotas():
    pages = {f"https://{name}.substack.com/feed": _articles_feed(
        f"Finance {name}", ["Finance analysis"] * 3
    ).replace("https://publisher.com/", f"https://{name}.substack.com/")
        for name in ("alpha", "beta", "gamma")}
    result, _ = _discover_fixture(pages, seeds=list(pages))
    assert len(result.candidates) == 3


def test_feed_budget_rotates_publishers_instead_of_exhausting_first_directory():
    links = [f"https://a.com/feed{i}.xml" for i in range(10)] + [
        "https://b.com/feed.xml", "https://c.com/feed.xml"
    ]
    pages = {"https://directory.com/list": "".join(
        f'<a href="{url}">Finance RSS</a>' for url in links
    )}
    _, requested = _discover_fixture(pages, max_feed_urls_checked=3)
    assert {httpx.URL(url).host for url in requested[1:]} == {"a.com", "b.com", "c.com"}


def test_declared_topical_feed_is_checked_before_guessed_cms_paths():
    pages = {
        "https://publisher.com/topic/AI/": '<link rel="alternate" type="application/rss+xml" title="AI" href="/updates">',
        "https://publisher.com/updates": _articles_feed("AI", ["AI research"] * 3),
    }
    result, requested = _discover_fixture(pages, query="AI", max_feed_urls_checked=1)
    assert len(result.candidates) == 1
    assert requested == list(pages)


def test_redirect_target_reuses_parsed_feed_and_budget_slot():
    requested = []
    def handler(request):
        requested.append(str(request.url))
        if request.url.path == "/old.xml":
            return httpx.Response(301, headers={"location": "/feed.xml"})
        return httpx.Response(200, text=_articles_feed("Finance", ["Finance analysis"] * 3))
    async def execute():
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            discovery = TopicFeedDiscovery(
                FakeSearchProvider(direct=[_result("https://publisher.com/old.xml", "Finance", "")]),
                http_client=client,
            )
            from adapters.retrieve_news_data.topic_feed_discovery import _DiscoverySession
            session = _DiscoverySession(discovery, client, 1)
            try:
                first = await session.read("https://publisher.com/old.xml", as_feed=True)
                second = await session.read("https://publisher.com/feed.xml", as_feed=True)
                assert first is second
                assert len(session.attempts) == session.validated_count == 1
            finally:
                await session.close()
    asyncio.run(execute())
    assert requested == ["https://publisher.com/old.xml", "https://publisher.com/feed.xml"]


def test_budget_and_cache_are_isolated_between_concurrent_requests():
    requested = []
    async def handler(request):
        requested.append(str(request.url))
        await asyncio.sleep(0)
        return httpx.Response(200, text=_articles_feed("Finance", ["Finance analysis"] * 3))
    async def execute():
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            discovery = TopicFeedDiscovery(FakeSearchProvider(direct=[
                _result(f"https://publisher{i}.com/feed", "Finance", "") for i in range(4)
            ]), http_client=client, max_feed_urls_checked=2)
            return await asyncio.gather(discovery.discover("finance"), discovery.discover("finance"))
    results = asyncio.run(execute())
    assert [result.stats.feeds_attempted for result in results] == [2, 2]
    assert len(requested) == 4


def test_cancelling_discovery_cleans_up_page_and_feed_tasks():
    async def execute():
        started = asyncio.Event()
        active = 0
        async def handler(request):
            nonlocal active
            active += 1
            started.set()
            try:
                await asyncio.Event().wait()
            finally:
                active -= 1
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            discovery = TopicFeedDiscovery(FakeSearchProvider(direct=[
                _result("https://publisher.com/feed", "Finance", "")
            ]), http_client=client)
            task = asyncio.create_task(discovery.discover("finance"))
            await asyncio.wait_for(started.wait(), timeout=1)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            assert active == 0
    asyncio.run(execute())


def test_concurrent_slow_probes_reserve_budget_before_http_and_bound_concurrency():
    requested = []
    active = 0
    peak_active = 0
    seeds = [f"https://directory{i}.com/list" for i in range(4)]
    async def handler(request):
        nonlocal active, peak_active
        url = str(request.url)
        requested.append(url)
        active += 1
        peak_active = max(peak_active, active)
        try:
            await asyncio.sleep(0.001)
            if url in seeds:
                index = seeds.index(url)
                return httpx.Response(200, text="".join(
                    f'<a href="https://publisher{index}-{j}.com/feed.xml">Finance RSS</a>'
                    for j in range(24)
                ))
            return httpx.Response(404)
        finally:
            active -= 1
    async def execute():
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            return await TopicFeedDiscovery(FakeSearchProvider(direct=[
                _result(url, "Finance", "") for url in seeds
            ]), http_client=client).discover("finance")
    result = asyncio.run(execute())
    assert result.stats.feeds_attempted == len(requested) - len(seeds) == 50
    assert peak_active == 4
    assert active == 0


def test_feed_index_html_does_not_consume_feed_budget_or_get_fetched_twice():
    pages = {
        "https://publisher.com/": '<a href="/rss">RSS directory</a>',
        "https://publisher.com/rss": '<a href="/feed.xml">Finance RSS</a>',
        "https://publisher.com/feed.xml": _articles_feed("Finance", ["Finance analysis"] * 3),
    }
    result, requested = _discover_fixture(pages, max_feed_urls_checked=1)
    assert result.stats.feeds_attempted == result.stats.feeds_validated == 1
    assert result.stats.pages_inspected == 2
    assert len(result.candidates) == 1
    assert requested == list(pages)


def test_case_sensitive_feed_paths_remain_distinct_and_labels_keep_topic_evidence():
    pages = {
        "https://directory.com/list": '<a href="https://a.com/Feed.xml">RSS</a>'
        '<a href="https://a.com/Feed.xml">Finance RSS</a>'
        '<a href="https://b.com/feed.xml">RSS</a>'
        '<a href="https://a.com/feed.xml">Finance RSS</a>',
        "https://a.com/Feed.xml": _articles_feed("Finance A", ["Finance analysis"] * 3),
        "https://a.com/feed.xml": _articles_feed("Finance B", ["Finance analysis"] * 3),
    }
    result, requested = _discover_fixture(pages, max_feed_urls_checked=2)
    assert set(requested[1:]) == {"https://a.com/Feed.xml", "https://a.com/feed.xml"}
    assert result.stats.feeds_validated == 2


@pytest.mark.parametrize("limit", [0, 51])
def test_feed_url_budget_cannot_disable_or_exceed_hard_limit(limit):
    with pytest.raises(ValueError, match="between 1 and 50"):
        TopicFeedDiscovery(FakeSearchProvider(), max_feed_urls_checked=limit)
