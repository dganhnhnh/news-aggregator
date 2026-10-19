import asyncio
from types import SimpleNamespace

import pytest

from adapters.retrieve_news_data import crawler as crawler_module
from adapters.retrieve_news_data.crawler import (
    DEFAULT_EXCLUDED_TAGS,
    Crawl4aiProvider,
    _build_crawl_result,
)
from core.content_quality import BLOCK_PAGE_CONTEXT_KEY
from core.urls import URL_ERROR_CONTEXT_KEY


def test_build_result_normalizes_crawl4ai_markdown_object() -> None:
    markdown = SimpleNamespace(
        raw_markdown="# Raw article",
        fit_markdown="# Main article",
    )
    inner = SimpleNamespace(
        markdown=markdown,
        cleaned_html="<h1>Main article</h1>",
        metadata={"title": "Main article"},
        error_message="",
    )

    result = _build_crawl_result("https://example.com", inner, success=True)

    assert result.markdown == "# Main article"
    assert type(result.markdown) is str
    assert result.content_length == len("# Main article")


def test_build_result_uses_final_redirect_url() -> None:
    inner = SimpleNamespace(
        url="https://example.com/requested",
        redirected_url="https://example.com/final",
        markdown="# Article",
        cleaned_html="",
        metadata={},
        error_message="",
    )

    result = _build_crawl_result(
        "https://example.com/requested", inner, success=True
    )

    assert result.url == "https://example.com/final"
    assert result.context["requested_url"] == "https://example.com/requested"


def test_build_result_does_not_treat_html_as_markdown() -> None:
    inner = SimpleNamespace(
        url="https://example.com/article",
        redirected_url=None,
        markdown="",
        cleaned_html="<main>HTML only</main>",
        metadata={},
        error_message="",
    )

    result = _build_crawl_result(
        "https://example.com/article", inner, success=True
    )

    assert result.markdown == ""
    assert result.success is False
    assert result.error_message == "Crawler returned empty Markdown content"


def test_build_result_rejects_an_anti_bot_interstitial() -> None:
    inner = SimpleNamespace(
        url="https://www.infoq.com/news/some-article/",
        redirected_url=None,
        markdown=(
            "# Human Verification\n\n"
            "Please complete the security check to access this site.\n"
        ),
        cleaned_html="<main>Human Verification</main>",
        metadata={"title": "Human Verification"},
        error_message="",
    )

    result = _build_crawl_result(
        "https://www.infoq.com/news/some-article/", inner, success=True
    )

    assert result.success is False
    assert result.error_message.startswith("Blocked page: ")
    assert result.context[BLOCK_PAGE_CONTEXT_KEY]


def test_build_result_keeps_a_real_article() -> None:
    inner = SimpleNamespace(
        url="https://example.com/article",
        redirected_url=None,
        markdown="# Nvidia beats estimates\n\nThe chipmaker reported...",
        cleaned_html="<main>...</main>",
        metadata={"title": "Nvidia beats estimates"},
        error_message="",
    )

    result = _build_crawl_result("https://example.com/article", inner, success=True)

    assert result.success is True
    assert BLOCK_PAGE_CONTEXT_KEY not in result.context


def test_batch_uses_one_browser_and_bounded_dispatcher(monkeypatch) -> None:
    calls: list[object] = []

    class FakeCrawler:
        def __init__(self, config) -> None:
            calls.append("created")

        async def __aenter__(self):
            calls.append("entered")
            return self

        async def __aexit__(self, *args):
            calls.append("closed")

        async def arun_many(self, *, urls, config, dispatcher):
            calls.append(("batch", list(urls), dispatcher.semaphore_count))
            return [
                SimpleNamespace(
                    url=url,
                    redirected_url=None,
                    markdown="# Article",
                    cleaned_html="",
                    metadata={},
                    success=True,
                    error_message="",
                )
                for url in urls
            ]

    monkeypatch.setattr(crawler_module, "AsyncWebCrawler", FakeCrawler)
    # Distinct hosts, so per-host throttling leaves this a single wave.
    results = asyncio.run(
        Crawl4aiProvider().crawl_batch(
            ["https://one.example.com/1", "https://two.example.com/2"],
            max_concurrency=2,
        )
    )

    assert len(results) == 2
    assert calls == [
        "created",
        "entered",
        (
            "batch",
            ["https://one.example.com/1", "https://two.example.com/2"],
            2,
        ),
        "closed",
    ]


def _fake_crawler(calls: list[object]):
    """A crawl4ai stand-in that records every URL it is actually handed."""

    class FakeCrawler:
        def __init__(self, config) -> None:
            calls.append("created")

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return None

        def _page(self, url: str):
            return SimpleNamespace(
                url=url,
                redirected_url=None,
                markdown="# Article",
                cleaned_html="",
                metadata={},
                success=True,
                error_message="",
            )

        async def arun(self, *, url, config):
            calls.append(("single", url))
            return self._page(url)

        async def arun_many(self, *, urls, config, dispatcher):
            calls.append(("batch", list(urls)))
            return [self._page(url) for url in urls]

    return FakeCrawler


@pytest.mark.parametrize(
    "url", ["/2020/06/22/post.html", "//host/path", "mailto:editor@example.com", ""]
)
def test_unusable_url_never_reaches_the_browser(monkeypatch, url: str) -> None:
    calls: list[object] = []
    monkeypatch.setattr(crawler_module, "AsyncWebCrawler", _fake_crawler(calls))

    result = asyncio.run(Crawl4aiProvider().crawl(url))

    assert result.success is False
    assert result.context[URL_ERROR_CONTEXT_KEY]
    assert calls == []


def test_batch_results_stay_aligned_when_some_urls_are_rejected(monkeypatch) -> None:
    # Callers zip this list against their own input, so a rejected URL has to
    # occupy its original slot rather than shrink or reorder the results.
    calls: list[object] = []
    monkeypatch.setattr(crawler_module, "AsyncWebCrawler", _fake_crawler(calls))

    urls = [
        "https://one.example.com/first",
        "/relative/second",
        "https://two.example.com/third",
    ]
    results = asyncio.run(
        Crawl4aiProvider(per_host_delay_seconds=0).crawl_batch(urls)
    )

    assert [result.url for result in results] == [
        "https://one.example.com/first",
        "/relative/second",
        "https://two.example.com/third",
    ]
    assert [result.success for result in results] == [True, False, True]
    assert results[1].context[URL_ERROR_CONTEXT_KEY] == "relative"
    assert calls == [
        "created",
        ("batch", ["https://one.example.com/first", "https://two.example.com/third"]),
    ]


def test_batch_of_only_unusable_urls_does_not_open_a_browser(monkeypatch) -> None:
    calls: list[object] = []
    monkeypatch.setattr(crawler_module, "AsyncWebCrawler", _fake_crawler(calls))

    results = asyncio.run(Crawl4aiProvider().crawl_batch(["/a", "/b"]))

    assert [result.success for result in results] == [False, False]
    assert calls == []


def test_missing_container_still_produces_a_result_per_url(monkeypatch) -> None:
    class ShortCrawler:
        def __init__(self, config) -> None:
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return None

        async def arun_many(self, *, urls, config, dispatcher):
            return []

    monkeypatch.setattr(crawler_module, "AsyncWebCrawler", ShortCrawler)

    results = asyncio.run(
        Crawl4aiProvider().crawl_batch(
            ["https://example.com/1", "https://example.com/2"]
        )
    )

    assert len(results) == 2
    assert [result.url for result in results] == [
        "https://example.com/1",
        "https://example.com/2",
    ]
    assert all(result.success is False for result in results)


def test_same_host_urls_are_split_into_waves(monkeypatch) -> None:
    # Ten articles from one feed are usually ten URLs on one host, which is
    # exactly what trips rate limits. Only one may be in flight at a time.
    calls: list[object] = []
    monkeypatch.setattr(crawler_module, "AsyncWebCrawler", _fake_crawler(calls))

    urls = [f"https://blog.example.com/{index}" for index in range(3)]
    results = asyncio.run(
        Crawl4aiProvider(per_host_delay_seconds=0).crawl_batch(urls)
    )

    assert [result.url for result in results] == urls
    assert [call for call in calls if call != "created"] == [
        ("batch", ["https://blog.example.com/0"]),
        ("batch", ["https://blog.example.com/1"]),
        ("batch", ["https://blog.example.com/2"]),
    ]


def test_a_higher_per_host_cap_packs_more_into_each_wave(monkeypatch) -> None:
    calls: list[object] = []
    monkeypatch.setattr(crawler_module, "AsyncWebCrawler", _fake_crawler(calls))

    urls = [f"https://blog.example.com/{index}" for index in range(3)]
    asyncio.run(
        Crawl4aiProvider(
            max_per_host=2, per_host_delay_seconds=0
        ).crawl_batch(urls)
    )

    assert [call for call in calls if call != "created"] == [
        ("batch", ["https://blog.example.com/0", "https://blog.example.com/1"]),
        ("batch", ["https://blog.example.com/2"]),
    ]


def test_distinct_hosts_still_run_in_one_wave(monkeypatch) -> None:
    # Per-host throttling must not serialize a batch of unrelated publishers.
    calls: list[object] = []
    monkeypatch.setattr(crawler_module, "AsyncWebCrawler", _fake_crawler(calls))

    urls = [f"https://host{index}.example.com/a" for index in range(5)]
    asyncio.run(Crawl4aiProvider(per_host_delay_seconds=0).crawl_batch(urls))

    assert [call for call in calls if call != "created"] == [("batch", urls)]


def test_the_delay_falls_between_waves_of_the_same_host(monkeypatch) -> None:
    calls: list[object] = []
    monkeypatch.setattr(crawler_module, "AsyncWebCrawler", _fake_crawler(calls))
    slept: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        slept.append(seconds)

    monkeypatch.setattr(crawler_module.asyncio, "sleep", fake_sleep)

    asyncio.run(
        Crawl4aiProvider(per_host_delay_seconds=2.5).crawl_batch(
            [
                "https://blog.example.com/a",
                "https://blog.example.com/b",
                "https://other.example.com/c",
            ]
        )
    )

    # Two waves means exactly one gap, not one sleep per URL.
    assert slept == [2.5]


def test_waves_keep_results_aligned_with_the_input(monkeypatch) -> None:
    calls: list[object] = []
    monkeypatch.setattr(crawler_module, "AsyncWebCrawler", _fake_crawler(calls))

    urls = [
        "https://blog.example.com/a",
        "https://other.example.com/b",
        "https://blog.example.com/c",
        "/relative",
    ]
    results = asyncio.run(
        Crawl4aiProvider(per_host_delay_seconds=0).crawl_batch(urls)
    )

    assert [result.url for result in results] == urls
    assert [result.success for result in results] == [True, True, True, False]


def test_crawl4ai_provider_default_excluded_tags() -> None:
    provider = Crawl4aiProvider()
    assert DEFAULT_EXCLUDED_TAGS == ("nav", "header", "footer", "aside", "form")
    assert provider._run_config.excluded_tags == ["nav", "header", "footer", "aside", "form"]
    assert provider._run_config.only_text is True


def test_http_error_body_cannot_be_a_successful_article():
    inner = SimpleNamespace(markdown="# Not found", metadata={}, status_code=404)
    result = _build_crawl_result("https://example.com/gone", inner, success=True)
    from core.content_quality import crawl_failure_error_type
    assert not result.success
    assert crawl_failure_error_type(result.context) == "HttpError:404"

