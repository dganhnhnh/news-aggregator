import asyncio
from types import SimpleNamespace

from adapters.retrieve_news_data import crawler as crawler_module
from adapters.retrieve_news_data.crawler import Crawl4aiProvider, _build_crawl_result


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
    results = asyncio.run(
        Crawl4aiProvider().crawl_batch(
            ["https://example.com/1", "https://example.com/2"],
            max_concurrency=2,
        )
    )

    assert len(results) == 2
    assert calls == [
        "created",
        "entered",
        (
            "batch",
            ["https://example.com/1", "https://example.com/2"],
            2,
        ),
        "closed",
    ]
