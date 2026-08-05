from types import SimpleNamespace

from adapters.retrieve_news_data.crawler import _build_crawl_result


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
    assert result.content_length == len("# Main article")
