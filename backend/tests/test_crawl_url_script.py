import importlib.util
from pathlib import Path
from types import SimpleNamespace


SCRIPT_PATH = Path(__file__).parents[2] / "scripts" / "crawl_url.py"
SPEC = importlib.util.spec_from_file_location("crawl_url_script", SCRIPT_PATH)
assert SPEC is not None and SPEC.loader is not None
crawl_url_script = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(crawl_url_script)

FORMAT_FIELDS = crawl_url_script.FORMAT_FIELDS
build_parser = crawl_url_script.build_parser
result_payload = crawl_url_script.result_payload


def test_parser_defaults_to_markdown() -> None:
    args = build_parser().parse_args(["https://example.com/article"])

    assert args.formats == ["markdown"]


def test_parser_accepts_multiple_formats() -> None:
    args = build_parser().parse_args(
        ["https://example.com/article", "-f", "cleaned_html", "metadata"]
    )

    assert args.formats == ["cleaned_html", "metadata"]


def test_result_payload_selects_and_serializes_markdown_fields() -> None:
    markdown = SimpleNamespace(
        raw_markdown="# Raw",
        fit_markdown="# Fit",
        markdown_with_citations="# Cited",
        references_markdown="[1]: https://example.com",
        fit_html="<main>Fit</main>",
        __str__=lambda self: "# Raw",
    )
    result = SimpleNamespace(
        url="https://example.com/article",
        redirected_url=None,
        success=True,
        status_code=200,
        error_message="",
        markdown=markdown,
        cleaned_html="<main>Raw</main>",
        metadata={"title": "Example"},
    )

    payload = result_payload(
        "https://example.com/article",
        result,
        ["fit_markdown", "cleaned_html", "metadata"],
    )

    assert payload["fit_markdown"] == "# Fit"
    assert payload["cleaned_html"] == "<main>Raw</main>"
    assert payload["metadata"] == {"title": "Example"}
    assert "html" not in payload


def test_all_format_names_are_unique() -> None:
    assert len(FORMAT_FIELDS) == len(set(FORMAT_FIELDS))
