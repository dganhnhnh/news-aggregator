#!/usr/bin/env python3
"""Crawl one URL with crawl4AI and print selected fields as pretty JSON.

Examples:
    uv run python scripts/crawl_url.py https://example.com/article
    uv run python scripts/crawl_url.py URL -f cleaned_html
    uv run python scripts/crawl_url.py URL -f markdown cleaned_html metadata
    uv run python scripts/crawl_url.py URL -f all > crawl.json

The crawler configuration intentionally matches the production adapter: text
content is preferred and common boilerplate elements are excluded.
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import json
import sys
from collections.abc import Mapping
from enum import Enum
from pathlib import Path
from typing import Any


sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "backend"))

from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig  # noqa: E402

from adapters.retrieve_news_data.crawler import (  # noqa: E402
    DEFAULT_EXCLUDED_TAGS,
    _unwrap_result,
)
from core.urls import resolve_crawl_url  # noqa: E402


FORMAT_FIELDS = (
    "markdown",
    "raw_markdown",
    "fit_markdown",
    "markdown_with_citations",
    "references_markdown",
    "html",
    "cleaned_html",
    "fit_html",
    "extracted_content",
    "metadata",
    "links",
    "media",
    "tables",
    "response_headers",
    "downloaded_files",
    "js_execution_result",
    "screenshot",
    "pdf",
    "mhtml",
    "ssl_certificate",
    "crawl_stats",
)


def _jsonable(value: Any) -> Any:
    """Convert crawl4AI/Pydantic values into objects accepted by json.dumps."""
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, bytes):
        return base64.b64encode(value).decode("ascii")
    if isinstance(value, Enum):
        return _jsonable(value.value)
    if hasattr(value, "model_dump"):
        return _jsonable(value.model_dump(mode="json"))
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_jsonable(item) for item in value]
    return str(value)


def _markdown_field(result: Any, field: str) -> Any:
    markdown = getattr(result, "markdown", None)
    if field == "markdown":
        return None if markdown is None else str(markdown)
    value = getattr(markdown, field, None)
    if value is None and field == "fit_html":
        value = getattr(result, "fit_html", None)
    return value


def result_payload(requested_url: str, result: Any, fields: list[str]) -> dict[str, Any]:
    """Build the stable JSON envelope plus the requested crawl4AI fields."""
    payload: dict[str, Any] = {
        "requested_url": requested_url,
        "url": getattr(result, "url", None),
        "redirected_url": getattr(result, "redirected_url", None),
        "success": bool(getattr(result, "success", False)),
        "status_code": getattr(result, "status_code", None),
        "error_message": getattr(result, "error_message", "") or "",
    }
    markdown_fields = {
        "markdown",
        "raw_markdown",
        "fit_markdown",
        "markdown_with_citations",
        "references_markdown",
        "fit_html",
    }
    for field in fields:
        value = (
            _markdown_field(result, field)
            if field in markdown_fields
            else getattr(result, field, None)
        )
        payload[field] = _jsonable(value)
    return payload


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Crawl one web URL with crawl4AI and emit pretty JSON.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Formats:\n  "
            + ", ".join(FORMAT_FIELDS)
            + "\n  all (all formats above)"
        ),
    )
    parser.add_argument("url", help="Absolute http(s) URL to crawl")
    parser.add_argument(
        "-f",
        "--format",
        dest="formats",
        nargs="+",
        choices=(*FORMAT_FIELDS, "all"),
        default=["markdown"],
        help="One or more result fields (default: markdown)",
    )
    parser.add_argument(
        "--css-selector",
        help="Restrict extraction to elements matching this CSS selector",
    )
    parser.add_argument(
        "--timeout-ms",
        type=int,
        default=60_000,
        help="Page timeout in milliseconds (default: 60000)",
    )
    parser.add_argument(
        "--headed",
        action="store_true",
        help="Show the Chromium window instead of running headless",
    )
    parser.add_argument(
        "--keep-boilerplate",
        action="store_true",
        help="Do not exclude nav/header/footer/aside/form elements",
    )
    parser.add_argument(
        "--indent",
        type=int,
        default=2,
        help="JSON indentation (default: 2)",
    )
    return parser


async def crawl(args: argparse.Namespace) -> dict[str, Any]:
    requested_url = resolve_crawl_url(args.url)
    fields = list(FORMAT_FIELDS) if "all" in args.formats else list(dict.fromkeys(args.formats))
    run_config = CrawlerRunConfig(
        only_text=True,
        excluded_tags=[] if args.keep_boilerplate else list(DEFAULT_EXCLUDED_TAGS),
        css_selector=args.css_selector,
        page_timeout=args.timeout_ms,
        screenshot="screenshot" in fields,
        pdf="pdf" in fields,
        capture_mhtml="mhtml" in fields,
        fetch_ssl_certificate="ssl_certificate" in fields,
        verbose=False,
    )
    browser_config = BrowserConfig(headless=not args.headed, verbose=False)

    async with AsyncWebCrawler(config=browser_config) as crawler:
        container = await crawler.arun(url=requested_url, config=run_config)
    result, _ = _unwrap_result(container)
    return result_payload(requested_url, result, fields)


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    if args.timeout_ms <= 0:
        parser.error("--timeout-ms must be greater than zero")
    if args.indent < 0:
        parser.error("--indent must be zero or greater")

    try:
        payload = asyncio.run(crawl(args))
    except (KeyboardInterrupt, asyncio.CancelledError):
        return 130
    except Exception as exc:
        payload = {
            "requested_url": args.url,
            "success": False,
            "error_message": str(exc),
        }

    print(json.dumps(payload, ensure_ascii=False, indent=args.indent))
    return 0 if payload.get("success") else 1


if __name__ == "__main__":
    raise SystemExit(main())
