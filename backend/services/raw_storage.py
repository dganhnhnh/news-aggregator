"""Transform crawler output into compact raw-article records."""

from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import re
from urllib.parse import urlsplit

# Canonicalization lives in core.urls with the rest of the URL rules; it stays
# importable from here because callers across the codebase already do so.
from core.urls import normalize_url, source_slug
from models.search_and_crawl_models import CrawlResult
from models.storage_models import RawArticle


__all__ = [
    "build_excerpt",
    "content_hash",
    "normalize_content",
    "normalize_url",
    "prepare_articles",
    "source_slug",
    "to_raw_article",
]


def build_excerpt(raw: str, limit: int = 280) -> str:
    """Clean a leading slice of markdown content into a plain-text preview."""
    text = re.sub(r"[#>*_`\[\]]+", " ", raw)
    text = re.sub(r"\s+", " ", text).strip()
    if len(text) <= limit:
        return text
    truncated = text[:limit].rsplit(" ", 1)[0]
    return truncated + "…"


def normalize_content(content: str) -> str:
    """Normalize Markdown without changing its meaningful structure."""
    return content.replace("\r\n", "\n").replace("\r", "\n").strip()


def content_hash(content: str) -> bytes:
    return hashlib.sha256(content.encode("utf-8")).digest()


def _optional_text(value) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip()
    return normalized or None


def _published_at(result: CrawlResult) -> datetime | None:
    value = result.context.get("published_date")
    if not isinstance(value, datetime):
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value


def article_source(result: CrawlResult, normalized_url: str) -> str:
    return (
        _optional_text(result.context.get("feed_title"))
        or _optional_text(result.context.get("query_label"))
        or (urlsplit(normalized_url).hostname or "")
    )


def crawl_result_to_article(result: CrawlResult) -> RawArticle:
    if not result.success:
        raise ValueError("Cannot persist an unsuccessful crawl result")
    content = normalize_content(result.markdown)
    if not content:
        raise ValueError("Cannot persist an empty crawl result")
    url_original = result.url.strip()
    url = normalize_url(url_original)
    return RawArticle(
        url=url,
        url_original=url_original,
        title=_optional_text(result.metadata.title),
        source=article_source(result, url),
        lang=_optional_text(result.metadata.language),
        content=content,
        content_hash=content_hash(content),
        published_at=_published_at(result),
        crawled_at=result.fetched_at,
    )


def prepare_articles(results: list[CrawlResult]) -> tuple[list[RawArticle], int]:
    """Convert valid results and keep the newest result for each normalized URL."""
    by_url: dict[str, RawArticle] = {}
    rejected = 0
    for result in results:
        try:
            article = crawl_result_to_article(result)
        except ValueError:
            rejected += 1
            continue
        current = by_url.get(article.url)
        if current is None or article.crawled_at > current.crawled_at:
            by_url[article.url] = article
    return list(by_url.values()), rejected
