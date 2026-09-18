"""Transform crawler output into compact raw-article records."""

from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import re
from urllib.parse import urlsplit

# Canonicalization lives in core.urls with the rest of the URL rules; it stays
# importable from here because callers across the codebase already do so.
from core.html_content import FEED_CONTENT_KEY, FEED_SUMMARY_KEY
from core.urls import URL_ERROR_CONTEXT_KEY, normalize_url, source_slug
from models.search_and_crawl_models import CrawlResult
from models.storage_models import ContentSource, HighlightSegment, RawArticle


__all__ = [
    "HEADLINE_START_SEL",
    "HEADLINE_STOP_SEL",
    "build_excerpt",
    "content_hash",
    "crawl_result_to_article",
    "normalize_content",
    "normalize_url",
    "parse_headline",
    "prepare_articles",
    "source_slug",
]

# Control characters, not markdown or plausible article text, so they can
# never collide with a match. Postgres `ts_headline()` wraps every matched
# term in these; `parse_headline` below turns that back into segments.
HEADLINE_START_SEL = "\x01"
HEADLINE_STOP_SEL = "\x02"
_HEADLINE_SEL_RE = re.compile(f"[{HEADLINE_START_SEL}{HEADLINE_STOP_SEL}]")


def build_excerpt(raw: str, limit: int = 280) -> str:
    """Clean a leading slice of markdown content into a plain-text preview."""
    text = re.sub(r"[#>*_`\[\]]+", " ", raw)
    text = re.sub(r"\s+", " ", text).strip()
    if len(text) <= limit:
        return text
    truncated = text[:limit].rsplit(" ", 1)[0]
    return truncated + "…"


def parse_headline(raw: str) -> tuple[HighlightSegment, ...]:
    """Clean a `ts_headline()` result into matched/unmatched segments.

    Splitting on `HEADLINE_START_SEL`/`HEADLINE_STOP_SEL` alternates
    unmatched, matched, unmatched, ... since Postgres always emits a
    balanced start/stop pair around each match.
    """
    text = re.sub(r"[#>*_`\[\]]+", " ", raw)
    text = re.sub(r"\s+", " ", text).strip()
    return tuple(
        HighlightSegment(text=part, highlight=bool(i % 2))
        for i, part in enumerate(_HEADLINE_SEL_RE.split(text))
        if part
    )


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


def _content_source(result: CrawlResult, content: str) -> ContentSource:
    if not content:
        return "feed_summary"
    return "feed_content" if result.context.get(FEED_CONTENT_KEY) else "crawl"


def crawl_result_to_article(result: CrawlResult) -> RawArticle:
    """Build the row to persist, or raise ValueError when there is nothing to.

    A failed crawl is no longer automatically nothing. When the feed supplied a
    dek, the article still deserves a row — with empty `content` — because that
    dek is searchable, embeddable, and gone forever once the entry drops out of
    the feed. The row does not resolve the crawl failure, so the retry queue
    keeps its chance to upgrade it to a real body later.
    """
    if result.context.get(URL_ERROR_CONTEXT_KEY):
        # The URL never resolved to something fetchable, so there is no stable
        # identity to key a row on. Storage identity is the normalized URL.
        raise ValueError("Cannot persist a result for an unusable URL")
    content = normalize_content(result.markdown) if result.success else ""
    feed_summary = normalize_content(result.context.get(FEED_SUMMARY_KEY, "") or "")
    if not content and not feed_summary:
        raise ValueError("Cannot persist a result with neither content nor summary")
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
        feed_summary=feed_summary,
        content_source=_content_source(result, content),
        published_at=_published_at(result),
        crawled_at=result.fetched_at,
    )


def _supersedes(candidate: RawArticle, current: RawArticle) -> bool:
    """Whether `candidate` should replace `current` for the same URL.

    Real content always beats a summary-only row regardless of age, so a failed
    retry landing in the same batch as a successful crawl cannot blank it out.
    Between two rows of the same kind, the newer one wins.
    """
    if bool(candidate.content) != bool(current.content):
        return bool(candidate.content)
    return candidate.crawled_at > current.crawled_at


def prepare_articles(results: list[CrawlResult]) -> tuple[list[RawArticle], int]:
    """Convert valid results and keep the best result for each normalized URL."""
    by_url: dict[str, RawArticle] = {}
    rejected = 0
    for result in results:
        try:
            article = crawl_result_to_article(result)
        except ValueError:
            rejected += 1
            continue
        current = by_url.get(article.url)
        if current is None or _supersedes(article, current):
            by_url[article.url] = article
    return list(by_url.values()), rejected
