"""Transform crawler output into compact raw-article records."""

from __future__ import annotations

from datetime import datetime, timezone
import hashlib
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from models.search_and_crawl_models import CrawlResult
from models.storage_models import RawArticle


_TRACKING_PARAMETERS = {"fbclid", "gclid"}
_SOURCE_HOST_PREFIXES = ("www.", "m.", "amp.")


def normalize_url(url: str) -> str:
    """Conservatively canonicalize a fetched URL for deduplication."""
    parts = urlsplit(url.strip())
    scheme = parts.scheme.lower()
    hostname = (parts.hostname or "").lower()
    if not scheme or not hostname:
        raise ValueError(f"Invalid article URL: {url!r}")

    port = parts.port
    if port is not None and not (
        (scheme == "http" and port == 80) or (scheme == "https" and port == 443)
    ):
        hostname = f"{hostname}:{port}"
    if parts.username or parts.password:
        credentials = parts.username or ""
        if parts.password:
            credentials += f":{parts.password}"
        hostname = f"{credentials}@{hostname}"

    query_items = [
        (key, value)
        for key, value in parse_qsl(parts.query, keep_blank_values=True)
        if key.lower() not in _TRACKING_PARAMETERS
        and not key.lower().startswith("utm_")
    ]
    return urlunsplit((scheme, hostname, parts.path or "/", urlencode(query_items), ""))


def source_slug(normalized_url: str) -> str:
    """Return the stable publisher hostname used to identify an article source."""
    hostname = (urlsplit(normalized_url).hostname or "").lower()
    if not hostname:
        raise ValueError(f"Invalid article URL: {normalized_url!r}")
    for prefix in _SOURCE_HOST_PREFIXES:
        if hostname.startswith(prefix):
            return hostname.removeprefix(prefix)
    return hostname


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
    url = normalize_url(result.url)
    return RawArticle(
        url=url,
        title=_optional_text(result.metadata.title),
        source=article_source(result, url),
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
