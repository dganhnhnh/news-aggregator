"""Transform crawler output into compact raw-article records."""

from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import re
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from models.search_and_crawl_models import CrawlResult
from models.storage_models import RawArticle


_TRACKING_PARAMETERS = {
    "fbclid",
    "gclid",
    "mc_cid",
    "mc_eid",
    "igshid",
    "ref",
    "ref_src",
    "_hsenc",
}
_SOURCE_HOST_PREFIXES = ("www.", "m.", "amp.")
# Only m./amp. are collapsed at the URL level; www. is left alone since it is
# not reliably an alias of the bare host the way AMP/mobile mirrors are.
_AMBIGUOUS_HOST_PREFIXES = ("m.", "amp.")
_AMP_SUFFIX_RE = re.compile(r"(?:/amp/?|\.amp)$", re.IGNORECASE)


def _strip_trailing_slash(path: str) -> str:
    if path == "/" or not path.endswith("/"):
        return path
    return path.rstrip("/") or "/"


def build_excerpt(raw: str, limit: int = 280) -> str:
    """Clean a leading slice of markdown content into a plain-text preview."""
    text = re.sub(r"[#>*_`\[\]]+", " ", raw)
    text = re.sub(r"\s+", " ", text).strip()
    if len(text) <= limit:
        return text
    truncated = text[:limit].rsplit(" ", 1)[0]
    return truncated + "…"


def normalize_url(url: str) -> str:
    """Conservatively canonicalize a fetched URL for deduplication."""
    parts = urlsplit(url.strip())
    scheme = parts.scheme.lower()
    hostname = (parts.hostname or "").lower()
    if not scheme or not hostname:
        raise ValueError(f"Invalid article URL: {url!r}")

    for prefix in _AMBIGUOUS_HOST_PREFIXES:
        if hostname.startswith(prefix):
            hostname = hostname[len(prefix):]
            break

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

    path = _AMP_SUFFIX_RE.sub("", parts.path or "/") or "/"
    path = _strip_trailing_slash(path)

    query_items = sorted(
        (key, value)
        for key, value in parse_qsl(parts.query, keep_blank_values=True)
        if key.lower() not in _TRACKING_PARAMETERS
        and not key.lower().startswith("utm_")
    )
    return urlunsplit((scheme, hostname, path, urlencode(query_items), ""))


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
