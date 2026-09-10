"""URL policy shared by every crawl entry point.

This module is deliberately I/O-free and depends only on the standard library,
so adapters, services, pipelines and workers can all import it without pulling
in a browser, an HTTP client or a database driver.

Two distinct concerns live here, and they are kept separate on purpose:

* **Structural validity** (`resolve_crawl_url`) — is this string something a
  crawler could fetch at all? A relative href is not, regardless of who
  supplied it, so every path into the crawler enforces this.
* **Article policy** (`is_probably_article`) — is this link worth crawling as a
  news article? Only link *discovery* applies this. A caller that explicitly
  asks for a PDF gets a PDF.

`normalize_url`/`source_slug` (canonicalization for deduplication) also live
here so that all URL rules have one home; `services.raw_storage` re-exports
them for its existing callers.
"""

from __future__ import annotations

import re
from urllib.parse import parse_qsl, urlencode, urljoin, urlsplit, urlunsplit


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

_CRAWLABLE_SCHEMES = ("http", "https")

# CrawlResult.context key marking a result the crawler rejected before
# fetching. Callers persist such a failure as permanent, since no retry can
# turn a malformed URL into a page.
URL_ERROR_CONTEXT_KEY = "url_error_reason"

# A base that turns a protocol-relative href into an https one. Protocol-
# relative links are unambiguous about the host and only omit the scheme, so a
# caller with no base document (the CLI) can safely default them to https.
SCHEME_RELATIVE_BASE = "https://"

# Links pointing at a media file are not articles: podcast feeds put the
# episode audio in the enclosure (and sometimes in <link> itself), and crawling
# it just burns a browser page load and lands in the failure queue.
_NON_ARTICLE_SUFFIXES = (
    ".mp3", ".m4a", ".m4v", ".aac", ".ogg", ".oga", ".opus", ".wav", ".flac",
    ".mp4", ".mov", ".avi", ".mkv", ".webm",
    ".jpg", ".jpeg", ".png", ".gif", ".webp", ".svg",
    ".pdf", ".epub", ".zip", ".gz", ".tar", ".dmg", ".exe",
)
_NON_ARTICLE_TYPE_PREFIXES = ("audio/", "video/", "image/")
_NON_ARTICLE_TYPES = ("application/pdf", "application/zip")


class UnusableUrl(ValueError):
    """A URL no crawler could fetch, carrying a stable machine-readable reason.

    `reason` is one of ``empty``, ``relative`` or ``unsupported_scheme``. It is
    stable because callers persist it as an error type and dead-letter on it:
    re-attempting a structurally invalid URL can never produce a new outcome.
    """

    def __init__(self, url: str, reason: str, detail: str) -> None:
        super().__init__(detail)
        self.url = url
        self.reason = reason


def resolve_crawl_url(href: str, *, base_url: str = "") -> str:
    """Return `href` as an absolute http(s) URL, or raise `UnusableUrl`.

    `base_url` is the document the href was found in — a feed's final URL after
    redirects, for example. Pass `SCHEME_RELATIVE_BASE` when there is no such
    document but protocol-relative input should still be accepted.
    """
    candidate = (href or "").strip()
    if not candidate:
        raise UnusableUrl(href, "empty", "No URL was supplied")

    absolute = urljoin(base_url, candidate) if base_url else candidate
    parts = urlsplit(absolute)
    # Scheme first: `mailto:` and `javascript:` parse with a scheme but no
    # host, and reporting those as merely "relative" would be misleading.
    if parts.scheme and parts.scheme.lower() not in _CRAWLABLE_SCHEMES:
        raise UnusableUrl(
            href,
            "unsupported_scheme",
            f"URL scheme {parts.scheme!r} is not crawlable: {href!r}",
        )
    if not parts.scheme or not parts.hostname:
        raise UnusableUrl(
            href,
            "relative",
            f"URL is not absolute and no base URL resolved it: {href!r}",
        )
    return absolute


def is_probably_article(href: str, link_type: str = "") -> bool:
    """Whether a discovered link plausibly points at an HTML article.

    `link_type` is the feed-declared media type when one is available; the file
    extension is the fallback for feeds that declare nothing.
    """
    if link_type:
        normalized_type = link_type.split(";", 1)[0].strip().lower()
        if normalized_type.startswith(_NON_ARTICLE_TYPE_PREFIXES):
            return False
        if normalized_type in _NON_ARTICLE_TYPES:
            return False
    return not urlsplit(href).path.lower().endswith(_NON_ARTICLE_SUFFIXES)


def _strip_trailing_slash(path: str) -> str:
    if path == "/" or not path.endswith("/"):
        return path
    return path.rstrip("/") or "/"


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

    is_reddit = hostname == "reddit.com" or hostname.endswith(".reddit.com")
    # A Reddit post's stable identity is its path. Anti-bot, share and display
    # flows append query parameters whose names and values can change over time;
    # allowlisting/denylisting individual names would therefore recreate the
    # same duplicate whenever Reddit introduces a new parameter. Keep this rule
    # host-scoped because query strings can be article identity on other sites.
    query_items = (
        []
        if is_reddit
        else sorted(
            (key, value)
            for key, value in parse_qsl(parts.query, keep_blank_values=True)
            if key.lower() not in _TRACKING_PARAMETERS
            and not key.lower().startswith("utm_")
        )
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
