"""Reject crawl results that are anti-bot interstitials rather than articles.

crawl4ai already ships a layered anti-bot detector that runs on the raw HTML of
every response, and it catches most blocks: 403 shells, Cloudflare challenges,
DataDome captchas. What it cannot catch is a challenge a vendor renders as an
ordinary document — HTTP 200, a complete `<body>`, semantic elements, a
paragraph of prose. Imperva's "Human Verification" page is exactly that, and
fourteen of them were stored as articles (and embedded) before this module
existed.

So the check here deliberately runs on the *rendered Markdown*, the text that
would actually be persisted and embedded, and complements rather than replaces
crawl4ai's HTML-level detection.

Detection is two rules, ordered by confidence:

* **Page title** — a page titled "Human Verification" is never an article. This
  is close to zero-risk: publishers do not title articles this way.
* **Interstitial phrase** — the wording of a challenge page, but only when it
  appears near the *start* of a *short* document. Both bounds matter: Ars,
  Slashdot and The Register all publish real articles about Cloudflare and
  captchas, and in those the phrase sits mid-body in a long page.

Like `core.urls` and `core.html_content`, this module is I/O-free so any layer
can import it.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Any
from urllib.parse import urlsplit

from core.urls import URL_ERROR_CONTEXT_KEY


__all__ = [
    "BLOCK_PAGE_CONTEXT_KEY",
    "BLOCK_PAGE_TITLES",
    "block_page_reason",
    "crawl_failure_error_type",
    "historical_summary_title_reason",
    "is_nav_dense",
    "leading_link_count",
]


# CrawlResult.context key holding why a result was judged a block page. Mirrors
# `URL_ERROR_CONTEXT_KEY`: the crawler records the reason, and the persistence
# layer turns it into a typed `crawl_failures.error_type`.
BLOCK_PAGE_CONTEXT_KEY = "block_page_reason"

# Titles that are never an article's title. Stored already normalized by
# `_normalize_title`, so no trailing punctuation and no internal double spaces.
BLOCK_PAGE_TITLES = frozenset(
    {
        "human verification",
        "just a moment",
        "attention required! | cloudflare",
        "access denied",
        "access to this page has been denied",
        "access to this site has been blocked",
        "you have been blocked",
        "security check",
        "bot verification",
        "are you a robot",
        "pardon our interruption",
        "one moment, please",
        "please wait",
        "checking your browser",
        "checking your browser before accessing",
        "verifying you are human",
        "verify you are human",
        "error: the request could not be satisfied",
    }
)

_INTERSTITIAL_PHRASES = re.compile(
    "|".join(
        (
            r"confirm you are human",
            r"verify (?:that )?you are (?:a )?human",
            r"complete the security check",
            r"checking your browser before accessing",
            r"enable javascript and cookies to continue",
            r"you need to solve a puzzle",
            r"additional security check is required",
            r"why have i been blocked",
            r"performance & security by cloudflare",
            r"ddos protection by",
            r"ray id:",
            r"unusual traffic from your",
            r"press & hold to confirm",
            r"your request has been blocked",
        )
    ),
    re.IGNORECASE,
)

# A challenge page says so immediately and has nothing else in it. An article
# that merely discusses one says it somewhere in the middle of a long body.
INTERSTITIAL_HEAD_CHARS = 600
INTERSTITIAL_MAX_CHARS = 2500


def _normalize_title(title: str | None) -> str:
    """Collapse a page title so cosmetic variants compare equal.

    "Just a moment...", "Just a moment…" and "Just a moment" are one page.
    """
    collapsed = re.sub(r"\s+", " ", title or "").strip().lower()
    return collapsed.rstrip(" .…!?")


def block_page_reason(title: str | None, markdown: str) -> str:
    """Why this crawl output is a block page, or "" when it looks like an article."""
    normalized = _normalize_title(title)
    if normalized and normalized in BLOCK_PAGE_TITLES:
        return f"block-page title: {normalized!r}"

    match = _INTERSTITIAL_PHRASES.search(markdown[:INTERSTITIAL_HEAD_CHARS])
    if match and len(markdown) < INTERSTITIAL_MAX_CHARS:
        return (
            f"interstitial phrase {match.group(0).lower()!r} "
            f"in a {len(markdown)}-character page"
        )
    return ""


def historical_summary_title_reason(title: str | None, url: str) -> str:
    """Why a legacy summary-only row inherited an untrusted crawl title.

    Older RSS enrichment kept the title of a failed crawl even though the body
    was correctly discarded in favour of the feed summary. Most such titles
    are ordinary block-page titles. DataDome's NYT shell is the less obvious
    case: its title is only the publisher hostname. This helper is deliberately
    used only by the cleanup command on rows already known to be summary-only;
    a fetched article is never rejected merely because its title resembles its
    host.
    """
    reason = block_page_reason(title, "")
    if reason:
        return reason

    normalized = _normalize_title(title)
    hostname = (urlsplit(url).hostname or "").lower()
    if hostname.startswith("www."):
        hostname = hostname.removeprefix("www.")
    if normalized and normalized == hostname:
        return f"summary-only row inherited publisher-host title: {hostname!r}"
    return ""


# Only the opening window matters. Nothing downstream embeds more than ~1200
# characters, so a nav bar past that point costs nothing, while one before it
# displaces the entire article from the vector.
NAV_WINDOW_CHARS = 1200

# Measured over the stored corpus: article bodies run 0-6 links in the opening
# window, site chrome runs far higher (CNET averages 34, Slashdot 48, Google's
# developer blog 31). Fifteen sits in the empty band between the two.
NAV_DENSE_MIN_LINKS = 15

_MARKDOWN_LINK_RE = re.compile(r"\[[^\]]*\]\(")


def leading_link_count(markdown: str, *, window: int = NAV_WINDOW_CHARS) -> int:
    """How many Markdown links crowd the opening window of a document."""
    return len(_MARKDOWN_LINK_RE.findall(markdown[:window]))


def is_nav_dense(markdown: str, *, window: int = NAV_WINDOW_CHARS) -> bool:
    """Whether a crawl's opening text is site chrome rather than the article.

    This is not a reason to throw the article away — the body is usually still
    down there, past the nav. It is a reason to prefer the feed's own dek when
    picking what to embed, because the embedder never reads far enough to
    reach the body.
    """
    return leading_link_count(markdown, window=window) >= NAV_DENSE_MIN_LINKS


def crawl_failure_error_type(context: Mapping[str, Any]) -> str:
    """The `crawl_failures.error_type` a failed crawl result deserves.

    Lives here rather than in the persistence layer because every caller —
    the RSS adapter, the retry loop and the direct-crawl pipeline — needs the
    same answer, and adapters cannot import services.
    """
    rejection = context.get(URL_ERROR_CONTEXT_KEY)
    if rejection:
        return f"UnusableUrl:{rejection}"
    if context.get(BLOCK_PAGE_CONTEXT_KEY):
        return "BlockedPage"
    return "CrawlError"
