"""Convert feed-supplied HTML into the same Markdown shape a crawl produces.

Many feeds ship the whole article in `content:encoded` (RSS) or `<content>`
(Atom). Rendering that is free, cannot be blocked by an anti-bot check, and
costs no browser page load — so when a feed hands us the article, crawling the
page again is pure waste.

`html2text` is the same library crawl4ai renders its Markdown with, so articles
taken from a feed and articles taken from a crawl stay comparable rather than
splitting into two dialects of Markdown.

Like `core.urls`, this module is I/O-free so any layer can import it.
"""

from __future__ import annotations

import html2text


# Below this, feed content is a teaser ("Read more…") rather than the article.
# Crawling still gives a better result in that case, so the threshold decides
# whether the feed body is worth trusting at all.
MIN_FULL_CONTENT_CHARS = 1200

# CrawlResult.context key marking a result rendered from the feed body rather
# than fetched. Used for run statistics and for debugging a source's content.
FEED_CONTENT_KEY = "from_feed_content"


def _converter() -> html2text.HTML2Text:
    # A fresh converter per call: HTML2Text keeps mutable per-document state,
    # so a shared instance would not be safe across concurrent entries.
    converter = html2text.HTML2Text()
    converter.body_width = 0          # never hard-wrap; wrapping breaks diffing
    converter.ignore_images = True    # image markup is noise in stored articles
    converter.ignore_emphasis = False
    converter.ignore_links = False
    converter.protect_links = True
    converter.unicode_snob = True     # keep real characters, not entities
    converter.single_line_break = True
    return converter


def html_to_markdown(html: str) -> str:
    """Render article HTML as Markdown, or "" when nothing renders."""
    if not html or not html.strip():
        return ""
    try:
        rendered = _converter().handle(html)
    except Exception:
        # A feed body is untrusted input; malformed markup must not take a run
        # down, and the caller can still fall back to crawling the page.
        return ""
    return rendered.strip()


def is_full_article(markdown: str, *, min_chars: int = MIN_FULL_CONTENT_CHARS) -> bool:
    """Whether rendered feed content is substantial enough to store as-is."""
    return len(markdown) >= min_chars
