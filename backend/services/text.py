"""Build the text handed to the embedding model.

MiniLM truncates around 256 tokens (~1000-1200 characters), so embedding the
full Markdown article is pointless past that point -- only the leading
paragraphs matter.

That truncation is also why picking *which* text to embed is a real decision
rather than an obvious one. A page that crawls into a navigation bar puts the
nav, not the article, inside those 1200 characters, while the feed's own dek is
short, clean and written by a human. `article_text` is the single place that
decision is made, so embed and cluster cannot answer it differently.
"""

from __future__ import annotations

import re

from core.content_quality import is_nav_dense
from core.html_content import MIN_FEED_SUMMARY_CHARS
from models.storage_models import ContentSource, TextKind


_CODE_FENCE_RE = re.compile(r"```.*?```", re.DOTALL)
_LINK_ONLY_LINE_RE = re.compile(r"^\s*!?\[[^\]]*\]\([^)]*\)\s*$")


def first_paragraphs(content: str, max_chars: int = 1200) -> str:
    """Leading paragraphs of Markdown, stripped of code fences and bare links."""
    stripped = _CODE_FENCE_RE.sub(" ", content)
    lines = [
        line
        for line in stripped.splitlines()
        if line.strip() and not _LINK_ONLY_LINE_RE.match(line.strip())
    ]
    text = "\n".join(lines).strip()
    if len(text) <= max_chars:
        return text
    return text[:max_chars].rsplit(" ", 1)[0]


def article_text(
    *,
    content: str,
    feed_summary: str,
    content_source: ContentSource,
) -> tuple[str, TextKind]:
    """Pick the body text to represent an article, and say which one it is.

    The order is by observed cleanliness, not by how much work produced the
    text:

    1. **Feed-supplied body.** Rendered from the publisher's own article HTML,
       so it has no site chrome by construction and no anti-bot layer can
       corrupt it.
    2. **Crawled body, if it opens with the article.** Subject to crawl4ai's
       boilerplate stripping, which fails on roughly a third of sources.
    3. **Feed dek.** Short, but short and on-topic beats 1200 characters of
       navigation menu.
    4. **Crawled body anyway.** Nav-dense and no dek to fall back on; the
       article is in there somewhere, and it still beats nothing.

    The caller adds the title, which is why there is no title rung here: a
    title-only article is `("", "title")`.
    """
    body = first_paragraphs(content) if content else ""
    summary = feed_summary.strip()

    if body and content_source == "feed_content":
        return body, "content"
    if body and not is_nav_dense(content):
        return body, "content"
    if len(summary) >= MIN_FEED_SUMMARY_CHARS:
        return first_paragraphs(summary), "feed_summary"
    if body:
        return body, "content"
    return "", "title"


def article_embed_text(
    *,
    title: str | None,
    content: str,
    feed_summary: str,
    content_source: ContentSource,
) -> tuple[str, TextKind]:
    """The exact string to embed, plus which text it was built from."""
    body, kind = article_text(
        content=content,
        feed_summary=feed_summary,
        content_source=content_source,
    )
    clean_title = (title or "").strip()
    if clean_title and body:
        return f"{clean_title}\n\n{body}", kind
    return (clean_title or body), kind
