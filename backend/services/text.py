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

from core.boilerplate import strip_boilerplate
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
    boilerplate: frozenset[str] = frozenset(),
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
    4. **Title only.** A nav-dense crawl with no usable dek contains the article
       somewhere, but not inside the model's 1200-character horizon. Returning
       that chrome would make unrelated articles from one publisher look alike.

    The caller adds the title itself; this function represents that final rung
    as `("", "title")` so no publisher chrome leaks into the text.

    `boilerplate` is the calling source's chrome profile (see
    `core/boilerplate.py`). It is applied before the ranking, not after, because
    removing a consent panel is what decides whether rung 2 holds an article or
    a cookie policy. The profile is applied to feed-supplied bodies too: rung 1
    trusts publisher HTML, and a line the profile names is one the publisher
    repeats on every article either way.
    """
    content = strip_boilerplate(content, boilerplate)
    body = first_paragraphs(content) if content else ""
    summary = feed_summary.strip()

    if body and content_source == "feed_content":
        return body, "content"
    if body and not is_nav_dense(content):
        return body, "content"
    if len(summary) >= MIN_FEED_SUMMARY_CHARS:
        return first_paragraphs(summary), "feed_summary"
    return "", "title"


def article_embed_text(
    *,
    title: str | None,
    content: str,
    feed_summary: str,
    content_source: ContentSource,
    boilerplate: frozenset[str] = frozenset(),
) -> tuple[str, TextKind]:
    """The exact string to embed, plus which text it was built from."""
    body, kind = article_text(
        content=content,
        feed_summary=feed_summary,
        content_source=content_source,
        boilerplate=boilerplate,
    )
    clean_title = (title or "").strip()
    if clean_title and body:
        return f"{clean_title}\n\n{body}", kind
    return (clean_title or body), kind
