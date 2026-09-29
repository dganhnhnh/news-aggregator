"""I/O-free policy for removing a publisher's own chrome from crawled Markdown.

crawl4ai strips most page furniture, but what survives is what a site repeats on
every article: a cookie-consent panel, an affiliate disclaimer, a navigation
menu. `core/content_quality.py` cannot catch those — a consent panel is prose,
not nav-dense markup — and they land inside the ~1200 characters
`services/text.py` hands to the embedding model, so every article from that
publisher embeds to nearly the same vector. Measured on WIRED: 37 of 37 crawls
opened with the same Condé Nast consent panel, and all 29 members of one cluster
shared a single embedding body (see `docs/measurements.md`, T14).

The rule is deliberately not a list of sites. A line is chrome when *this
source* repeats it, which the corpus says on its own; profiles are built by
`build_profile` and stored per source, and only lines a profile names are ever
removed.
"""

from __future__ import annotations

import collections
import re
from collections.abc import Iterable


# How far into a document chrome is looked for. Past this, repeated lines are
# more likely a publisher's standard sign-off than page furniture.
LEAD_LINES = 300
# The same bound expressed in characters, for the profile builder's SQL: a line
# budget cannot be pushed into `left(content, ...)`, and 300 lines of chrome and
# short paragraphs have never come close to this many characters. Only the
# builder needs it -- `strip_boilerplate` counts real lines.
LEAD_CHARS = 40000
# Articles per source the profile is built from, newest first. Chrome repeats
# from the first article on, so a larger sample only costs memory.
PROFILE_SAMPLE_SIZE = 200
# A source needs this many articles before its repetitions mean anything.
MIN_DOCS_PER_SOURCE = 5
# Share of a source's articles a line must appear in to count as chrome.
DF_RATIO = 0.30
# Visible characters that make a line substantial enough to be article text.
MIN_LINE_CHARS = 25
# Links on one line that make it a menu row rather than a sentence.
NAV_LINKS = 3

_WS_RE = re.compile(r"\s+")
_LINK_RE = re.compile(r"!?\[([^\]]*)\]\([^)]*\)")
_MD_NOISE_RE = re.compile(r"[*_#>`|-]+")


def visible_text(line: str) -> str:
    """The line as a reader sees it: link targets and Markdown syntax removed."""
    return _MD_NOISE_RE.sub(" ", _LINK_RE.sub(r"\1", line)).strip()


def normalize_line(line: str) -> str:
    """Key a line by its visible text.

    "  * Notifications", "Notifications" and "[Notifications](/x)" are one line
    to a reader, and keying on the raw string would split one chrome line's
    document frequency three ways and push every variant under the threshold.
    """
    return _WS_RE.sub(" ", visible_text(line)).lower()


def is_navline(line: str) -> bool:
    """A menu row: several links and no sentence around them."""
    return len(_LINK_RE.findall(line)) >= NAV_LINKS and "." not in visible_text(line)


def build_profile(contents: Iterable[str]) -> frozenset[str]:
    """Normalized chrome lines for one source, or empty if it has too few docs.

    Two ways a line qualifies. Repeating across a large share of the source's
    articles makes it chrome whatever it looks like — that is what catches a
    consent panel or an affiliate disclaimer. A menu row qualifies as soon as it
    repeats at all, because a site that varies its nav per section (WIRED does)
    never gets any single variant over the document-frequency bar.
    """
    line_docs: collections.Counter[str] = collections.Counter()
    navlines: set[str] = set()
    documents = 0
    for content in contents:
        if not content:
            continue
        documents += 1
        seen: set[str] = set()
        for line in content.splitlines()[:LEAD_LINES]:
            if not line.strip():
                continue
            key = normalize_line(line)
            seen.add(key)
            if is_navline(line):
                navlines.add(key)
        line_docs.update(seen)

    if documents < MIN_DOCS_PER_SOURCE:
        return frozenset()
    floor = max(2, documents * DF_RATIO)
    return frozenset(
        line
        for line, count in line_docs.items()
        if count >= floor or (count >= 2 and line in navlines)
    )


def strip_boilerplate(content: str, profile: frozenset[str]) -> str:
    """Remove the source's chrome lines from the lead region of one document.

    Chrome is removed line by line rather than as one leading block. A site
    interleaves its own text with the article's — The Verge puts "Loading
    comments" between the headline and the body — so cutting only a contiguous
    run leaves chrome behind, while cutting everything above the *last* chrome
    line deletes real article text whenever a recurring template heading sits
    under it. Lines outside the profile are never removed; the only exception is
    the filler above the first substantial line, which is menu labels and bare
    links by construction.
    """
    if not content or not profile:
        return content
    lines = content.splitlines()
    head = [
        line
        for line in lines[:LEAD_LINES]
        # Blank lines are kept even though they normalize into the profile
        # alongside rules and bullets: `content_quality.is_nav_dense` reads the
        # opening N *characters*, so closing up the paragraph breaks would pull
        # text into its window that the crawl did not put there.
        if not line.strip() or normalize_line(line) not in profile
    ]
    return "\n".join(_drop_leading_filler(head) + lines[LEAD_LINES:])


def _drop_leading_filler(lines: list[str]) -> list[str]:
    """Drop menu labels and bare links above the first substantial line."""
    for index, line in enumerate(lines):
        if not line.strip():
            continue
        if len(visible_text(line)) >= MIN_LINE_CHARS:
            return lines[index:]
    return []
