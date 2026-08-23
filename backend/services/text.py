"""Build the text handed to the embedding model.

MiniLM truncates around 256 tokens (~1000-1200 characters), so embedding the
full Markdown article is pointless past that point -- only the leading
paragraphs matter.
"""

from __future__ import annotations

import re


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


def embed_text(title: str | None, content: str, *, max_chars: int = 1200) -> str:
    body = first_paragraphs(content, max_chars=max_chars)
    return f"{title}\n\n{body}" if title else body
