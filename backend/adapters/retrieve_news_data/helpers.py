"""
Shared helper utilities for adapters.
"""
import re
from datetime import datetime, timezone
from typing import Optional

# ─── Date / datetime parsing ─────────────────────────────────────────────────

# Ordered list of (regex_pattern, strptime_format) pairs.
# Patterns are tried in order; the first match wins.
_DATE_PATTERNS: list[tuple[re.Pattern, str]] = [
    # ISO-8601 with timezone offset: 2026-03-30T14:05:00+07:00
    (
        re.compile(r"\b(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}[+-]\d{2}:\d{2})\b"),
        "%Y-%m-%dT%H:%M:%S%z",
    ),
    # ISO-8601 with Z: 2026-03-30T14:05:00Z
    (
        re.compile(r"\b(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z)\b"),
        "%Y-%m-%dT%H:%M:%SZ",
    ),
    # ISO-8601 no timezone: 2026-03-30T14:05:00
    (
        re.compile(r"\b(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})\b"),
        "%Y-%m-%dT%H:%M:%S",
    ),
    # ISO date only in URL or text: 2026/03/30  or  2026-03-30
    (
        re.compile(r"\b(\d{4}[/-]\d{2}[/-]\d{2})\b"),
        None,  # handled specially below
    ),
    # "March 30, 2026" / "march 30 2026"
    (
        re.compile(
            r"\b((?:January|February|March|April|May|June|July|August|September|"
            r"October|November|December)\s+\d{1,2},?\s+\d{4})\b",
            re.IGNORECASE,
        ),
        "%B %d, %Y",
    ),
    # "30 March 2026"
    (
        re.compile(
            r"\b(\d{1,2}\s+(?:January|February|March|April|May|June|July|August|"
            r"September|October|November|December)\s+\d{4})\b",
            re.IGNORECASE,
        ),
        "%d %B %Y",
    ),
    # "Mar 30, 2026" / "Mar 30 2026"
    (
        re.compile(
            r"\b((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)"
            r"\.?\s+\d{1,2},?\s+\d{4})\b",
            re.IGNORECASE,
        ),
        "%b %d, %Y",
    ),
    # "30 Mar 2026"
    (
        re.compile(
            r"\b(\d{1,2}\s+(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)"
            r"\.?\s+\d{4})\b",
            re.IGNORECASE,
        ),
        "%d %b %Y",
    ),
]


def _parse_datetime(dt_str: Optional[str]) -> Optional[datetime]:
    """Parse an ISO-8601 string into a datetime, returning None on failure."""
    if not dt_str:
        return None
    try:
        return datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
    except Exception:
        return None


def extract_date_from_text(text: str) -> Optional[datetime]:
    """
    Try to extract a date/datetime from arbitrary text using multiple regex
    patterns.  Returns the first successfully parsed datetime, or None.
    """
    if not text:
        return None

    for pattern, fmt in _DATE_PATTERNS:
        m = pattern.search(text)
        if not m:
            continue
        raw = m.group(1).strip().rstrip(",")

        # Special case: YYYY/MM/DD or YYYY-MM-DD (no time component)
        if fmt is None:
            normalized = raw.replace("/", "-")
            try:
                return datetime.strptime(normalized, "%Y-%m-%d").replace(
                    tzinfo=timezone.utc
                )
            except ValueError:
                continue

        # Normalise comma-less abbreviated month strings, e.g. "Mar 30 2026"
        if "%b" in fmt or "%B" in fmt:
            # ensure comma after day number when format expects it
            raw_for_parse = re.sub(r"(\d{1,2})\s+(\d{4})", r"\1, \2", raw)
        else:
            raw_for_parse = raw

        try:
            dt = datetime.strptime(raw_for_parse, fmt)
            # Attach UTC if no tzinfo
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except ValueError:
            continue

    return None


def extract_date_from_result(
    published_date: Optional[datetime],
    title: str = "",
    url: str = "",
    content_snippet: str = "",
) -> Optional[datetime]:
    """
    Return *published_date* if it is already set; otherwise attempt to extract
    a date from the title, URL, and content_snippet (in that priority order).
    """
    if published_date is not None:
        return published_date

    for text in (title, url, content_snippet):
        dt = extract_date_from_text(text)
        if dt is not None:
            return dt

    return None