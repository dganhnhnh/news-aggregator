"""I/O-free, bounded error categories shared by crawl adapters and retries."""

from __future__ import annotations

import re

ERROR_TYPE_KEY = "crawl_error_type"
HTTP_STATUS_KEY = "http_status"
ERROR_TYPES = frozenset({
    "DecodeError", "Timeout", "NetworkError", "HttpError", "BrowserError",
    "EmptyContent", "BatchError", "FeedParseError", "CrawlError",
})


def monitoring_error_type(value: str) -> str:
    """Keep free-form exception names and messages out of metric labels."""
    if value.startswith("UnusableUrl:"):
        return "UnusableUrl"
    if value in ERROR_TYPES or value == "BlockedPage":
        return value
    if re.fullmatch(r"HttpError:[45]\d{2}", value):
        return value
    return "Other"


def error_context(
    error: BaseException | str, *, default: str = "CrawlError",
    http_status: int | None = None,
) -> dict:
    """Prefer structured status/exception data; conservatively classify library text."""
    message = str(error).lower()
    name = type(error).__name__.lower() if isinstance(error, BaseException) else ""
    response = getattr(error, "response", None)
    status = http_status or getattr(response, "status_code", None)
    if status is None:
        match = re.search(r"\bhttp(?:\s+status(?:\s+code)?)?\s*[:=]?\s*([45]\d{2})\b", message)
        status = int(match[1]) if match else None
    if status is not None and 400 <= status <= 599:
        return {ERROR_TYPE_KEY: "HttpError", HTTP_STATUS_KEY: status}
    if isinstance(error, TimeoutError) or "timeout" in name or "timeout" in message or "timed out" in message:
        category = "Timeout"
    elif any(token in name + " " + message for token in (
        "connecterror", "networkerror", "connectionerror", "dns", "err_name_not_resolved",
        "err_connection", "certificate", "sslerror", "tls",
    )):
        category = "NetworkError"
    elif any(token in message for token in (
        "browser", "chromium", "playwright", "targetclosed", "target closed",
    )):
        category = "BrowserError"
    else:
        category = default
    return {ERROR_TYPE_KEY: category if category in ERROR_TYPES else "CrawlError"}
