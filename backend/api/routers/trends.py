"""Read-only per-day mention counts for the same search the feed runs.

Its own prefix rather than a path under `/api/articles`, because that router
declares `GET /{article_id}` and Starlette matches in registration order: a
literal sibling segment added after it resolves to `get_article` and fails
`int` parsing, which surfaces as a 422 rather than anything routing-shaped.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends, HTTPException, Query

from adapters.storage import PostgresRawStorage
from api.deps import get_settings, get_storage
from api.schemas import TrendSeries
from core.config import Settings


router = APIRouter(prefix="/api/trends", tags=["trends"])

# One bar per day. 400 keeps the response under roughly 40 KB, is wider than
# the longest range the UI offers, and is about where a bar stops being wide
# enough to read -- which is the signal that week buckets are needed, not a
# reason to raise the cap.
MAX_TREND_DAYS = 400


def _as_utc(value: datetime) -> datetime:
    """A query param with no offset means UTC, not the server's locale.

    Without this, `?since=2026-08-01T00:00:00` parses naive and the day
    bucketing shifts by whatever offset the host happens to run in.
    """
    return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)


@router.get("", response_model=TrendSeries)
async def article_trends(
    q: str = Query(..., min_length=1, max_length=200),
    since: datetime | None = None,
    until: datetime | None = None,
    source: list[str] | None = Query(None),
    topic: list[str] | None = Query(None),
    storage: PostgresRawStorage = Depends(get_storage),
    settings: Settings = Depends(get_settings),
) -> TrendSeries:
    # `q` is required: the whole response is a series for one query, and the UI
    # shows an empty state until there is one, so there is no path where a page
    # load triggers a full-corpus scan.
    query = q.strip()
    if not query:
        raise HTTPException(status_code=400, detail="q must not be blank")
    window_until = _as_utc(until) if until else datetime.now(timezone.utc)
    window_since = (
        _as_utc(since)
        if since
        else window_until - timedelta(days=settings.api_default_window_days)
    )
    if window_until <= window_since:
        raise HTTPException(status_code=400, detail="until must be after since")
    # Checked before the query runs: a ten-year window is a client bug, and it
    # should cost a 400 rather than a scan of the whole corpus.
    span_days = (window_until - window_since).days + 1
    if span_days > MAX_TREND_DAYS:
        raise HTTPException(
            status_code=400,
            detail=f"Window spans {span_days} days; the maximum is {MAX_TREND_DAYS}",
        )

    days = await storage.daily_article_counts(
        since=window_since,
        until=window_until,
        query=query,
        source_slugs=source,
        topic_slugs=topic,
    )
    return TrendSeries.from_storage(
        query=query, since=window_since, until=window_until, days=days
    )
