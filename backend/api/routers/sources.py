"""Read-only endpoints over `sources` and per-source article feeds."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends, HTTPException, Query

from adapters.storage import PostgresRawStorage
from api.cursor import decode_cursor, encode_cursor
from api.deps import get_settings, get_storage
from api.schemas import ArticleSummary, CursorPage, FeedWindow, SourceItem
from core.config import Settings


router = APIRouter(prefix="/api/sources", tags=["sources"])


@router.get("", response_model=list[SourceItem])
async def list_sources(
    since: datetime | None = None,
    until: datetime | None = None,
    storage: PostgresRawStorage = Depends(get_storage),
) -> list[SourceItem]:
    items = await storage.list_sources(
        since=since,
        until=until or datetime.now(timezone.utc),
    )
    return [
        SourceItem(slug=item.slug, name=item.name, homepage=item.homepage) for item in items
    ]


@router.get("/{slug}/articles", response_model=CursorPage[ArticleSummary])
async def source_articles(
    slug: str,
    since: datetime | None = None,
    until: datetime | None = None,
    limit: int = Query(20, ge=1, le=100),
    cursor: str | None = None,
    topic: list[str] | None = Query(None),
    q: str | None = None,
    storage: PostgresRawStorage = Depends(get_storage),
    settings: Settings = Depends(get_settings),
) -> CursorPage[ArticleSummary]:
    window_until = until or datetime.now(timezone.utc)
    window_since = since or window_until - timedelta(days=settings.api_default_window_days)
    decoded_cursor = decode_cursor(cursor) if cursor else None

    items = await storage.source_articles(
        slug,
        since=window_since,
        until=window_until,
        limit=limit,
        cursor=decoded_cursor,
        topic_slugs=topic,
        query=q,
    )
    if items is None:
        raise HTTPException(status_code=404, detail="Source not found")
    next_cursor = (
        encode_cursor(items[-1].event_at, items[-1].id) if len(items) == limit else None
    )
    return CursorPage(
        items=[ArticleSummary.from_feed_item(item) for item in items],
        next_cursor=next_cursor,
        window=FeedWindow(since=window_since, until=window_until),
    )
