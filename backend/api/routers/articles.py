"""Read-only endpoints over the `raw_articles` table."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends, HTTPException, Query

from adapters.storage import PostgresRawStorage
from api.cursor import decode_cursor, encode_cursor
from api.deps import get_settings, get_storage
from api.schemas import (
    ArticleDetail,
    ArticleSummary,
    CursorPage,
    FeedWindow,
    SourceRef,
    TopicRef,
)
from core.config import Settings


router = APIRouter(prefix="/api/articles", tags=["articles"])


@router.get("", response_model=CursorPage[ArticleSummary])
async def list_articles(
    since: datetime | None = None,
    until: datetime | None = None,
    limit: int = Query(20, ge=1, le=100),
    cursor: str | None = None,
    source: list[str] | None = Query(None),
    topic: list[str] | None = Query(None),
    q: str | None = None,
    max_per_source: int = Query(3, ge=1, le=20),
    storage: PostgresRawStorage = Depends(get_storage),
    settings: Settings = Depends(get_settings),
) -> CursorPage[ArticleSummary]:
    window_until = until or datetime.now(timezone.utc)
    window_since = since or window_until - timedelta(days=settings.api_default_window_days)
    decoded_cursor = decode_cursor(cursor) if cursor else None

    items = await storage.feed_articles(
        since=window_since,
        until=window_until,
        limit=limit,
        cursor=decoded_cursor,
        source_slugs=source,
        topic_slugs=topic,
        query=q,
        max_per_source=max_per_source,
    )
    next_cursor = (
        encode_cursor(items[-1].event_at, items[-1].id) if len(items) == limit else None
    )
    return CursorPage(
        items=[ArticleSummary.from_feed_item(item) for item in items],
        next_cursor=next_cursor,
        window=FeedWindow(since=window_since, until=window_until),
    )


@router.get("/{article_id}", response_model=ArticleDetail)
async def get_article(
    article_id: int,
    storage: PostgresRawStorage = Depends(get_storage),
) -> ArticleDetail:
    article = await storage.get_article(article_id)
    if article is None:
        raise HTTPException(status_code=404, detail="Article not found")
    source = (
        SourceRef(slug=article.source_slug, name=article.source_name)
        if article.source_slug is not None
        else None
    )
    return ArticleDetail(
        id=article_id,
        url=article.url,
        url_original=article.url_original,
        title=article.title,
        content=article.content,
        source=source,
        published_at=article.published_at,
        crawled_at=article.crawled_at,
        event_at=article.event_at,
        topics=[TopicRef.from_storage(topic) for topic in article.topics],
    )
