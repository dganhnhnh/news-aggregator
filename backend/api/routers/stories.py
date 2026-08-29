"""Read-only endpoints over canonical story clusters and their topic labels."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends, HTTPException, Query

from adapters.storage import PostgresRawStorage
from api.cursor import decode_cursor, encode_cursor
from api.deps import get_settings, get_storage
from api.schemas import (
    CursorPage,
    FeedWindow,
    SourceRef,
    StoryDetail,
    StoryMember,
    StorySummary,
    TopicRef,
)
from core.config import Settings


router = APIRouter(prefix="/api/stories", tags=["stories"])


@router.get("", response_model=CursorPage[StorySummary])
async def list_stories(
    since: datetime | None = None,
    until: datetime | None = None,
    limit: int = Query(20, ge=1, le=100),
    cursor: str | None = None,
    source: list[str] | None = Query(None),
    topic: list[str] | None = Query(None),
    min_sources: int = Query(1, ge=1, le=100),
    min_articles: int = Query(1, ge=1, le=100),
    storage: PostgresRawStorage = Depends(get_storage),
    settings: Settings = Depends(get_settings),
) -> CursorPage[StorySummary]:
    window_until = until or datetime.now(timezone.utc)
    window_since = since or window_until - timedelta(days=settings.api_default_window_days)
    decoded_cursor = decode_cursor(cursor) if cursor else None

    items = await storage.list_stories(
        since=window_since,
        until=window_until,
        limit=limit,
        cursor=decoded_cursor,
        source_slugs=source,
        topic_slugs=topic,
        min_sources=min_sources,
        min_articles=min_articles,
    )
    next_cursor = (
        encode_cursor(items[-1].last_seen_at, items[-1].id) if len(items) == limit else None
    )
    return CursorPage(
        items=[StorySummary.from_feed_item(item) for item in items],
        next_cursor=next_cursor,
        window=FeedWindow(since=window_since, until=window_until),
    )


@router.get("/{story_id}", response_model=StoryDetail)
async def get_story(
    story_id: int,
    storage: PostgresRawStorage = Depends(get_storage),
) -> StoryDetail:
    story = await storage.get_story(story_id)
    if story is None:
        raise HTTPException(status_code=404, detail="Story not found")
    return StoryDetail(
        id=story.id,
        headline=story.headline,
        first_seen_at=story.first_seen_at,
        last_seen_at=story.last_seen_at,
        article_count=story.article_count,
        source_count=story.source_count,
        topics=[TopicRef.from_storage(topic) for topic in story.topics],
        members=[
            StoryMember(
                article_id=member.article_id,
                title=member.title,
                url=member.url,
                source=(
                    SourceRef(slug=member.source_slug, name=member.source_name)
                    if member.source_slug is not None
                    else None
                ),
                published_at=member.published_at,
                event_at=member.event_at,
            )
            for member in story.members
        ],
    )
