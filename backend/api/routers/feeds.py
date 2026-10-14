"""Read-only listing of the feeds articles were collected from."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends

from adapters.storage import PostgresRawStorage
from api.deps import get_settings, get_storage
from api.schemas import FeedSourceItem
from core.config import Settings
from models.storage_models import FeedTier


router = APIRouter(prefix="/api/feeds", tags=["feeds"])


@router.get("", response_model=list[FeedSourceItem])
async def list_feeds(
    since: datetime | None = None,
    until: datetime | None = None,
    tier: FeedTier | None = None,
    storage: PostgresRawStorage = Depends(get_storage),
    settings: Settings = Depends(get_settings),
) -> list[FeedSourceItem]:
    window_until = until or datetime.now(timezone.utc)
    window_since = since or window_until - timedelta(days=settings.api_default_window_days)
    items = await storage.list_article_feeds(
        since=window_since, until=window_until, tier=tier
    )
    return [
        FeedSourceItem(
            feed_url=item.feed_url,
            label=item.source_label,
            tier=item.tier,
            article_count=item.article_count,
        )
        for item in items
    ]
