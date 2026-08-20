"""Pydantic response models for the public API."""

from __future__ import annotations

from datetime import datetime
from typing import Generic, TypeVar

from pydantic import BaseModel

from models.storage_models import ArticleFeedItem

T = TypeVar("T")


class SourceRef(BaseModel):
    slug: str
    name: str


class ArticleSummary(BaseModel):
    id: int
    url: str
    title: str | None
    excerpt: str
    source: SourceRef | None
    published_at: datetime | None
    crawled_at: datetime
    event_at: datetime

    @classmethod
    def from_feed_item(cls, item: ArticleFeedItem) -> "ArticleSummary":
        source = (
            SourceRef(slug=item.source_slug, name=item.source_name)
            if item.source_slug is not None
            else None
        )
        return cls(
            id=item.id,
            url=item.url,
            title=item.title,
            excerpt=item.excerpt,
            source=source,
            published_at=item.published_at,
            crawled_at=item.crawled_at,
            event_at=item.event_at,
        )


class ArticleDetail(BaseModel):
    id: int
    url: str
    url_original: str | None
    title: str | None
    content: str
    source: SourceRef | None
    published_at: datetime | None
    crawled_at: datetime
    event_at: datetime


class SourceItem(BaseModel):
    slug: str
    name: str
    homepage: str | None


class FeedWindow(BaseModel):
    since: datetime
    until: datetime


class CursorPage(BaseModel, Generic[T]):
    items: list[T]
    next_cursor: str | None
    window: FeedWindow
