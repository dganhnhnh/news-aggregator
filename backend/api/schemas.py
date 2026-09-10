"""Pydantic response models for the public API."""

from __future__ import annotations

from datetime import datetime
from typing import Generic, TypeVar

from pydantic import BaseModel

from models.storage_models import ArticleFeedItem, StoryFeedItem, TopicRefItem

T = TypeVar("T")


class SourceRef(BaseModel):
    slug: str
    name: str


class TopicRef(BaseModel):
    id: int
    slug: str
    name: str

    @classmethod
    def from_storage(cls, item: TopicRefItem) -> "TopicRef":
        return cls(id=item.id, slug=item.slug, name=item.display_name)


class ArticleSummary(BaseModel):
    id: int
    url: str
    title: str | None
    excerpt: str
    source: SourceRef | None
    published_at: datetime | None
    crawled_at: datetime
    event_at: datetime
    topics: list[TopicRef]

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
            topics=[TopicRef.from_storage(topic) for topic in item.topics],
        )


class ArticleDetail(BaseModel):
    id: int
    url: str
    url_original: str | None
    title: str | None
    content: str
    feed_summary: str
    source: SourceRef | None
    published_at: datetime | None
    crawled_at: datetime
    event_at: datetime
    topics: list[TopicRef]


class StorySummary(BaseModel):
    id: int
    headline: str | None
    first_seen_at: datetime
    last_seen_at: datetime
    article_count: int
    source_count: int
    sources: list[SourceRef]
    topics: list[TopicRef]

    @classmethod
    def from_feed_item(cls, item: StoryFeedItem) -> "StorySummary":
        return cls(
            id=item.id,
            headline=item.headline,
            first_seen_at=item.first_seen_at,
            last_seen_at=item.last_seen_at,
            article_count=item.article_count,
            source_count=item.source_count,
            sources=[SourceRef(slug=s.slug, name=s.name) for s in item.sources],
            topics=[TopicRef.from_storage(topic) for topic in item.topics],
        )


class StoryMember(BaseModel):
    article_id: int
    title: str | None
    url: str
    source: SourceRef | None
    published_at: datetime | None
    event_at: datetime


class StoryDetail(BaseModel):
    id: int
    headline: str | None
    first_seen_at: datetime
    last_seen_at: datetime
    article_count: int
    source_count: int
    topics: list[TopicRef]
    members: list[StoryMember]


class TopicItem(TopicRef):
    description: str | None


class TaxonomyNode(BaseModel):
    slug: str
    name: str
    topic_slugs: list[str]
    children: list["TaxonomyNode"]


class TaxonomyInfo(BaseModel):
    slug: str
    name: str
    revision: int


class TopicCatalog(BaseModel):
    taxonomy: TaxonomyInfo
    topics: list[TopicItem]
    nodes: list[TaxonomyNode]


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
