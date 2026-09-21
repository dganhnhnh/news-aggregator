"""Pydantic response models for the public API."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date, datetime
from typing import Generic, TypeVar
from uuid import UUID

from pydantic import BaseModel, Field, field_validator

from models.storage_models import (
    ArticleFeedItem,
    DailyArticleCount,
    HighlightSegment,
    StoryFeedItem,
    TopicRefItem,
)

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


class TextHighlight(BaseModel):
    text: str
    highlight: bool

    @classmethod
    def from_storage(cls, segment: HighlightSegment) -> "TextHighlight":
        return cls(text=segment.text, highlight=segment.highlight)


class ArticleSummary(BaseModel):
    id: int
    url: str
    title: str | None
    title_highlights: list[TextHighlight] | None = None
    excerpt: str
    excerpt_highlights: list[TextHighlight] | None = None
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
            title_highlights=(
                [TextHighlight.from_storage(seg) for seg in item.title_highlights]
                if item.title_highlights is not None
                else None
            ),
            excerpt=item.excerpt,
            excerpt_highlights=(
                [TextHighlight.from_storage(seg) for seg in item.excerpt_highlights]
                if item.excerpt_highlights is not None
                else None
            ),
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


class TrendDay(BaseModel):
    # Named `day` rather than `date`: this module uses postponed annotation
    # evaluation, and a field named after its own type is a resolution hazard.
    day: date
    matches: int
    articles: int
    partial: bool


class TrendTotals(BaseModel):
    matches: int
    articles: int


class TrendSeries(BaseModel):
    """A query's daily mention counts over one window.

    No `share` field: the client divides `matches` by `articles` itself, so
    there is exactly one place the ratio can be computed wrong.
    """

    query: str
    window: FeedWindow
    totals: TrendTotals
    days: list[TrendDay]

    @classmethod
    def from_storage(
        cls,
        *,
        query: str,
        since: datetime,
        until: datetime,
        days: Sequence[DailyArticleCount],
    ) -> "TrendSeries":
        return cls(
            query=query,
            window=FeedWindow(since=since, until=until),
            # Summed from the buckets rather than counted separately, so the
            # header and the bars cannot tell different stories.
            totals=TrendTotals(
                matches=sum(day.matches for day in days),
                articles=sum(day.articles for day in days),
            ),
            days=[
                TrendDay(
                    day=day.day,
                    matches=day.matches,
                    articles=day.articles,
                    partial=day.partial,
                )
                for day in days
            ],
        )


class ResearchRequest(BaseModel):
    query: str = Field(min_length=1, max_length=4000)

    @field_validator("query", mode="before")
    @classmethod
    def trim_query(cls, value: object) -> object:
        return value.strip() if isinstance(value, str) else value


class ResearchStreamRequest(ResearchRequest):
    thread_id: UUID


class ResearchCitation(BaseModel):
    article_id: int
    title: str | None
    url: str
    source: SourceRef | None
    published_at: datetime | None


class ResearchResponse(BaseModel):
    answer: str
    citations: list[ResearchCitation]
