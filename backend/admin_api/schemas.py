"""Request and response schemas for operator-managed feeds."""

from __future__ import annotations

from datetime import datetime
from typing import Literal, Self

from pydantic import BaseModel, Field, field_validator, model_validator

from core.feed_urls import validate_feed_url
from models.discovery_models import FeedDiscoveryResult
from models.storage_models import FeedStateItem


FeedPriority = Literal["instant", "normal"]
FeedStatus = Literal["active", "inactive", "all"]


def priority_to_storage(priority: FeedPriority) -> int:
    return 0 if priority == "instant" else 1


def priority_from_storage(priority: int) -> FeedPriority:
    return "instant" if priority == 0 else "normal"


class FeedCreate(BaseModel):
    feed_url: str
    source_label: str | None = None
    priority: FeedPriority = "normal"

    @field_validator("feed_url")
    @classmethod
    def valid_feed_url(cls, value: str) -> str:
        return validate_feed_url(value)

    @field_validator("source_label")
    @classmethod
    def clean_source_label(cls, value: str | None) -> str | None:
        return value.strip() or None if value is not None else None


class FeedDiscoveryRequest(BaseModel):
    query: str = Field(min_length=1, max_length=300)
    language: str = Field(default="en", pattern=r"^[A-Za-z]{2,8}$")
    country: str = Field(default="US", pattern=r"^[A-Za-z]{2}$")
    limit: int = Field(default=25, ge=1, le=25)

    @field_validator("query")
    @classmethod
    def clean_query(cls, value: str) -> str:
        cleaned = value.strip()
        if not cleaned:
            raise ValueError("query must not be blank")
        return cleaned


class FeedDiscoveryCandidateResponse(BaseModel):
    feed_url: str
    source_label: str
    website_url: str
    feed_scope: Literal["category", "tag", "sitewide"]
    discovery_source: Literal[
        "tavily_direct", "tavily_publisher", "google_fallback"
    ]
    relevance_reasons: list[str]
    sample_titles: list[str]
    last_published_at: datetime | None
    subscribed: bool


class FeedDiscoveryStatsResponse(BaseModel):
    search_results: int
    publishers_inspected: int
    pages_inspected: int
    feeds_validated: int
    google_fallback_used: bool
    feeds_attempted: int
    feeds_relevant: int


class FeedDiscoveryResponse(BaseModel):
    query: str
    candidates: list[FeedDiscoveryCandidateResponse]
    stats: FeedDiscoveryStatsResponse
    warnings: list[str]

    @classmethod
    def from_domain(
        cls,
        result: FeedDiscoveryResult,
        *,
        subscribed_urls: set[str],
    ) -> "FeedDiscoveryResponse":
        return cls(
            query=result.query,
            candidates=[
                FeedDiscoveryCandidateResponse(
                    feed_url=item.feed_url,
                    source_label=item.source_label,
                    website_url=item.website_url,
                    feed_scope=item.feed_scope,
                    discovery_source=item.discovery_source,
                    relevance_reasons=list(item.relevance_reasons),
                    sample_titles=list(item.sample_titles),
                    last_published_at=item.last_published_at,
                    subscribed=item.feed_url in subscribed_urls,
                )
                for item in result.candidates
            ],
            stats=FeedDiscoveryStatsResponse(
                search_results=result.stats.search_results,
                publishers_inspected=result.stats.publishers_inspected,
                pages_inspected=result.stats.pages_inspected,
                feeds_validated=result.stats.feeds_validated,
                google_fallback_used=result.stats.google_fallback_used,
                feeds_attempted=result.stats.feeds_attempted,
                feeds_relevant=result.stats.feeds_relevant,
            ),
            warnings=list(result.warnings),
        )


class FeedBatchItem(BaseModel):
    feed_url: str
    source_label: str | None = None

    @field_validator("feed_url")
    @classmethod
    def valid_feed_url(cls, value: str) -> str:
        return validate_feed_url(value)

    @field_validator("source_label")
    @classmethod
    def clean_source_label(cls, value: str | None) -> str | None:
        return value.strip() or None if value is not None else None


class FeedBatchCreate(BaseModel):
    feeds: list[FeedBatchItem] = Field(min_length=1, max_length=50)
    priority: FeedPriority = "normal"

    @field_validator("feeds")
    @classmethod
    def unique_feeds(cls, value: list[FeedBatchItem]) -> list[FeedBatchItem]:
        unique: list[FeedBatchItem] = []
        seen: set[str] = set()
        for item in value:
            if item.feed_url in seen:
                continue
            seen.add(item.feed_url)
            unique.append(item)
        return unique


class FeedBatchCreateResponse(BaseModel):
    requested_count: int
    created_count: int
    skipped_count: int


class FeedUpdate(BaseModel):
    feed_url: str
    priority: FeedPriority | None = None
    active: bool | None = None

    @field_validator("feed_url")
    @classmethod
    def valid_feed_url(cls, value: str) -> str:
        return validate_feed_url(value)

    @model_validator(mode="after")
    def has_change(self) -> Self:
        if self.priority is None and self.active is None:
            raise ValueError("At least one of priority or active is required")
        return self


class FeedAdminItem(BaseModel):
    feed_url: str
    source_label: str
    priority: FeedPriority
    active: bool
    resolved_url: str | None
    last_polled_at: datetime | None
    last_success_at: datetime | None
    next_poll_at: datetime
    consecutive_failures: int
    disabled_at: datetime | None
    disabled_reason: str | None
    updated_at: datetime

    @classmethod
    def from_storage(cls, item: FeedStateItem) -> "FeedAdminItem":
        return cls(
            feed_url=item.feed_url,
            source_label=item.source_label or "",
            priority=priority_from_storage(item.priority),
            active=item.active,
            resolved_url=item.resolved_url,
            last_polled_at=item.last_polled_at,
            last_success_at=item.last_success_at,
            next_poll_at=item.next_poll_at,
            consecutive_failures=item.consecutive_failures,
            disabled_at=item.disabled_at,
            disabled_reason=item.disabled_reason,
            updated_at=item.updated_at,
        )
