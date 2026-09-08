"""Request and response schemas for operator-managed feeds."""

from __future__ import annotations

from datetime import datetime
from typing import Literal, Self

from pydantic import BaseModel, field_validator, model_validator

from core.feed_urls import validate_feed_url
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
