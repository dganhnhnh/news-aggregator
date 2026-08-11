"""Pydantic response models for the public API."""

from __future__ import annotations

from datetime import datetime
from typing import Generic, TypeVar

from pydantic import BaseModel

T = TypeVar("T")


class ArticleSummary(BaseModel):
    id: int
    url: str
    title: str | None
    source: str | None
    published_at: datetime | None
    crawled_at: datetime


class ArticleDetail(BaseModel):
    id: int
    url: str
    title: str | None
    source: str | None
    content: str
    published_at: datetime | None
    crawled_at: datetime


class Page(BaseModel, Generic[T]):
    items: list[T]
    total: int
    limit: int
    offset: int
