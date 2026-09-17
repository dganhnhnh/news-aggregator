"""Dependencies and lifecycle for the operator-only admin API."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Request, status

from adapters.retrieve_news_data import (
    GoogleNewsFeedDiscovery,
    TavilySearcher,
    TopicFeedDiscovery,
)
from adapters.storage import PostgresRawStorage, StorageError
from admin_api.metrics import MetricsCollector
from core.config import Settings


def get_settings() -> Settings:
    return Settings.from_env()


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    settings = get_settings()
    if not settings.database_url:
        raise StorageError("DATABASE_URL is required for news-admin-api")
    storage = PostgresRawStorage(settings.database_url)
    await storage.check_ready()
    app.state.storage = storage
    # One collector for the whole process: its accumulated counters and cache
    # are only meaningful if every scrape goes through the same instance.
    app.state.metrics_collector = MetricsCollector(
        storage,
        cache_seconds=settings.metrics_cache_seconds,
        recent_window_hours=settings.metrics_recent_window_hours,
        max_error_types=settings.metrics_max_error_types,
        feed_overdue_grace_seconds=settings.metrics_feed_overdue_grace_seconds,
    )
    try:
        yield
    finally:
        await storage.close()


def get_storage(request: Request) -> PostgresRawStorage:
    return request.app.state.storage


def get_metrics_collector(request: Request) -> MetricsCollector:
    return request.app.state.metrics_collector


def get_feed_discovery() -> TopicFeedDiscovery:
    """Build a stateless, feed-first discovery adapter for one admin request."""
    settings = get_settings()
    if not settings.tavily_api_key:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=(
                "Feed discovery is not configured: set TAVILY_API_KEY for "
                "news-admin-api"
            ),
        )
    return TopicFeedDiscovery(
        TavilySearcher(settings.tavily_api_key),
        google_fallback=GoogleNewsFeedDiscovery(),
    )
