"""Dependencies and lifecycle for the operator-only admin API."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request

from adapters.retrieve_news_data import GoogleNewsFeedDiscovery
from adapters.storage import PostgresRawStorage, StorageError
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
    try:
        yield
    finally:
        await storage.close()


def get_storage(request: Request) -> PostgresRawStorage:
    return request.app.state.storage


def get_feed_discovery() -> GoogleNewsFeedDiscovery:
    """Build a stateless discovery adapter for one admin request."""
    return GoogleNewsFeedDiscovery()
