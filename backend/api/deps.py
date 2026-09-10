"""Shared FastAPI dependencies: settings and the storage repository."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from functools import lru_cache

from fastapi import FastAPI, Request

from adapters.storage import PostgresRawStorage, StorageError
from agent.service import ResearchService
from core.config import Settings


@lru_cache
def get_settings() -> Settings:
    return Settings.from_env()


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    settings = get_settings()
    if not settings.database_url:
        raise StorageError("DATABASE_URL is required for news-api")
    storage = PostgresRawStorage(settings.database_url)
    await storage.check_ready()
    app.state.storage = storage
    app.state.research_service = ResearchService.from_settings(settings, storage)
    try:
        yield
    finally:
        await storage.close()


def get_storage(request: Request) -> PostgresRawStorage:
    return request.app.state.storage


def get_research_service(request: Request) -> ResearchService:
    return request.app.state.research_service
