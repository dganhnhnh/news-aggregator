"""Operator endpoints for configured RSS/Atom feeds."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query, status

from adapters.retrieve_news_data import FeedDiscoveryError, GoogleNewsFeedDiscovery
from adapters.storage import PostgresRawStorage
from admin_api.deps import get_feed_discovery, get_storage
from admin_api.schemas import (
    FeedAdminItem,
    FeedBatchCreate,
    FeedBatchCreateResponse,
    FeedCreate,
    FeedDiscoveryRequest,
    FeedDiscoveryResponse,
    FeedStatus,
    FeedUpdate,
    priority_to_storage,
)
from core.feed_urls import infer_feed_source_label
from models.storage_models import FeedSeed


router = APIRouter(prefix="/api/admin/feeds", tags=["feeds"])


@router.get("", response_model=list[FeedAdminItem])
async def list_feeds(
    status_filter: FeedStatus = Query("active", alias="status"),
    storage: PostgresRawStorage = Depends(get_storage),
) -> list[FeedAdminItem]:
    active = {"active": True, "inactive": False, "all": None}[status_filter]
    items = await storage.list_feeds(active=active)
    return [FeedAdminItem.from_storage(item) for item in items]


@router.post("/discover", response_model=FeedDiscoveryResponse)
async def discover_feeds(
    payload: FeedDiscoveryRequest,
    storage: PostgresRawStorage = Depends(get_storage),
    discovery: GoogleNewsFeedDiscovery = Depends(get_feed_discovery),
) -> FeedDiscoveryResponse:
    try:
        result = await discovery.discover(
            payload.query,
            language=payload.language,
            country=payload.country,
            limit=payload.limit,
        )
    except FeedDiscoveryError as exc:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"Google News feed discovery failed: {exc}",
        ) from exc

    configured = await storage.list_feeds(active=None)
    subscribed_urls = {
        url
        for item in configured
        for url in (item.feed_url, item.resolved_url)
        if url
    }
    return FeedDiscoveryResponse.from_domain(
        result,
        subscribed_urls=subscribed_urls,
    )


@router.post(
    "/batch",
    response_model=FeedBatchCreateResponse,
    status_code=status.HTTP_201_CREATED,
)
async def create_feed_batch(
    payload: FeedBatchCreate,
    storage: PostgresRawStorage = Depends(get_storage),
) -> FeedBatchCreateResponse:
    priority = priority_to_storage(payload.priority)
    seeds = [
        FeedSeed(
            feed_url=item.feed_url,
            source_label=(
                item.source_label or infer_feed_source_label(item.feed_url)
            ),
            priority=priority,
        )
        for item in payload.feeds
    ]
    created = await storage.seed_feeds(seeds)
    return FeedBatchCreateResponse(
        requested_count=len(seeds),
        created_count=created,
        skipped_count=len(seeds) - created,
    )


@router.post("", response_model=FeedAdminItem, status_code=status.HTTP_201_CREATED)
async def create_feed(
    payload: FeedCreate,
    storage: PostgresRawStorage = Depends(get_storage),
) -> FeedAdminItem:
    source_label = payload.source_label or infer_feed_source_label(payload.feed_url)
    item = await storage.create_feed(
        FeedSeed(
            feed_url=payload.feed_url,
            source_label=source_label,
            priority=priority_to_storage(payload.priority),
        )
    )
    if item is None:
        raise HTTPException(status_code=409, detail="Feed URL already exists")
    return FeedAdminItem.from_storage(item)


@router.patch("", response_model=FeedAdminItem)
async def update_feed(
    payload: FeedUpdate,
    storage: PostgresRawStorage = Depends(get_storage),
) -> FeedAdminItem:
    item = await storage.update_feed(
        payload.feed_url,
        priority=(
            priority_to_storage(payload.priority)
            if payload.priority is not None
            else None
        ),
        active=payload.active,
    )
    if item is None:
        raise HTTPException(status_code=404, detail="Feed not found")
    return FeedAdminItem.from_storage(item)
