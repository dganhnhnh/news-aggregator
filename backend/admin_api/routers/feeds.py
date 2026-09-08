"""Operator endpoints for configured RSS/Atom feeds."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query, status

from adapters.storage import PostgresRawStorage
from admin_api.deps import get_storage
from admin_api.schemas import (
    FeedAdminItem,
    FeedCreate,
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
