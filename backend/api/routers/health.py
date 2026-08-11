"""Liveness and readiness endpoints."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException

from adapters.storage import PostgresRawStorage, StorageError
from api.deps import get_storage


router = APIRouter(tags=["health"])


@router.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@router.get("/health/db")
async def health_db(
    storage: PostgresRawStorage = Depends(get_storage),
) -> dict[str, str]:
    try:
        await storage.check_ready()
    except StorageError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    return {"status": "ok"}
