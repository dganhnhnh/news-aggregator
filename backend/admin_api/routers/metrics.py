"""Prometheus scrape endpoint.

It lives on the admin API, not the public one: queue depth, corpus size and
failure counts describe how the system is doing, which is operator
information. `news-admin-api` is published on the loopback interface only, so
Prometheus (on the same host) can reach it and nobody else can.
"""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Response, status

from adapters.storage import StorageError
from admin_api.deps import get_metrics_collector
from admin_api.metrics import CONTENT_TYPE_LATEST, MetricsCollector
from observability import process_metrics


router = APIRouter(tags=["metrics"])


@router.get("/metrics", include_in_schema=False)
async def metrics(
    collector: MetricsCollector = Depends(get_metrics_collector),
) -> Response:
    try:
        payload = await collector.expose()
    except StorageError as error:
        # A scrape that cannot read the database is a failed scrape, not an
        # exposition of zeros: `up == 0` says "unknown", empty gauges would
        # claim the queues are empty.
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Metrics collection failed: {error}",
        ) from error
    # The database-derived families and this process's own instruments are
    # two registries with no metric name in common, so the exposition is just
    # one appended to the other; only the database half is cached.
    return Response(
        content=payload + process_metrics(), media_type=CONTENT_TYPE_LATEST
    )
