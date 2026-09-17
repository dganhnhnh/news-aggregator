"""Prometheus instrumentation for the three long-lived processes.

Everything here is *in-process*: it only survives as long as the process that
recorded it, which is why it covers `news-api` and `news-admin-api` and not
the workers. Worker numbers are derived from PostgreSQL by
`admin_api/metrics.py` instead, because an Airflow task container is deleted
before a scrape could ever reach it.

The two halves are exposed differently. `news-admin-api` already serves
`/metrics` on loopback, so its registry is appended to that response.
`news-api` is published on `0.0.0.0`, so it serves its registry on a second
listener that compose publishes on loopback alone -- request timings and
model usage are operator data too.
"""

from __future__ import annotations

from observability.http import (
    UNMATCHED_ROUTE,
    HttpMetricsMiddleware,
    metrics_asgi_app,
    process_metrics,
)
from observability.instruments import (
    observe_agent_run,
    record_llm_usage,
    search_timer,
    tavily_timer,
    timed_search,
)


__all__ = [
    "HttpMetricsMiddleware",
    "UNMATCHED_ROUTE",
    "metrics_asgi_app",
    "observe_agent_run",
    "process_metrics",
    "record_llm_usage",
    "search_timer",
    "tavily_timer",
    "timed_search",
]
