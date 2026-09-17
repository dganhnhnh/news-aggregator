"""Request timing middleware and the exposition helpers around it."""

from __future__ import annotations

import time
from collections.abc import Awaitable, Callable, MutableMapping, Sequence
from functools import lru_cache
from typing import Any

from prometheus_client import (
    REGISTRY,
    CollectorRegistry,
    Gauge,
    Histogram,
    generate_latest,
    make_asgi_app,
)


Scope = MutableMapping[str, Any]
Message = MutableMapping[str, Any]
Receive = Callable[[], Awaitable[Message]]
Send = Callable[[Message], Awaitable[None]]
ASGIApp = Callable[[Scope, Receive, Send], Awaitable[None]]

PREFIX = "news_radar"
UNMATCHED_ROUTE = "__unmatched__"
# Reaches 60s because /api/research streams a model run, not a database read;
# the low end is tight enough to see a feed query lose its index.
DURATION_BUCKETS = (
    0.005,
    0.01,
    0.025,
    0.05,
    0.1,
    0.25,
    0.5,
    1.0,
    2.5,
    5.0,
    10.0,
    30.0,
    60.0,
)

__all__ = [
    "DURATION_BUCKETS",
    "HttpMetricsMiddleware",
    "UNMATCHED_ROUTE",
    "metrics_asgi_app",
    "process_metrics",
]


def route_template(scope: Scope) -> str:
    """The pattern a request was routed to, once the router has matched it.

    The label has to be the template (`/api/articles/{article_id}`), never the
    raw path: one article id per series would bury Prometheus, and a scanner
    probing random URLs could do it deliberately. Anything that reached no
    route -- 404s, and the 405s that only matched on path -- collapses into a
    single bucket.

    Starlette records the route it chose in the scope, which is the only
    dependable source: FastAPI wraps an included router in an object that
    matches a request without revealing which of its routes will serve it, so
    re-running the match here would answer `__unmatched__` for every endpoint
    the app actually has.
    """
    return str(getattr(scope.get("route"), "path", UNMATCHED_ROUTE))


@lru_cache(maxsize=None)
def _instruments(
    registry: CollectorRegistry, buckets: tuple[float, ...]
) -> tuple[Histogram, Gauge]:
    """One set of instruments per registry, created on first use.

    A metric may only be registered once, but an app is built more than once
    per process: `create_app()` runs at import and again in every test that
    wants a fresh application. Caching on the registry keeps those sharing
    the series they are all describing.
    """
    duration = Histogram(
        f"{PREFIX}_http_request_duration_seconds",
        "HTTP request latency by method, route template and status.",
        ["method", "route", "status"],
        buckets=buckets,
        registry=registry,
    )
    in_progress = Gauge(
        f"{PREFIX}_http_requests_in_progress",
        "Requests currently being served, by method. Not by route: which "
        "route serves a request is only known once it has been answered, "
        "and this has to count requests that never are.",
        ["method"],
        registry=registry,
    )
    return duration, in_progress


class HttpMetricsMiddleware:
    """Times every HTTP request, labelled by method, route and status.

    Written against the raw ASGI interface rather than `BaseHTTPMiddleware`
    because `/api/research/stream` is a server-sent-event response: the
    request/response wrapper would hold the stream in a queue and turn a live
    answer into one delivered at the end.

    The registry is injectable so a test can measure one app in isolation
    instead of reading through everything the process has recorded.
    """

    def __init__(
        self,
        app: ASGIApp,
        *,
        registry: CollectorRegistry = REGISTRY,
        buckets: Sequence[float] = DURATION_BUCKETS,
    ) -> None:
        self._app = app
        self._duration, self._in_progress = _instruments(registry, tuple(buckets))

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self._app(scope, receive, send)
            return

        method = str(scope.get("method", "GET"))
        # An exception propagates past us to Starlette's error middleware,
        # which answers 500; assuming that up front keeps the failure in the
        # histogram instead of dropping it.
        status = 500

        async def observed_send(message: Message) -> None:
            nonlocal status
            if message["type"] == "http.response.start":
                status = int(message["status"])
            await send(message)

        started = time.perf_counter()
        self._in_progress.labels(method).inc()
        try:
            await self._app(scope, receive, observed_send)
        finally:
            self._in_progress.labels(method).dec()
            self._duration.labels(
                method, route_template(scope), str(status)
            ).observe(time.perf_counter() - started)


def metrics_asgi_app(registry: CollectorRegistry = REGISTRY) -> ASGIApp:
    """A standalone exposition app, for serving a registry on its own port."""
    return make_asgi_app(registry)


def process_metrics(registry: CollectorRegistry = REGISTRY) -> bytes:
    """Exposition of this process's own metrics, for appending to another."""
    return generate_latest(registry)
