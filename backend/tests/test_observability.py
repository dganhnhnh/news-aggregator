"""Unit tests for the in-process instrumentation; no database required."""

from __future__ import annotations

import asyncio

import pytest
from fastapi import APIRouter, FastAPI, HTTPException
from fastapi.testclient import TestClient
from prometheus_client import REGISTRY, CollectorRegistry

from observability import (
    UNMATCHED_ROUTE,
    HttpMetricsMiddleware,
    observe_agent_run,
    record_llm_usage,
    search_timer,
    tavily_timer,
)


DURATION = "news_radar_http_request_duration_seconds_count"
IN_PROGRESS = "news_radar_http_requests_in_progress"


def _instrumented_app() -> tuple[TestClient, CollectorRegistry]:
    """An app shaped like the real one: endpoints arrive via `include_router`.

    That detail is the test. FastAPI wraps an included router in an object
    that matches a request without saying which route will serve it, so a
    middleware that re-runs the match sees every real endpoint as unmatched
    while a flat test app looks perfectly fine.
    """
    registry = CollectorRegistry()
    router = APIRouter(prefix="/api/articles")

    @router.get("/{article_id}")
    async def read_article(article_id: int) -> dict[str, int]:
        return {"id": article_id}

    app = FastAPI()
    app.include_router(router)

    @app.get("/boom")
    async def boom() -> None:
        raise HTTPException(status_code=418, detail="teapot")

    @app.get("/crash")
    async def crash() -> None:
        raise RuntimeError("unhandled")

    app.add_middleware(HttpMetricsMiddleware, registry=registry)
    return TestClient(app, raise_server_exceptions=False), registry


def _count(registry: CollectorRegistry, **labels: str) -> float:
    return registry.get_sample_value(DURATION, labels) or 0.0


def test_requests_are_labelled_by_route_template_not_path() -> None:
    client, registry = _instrumented_app()

    client.get("/api/articles/1")
    client.get("/api/articles/2")

    labels = {
        "method": "GET",
        "route": "/api/articles/{article_id}",
        "status": "200",
    }
    assert _count(registry, **labels) == 2.0
    # The whole point: two ids share one series instead of minting two.
    assert registry.get_sample_value(DURATION, {**labels, "route": "/api/articles/1"}) is None


def test_unmatched_paths_collapse_into_one_series() -> None:
    client, registry = _instrumented_app()

    assert client.get("/nope/12345").status_code == 404
    assert client.get("/nope/67890").status_code == 404

    assert _count(registry, method="GET", route=UNMATCHED_ROUTE, status="404") == 2.0


def test_handled_and_unhandled_failures_keep_their_status() -> None:
    client, registry = _instrumented_app()

    assert client.get("/boom").status_code == 418
    assert client.get("/crash").status_code == 500

    assert _count(registry, method="GET", route="/boom", status="418") == 1.0
    # An unhandled exception never reaches `http.response.start`, so the 500
    # the client sees has to be the middleware's own assumption.
    assert _count(registry, method="GET", route="/crash", status="500") == 1.0


def test_in_progress_returns_to_zero_including_after_a_crash() -> None:
    client, registry = _instrumented_app()

    client.get("/api/articles/1")
    client.get("/crash")

    assert registry.get_sample_value(IN_PROGRESS, {"method": "GET"}) == 0.0


def _delta(name: str, labels: dict[str, str], action) -> float:
    before = REGISTRY.get_sample_value(name, labels) or 0.0
    action()
    after = REGISTRY.get_sample_value(name, labels) or 0.0
    return after - before


def test_search_timer_counts_both_outcomes() -> None:
    def succeed() -> None:
        with search_timer("hybrid"):
            pass

    def fail() -> None:
        with pytest.raises(ValueError):
            with search_timer("lexical"):
                raise ValueError("no index")

    assert _delta(
        "news_radar_search_requests_total",
        {"mode": "hybrid", "outcome": "success"},
        succeed,
    ) == 1.0
    assert _delta(
        "news_radar_search_duration_seconds_count", {"mode": "lexical"}, fail
    ) == 1.0
    assert (
        REGISTRY.get_sample_value(
            "news_radar_search_requests_total",
            {"mode": "lexical", "outcome": "error"},
        )
        or 0.0
    ) >= 1.0


def test_tavily_timer_separates_operations() -> None:
    def extract() -> None:
        with tavily_timer("extract"):
            pass

    assert _delta(
        "news_radar_tavily_requests_total",
        {"operation": "extract", "outcome": "success"},
        extract,
    ) == 1.0


def _run_outcome(error: BaseException) -> str:
    return "timeout" if isinstance(error, TimeoutError) else "error"


async def _events(count: int, failure: BaseException | None = None):
    for index in range(count):
        yield index
    if failure is not None:
        raise failure


def _agent_runs(model: str, outcome: str) -> float:
    return (
        REGISTRY.get_sample_value(
            "news_radar_agent_runs_total", {"model": model, "outcome": outcome}
        )
        or 0.0
    )


def test_agent_run_is_counted_when_the_stream_ends() -> None:
    async def scenario() -> None:
        before = _agent_runs("test/model", "success")
        events = observe_agent_run(
            _events(2), model="test/model", outcome_for=_run_outcome
        )
        assert [event async for event in events] == [0, 1]
        assert _agent_runs("test/model", "success") == before + 1.0

    asyncio.run(scenario())


def test_agent_run_failures_use_the_callers_outcome_names() -> None:
    async def scenario() -> None:
        before = _agent_runs("test/model", "timeout")
        events = observe_agent_run(
            _events(1, TimeoutError()),
            model="test/model",
            outcome_for=_run_outcome,
        )
        with pytest.raises(TimeoutError):
            async for _ in events:
                pass
        assert _agent_runs("test/model", "timeout") == before + 1.0

    asyncio.run(scenario())


def test_an_abandoned_stream_is_not_an_answer() -> None:
    """A client that disconnects mid-answer must not look like a success."""

    async def scenario() -> None:
        before = _agent_runs("test/model", "abandoned")
        events = observe_agent_run(
            _events(5), model="test/model", outcome_for=_run_outcome
        )
        async for _ in events:
            break
        await events.aclose()
        assert _agent_runs("test/model", "abandoned") == before + 1.0

    asyncio.run(scenario())


def test_llm_usage_records_only_what_was_reported() -> None:
    def record() -> None:
        record_llm_usage(
            model="test/model", calls=3, total_tokens=1200, cached_tokens=0
        )

    assert _delta("news_radar_llm_calls_total", {"model": "test/model"}, record) == 3.0
    # A provider that reports no cache read must not leave a zero series
    # claiming the cache was measured.
    assert (
        REGISTRY.get_sample_value(
            "news_radar_llm_tokens_total", {"model": "test/model", "kind": "cached"}
        )
        is None
    )
