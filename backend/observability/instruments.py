"""Counters for the work a request timer cannot see.

Three costs sit behind the API and none of them show up as a route: how long
retrieval takes and which half of the hybrid search is slow, how a research
run ends and what it spent, and how often the paid Tavily quota is used.

Every label here is a closed set -- search mode, model name, operation,
outcome. Nothing derived from user input (a query, a URL, an upstream error
message) is ever a label value: those are unbounded, and Prometheus keeps a
series it has seen once for as long as the retention window.

`Counter("x")` exposes `x_total`, so the names below deliberately stop short
of the suffix.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncIterator, Awaitable, Callable, Iterator
from contextlib import AbstractContextManager, contextmanager
from typing import TypeVar

from prometheus_client import Counter, Histogram


T = TypeVar("T")

PREFIX = "news_radar"

SEARCH_REQUESTS = Counter(
    f"{PREFIX}_search_requests",
    "Article searches by retrieval mode and outcome.",
    ["mode", "outcome"],
)
SEARCH_DURATION = Histogram(
    f"{PREFIX}_search_duration_seconds",
    "Article search latency by retrieval mode; 'hybrid' is the whole "
    "request, including the rank fusion over the other two.",
    ["mode"],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0),
)

AGENT_RUNS = Counter(
    f"{PREFIX}_agent_runs",
    "Research runs by model and how the stream ended.",
    ["model", "outcome"],
)
AGENT_RUN_DURATION = Histogram(
    f"{PREFIX}_agent_run_duration_seconds",
    "Wall-clock duration of a research run, model calls and tool calls "
    "included.",
    ["model"],
    buckets=(1.0, 2.0, 5.0, 10.0, 20.0, 30.0, 45.0, 60.0, 90.0, 120.0, 180.0),
)
LLM_CALLS = Counter(
    f"{PREFIX}_llm_calls",
    "Model invocations inside completed research runs.",
    ["model"],
)
LLM_TOKENS = Counter(
    f"{PREFIX}_llm_tokens",
    "Tokens reported by completed research runs; 'cached' is the subset of "
    "input tokens served from the provider's prompt cache.",
    ["model", "kind"],
)

TAVILY_REQUESTS = Counter(
    f"{PREFIX}_tavily_requests",
    "Calls to the Tavily API by operation and outcome. The quota is paid "
    "for, so this counts requests rather than sampling them.",
    ["operation", "outcome"],
)
TAVILY_DURATION = Histogram(
    f"{PREFIX}_tavily_request_duration_seconds",
    "Tavily API latency by operation.",
    ["operation"],
    buckets=(0.1, 0.25, 0.5, 1.0, 2.0, 5.0, 10.0, 20.0, 30.0, 60.0),
)

__all__ = [
    "observe_agent_run",
    "record_llm_usage",
    "search_timer",
    "tavily_timer",
    "timed_search",
]


@contextmanager
def _observed(
    requests: Counter, duration: Histogram, label: str
) -> Iterator[None]:
    """Count one attempt and time it, whether or not it succeeds.

    Failures are timed too: a provider that hangs until its timeout is the
    case worth seeing, and dropping those samples would make the latency
    histogram flatter exactly when things break.
    """
    started = time.perf_counter()
    outcome = "success"
    try:
        yield
    except asyncio.CancelledError:
        outcome = "cancelled"
        raise
    except Exception:
        outcome = "error"
        raise
    finally:
        duration.labels(label).observe(time.perf_counter() - started)
        requests.labels(label, outcome).inc()


def search_timer(mode: str) -> AbstractContextManager[None]:
    """Time one retrieval pass: `lexical`, `semantic` or `hybrid`."""
    return _observed(SEARCH_REQUESTS, SEARCH_DURATION, mode)


async def timed_search(mode: str, awaitable: Awaitable[T]) -> T:
    """`search_timer` for a coroutine, so gathered passes can be timed."""
    with search_timer(mode):
        return await awaitable


def tavily_timer(operation: str) -> AbstractContextManager[None]:
    """Time one Tavily call: `search` or `extract`."""
    return _observed(TAVILY_REQUESTS, TAVILY_DURATION, operation)


async def observe_agent_run(
    events: AsyncIterator[T],
    *,
    model: str,
    outcome_for: Callable[[BaseException], str],
) -> AsyncIterator[T]:
    """Wrap a research event stream in a run counter and a duration.

    The run is only over once the stream is, so this has to sit around the
    iteration rather than around the call that creates it. `outcome_for`
    belongs to the caller: the service knows which of its own errors means
    "timed out" and which means "retrieval is down", and this module should
    not learn that to find out.

    A client that walks away mid-answer closes the generator, which arrives
    here as `GeneratorExit`. That is neither success nor failure, so it gets
    its own outcome instead of being counted as an answer delivered.
    """
    started = time.perf_counter()
    outcome = "success"
    try:
        async for event in events:
            yield event
    except asyncio.CancelledError:
        outcome = "cancelled"
        raise
    except GeneratorExit:
        outcome = "abandoned"
        raise
    except Exception as error:
        outcome = outcome_for(error)
        raise
    finally:
        AGENT_RUNS.labels(model, outcome).inc()
        AGENT_RUN_DURATION.labels(model).observe(time.perf_counter() - started)


def record_llm_usage(
    *, model: str, calls: int, total_tokens: int, cached_tokens: int
) -> None:
    """Record what a finished run spent, as reported by the provider."""
    if calls:
        LLM_CALLS.labels(model).inc(calls)
    if total_tokens:
        LLM_TOKENS.labels(model, "total").inc(total_tokens)
    if cached_tokens:
        LLM_TOKENS.labels(model, "cached").inc(cached_tokens)
