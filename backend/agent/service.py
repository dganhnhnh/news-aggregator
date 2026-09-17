"""Application service that runs research and validates article citations."""

from __future__ import annotations

import asyncio
import json
import re
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import datetime, timezone
from time import monotonic
from typing import Any
from uuid import uuid4

from langchain.messages import AIMessage, HumanMessage, ToolMessage
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.types import Overwrite
from sqlalchemy.exc import SQLAlchemyError

from adapters.storage import PostgresRawStorage, StorageError
from agent.config import RECURSION_LIMIT, RUN_TIMEOUT_SECONDS, build_openrouter_model
from agent.embedding import QueryEmbedder, QueryEmbedderProtocol, QueryEmbeddingError
from agent.graph import build_graph
from agent.state import Evidence, ResearchContext
from core.config import Settings
from core.logger import get_logger
from observability.instruments import observe_agent_run, record_llm_usage


logger = get_logger(__name__)
CITATION_RE = re.compile(r"\[A(\d+)\]")
LOG_CONTENT_LIMIT = 2_000
SECRET_VALUE_RE = re.compile(
    r"(?i)(api[_-]?key|authorization)(\s*[:=]\s*)([^\s,;]+)"
)
OPENAI_KEY_RE = re.compile(r"\bsk-[A-Za-z0-9_-]{8,}\b")
DATABASE_URL_RE = re.compile(
    r"(?i)(postgres(?:ql)?://)([^\s/@:]+):([^\s/@]+)@"
)


def _safe_text(value: object, *, limit: int = LOG_CONTENT_LIMIT) -> str:
    text = SECRET_VALUE_RE.sub(r"\1\2<redacted>", str(value))
    text = OPENAI_KEY_RE.sub("<redacted>", text)
    text = DATABASE_URL_RE.sub(r"\1<redacted>:<redacted>@", text)
    if len(text) > limit:
        return f"{text[:limit]}... <truncated {len(text) - limit} chars>"
    return text


class ResearchError(RuntimeError):
    """Base error carrying a safe diagnostic for the API response."""

    def __init__(
        self,
        message: str,
        *,
        request_id: str | None = None,
        error_type: str | None = None,
        reason: str | None = None,
    ) -> None:
        super().__init__(message)
        self.request_id = request_id
        self.error_type = error_type or type(self).__name__
        self.reason = _safe_text(reason or message, limit=1_000)

    def public_detail(self) -> dict[str, str]:
        detail = {
            "message": str(self),
            "error_type": self.error_type,
            "reason": self.reason,
        }
        if self.request_id:
            detail["request_id"] = self.request_id
        return detail


class ResearchUnavailable(ResearchError):
    """The configured research dependencies cannot currently serve requests."""


class ResearchUpstreamError(ResearchError):
    """The model invocation failed or timed out."""


class InvalidResearchCitation(ResearchError):
    """The model emitted a citation not backed by tool evidence."""


@dataclass(frozen=True)
class ResearchCitation:
    article_id: int
    title: str | None
    url: str
    source: dict[str, str] | None
    published_at: datetime | None


@dataclass(frozen=True)
class ResearchResult:
    answer: str
    citations: tuple[ResearchCitation, ...]
    request_id: str = ""


@dataclass(frozen=True)
class ResearchStreamEvent:
    event: str
    data: dict[str, Any]


@dataclass(frozen=True)
class ResearchStreamSession:
    request_id: str
    thread_id: str
    events: AsyncIterator[ResearchStreamEvent]


def _message_text(message: Any) -> str:
    content = getattr(message, "content", "")
    if isinstance(content, str):
        return content.strip()
    if isinstance(content, list):
        parts: list[str] = []
        for block in content:
            if isinstance(block, str):
                parts.append(block)
            elif isinstance(block, dict) and isinstance(block.get("text"), str):
                parts.append(block["text"])
        return "".join(parts).strip()
    return str(content).strip()


def _message_delta_text(message: Any) -> str:
    content = getattr(message, "content", "")
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts: list[str] = []
        for block in content:
            if isinstance(block, str):
                parts.append(block)
            elif isinstance(block, dict) and isinstance(block.get("text"), str):
                parts.append(block["text"])
        return "".join(parts)
    return str(content)


def _root_exception(exc: BaseException) -> BaseException:
    current = exc
    seen: set[int] = set()
    while id(current) not in seen:
        seen.add(id(current))
        nested = getattr(current, "exceptions", None)
        if nested:
            current = nested[0]
            continue
        cause = current.__cause__ or current.__context__
        if cause is None:
            break
        current = cause
    return current


def _contains_exception(
    exc: BaseException, classes: tuple[type[BaseException], ...]
) -> bool:
    if isinstance(exc, classes):
        return True
    nested = getattr(exc, "exceptions", None) or ()
    if any(_contains_exception(item, classes) for item in nested):
        return True
    cause = exc.__cause__ or exc.__context__
    return cause is not None and _contains_exception(cause, classes)


def _diagnostic(exc: BaseException) -> tuple[str, str]:
    root = _root_exception(exc)
    reason = _safe_text(root, limit=1_000)
    if not reason:
        reason = "No error detail was supplied by the dependency"
    return type(root).__name__, reason


def _sanitize_tool_payload(value: Any, *, key: str | None = None) -> Any:
    if key in {"content", "feed_summary"} and isinstance(value, str):
        return f"<omitted {len(value)} chars>"
    if isinstance(value, dict):
        return {
            str(item_key): _sanitize_tool_payload(item_value, key=str(item_key))
            for item_key, item_value in value.items()
        }
    if isinstance(value, list):
        return [_sanitize_tool_payload(item) for item in value]
    return value


def _logged_message_content(message: Any) -> str:
    text = _message_text(message)
    if isinstance(message, ToolMessage):
        try:
            payload = json.loads(text)
        except (TypeError, ValueError):
            return _safe_text(text)
        text = json.dumps(
            _sanitize_tool_payload(payload),
            ensure_ascii=False,
            default=str,
        )
    return _safe_text(text)


def _log_new_messages(
    request_id: str, messages: list[Any], already_logged: int
) -> int:
    for index, message in enumerate(messages[already_logged:], start=already_logged):
        if isinstance(message, HumanMessage):
            role = "user"
        elif isinstance(message, AIMessage):
            role = "assistant"
        elif isinstance(message, ToolMessage):
            role = "tool"
        else:
            role = getattr(message, "type", type(message).__name__)
        tool_calls = getattr(message, "tool_calls", None) or []
        logger.info(
            "Research message request_id=%s index=%d role=%s tool_call_id=%s "
            "tool_calls=%s content=%s",
            request_id,
            index,
            role,
            getattr(message, "tool_call_id", None),
            _safe_text(json.dumps(tool_calls, ensure_ascii=False, default=str)),
            _logged_message_content(message),
        )
    return len(messages)


def _tool_status(tool_name: str) -> str:
    return "searching" if tool_name.startswith("search_") else "reading"


def _run_outcome(error: BaseException) -> str:
    """Map a failed run onto a small, fixed set of metric outcomes.

    Deliberately not `type(error).__name__`: the upstream exception classes
    that reach here come from HTTP clients and the model SDK, and one new
    class name upstream would mint a new Prometheus series forever.
    """
    if isinstance(error, ResearchUnavailable):
        return "unavailable"
    if isinstance(error, InvalidResearchCitation):
        return "invalid_citation"
    if isinstance(error, ResearchError):
        return "timeout" if error.error_type == "TimeoutError" else "error"
    return "error"


def _cached_tokens(messages: list[Any]) -> int:
    total = 0
    for message in messages:
        usage = getattr(message, "usage_metadata", None) or {}
        input_details = usage.get("input_token_details") or {}
        cached = input_details.get("cache_read") or input_details.get("cached_tokens")
        if cached:
            total += int(cached)
            continue
        response = getattr(message, "response_metadata", None) or {}
        prompt_details = (response.get("token_usage") or {}).get(
            "prompt_tokens_details"
        ) or {}
        total += int(prompt_details.get("cached_tokens") or 0)
    return total


def citations_from_evidence(
    answer: str, evidence: list[Evidence]
) -> tuple[ResearchCitation, ...]:
    articles = {
        int(item["article_id"]): item
        for item in evidence
        if item.get("kind") == "article" and item.get("article_id") is not None
    }
    ordered_ids: list[int] = []
    for raw_id in CITATION_RE.findall(answer):
        article_id = int(raw_id)
        if article_id not in articles:
            raise InvalidResearchCitation(
                f"Citation A{article_id} is not present in retrieved evidence"
            )
        if article_id not in ordered_ids:
            ordered_ids.append(article_id)
    citations: list[ResearchCitation] = []
    for article_id in ordered_ids:
        item = articles[article_id]
        url = item.get("url")
        if not isinstance(url, str) or not url:
            raise InvalidResearchCitation(f"Citation A{article_id} has no source URL")
        source = item.get("source")
        citations.append(
            ResearchCitation(
                article_id=article_id,
                title=item.get("title"),
                url=url,
                source=source if isinstance(source, dict) else None,
                published_at=item.get("published_at"),
            )
        )
    return tuple(citations)


class ResearchService:
    def __init__(
        self,
        *,
        storage: PostgresRawStorage,
        model: Any | None,
        embedder: QueryEmbedderProtocol | None = None,
        timeout_seconds: float = RUN_TIMEOUT_SECONDS,
        checkpointer: InMemorySaver | None = None,
    ) -> None:
        self._storage = storage
        self._model = model
        self._embedder = embedder or QueryEmbedder()
        self._timeout_seconds = timeout_seconds
        self._checkpointer = checkpointer or InMemorySaver()

    @classmethod
    def from_settings(
        cls, settings: Settings, storage: PostgresRawStorage
    ) -> "ResearchService":
        model = None
        if settings.openrouter_api_key:
            model = build_openrouter_model(
                api_key=settings.openrouter_api_key,
                model=settings.news_agent_model,
                site_url=settings.openrouter_site_url,
                timeout_seconds=settings.openrouter_timeout_seconds,
            )
        return cls(
            storage=storage,
            model=model,
            timeout_seconds=settings.news_agent_run_timeout_seconds,
        )

    def _model_label(self) -> str:
        """The model id, as a metric label; a closed set per deployment."""
        if self._model is None:
            return "none"
        return str(
            getattr(self._model, "model_name", None)
            or getattr(self._model, "model", "")
            or "unknown"
        )

    def _configured_model(self, thread_id: str) -> Any:
        if self._model is None:
            return None
        if not hasattr(self._model, "extra_body"):
            return self._model
        extra_body = dict(getattr(self._model, "extra_body", None) or {})
        extra_body["session_id"] = thread_id
        model_name = str(
            getattr(self._model, "model_name", None)
            or getattr(self._model, "model", "")
        )
        if model_name.startswith("anthropic/"):
            extra_body["cache_control"] = {"type": "ephemeral"}
        return self._model.model_copy(update={"extra_body": extra_body})

    def start_stream(self, query: str, *, thread_id: str) -> ResearchStreamSession:
        request_id = uuid4().hex
        if self._model is None:
            reason = "OPENROUTER_API_KEY is not configured"
            logger.warning(
                "Research unavailable request_id=%s error_type=ConfigurationError "
                "reason=%s",
                request_id,
                reason,
            )
            raise ResearchUnavailable(
                reason,
                request_id=request_id,
                error_type="ConfigurationError",
                reason=reason,
            )
        try:
            graph = build_graph(
                self._configured_model(thread_id),
                checkpointer=self._checkpointer,
            )
        except Exception as exc:
            error_type, reason = _diagnostic(exc)
            logger.exception(
                "Research failed request_id=%s phase=graph_setup error_type=%s "
                "reason=%s",
                request_id,
                error_type,
                reason,
            )
            raise ResearchUpstreamError(
                "Research agent setup failed",
                request_id=request_id,
                error_type=error_type,
                reason=reason,
            ) from exc
        return ResearchStreamSession(
            request_id=request_id,
            thread_id=thread_id,
            # The run is not over until the stream is, so the measurement has
            # to wrap the iteration rather than this call.
            events=observe_agent_run(
                self._stream_graph(
                    graph,
                    query=query,
                    thread_id=thread_id,
                    request_id=request_id,
                ),
                model=self._model_label(),
                outcome_for=_run_outcome,
            ),
        )

    async def _stream_graph(
        self,
        graph: Any,
        *,
        query: str,
        thread_id: str,
        request_id: str,
    ) -> AsyncIterator[ResearchStreamEvent]:
        started = monotonic()
        now = datetime.now(timezone.utc)
        context = ResearchContext(
            storage=self._storage,
            embedder=self._embedder,
            now=now,
            request_id=request_id,
        )
        config = {
            "recursion_limit": RECURSION_LIMIT,
            "configurable": {"thread_id": thread_id},
        }
        snapshot = await graph.aget_state(config)
        baseline_messages = len((snapshot.values or {}).get("messages") or [])
        logged_messages = baseline_messages
        observed_messages = baseline_messages
        emitted_tool_calls: set[str] = set()
        current_generation: str | None = None
        current_generation_uses_tools = False
        visible_answer = ""
        result: dict[str, Any] | None = None

        logger.info(
            "Research started request_id=%s thread_id=%s query_chars=%d "
            "history_messages=%d timeout_seconds=%s",
            request_id,
            thread_id,
            len(query),
            baseline_messages,
            self._timeout_seconds,
        )
        yield ResearchStreamEvent(
            "start", {"request_id": request_id, "thread_id": thread_id}
        )

        turn_message = (
            f"[Turn context: current UTC time is {now.isoformat()}]\n\n{query}"
        )
        try:
            async with asyncio.timeout(self._timeout_seconds):
                async for part in graph.astream(
                    {
                        "messages": [HumanMessage(content=turn_message)],
                        "evidence": Overwrite([]),
                    },
                    config=config,
                    context=context,
                    stream_mode=["messages", "values"],
                    version="v2",
                ):
                    part_type = part.get("type")
                    if part_type == "messages":
                        message, metadata = part["data"]
                        if metadata.get("langgraph_node") != "model":
                            continue
                        generation = str(getattr(message, "id", None) or "model")
                        if generation != current_generation:
                            current_generation = generation
                            current_generation_uses_tools = False
                        calls = list(getattr(message, "tool_calls", None) or [])
                        call_chunks = list(
                            getattr(message, "tool_call_chunks", None) or []
                        )
                        if calls or call_chunks:
                            if not current_generation_uses_tools and visible_answer:
                                visible_answer = ""
                                yield ResearchStreamEvent("reset", {})
                            current_generation_uses_tools = True
                            for call in [*calls, *call_chunks]:
                                tool_name = str(call.get("name") or "")
                                call_id = str(
                                    call.get("id")
                                    or f"{generation}:{tool_name or 'tool'}"
                                )
                                if tool_name and call_id not in emitted_tool_calls:
                                    emitted_tool_calls.add(call_id)
                                    yield ResearchStreamEvent(
                                        "status",
                                        {
                                            "phase": _tool_status(tool_name),
                                            "tool": tool_name,
                                        },
                                    )
                            continue
                        if current_generation_uses_tools:
                            continue
                        delta = _message_delta_text(message)
                        if delta:
                            visible_answer += delta
                            yield ResearchStreamEvent("delta", {"text": delta})
                        continue

                    if part_type != "values":
                        continue
                    state = part["data"]
                    result = state
                    messages = list(state.get("messages") or [])
                    logged_messages = _log_new_messages(
                        request_id, messages, logged_messages
                    )
                    for message in messages[observed_messages:]:
                        if isinstance(message, AIMessage):
                            for call in message.tool_calls:
                                tool_name = str(call.get("name") or "")
                                call_id = str(call.get("id") or tool_name)
                                if tool_name and call_id not in emitted_tool_calls:
                                    emitted_tool_calls.add(call_id)
                                    if visible_answer:
                                        visible_answer = ""
                                        yield ResearchStreamEvent("reset", {})
                                    yield ResearchStreamEvent(
                                        "status",
                                        {
                                            "phase": _tool_status(tool_name),
                                            "tool": tool_name,
                                        },
                                    )
                        elif isinstance(message, ToolMessage):
                            yield ResearchStreamEvent(
                                "status", {"phase": "synthesizing"}
                            )
                    observed_messages = len(messages)
        except TimeoutError as exc:
            reason = f"Agent exceeded the {self._timeout_seconds:g}s run timeout"
            logger.exception(
                "Research failed request_id=%s thread_id=%s phase=graph "
                "error_type=TimeoutError reason=%s duration_ms=%d",
                request_id,
                thread_id,
                reason,
                int((monotonic() - started) * 1000),
            )
            raise ResearchUpstreamError(
                "Research request timed out",
                request_id=request_id,
                error_type="TimeoutError",
                reason=reason,
            ) from exc
        except asyncio.CancelledError:
            logger.info(
                "Research cancelled request_id=%s thread_id=%s duration_ms=%d",
                request_id,
                thread_id,
                int((monotonic() - started) * 1000),
            )
            raise
        except Exception as exc:
            error_type, reason = _diagnostic(exc)
            duration_ms = int((monotonic() - started) * 1000)
            if _contains_exception(
                exc, (StorageError, QueryEmbeddingError, SQLAlchemyError)
            ):
                logger.exception(
                    "Research failed request_id=%s thread_id=%s phase=retrieval "
                    "error_type=%s reason=%s duration_ms=%d",
                    request_id,
                    thread_id,
                    error_type,
                    reason,
                    duration_ms,
                )
                raise ResearchUnavailable(
                    "Research retrieval is unavailable",
                    request_id=request_id,
                    error_type=error_type,
                    reason=reason,
                ) from exc
            logger.exception(
                "Research failed request_id=%s thread_id=%s phase=graph "
                "error_type=%s reason=%s duration_ms=%d",
                request_id,
                thread_id,
                error_type,
                reason,
                duration_ms,
            )
            raise ResearchUpstreamError(
                "Research model invocation failed",
                request_id=request_id,
                error_type=error_type,
                reason=reason,
            ) from exc

        if result is None:
            reason = "The agent graph completed without emitting any state"
            raise ResearchUpstreamError(
                "Research model returned no result",
                request_id=request_id,
                error_type="EmptyGraphResult",
                reason=reason,
            )

        messages = list(result.get("messages") or [])
        final = next(
            (
                message
                for message in reversed(messages[baseline_messages:])
                if isinstance(message, AIMessage) and not message.tool_calls
            ),
            None,
        )
        if final is None or not _message_text(final):
            reason = "The current turn did not produce a non-empty final AI message"
            raise ResearchUpstreamError(
                "Research model returned no final answer",
                request_id=request_id,
                error_type="MissingFinalAnswer",
                reason=reason,
            )

        answer = _message_text(final)
        evidence = list(result.get("evidence") or [])
        try:
            citations = citations_from_evidence(answer, evidence)
        except InvalidResearchCitation as exc:
            reason = _safe_text(exc, limit=1_000)
            logger.warning(
                "Research failed request_id=%s thread_id=%s "
                "phase=citation_validation error_type=%s reason=%s evidence=%d",
                request_id,
                thread_id,
                type(exc).__name__,
                reason,
                len(evidence),
            )
            raise InvalidResearchCitation(
                "Research model returned an invalid citation",
                request_id=request_id,
                error_type=type(exc).__name__,
                reason=reason,
            ) from exc

        if visible_answer != answer:
            if visible_answer:
                yield ResearchStreamEvent("reset", {})
            yield ResearchStreamEvent("delta", {"text": answer})
        yield ResearchStreamEvent("citations", {"citations": citations})
        yield ResearchStreamEvent(
            "done", {"request_id": request_id, "thread_id": thread_id}
        )

        current_messages = messages[baseline_messages:]
        tool_calls = sum(
            len(message.tool_calls)
            for message in current_messages
            if isinstance(message, AIMessage)
        )
        tool_messages = sum(
            isinstance(message, ToolMessage) for message in current_messages
        )
        total_tokens = sum(
            int((getattr(message, "usage_metadata", None) or {}).get("total_tokens", 0))
            for message in current_messages
            if isinstance(message, AIMessage)
        )
        model_calls = sum(
            isinstance(message, AIMessage) for message in current_messages
        )
        cached_tokens = _cached_tokens(current_messages)
        record_llm_usage(
            model=self._model_label(),
            calls=model_calls,
            total_tokens=total_tokens,
            cached_tokens=cached_tokens,
        )
        logger.info(
            "Research completed request_id=%s thread_id=%s duration_ms=%d "
            "model_calls=%d tool_calls=%d tool_messages=%d evidence=%d "
            "citations=%d total_tokens=%d cached_tokens=%d",
            request_id,
            thread_id,
            int((monotonic() - started) * 1000),
            model_calls,
            tool_calls,
            tool_messages,
            len(evidence),
            len(citations),
            total_tokens,
            cached_tokens,
        )

    async def research(self, query: str) -> ResearchResult:
        thread_id = uuid4().hex
        session = self.start_stream(query, thread_id=thread_id)
        answer = ""
        citations: tuple[ResearchCitation, ...] = ()
        completed = False
        try:
            async for event in session.events:
                if event.event == "delta":
                    answer += str(event.data.get("text") or "")
                elif event.event == "reset":
                    answer = ""
                elif event.event == "citations":
                    citations = tuple(event.data.get("citations") or ())
                elif event.event == "done":
                    completed = True
        finally:
            await self.delete_thread(thread_id)
        if not completed:
            raise ResearchUpstreamError(
                "Research stream ended before completion",
                request_id=session.request_id,
                error_type="IncompleteResearchStream",
            )
        return ResearchResult(
            answer=answer,
            citations=citations,
            request_id=session.request_id,
        )

    async def delete_thread(self, thread_id: str) -> None:
        await self._checkpointer.adelete_thread(thread_id)
