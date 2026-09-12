from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from uuid import UUID

import pytest
from fastapi.testclient import TestClient
from langchain_core.language_models.chat_models import BaseChatModel
from langchain_core.messages import AIMessage
from langchain_core.outputs import ChatGeneration, ChatResult

from adapters.storage.postgres import reciprocal_rank_fusion
from agent.config import build_openrouter_model
from agent.graph import build_graph
from agent.service import (
    InvalidResearchCitation,
    ResearchCitation as ServiceCitation,
    ResearchResult,
    ResearchService,
    ResearchStreamEvent,
    ResearchStreamSession,
    ResearchUnavailable,
    ResearchUpstreamError,
    _message_delta_text,
    citations_from_evidence,
)
from agent.state import ResearchContext, merge_evidence
from api.deps import get_research_service
from api.main import create_app
from api.routers.research import _stream_events
from core.config import Settings
from models.storage_models import ArticleSearchHit


NOW = datetime(2026, 9, 10, tzinfo=timezone.utc)


def test_news_agent_model_uses_documented_environment_name(monkeypatch) -> None:
    monkeypatch.setenv("OPENROUTER_MODEL", "anthropic/claude-sonnet-4")
    assert Settings.from_env().news_agent_model == "anthropic/claude-sonnet-4"


def test_research_timeouts_are_configurable_from_environment(monkeypatch) -> None:
    monkeypatch.setenv("OPENROUTER_TIMEOUT_SECONDS", "75.5")
    monkeypatch.setenv("NEWS_AGENT_RUN_TIMEOUT_SECONDS", "180")
    settings = Settings.from_env()
    assert settings.openrouter_timeout_seconds == 75.5
    assert settings.news_agent_run_timeout_seconds == 180


def test_reciprocal_rank_fusion_rewards_both_channels_and_uses_time_tie_break() -> None:
    assert reciprocal_rank_fusion([1, 2], [2, 3], limit=3)[0] == 2
    assert reciprocal_rank_fusion(
        [1],
        [2],
        limit=2,
        tie_break={1: (NOW, 1), 2: (NOW + timedelta(hours=1), 2)},
    ) == [2, 1]


def test_evidence_reducer_keeps_order_and_enriches_search_hit() -> None:
    merged = merge_evidence(
        [{"kind": "article", "article_id": 1, "snippet": "preview"}],
        [
            {"kind": "article", "article_id": 1, "content": "full body"},
            {"kind": "storyline", "storyline_id": 9, "title": "Story"},
        ],
    )
    assert merged == [
        {
            "kind": "article",
            "article_id": 1,
            "snippet": "preview",
            "content": "full body",
        },
        {"kind": "storyline", "storyline_id": 9, "title": "Story"},
    ]


def test_citations_are_validated_deduplicated_and_built_from_evidence() -> None:
    evidence = [
        {
            "kind": "article",
            "article_id": 7,
            "title": "Evidence title",
            "url": "https://example.com/evidence",
            "source": {"slug": "example.com", "name": "Example"},
            "published_at": NOW,
        }
    ]
    citations = citations_from_evidence("Claim [A7], repeated [A7].", evidence)
    assert [item.article_id for item in citations] == [7]
    assert citations[0].url == "https://example.com/evidence"

    with pytest.raises(InvalidResearchCitation):
        citations_from_evidence("Invented [A99].", evidence)


def test_stream_delta_preserves_token_whitespace() -> None:
    assert _message_delta_text(AIMessage(content=" next")) == " next"


class FakeEmbedder:
    model_name = "test-model"
    dim = 384

    async def embed_query(self, query: str) -> list[float]:
        assert query == "chip policy"
        return [1.0] + [0.0] * 383


class FakeResearchStorage:
    async def search_articles(self, query, embedding, **kwargs):
        assert query == "chip policy"
        assert kwargs["model"] == "test-model"
        return [
            ArticleSearchHit(
                id=1,
                url="https://example.com/chips",
                title="Chip policy",
                snippet="The policy changed.",
                source_slug="example.com",
                source_name="Example",
                published_at=NOW,
                event_at=NOW,
            )
        ]


class ScriptedModel(BaseChatModel):
    responses: list[AIMessage]

    @property
    def _llm_type(self) -> str:
        return "scripted-research-test"

    def bind_tools(self, tools, **kwargs):
        return self

    def _generate(self, messages, stop=None, run_manager=None, **kwargs):
        return ChatResult(
            generations=[ChatGeneration(message=self.responses.pop(0))]
        )


class RecordingModel(ScriptedModel):
    seen_messages: list[list[object]] = []

    def _generate(self, messages, stop=None, run_manager=None, **kwargs):
        self.seen_messages.append(list(messages))
        return super()._generate(messages, stop, run_manager, **kwargs)


def test_graph_runs_search_tool_and_accumulates_evidence() -> None:
    model = ScriptedModel(
        responses=[
            AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "search_articles",
                        "args": {"query": "chip policy"},
                        "id": "call-1",
                        "type": "tool_call",
                    }
                ],
            ),
            AIMessage(content="The policy changed. [A1]"),
        ]
    )
    graph = build_graph(model, now=NOW)
    result = asyncio.run(
        graph.ainvoke(
            {"messages": [{"role": "user", "content": "What changed?"}], "evidence": []},
            config={"recursion_limit": 20},
            context=ResearchContext(
                storage=FakeResearchStorage(), embedder=FakeEmbedder(), now=NOW
            ),
        )
    )
    assert result["messages"][-1].content == "The policy changed. [A1]"
    assert result["evidence"][0]["article_id"] == 1


def test_research_service_returns_only_validated_citation_metadata() -> None:
    model = ScriptedModel(
        responses=[
            AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "search_articles",
                        "args": {"query": "chip policy"},
                        "id": "call-2",
                        "type": "tool_call",
                    }
                ],
            ),
            AIMessage(content="The policy changed. [A1]"),
        ]
    )
    service = ResearchService(
        storage=FakeResearchStorage(), model=model, embedder=FakeEmbedder()
    )
    result = asyncio.run(service.research("What changed?"))
    assert result.answer == "The policy changed. [A1]"
    assert [citation.article_id for citation in result.citations] == [1]
    assert result.citations[0].url == "https://example.com/chips"
    assert result.request_id


def test_research_stream_emits_tool_status_answer_and_citations() -> None:
    model = ScriptedModel(
        responses=[
            AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "search_articles",
                        "args": {"query": "chip policy"},
                        "id": "call-stream",
                        "type": "tool_call",
                    }
                ],
            ),
            AIMessage(content="The policy changed. [A1]"),
        ]
    )
    service = ResearchService(
        storage=FakeResearchStorage(), model=model, embedder=FakeEmbedder()
    )

    async def collect():
        session = service.start_stream(
            "What changed?",
            thread_id="00000000-0000-4000-8000-000000000001",
        )
        return [event async for event in session.events]

    events = asyncio.run(collect())
    assert [event.event for event in events] == [
        "start",
        "status",
        "status",
        "delta",
        "citations",
        "done",
    ]
    assert events[1].data == {"phase": "searching", "tool": "search_articles"}
    assert events[3].data == {"text": "The policy changed. [A1]"}
    assert events[4].data["citations"][0].article_id == 1


def test_research_stream_uses_langgraph_thread_memory() -> None:
    model = RecordingModel(
        responses=[
            AIMessage(content="First answer."),
            AIMessage(content="Follow-up answer."),
        ]
    )
    service = ResearchService(
        storage=FakeResearchStorage(), model=model, embedder=FakeEmbedder()
    )

    async def run_turn(query: str):
        session = service.start_stream(
            query,
            thread_id="00000000-0000-4000-8000-000000000002",
        )
        return [event async for event in session.events]

    asyncio.run(run_turn("First question"))
    asyncio.run(run_turn("Follow-up question"))
    second_call = model.seen_messages[1]
    contents = [getattr(message, "content", "") for message in second_call]
    assert any("First question" in str(content) for content in contents)
    assert "First answer." in contents
    assert any("Follow-up question" in str(content) for content in contents)


def test_research_stream_resets_evidence_for_each_turn() -> None:
    model = ScriptedModel(
        responses=[
            AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "search_articles",
                        "args": {"query": "chip policy"},
                        "id": "call-first-turn",
                        "type": "tool_call",
                    }
                ],
            ),
            AIMessage(content="First answer. [A1]"),
            AIMessage(content="Stale claim. [A1]"),
        ]
    )
    service = ResearchService(
        storage=FakeResearchStorage(), model=model, embedder=FakeEmbedder()
    )
    thread_id = "00000000-0000-4000-8000-000000000004"

    async def run():
        first = service.start_stream("First question", thread_id=thread_id)
        _ = [event async for event in first.events]
        second = service.start_stream("Follow-up question", thread_id=thread_id)
        return [event async for event in second.events]

    with pytest.raises(InvalidResearchCitation):
        asyncio.run(run())


def test_openrouter_thread_configuration_enables_cache_and_sticky_session() -> None:
    model = build_openrouter_model(
        api_key="test-key",
        model="anthropic/claude-haiku-4.5",
        site_url=None,
    )
    service = ResearchService(storage=FakeResearchStorage(), model=model)
    configured = service._configured_model("thread-123")
    assert configured.extra_body == {
        "session_id": "thread-123",
        "cache_control": {"type": "ephemeral"},
    }


class FailingModel(BaseChatModel):
    @property
    def _llm_type(self) -> str:
        return "failing-research-test"

    def bind_tools(self, tools, **kwargs):
        return self

    def _generate(self, messages, stop=None, run_manager=None, **kwargs):
        raise ValueError("provider rejected tool schema")


def test_research_service_preserves_safe_root_cause() -> None:
    service = ResearchService(
        storage=FakeResearchStorage(),
        model=FailingModel(),
        embedder=FakeEmbedder(),
    )
    with pytest.raises(ResearchUpstreamError) as raised:
        asyncio.run(service.research("What changed?"))
    assert raised.value.error_type == "ValueError"
    assert raised.value.reason == "provider rejected tool schema"
    assert raised.value.request_id


class FakeResearchService:
    async def research(self, query: str) -> ResearchResult:
        assert query == "trim me"
        return ResearchResult(
            answer="Supported claim. [A7]",
            citations=(
                ServiceCitation(
                    article_id=7,
                    title="Evidence title",
                    url="https://example.com/evidence",
                    source={"slug": "example.com", "name": "Example"},
                    published_at=NOW,
                ),
            ),
        )


class UnavailableResearchService:
    async def research(self, query: str) -> ResearchResult:
        raise ResearchUnavailable("OPENROUTER_API_KEY is not configured")


class InvalidCitationResearchService:
    async def research(self, query: str) -> ResearchResult:
        raise InvalidResearchCitation("Citation A99 is not present in retrieved evidence")


class EmptyResearchService:
    async def research(self, query: str) -> ResearchResult:
        return None  # type: ignore[return-value]


class StreamingResearchService:
    def start_stream(self, query: str, *, thread_id: str) -> ResearchStreamSession:
        assert query == "stream me"
        UUID(thread_id)

        async def events():
            yield ResearchStreamEvent(
                "start", {"request_id": "req-stream", "thread_id": thread_id}
            )
            yield ResearchStreamEvent("delta", {"text": "Streaming answer."})
            yield ResearchStreamEvent("citations", {"citations": ()})
            yield ResearchStreamEvent(
                "done", {"request_id": "req-stream", "thread_id": thread_id}
            )

        return ResearchStreamSession("req-stream", thread_id, events())

    async def delete_thread(self, thread_id: str) -> None:
        UUID(thread_id)


class InvalidCitationStreamingResearchService:
    def start_stream(self, query: str, *, thread_id: str) -> ResearchStreamSession:
        async def events():
            yield ResearchStreamEvent(
                "start", {"request_id": "req-invalid", "thread_id": thread_id}
            )
            yield ResearchStreamEvent("delta", {"text": "Unsupported [A99]"})
            raise InvalidResearchCitation(
                "Research model returned an invalid citation",
                request_id="req-invalid",
                reason="Citation A99 is not present in retrieved evidence",
            )

        return ResearchStreamSession("req-invalid", thread_id, events())


class UnavailableStreamingResearchService:
    def start_stream(self, query: str, *, thread_id: str) -> ResearchStreamSession:
        raise ResearchUnavailable(
            "OPENROUTER_API_KEY is not configured",
            request_id="req-unavailable",
            error_type="ConfigurationError",
        )


def test_research_api_trims_input_and_returns_structured_citations() -> None:
    app = create_app()
    app.dependency_overrides[get_research_service] = FakeResearchService
    response = TestClient(app).post("/api/research", json={"query": "  trim me  "})
    assert response.status_code == 200
    assert response.json() == {
        "answer": "Supported claim. [A7]",
        "citations": [
            {
                "article_id": 7,
                "title": "Evidence title",
                "url": "https://example.com/evidence",
                "source": {"slug": "example.com", "name": "Example"},
                "published_at": "2026-09-10T00:00:00Z",
            }
        ],
    }


def test_research_api_rejects_blank_query_and_reports_unavailable_agent() -> None:
    app = create_app()
    app.dependency_overrides[get_research_service] = UnavailableResearchService
    client = TestClient(app)
    assert client.post("/api/research", json={"query": "   "}).status_code == 422
    response = client.post("/api/research", json={"query": "news"})
    assert response.status_code == 503
    assert "OPENROUTER_API_KEY" in response.json()["detail"]["reason"]


def test_research_api_maps_invalid_model_citation_to_bad_gateway() -> None:
    app = create_app()
    app.dependency_overrides[get_research_service] = InvalidCitationResearchService
    response = TestClient(app).post("/api/research", json={"query": "news"})
    assert response.status_code == 502
    assert "A99" in response.json()["detail"]["reason"]


def test_research_api_maps_empty_service_result_to_diagnostic_bad_gateway() -> None:
    app = create_app()
    app.dependency_overrides[get_research_service] = EmptyResearchService
    response = TestClient(app).post("/api/research", json={"query": "news"})
    assert response.status_code == 502
    assert response.json()["detail"]["error_type"] == "EmptyServiceResult"
    assert response.json()["detail"]["request_id"] == response.headers["x-request-id"]


def test_research_stream_api_returns_sse_and_request_id() -> None:
    app = create_app()
    app.dependency_overrides[get_research_service] = StreamingResearchService
    thread_id = "00000000-0000-4000-8000-000000000003"
    response = TestClient(app).post(
        "/api/research/stream",
        json={"query": "stream me", "thread_id": thread_id},
        headers={"accept": "text/event-stream"},
    )
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/event-stream")
    assert response.headers["x-request-id"] == "req-stream"
    assert "event: start\ndata:" in response.text
    assert "event: delta\ndata: {\"text\":\"Streaming answer.\"}" in response.text
    assert "event: done\ndata:" in response.text


def test_research_stream_rejects_missing_key_before_opening_stream() -> None:
    app = create_app()
    app.dependency_overrides[get_research_service] = (
        UnavailableStreamingResearchService
    )
    response = TestClient(app).post(
        "/api/research/stream",
        json={
            "query": "stream me",
            "thread_id": "00000000-0000-4000-8000-000000000003",
        },
    )
    assert response.status_code == 503
    assert response.headers["x-request-id"] == "req-unavailable"


def test_research_stream_resets_unvalidated_draft_before_error() -> None:
    app = create_app()
    app.dependency_overrides[get_research_service] = (
        InvalidCitationStreamingResearchService
    )
    response = TestClient(app).post(
        "/api/research/stream",
        json={
            "query": "stream me",
            "thread_id": "00000000-0000-4000-8000-000000000003",
        },
    )
    assert response.status_code == 200
    assert response.text.index("event: delta") < response.text.index("event: reset")
    assert response.text.index("event: reset") < response.text.index("event: error")
    assert "Citation A99 is not present" in response.text
    assert "event: done" not in response.text


def test_closing_sse_iterator_cancels_research_producer() -> None:
    async def run() -> None:
        cancelled = asyncio.Event()

        async def events():
            yield ResearchStreamEvent(
                "start", {"request_id": "req-cancel", "thread_id": "thread-cancel"}
            )
            try:
                await asyncio.Event().wait()
            finally:
                cancelled.set()

        session = ResearchStreamSession(
            "req-cancel", "thread-cancel", events()
        )
        stream = _stream_events(session)
        assert (await anext(stream)).startswith(b"event: start")
        await stream.aclose()
        await asyncio.wait_for(cancelled.wait(), timeout=1)

    asyncio.run(run())


def test_research_thread_delete_uses_validated_uuid() -> None:
    app = create_app()
    app.dependency_overrides[get_research_service] = StreamingResearchService
    response = TestClient(app).delete(
        "/api/research/threads/00000000-0000-4000-8000-000000000003"
    )
    assert response.status_code == 204
    assert TestClient(app).delete("/api/research/threads/not-a-uuid").status_code == 422
