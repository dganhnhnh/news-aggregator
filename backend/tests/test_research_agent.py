from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from uuid import UUID

import pytest
from fastapi.testclient import TestClient
from langchain_core.language_models.chat_models import BaseChatModel
from langchain_core.messages import AIMessage, AIMessageChunk, ToolMessage
from langchain_core.outputs import ChatGeneration, ChatGenerationChunk, ChatResult

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
    _contains_exception,
    _message_delta_text,
    _message_reasoning_text,
    _tool_result_event,
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
        "tool_call",
        "tool_result",
        "status",
        "delta",
        "citations",
        "done",
    ]
    assert events[1].data == {"phase": "searching", "tool": "search_articles"}
    assert events[2].data == {
        "id": "call-stream", "tool": "search_articles", 'input': '{"query": "chip policy"}',
    }
    assert events[3].data["id"] == "call-stream"
    assert events[3].data["status"] == "completed"
    assert events[5].data == {"text": "The policy changed. [A1]"}
    assert events[6].data["citations"][0].article_id == 1


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
            AIMessage(content="Still stale. [A1]"),
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
    assert configured.session_id == "thread-123"
    assert configured.model_kwargs == {
        "cache_control": {"type": "ephemeral"},
    }
    assert configured.request_timeout == 60_000
    assert configured.reasoning == {"effort": "low"}
    assert model.session_id is None


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


def test_provider_retry_exception_cycles_do_not_break_error_classification() -> None:
    original = ValueError("provider failure")
    wrapper = RuntimeError("retry failed")
    original.__context__ = wrapper
    wrapper.__cause__ = original
    assert not _contains_exception(original, (TimeoutError,))
    assert _contains_exception(wrapper, (ValueError,))


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
            yield ResearchStreamEvent("reasoning", {"id": "r", "text": "Checking sources."})
            yield ResearchStreamEvent("tool_call", {
                "id": "t", "tool": "search_articles", "input": '{"query":"news"}',
            })
            yield ResearchStreamEvent("tool_result", {
                "id": "t", "status": "completed", "output": '{"ok":true}',
            })
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
    assert "event: reasoning\ndata:" in response.text
    assert "event: tool_call\ndata:" in response.text
    assert "event: tool_result\ndata:" in response.text
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


class StreamingModel(ScriptedModel):
    chunks: list[list[AIMessageChunk]]
    finished: bool = False
    finish_gate: asyncio.Event | None = None

    async def _astream(self, messages, stop=None, run_manager=None, **kwargs):
        chunks = self.chunks.pop(0)
        for chunk in chunks:
            yield ChatGenerationChunk(message=chunk)
        if self.finish_gate is not None:
            await self.finish_gate.wait()
        self.finished = not self.chunks


def test_reasoning_and_answer_are_streamed_separately_without_waiting_for_finish():
    model = StreamingModel(responses=[], finish_gate=asyncio.Event(), chunks=[[
        AIMessageChunk(content="", additional_kwargs={"reasoning_content": "Checking"}),
        AIMessageChunk(content="", additional_kwargs={"reasoning_content": " evidence."}),
        AIMessageChunk(content="No"),
        AIMessageChunk(content=" evidence available."),
    ]])
    service = ResearchService(storage=FakeResearchStorage(), model=model)

    async def run():
        events = []
        async with asyncio.timeout(3):
            async for event in service.start_stream("Question", thread_id="reasoning").events:
                if event.event in {"reasoning", "delta"}:
                    assert not model.finished
                if event.event == "delta" and event.data["text"] == " evidence available.":
                    model.finish_gate.set()
                events.append(event)
        return events

    events = asyncio.run(run())
    assert [event.data["text"] for event in events if event.event == "reasoning"] == [
        "Checking", " evidence.",
    ]
    assert [event.data["text"] for event in events if event.event == "delta"] == [
        "No", " evidence available.",
    ]
    assert events[-1].event == "done"


def test_reasoning_excludes_encrypted_blocks_and_is_not_answer_text():
    message = AIMessage(content=[
        {"type": "reasoning", "reasoning": "Check sources."},
        {"type": "reasoning.text", "text": "Compare dates."},
        {"type": "reasoning.encrypted", "data": "secret-signature"},
        {"type": "text", "text": "Final answer."},
    ])
    assert _message_reasoning_text(message) == "Check sources.Compare dates."
    assert _message_delta_text(message) == "Final answer."
    message = AIMessage(content="", additional_kwargs={
        "reasoning_content": "Check sources.",
        "reasoning_details": [{"type": "reasoning.text", "text": "Check sources."}],
    })
    assert _message_reasoning_text(message) == "Check sources."


def test_tool_results_are_bounded_and_report_failures():
    import json

    event = _tool_result_event(ToolMessage(
        content=json.dumps({"ok": False, "content": "private body" * 1000,
                            "error": "article_not_found", "api_key": "sk-abcdefghijk"}),
        tool_call_id="failed-call",
    ))
    assert event.data["status"] == "error"
    assert event.data["id"] == "failed-call"
    assert "private body" not in event.data["output"]
    assert "sk-abcdefghijk" not in event.data["output"]
    assert "article_not_found" in event.data["output"]
    assert len(event.data["output"]) < 2100


def test_parallel_tool_results_stream_before_slowest_tool_finishes():
    release = asyncio.Event()

    class ParallelStorage:
        async def get_article(self, article_id):
            if article_id == 2:
                await release.wait()
            return None

    model = ScriptedModel(responses=[
        AIMessage(content="", tool_calls=[
            {"id": "fast", "name": "get_article", "args": {"article_id": 1}},
            {"id": "slow", "name": "get_article", "args": {"article_id": 2}},
        ]),
        AIMessage(content="Articles unavailable."),
    ])
    service = ResearchService(storage=ParallelStorage(), model=model)

    async def run():
        events = []
        async with asyncio.timeout(3):
            async for event in service.start_stream("Question", thread_id="parallel").events:
                events.append(event)
                if event.event == "tool_result" and event.data["id"] == "fast":
                    release.set()
        return events

    events = asyncio.run(run())
    results = [event for event in events if event.event == "tool_result"]
    assert [event.data["id"] for event in results] == ["fast", "slow"]
    assert all(event.data["status"] == "error" for event in results)


def test_late_tool_call_moves_preamble_to_progress_and_resets_answer():
    model = StreamingModel(responses=[], chunks=[[
        AIMessageChunk(content="Let me check."),
        AIMessageChunk(content="", tool_call_chunks=[{
            "index": 0, "id": "call-late", "name": "search_articles", "args": '{"query":',
        }]),
        AIMessageChunk(content="", tool_call_chunks=[{
            "index": 0, "args": '"chip policy"}',
        }]),
    ], [AIMessageChunk(content="The policy changed. [A1]")]])
    service = ResearchService(storage=FakeResearchStorage(), model=model, embedder=FakeEmbedder())

    async def run():
        return [event async for event in service.start_stream("Question", thread_id="late").events]

    events = asyncio.run(run())
    assert [event.data for event in events if event.event == "reset"] == [{"reason": "tool_call"}]
    assert [event.data["text"] for event in events if event.event == "reasoning"] == ["Let me check."]
    calls = [event for event in events if event.event == "tool_call"]
    assert len(calls) == 1
    assert calls[0].data["input"] == '{"query": "chip policy"}'


def test_openrouter_http_stream_preserves_reasoning_through_tool_loop():
    import json

    import httpx
    from openrouter import OpenRouter

    requests = []

    def handle(request):
        body = json.loads(request.content)
        requests.append(body)
        assert body['session_id'] == 'provider-test'
        assert body['reasoning'] == {'effort': 'low'}
        assert body['cache_control'] == {'type': 'ephemeral'}
        if len(requests) == 1:
            deltas = [
                {'role': 'assistant', 'content': '', 'reasoning': 'Checking sources.',
                 'reasoning_details': [{'type': 'reasoning.text', 'text': 'Checking sources.', 'index': 0}]},
                {'content': '', 'tool_calls': [{'index': 0, 'id': 'provider-call', 'type': 'function',
                                               'function': {'name': 'search_articles', 'arguments': '{"query":"chip policy"}'}}]},
            ]
            finish = 'tool_calls'
        else:
            assistant = next(message for message in body['messages'] if message['role'] == 'assistant')
            assert assistant['reasoning'] == 'Checking sources.'
            assert assistant['reasoning_details'][0]['text'] == 'Checking sources.'
            deltas = [{'role': 'assistant', 'content': 'The policy'}, {'content': ' changed. [A1]'}]
            finish = 'stop'
        chunks = [
            {'id': f'gen-{len(requests)}', 'object': 'chat.completion.chunk', 'created': 1,
             'model': 'anthropic/claude-haiku-4.5',
             'choices': [{'index': 0, 'delta': delta, 'finish_reason': None}]}
            for delta in deltas
        ]
        chunks.append({**chunks[-1], 'choices': [{'index': 0, 'delta': {}, 'finish_reason': finish}]})
        content = ''.join(f'data: {json.dumps(chunk)}\n\n' for chunk in chunks) + 'data: [DONE]\n\n'
        return httpx.Response(200, headers={'content-type': 'text/event-stream'}, text=content)

    async def run():
        async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
            model = build_openrouter_model(api_key='test-key', model='anthropic/claude-haiku-4.5', site_url=None)
            model = model.model_copy(update={'client': OpenRouter(api_key='test-key', async_client=client)})
            service = ResearchService(storage=FakeResearchStorage(), model=model, embedder=FakeEmbedder())
            return [event async for event in service.start_stream('Question', thread_id='provider-test').events]

    events = asyncio.run(run())
    assert len(requests) == 2
    assert [event.data['text'] for event in events if event.event == 'reasoning'] == ['Checking sources.']
    assert ''.join(event.data['text'] for event in events if event.event == 'delta') == 'The policy changed. [A1]'
    assert len([event for event in events if event.event == 'tool_result']) == 1


@pytest.mark.parametrize('value', ['', '  ', ' https://ark.ap-southeast.bytepluses.com/api/coding/v3/ '])
def test_model_base_url_comes_from_environment(monkeypatch, value):
    from agent.config import DEFAULT_OPENROUTER_BASE_URL

    monkeypatch.setenv('OPENROUTER_BASE_URL', value)
    settings = Settings.from_env()
    expected = value.strip().rstrip('/') or DEFAULT_OPENROUTER_BASE_URL
    assert settings.openrouter_base_url == expected


def test_model_settings_reach_compatible_client(monkeypatch):
    from agent.chat_model import CompatibleChatModel

    monkeypatch.setenv('OPENROUTER_API_KEY', 'byteplus-test-key')
    monkeypatch.setenv('OPENROUTER_BASE_URL', 'https://ark.ap-southeast.bytepluses.com/api/coding/v3')
    monkeypatch.setenv('OPENROUTER_MODEL', 'ark-code-latest')
    monkeypatch.setenv('OPENROUTER_EXTRA_BODY', '{"thinking":{"type":"enabled"}}')
    monkeypatch.setenv('OPENROUTER_TIMEOUT_SECONDS', '12.5')
    service = ResearchService.from_settings(Settings.from_env(), FakeResearchStorage())
    model = service._configured_model('test-thread')
    assert isinstance(model, CompatibleChatModel)
    assert model is service._model
    assert model.model_name == 'ark-code-latest'
    assert model.request_timeout == 12.5
    assert model.extra_body == {'thinking': {'type': 'enabled'}}
    assert model.use_responses_api is False


@pytest.mark.parametrize('value', ['[]', 'null', 'not-json', '1'])
def test_model_extra_body_must_be_json_object(monkeypatch, value):
    monkeypatch.setenv('OPENROUTER_EXTRA_BODY', value)
    with pytest.raises(ValueError, match='OPENROUTER_EXTRA_BODY must be a JSON object'):
        Settings.from_env()


@pytest.mark.parametrize('base_url', ['not-a-url', 'ftp://host/api', 'https://user:pass@host/v1', 'https://host/v1?key=secret'])
def test_model_rejects_invalid_base_urls(base_url):
    with pytest.raises(ValueError, match='OPENROUTER_BASE_URL'):
        build_openrouter_model(api_key='key', model='test', site_url=None, base_url=base_url)


@pytest.mark.parametrize('base_url,model_name', [
    ('https://ark.ap-southeast.bytepluses.com/api/coding/v3/', 'ark-code-latest'),
    ('http://localhost:9999/custom/v1', 'anthropic/local-model'),
])
def test_compatible_http_stream_preserves_reasoning_and_tool_history(base_url, model_name):
    import json
    import httpx
    from openai import AsyncOpenAI

    requests = []

    def handle(request):
        assert str(request.url) == base_url.rstrip('/') + '/chat/completions'
        assert request.headers['authorization'] == 'Bearer compatible-test-key'
        assert 'x-openrouter-title' not in request.headers
        assert 'http-referer' not in request.headers
        body = json.loads(request.content)
        requests.append(body)
        assert body['model'] == model_name
        assert body['stream'] is True
        assert body['max_tokens'] == 6000
        assert body['thinking'] == {'type': 'enabled'}
        assert body['messages'][0]['role'] == 'system'
        for field in ('session_id', 'cache_control', 'reasoning', 'stream_options', 'max_completion_tokens'):
            assert field not in body
        if len(requests) == 1:
            deltas = [
                {'role': 'assistant', 'content': '', 'reasoning_content': 'Checking '},
                {'content': None, 'reasoning_content': 'sources.'},
                {'content': None, 'encrypted_content': 'opaque-provider-token'},
                {'tool_calls': [{'index': 0, 'id': 'compat-call', 'type': 'function',
                                  'function': {'name': 'search_articles', 'arguments': '{"query":'}}]},
                {'tool_calls': [{'index': 0, 'function': {'arguments': '"chip policy"}'}}]},
            ]
            finish = 'tool_calls'
        else:
            assistant = next(message for message in body['messages'] if message['role'] == 'assistant')
            assert assistant['reasoning_content'] == 'Checking sources.'
            assert assistant['encrypted_content'] == 'opaque-provider-token'
            assert assistant['tool_calls'][0]['id'] == 'compat-call'
            assert body['messages'][-1]['tool_call_id'] == 'compat-call'
            deltas = [{'role': 'assistant', 'content': 'The policy'}, {'content': ' changed. [A1]'}]
            finish = 'stop'
        chunks = [
            {'id': f'compat-{len(requests)}', 'object': 'chat.completion.chunk', 'created': 1,
             'model': model_name, 'choices': [{'index': 0, 'delta': delta, 'finish_reason': None}]}
            for delta in deltas
        ]
        chunks.append({**chunks[-1], 'choices': [{'index': 0, 'delta': {}, 'finish_reason': finish}]})
        content = ''.join(f'data: {json.dumps(chunk)}\n\n' for chunk in chunks) + 'data: [DONE]\n\n'
        return httpx.Response(200, headers={'content-type': 'text/event-stream'}, text=content)

    async def run():
        async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
            model = build_openrouter_model(
                api_key='compatible-test-key', model=model_name, base_url=base_url,
                site_url='https://news.example.com', extra_body={'thinking': {'type': 'enabled'}},
            )
            sdk = AsyncOpenAI(api_key='compatible-test-key', base_url=base_url, http_client=client)
            model = model.model_copy(update={'root_async_client': sdk, 'async_client': sdk.chat.completions})
            service = ResearchService(storage=FakeResearchStorage(), model=model, embedder=FakeEmbedder())
            return [event async for event in service.start_stream('Question', thread_id='compat-test').events]

    events = asyncio.run(run())
    assert len(requests) == 2
    assert [event.data['text'] for event in events if event.event == 'reasoning'] == ['Checking ', 'sources.']
    assert ''.join(event.data['text'] for event in events if event.event == 'delta') == 'The policy changed. [A1]'
    assert len([event for event in events if event.event == 'tool_result']) == 1
    assert 'opaque-provider-token' not in str(events)
    assert events[-1].event == 'done'


def test_compatible_nonstreaming_response_preserves_reasoning_extensions():
    import httpx
    from openai import AsyncOpenAI

    def handle(request):
        return httpx.Response(200, json={
            'id': 'nonstream', 'object': 'chat.completion', 'created': 1, 'model': 'local',
            'choices': [{'index': 0, 'finish_reason': 'stop', 'message': {
                'role': 'assistant', 'content': 'Answer.', 'reasoning_content': 'Checked sources.',
                'encrypted_content': 'opaque',
            }}],
        })

    async def run():
        async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
            model = build_openrouter_model(api_key='test-key', model='local', site_url=None,
                                           base_url='https://compatible.example/v1')
            sdk = AsyncOpenAI(api_key='test-key', base_url='https://compatible.example/v1', http_client=client)
            model = model.model_copy(update={
                'streaming': False, 'root_async_client': sdk, 'async_client': sdk.chat.completions,
            })
            return await model.ainvoke('Question')

    message = asyncio.run(run())
    assert message.content == 'Answer.'
    assert message.additional_kwargs['encrypted_content'] == 'opaque'
    assert _message_reasoning_text(message) == 'Checked sources.'


def test_compatible_client_keeps_standard_defaults_without_provider_options():
    from agent.chat_model import CompatibleChatModel

    model = build_openrouter_model(api_key='key', model='local', site_url=None,
                                   base_url='https://compatible.example/v1')
    assert isinstance(model, CompatibleChatModel)
    assert model.extra_body == {}
    assert model.reasoning is None
    assert model.default_headers is None
    assert model.stream_usage is False


def test_citation_repair_rereads_previous_turn_source_before_accepting_it():
    from types import SimpleNamespace

    class Storage(FakeResearchStorage):
        reads = []

        async def get_article(self, article_id):
            self.reads.append(article_id)
            return SimpleNamespace(
                url='https://example.com/chips', url_original='https://example.com/chips',
                title='Chip policy', content='The policy changed.', feed_summary='',
                source_slug='example.com', source_name='Example', published_at=NOW,
                crawled_at=NOW, event_at=NOW, topics=[],
            )

    model = RecordingModel(responses=[
        AIMessage(content='', tool_calls=[{'name': 'search_articles', 'args': {'query': 'chip policy'}, 'id': 'first-search'}]),
        AIMessage(content='First answer. [A1]'),
        AIMessage(content='Unverified follow-up. [A1]'),
        AIMessage(content='', tool_calls=[{'name': 'get_article', 'args': {'article_id': 1}, 'id': 'reread'}]),
        AIMessage(content='Verified follow-up. [A1]'),
    ])
    storage = Storage()
    service = ResearchService(storage=storage, model=model, embedder=FakeEmbedder())

    async def run():
        first = service.start_stream('First question', thread_id='repair-followup')
        _ = [event async for event in first.events]
        second = service.start_stream('Follow-up', thread_id='repair-followup')
        return [event async for event in second.events]

    events = asyncio.run(run())
    assert storage.reads == [1]
    reset_index = next(i for i, event in enumerate(events) if event.event == 'reset')
    assert events[reset_index].data == {'reason': 'citation_repair'}
    assert events[reset_index + 1].data == {'phase': 'verifying'}
    assert next(i for i, event in enumerate(events) if event.event == 'tool_call') > reset_index
    assert [event.data['text'] for event in events[reset_index:] if event.event == 'delta'] == ['Verified follow-up. [A1]']
    assert events[-2].data['citations'][0].article_id == 1
    assert events[-1].event == 'done'
    feedback = model.seen_messages[3][-1].content
    assert 'Currently verified citation IDs: none' in feedback
    assert 'get_article' in feedback


def test_citation_repair_rewrites_using_current_evidence_without_repeating_search():
    model = RecordingModel(responses=[
        AIMessage(content='', tool_calls=[{'name': 'search_articles', 'args': {'query': 'chip policy'}, 'id': 'search'}]),
        AIMessage(content='The policy changed. [A99]'),
        AIMessage(content='The policy changed. [A1]'),
    ])
    service = ResearchService(storage=FakeResearchStorage(), model=model, embedder=FakeEmbedder())
    result = asyncio.run(service.research('Question'))
    assert result.answer == 'The policy changed. [A1]'
    assert [citation.article_id for citation in result.citations] == [1]
    assert len(model.seen_messages) == 3
    feedback = model.seen_messages[-1][-1].content
    assert 'Currently verified citation IDs: [A1]' in feedback
    assert 'not merely delete its citation' in feedback
    tool_result = next(message for message in model.seen_messages[1] if isinstance(message, ToolMessage))
    assert '"citation": "[A1]"' in tool_result.content


def test_citation_repair_stops_after_one_attempt_and_resets_budget_next_turn():
    model = RecordingModel(responses=[
        AIMessage(content='Wrong. [A99]'),
        AIMessage(content='Still wrong. [A99]'),
        AIMessage(content='Wrong again. [A98]'),
        AIMessage(content='I do not have enough evidence to answer.'),
    ])
    service = ResearchService(storage=FakeResearchStorage(), model=model)

    async def run():
        first_events = []
        with pytest.raises(InvalidResearchCitation):
            async for event in service.start_stream('First', thread_id='budget').events:
                first_events.append(event)
        assert len(model.seen_messages) == 2
        assert len([event for event in first_events if event.event == 'reset']) == 1
        assert not any(event.event in {'citations', 'done'} for event in first_events)
        return [event async for event in service.start_stream('Second', thread_id='budget').events]

    events = asyncio.run(run())
    assert len(model.seen_messages) == 4
    assert len([event for event in events if event.event == 'reset']) == 1
    assert events[-1].event == 'done'


def test_citation_repair_shares_original_run_timeout():
    model = StreamingModel(responses=[], chunks=[
        [AIMessageChunk(content='Wrong. [A99]')],
        [AIMessageChunk(content='Correcting')],
    ])
    service = ResearchService(storage=FakeResearchStorage(), model=model, timeout_seconds=0.2)

    async def run():
        events = []
        with pytest.raises(ResearchUpstreamError) as error:
            async for event in service.start_stream('Question', thread_id='repair-timeout').events:
                events.append(event)
                if event.event == 'reset':
                    model.finish_gate = asyncio.Event()
        assert error.value.error_type == 'TimeoutError'
        return events

    events = asyncio.run(run())
    assert any(event.event == 'reset' for event in events)
    assert not any(event.event == 'done' for event in events)
