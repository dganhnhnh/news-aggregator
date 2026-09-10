from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

import pytest
from fastapi.testclient import TestClient
from langchain_core.language_models.chat_models import BaseChatModel
from langchain_core.messages import AIMessage
from langchain_core.outputs import ChatGeneration, ChatResult

from adapters.storage.postgres import reciprocal_rank_fusion
from agent.graph import build_graph
from agent.service import (
    InvalidResearchCitation,
    ResearchCitation as ServiceCitation,
    ResearchResult,
    ResearchService,
    ResearchUnavailable,
    ResearchUpstreamError,
    citations_from_evidence,
)
from agent.state import ResearchContext, merge_evidence
from api.deps import get_research_service
from api.main import create_app
from models.storage_models import ArticleSearchHit


NOW = datetime(2026, 9, 10, tzinfo=timezone.utc)


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
