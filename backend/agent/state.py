"""Typed runtime context and ephemeral evidence state for one research run."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Annotated, Any

from langchain.agents import AgentState

from adapters.storage import PostgresRawStorage
from agent.embedding import QueryEmbedderProtocol


Evidence = dict[str, Any]


def _evidence_key(item: Evidence) -> tuple[str, int]:
    kind = str(item.get("kind", "unknown"))
    identity = item.get("article_id") if kind == "article" else item.get("storyline_id")
    return kind, int(identity or 0)


def merge_evidence(
    current: list[Evidence] | None, additions: list[Evidence] | None
) -> list[Evidence]:
    """Deduplicate evidence and let later, deeper reads enrich search hits."""
    merged: dict[tuple[str, int], Evidence] = {}
    order: list[tuple[str, int]] = []
    for item in [*(current or []), *(additions or [])]:
        key = _evidence_key(item)
        if key not in merged:
            merged[key] = dict(item)
            order.append(key)
            continue
        enriched = dict(merged[key])
        for field, value in item.items():
            if value is not None and value != "" and value != []:
                enriched[field] = value
        merged[key] = enriched
    return [merged[key] for key in order]


class ResearchState(AgentState):
    evidence: Annotated[list[Evidence], merge_evidence]


@dataclass(frozen=True)
class ResearchContext:
    storage: PostgresRawStorage
    embedder: QueryEmbedderProtocol
    now: datetime
    request_id: str = ""
