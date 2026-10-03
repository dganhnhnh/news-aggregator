"""Give the agent one chance to verify or rewrite unsupported citations."""

from typing import Any

from langchain.agents.middleware import after_model
from langchain_core.messages import AIMessage, HumanMessage
from langgraph.runtime import Runtime

from agent.citations import CITATION_RE, available_citation_ids
from agent.state import ResearchContext, ResearchState


@after_model(state_schema=ResearchState, can_jump_to=["model"])
def repair_citations(
    state: ResearchState, runtime: Runtime[ResearchContext],
) -> dict[str, Any] | None:
    message = state["messages"][-1]
    if not isinstance(message, AIMessage) or message.tool_calls:
        return None
    if state.get("citation_repair_attempts", 0) >= 1:
        return None
    allowed = available_citation_ids(state.get("evidence") or [])
    cited = {int(value) for value in CITATION_RE.findall(message.text)}
    missing = sorted(cited - allowed)
    if not missing:
        return None

    runtime.stream_writer({"type": "citation_repair", "article_ids": missing})
    missing_labels = ", ".join(f"[A{value}]" for value in missing)
    allowed_labels = ", ".join(f"[A{value}]" for value in sorted(allowed)) or "none"
    return {
        "citation_repair_attempts": 1,
        "messages": [HumanMessage(content=(
            "[Application citation validation feedback; continue the same user request.]\n"
            f"Your draft cites sources not verified in this turn: {missing_labels}.\n"
            f"Currently verified citation IDs: {allowed_labels}.\n"
            "Sources in earlier conversation turns are not automatically verified for this turn. "
            "Call get_article for missing sources you intend to use, then check that the returned "
            "evidence supports your claims and the user's requested period. Rewrite the full answer "
            "using only supported claims and verified IDs. If a source cannot be retrieved, remove "
            "or explicitly qualify the unsupported claim. Do not merely delete its citation or "
            "substitute an unrelated ID. If evidence is insufficient, say so."
        ))],
        "jump_to": "model",
    }
