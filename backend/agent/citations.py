"""Citation identifiers shared by generation feedback and final validation."""

import re

from agent.state import Evidence


CITATION_RE = re.compile(r"\[A(\d+)\]")


def available_citation_ids(evidence: list[Evidence]) -> set[int]:
    return {
        int(item["article_id"])
        for item in evidence
        if item.get("kind") == "article"
        and item.get("article_id") is not None
        and isinstance(item.get("url"), str)
        and item["url"]
    }
