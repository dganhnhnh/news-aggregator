"""Conservative lexical topic evidence shared by discovery and ranking.

Aliases expand concepts, never identify publishers. Unknown queries require all
meaningful words, so one common word cannot satisfy a multi-word topic.
"""
from __future__ import annotations

import re
from urllib.parse import unquote, urlsplit

_TOKENS = re.compile(r"[\w]+", re.UNICODE)
_STOP = frozenset("a an and about for from in of on or the to with news feed feeds rss atom".split())
_CONCEPTS = (
    (frozenset({"finance", "financial"}), (
        "finance", "financial", "investing", "investment", "investments", "banking",
        "stock market", "stocks", "equities", "monetary policy", "personal finance",
        "wealth management", "capital markets", "fixed income", "fintech",
        "stock", "investor", "investors", "financials", "portfolio", "portfolios",
        "bonds", "dividends", "earnings", "retirement", "pension", "savings",
        "interest rates", "central bank", "economy", "economics", "mortgage",
    )),
    (frozenset({"artificial intelligence", "ai"}), (
        "artificial intelligence", "ai", "machine learning", "deep learning",
        "large language model", "large language models", "llm", "llms",
        "generative ai", "neural network", "neural networks",
        "transformer architecture", "transformer models", "text generation",
        "retrieval augmented generation", "diffusion model", "diffusion models",
        "gpu inference", "model inference", "reinforcement learning",
        "natural language processing", "computer vision",
    )),
)


def tokens(text: str) -> set[str]:
    return set(_TOKENS.findall(unquote(text).casefold()))


def _url_path(match: re.Match) -> str:
    try:
        return urlsplit(match[0]).path
    except ValueError:
        # Article summaries sometimes contain redacted or malformed URLs.
        return " "


def matches_topic(query: str, text: str) -> bool:
    wanted = tokens(query) - _STOP
    if not wanted:
        return False
    # Hosts, especially .ai domains, are not editorial topic evidence.
    text = re.sub(r"https?://[^\s]+", _url_path, text)
    normalized_text = " " + " ".join(_TOKENS.findall(unquote(text).casefold())) + " "
    for names, aliases in _CONCEPTS:
        if any(wanted == tokens(name) for name in names):
            return any(f" {alias} " in normalized_text for alias in aliases)
    return wanted <= tokens(text)
