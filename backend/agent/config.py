"""OpenRouter chat-model construction for the research agent."""

from __future__ import annotations

OPENROUTER_BASE_URL = "https://openrouter.ai/api/v1"
OPENROUTER_TITLE = "News Radar"
MODEL_TIMEOUT_SECONDS = 60
MODEL_MAX_RETRIES = 2
RUN_TIMEOUT_SECONDS = 90
RECURSION_LIMIT = 20


def build_openrouter_model(*, api_key: str, model: str, site_url: str | None):
    """Build the OpenAI-compatible LangChain client used through OpenRouter."""
    from langchain_openai import ChatOpenAI

    headers = {"X-OpenRouter-Title": OPENROUTER_TITLE}
    if site_url:
        headers["HTTP-Referer"] = site_url
    return ChatOpenAI(
        model=model,
        base_url=OPENROUTER_BASE_URL,
        api_key=api_key,
        temperature=0,
        timeout=MODEL_TIMEOUT_SECONDS,
        max_retries=MODEL_MAX_RETRIES,
        streaming=False,
        default_headers=headers,
        max_tokens=3000,
    )
