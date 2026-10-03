"""Chat-model construction for OpenRouter and OpenAI-compatible providers."""

from __future__ import annotations

from urllib.parse import urlsplit

from core.config import DEFAULT_OPENROUTER_BASE_URL

OPENROUTER_TITLE = "News Radar"
MODEL_TIMEOUT_SECONDS = 60
MODEL_MAX_RETRIES = 2
RUN_TIMEOUT_SECONDS = 90
RECURSION_LIMIT = 20


def build_openrouter_model(
    *,
    api_key: str,
    model: str,
    site_url: str | None,
    timeout_seconds: float = MODEL_TIMEOUT_SECONDS,
    base_url: str = DEFAULT_OPENROUTER_BASE_URL,
    extra_body: dict | None = None,
):
    """Keep the existing factory name; the endpoint selects the wire protocol."""
    base_url = base_url.strip().rstrip("/")
    endpoint = urlsplit(base_url)
    if endpoint.scheme not in {"http", "https"} or not endpoint.hostname:
        raise ValueError("OPENROUTER_BASE_URL must be an absolute HTTP(S) base URL")
    if endpoint.query or endpoint.fragment or endpoint.username or endpoint.password:
        raise ValueError("OPENROUTER_BASE_URL must not contain credentials, query or fragment")
    if endpoint.hostname != "openrouter.ai":
        from agent.chat_model import CompatibleChatModel

        return CompatibleChatModel(
            model=model,
            base_url=base_url,
            api_key=api_key,
            timeout=timeout_seconds,
            max_retries=MODEL_MAX_RETRIES,
            streaming=True,
            # Optional usage chunks are not supported by every compatible server.
            stream_usage=False,
            use_responses_api=False,
            max_tokens=6000,
            extra_body=dict(extra_body or {}),
        )

    from langchain_openrouter import ChatOpenRouter

    options = dict(extra_body or {})
    reasoning = options.pop("reasoning", {"effort": "low"})
    headers = {"X-OpenRouter-Title": OPENROUTER_TITLE}
    if site_url:
        headers["HTTP-Referer"] = site_url
    return ChatOpenRouter(
        model=model,
        base_url=base_url,
        api_key=api_key,
        timeout=round(timeout_seconds * 1000),
        max_retries=MODEL_MAX_RETRIES,
        streaming=True,
        stream_usage=True,
        default_headers=headers,
        app_title=OPENROUTER_TITLE,
        app_url=site_url,
        reasoning=reasoning,
        model_kwargs=options,
        max_tokens=6000,
    )
