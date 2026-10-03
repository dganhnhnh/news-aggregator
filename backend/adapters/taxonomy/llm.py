"""Use the same configured provider as the news agent, without agent tools."""
import asyncio
from agent.config import build_openrouter_model
from core.config import Settings
from services.taxonomy_discovery import discovery_messages, parse_proposal


async def discover_topics(stories, registry, *, settings=None):
    settings = settings or Settings.from_env()
    if not settings.openrouter_api_key:
        raise ValueError("Taxonomy discovery requires OPENROUTER_API_KEY")
    model = build_openrouter_model(
        api_key=settings.openrouter_api_key, model=settings.news_agent_model,
        site_url=settings.openrouter_site_url, base_url=settings.openrouter_base_url,
        extra_body={**settings.openrouter_extra_body, **settings.taxonomy_llm_extra_body},
        timeout_seconds=settings.openrouter_timeout_seconds,
        # A scheduled run can retry later; avoid bursts of automatic API retries.
        max_retries=0,
    )
    async with asyncio.timeout(settings.openrouter_timeout_seconds * 3):
        response = await model.ainvoke(discovery_messages(stories, registry))
    content = response.content
    if isinstance(content, list):
        content = "".join(x.get("text", "") for x in content if isinstance(x, dict))
    if not content.strip():
        raise ValueError(f"LLM returned no JSON content (finish_reason={response.response_metadata.get('finish_reason')!r}); configure TAXONOMY_LLM_EXTRA_BODY for this provider")
    return parse_proposal(content)
