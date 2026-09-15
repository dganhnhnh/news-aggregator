"""
Search provider adapter — Tavily implementation + coordinator.

Domain models (SearchResult, Searcher ABC, SearchError) live in
backend/models/search_and_crawl_models.py so they can be imported without
pulling in any I/O dependencies.
"""
import os
import time
import uuid
from typing import Optional

from tavily import AsyncTavilyClient

from core.logger import get_logger
from models.search_and_crawl_models import (
    SearchProviderError,
    SearchResult,
    Searcher,
)
from adapters.retrieve_news_data.helpers import _parse_datetime, extract_date_from_result

logger = get_logger(__name__)


# ─── Adapter: Tavily ──────────────────────────────────────────────────────────

class TavilySearcher(Searcher):
    """
    Tavily-powered implementation of the Searcher port.

    Tavily is configured primarily via the TAVILY_API_KEY environment
    variable (or the explicit api_key constructor argument).
    """

    name = "tavily"

    def __init__(self, api_key: Optional[str] = None) -> None:
        resolved = api_key or os.getenv("TAVILY_API_KEY")
        if not resolved:
            raise SearchProviderError(
                "Missing TAVILY_API_KEY — set it in .env or pass api_key"
            )
        self._client = AsyncTavilyClient(api_key=resolved)
        self._logger = get_logger(__name__)

    # ── search ────────────────────────────────────────────────────────────────

    async def search(
        self,
        query: str,
        max_results: int = 10,
        *,
        search_depth: str = "basic",
        topic: str = "general",
        country: str | None = None,
    ) -> list[SearchResult]:
        t0 = time.monotonic()
        try:
            raw = await self._client.search(
                query=query,
                max_results=max_results,
                search_depth=search_depth,
                topic=topic,
                country=country,
            )
            elapsed = time.monotonic() - t0
            results = [self._normalize(item) for item in raw.get("results", [])]
            self._logger.info(
                "[Tavily.search] q=%r results=%d time=%.3fs",
                query,
                len(results),
                elapsed,
            )
            return results
        except Exception as exc:
            self._logger.error("[Tavily.search] failed: %s", exc)
            raise SearchProviderError(f"Tavily search failed: {exc}") from exc

    # ── search_and_extract ────────────────────────────────────────────────────

    async def search_and_extract(
        self, query: str, max_results: int = 5
    ) -> list[SearchResult]:
        # Phase 1: discover URLs
        search_results = await self.search(query=query, max_results=max_results)
        urls = [r.url for r in search_results if r.url]

        if not urls:
            self._logger.warning(
                "[Tavily.search_and_extract] no URLs discovered for q=%r", query
            )
            return []

        # Phase 2: extract full content from each URL
        t0 = time.monotonic()
        try:
            raw = await self._client.extract(urls)
            elapsed = time.monotonic() - t0
            self._logger.info(
                "[Tavily.extract] urls=%d time=%.3fs",
                len(urls),
                elapsed,
            )
        except Exception as exc:
            self._logger.error("[Tavily.extract] failed: %s", exc)
            raise SearchProviderError(f"Tavily extract failed: {exc}") from exc

        # Phase 3: merge extracted content back into SearchResult objects
        url_to_content: dict[str, str] = {}
        for item in raw.get("results", []):
            url = item.get("url") or ""
            content = (item.get("content") or "").strip()
            if url and content:
                url_to_content[url] = content

        enriched: list[SearchResult] = []
        for result in search_results:
            enriched_content = url_to_content.get(result.url, result.content_snippet)
            enriched.append(
                SearchResult(
                    id=result.id,
                    url=result.url,
                    title=result.title,
                    content_snippet=enriched_content,
                    published_date=result.published_date,
                    score=result.score,
                    source=self.name,
                    metadata=result.metadata,
                )
            )

        self._logger.info(
            "[Tavily.search_and_extract] q=%r enriched=%d/%d",
            query,
            len(enriched),
            len(search_results),
        )
        return enriched

    # ── internal ──────────────────────────────────────────────────────────────

    @staticmethod
    def _normalize(raw: dict) -> SearchResult:
        """Map a raw Tavily dict into a SearchResult."""
        title = (raw.get("title") or "").strip()
        url = raw.get("url") or ""
        content = (raw.get("content") or "").strip()
        
        raw_published = _parse_datetime(raw.get("published_date"))
        published_date = extract_date_from_result(
            published_date=raw_published,
            title=title,
            url=url,
            content_snippet=content,
        )

        return SearchResult(
            id=str(uuid.uuid4()),
            url=url,
            title=title,
            content_snippet=content,
            published_date=published_date,
            score=raw.get("score"),
            source="tavily",
        )


# ─── Coordinator ──────────────────────────────────────────────────────────────

class SearchFetcher:
    """
    Coordinator that tries configured search providers in order and
    returns the first non-empty result.

    Usage:
        fetcher = SearchFetcher()           # uses Tavily by default
        results = await fetcher.fetch("gold price analysis", max_results=5)
    """

    def __init__(self, sources: Optional[list[Searcher]] = None) -> None:
        self._sources: list[Searcher] = sources or [_default_tavily()]

    async def fetch(
        self,
        query: str,
        max_results: int = 10,
        *,
        extract: bool = False,
    ) -> list[SearchResult]:
        """
        Fetch search results, trying each provider until one succeeds.

        Args:
            query: Search query string.
            max_results: How many results to request per provider.
            extract: If True, call search_and_extract instead of plain search.

        Returns:
            List of SearchResult from the first provider that returns results.
            Empty list if all providers fail.
        """
        errors: list[str] = []

        for source in self._sources:
            try:
                if extract:
                    results = await source.search_and_extract(query, max_results=max_results)
                else:
                    results = await source.search(query, max_results=max_results)

                if results:
                    logger.info(
                        "Search provider '%s' returned %d results for q=%r",
                        source.name,
                        len(results),
                        query,
                    )
                    return results

                logger.warning("Search provider '%s' returned no results", source.name)
            except Exception as exc:
                errors.append(f"{source.name}: {exc}")
                logger.warning("Search provider failed (%s): %s", source.name, exc)

        if errors:
            logger.error("All search providers failed: %s", " | ".join(errors))
        return []


# ─── Helpers ─────────────────────────────────────────────────────────────────

def _default_tavily() -> TavilySearcher:
    """Create a TavilySearcher, raising a clear error if the key is missing."""
    if not os.getenv("TAVILY_API_KEY"):
        raise SearchProviderError(
            "TAVILY_API_KEY is not set. "
            "Either set it in .env or pass api_key to TavilySearcher()."
        )
    return TavilySearcher()
