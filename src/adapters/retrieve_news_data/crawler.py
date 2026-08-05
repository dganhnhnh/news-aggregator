"""
Crawl provider adapter — crawl4ai implementation + coordinator.

Domain models (CrawlResult, CrawlerProvider ABC, CrawlError) live in
src/models/search_and_crawl_models.py so they can be imported without
pulling in any I/O dependencies.
"""

from typing import Optional

from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig

from core.logger import get_logger

from models.search_and_crawl_models import (
    CrawlError,
    CrawlResult,
    CrawlMetadata,
    CrawlerProvider,
)

logger = get_logger(__name__)


# ─── Internal helpers ────────────────────────────────────────────────────────

def _unwrap_result(container) -> tuple[object, str]:
    """
    Unwrap crawl4ai's CrawlResultContainer into the inner result and url.

    crawl4ai 0.8.x wraps every result in a CrawlResultContainer with
    ``.url`` and ``._results[0]`` (the inner Pydantic model).
    """
    results = getattr(container, "_results", None)
    if results:
        return results[0], getattr(container, "url", "")
    return container, getattr(container, "url", "")


def _build_crawl_result(url: str, inner, success: bool) -> CrawlResult:
    """Build a domain CrawlResult from a crawl4ai inner Pydantic result."""
    raw_meta: dict = getattr(inner, "metadata", None) or {}
    markdown_value = getattr(inner, "markdown", "") or ""
    if isinstance(markdown_value, str):
        raw_markdown = markdown_value
    else:
        raw_markdown = (
            getattr(markdown_value, "fit_markdown", None)
            or getattr(markdown_value, "raw_markdown", None)
            or str(markdown_value)
        )

    # Fallback: use cleaned_html if markdown is empty
    if not raw_markdown:
        raw_markdown = getattr(inner, "cleaned_html", "") or ""

    return CrawlResult(
        url=url,
        markdown=raw_markdown,
        html=getattr(inner, "cleaned_html", "") or "",
        metadata=CrawlMetadata(
            title=raw_meta.get("title") if isinstance(raw_meta, dict) else None,
            description=raw_meta.get("description") if isinstance(raw_meta, dict) else None,
            language=None,
            author=raw_meta.get("author") if isinstance(raw_meta, dict) else None,
            provider="crawl4ai",
        ),
        success=success,
        error_message=getattr(inner, "error_message", "") or "",
    )


# ─── Adapter: crawl4ai ───────────────────────────────────────────────────────

class Crawl4aiProvider(CrawlerProvider):
    """
    crawl4ai-powered implementation of the CrawlerProvider port.

    crawl4ai uses a headless Chromium browser (via Playwright / patchright)
    to render JavaScript-heavy pages before extracting content.
    """

    name = "crawl4ai"

    def __init__(
        self,
        browser_config: Optional[BrowserConfig] = None,
        run_config: Optional[CrawlerRunConfig] = None,
    ) -> None:
        self._browser_config = browser_config or BrowserConfig()
        # Default: extract plain text/markdown, skip raw HTML
        self._run_config = run_config or CrawlerRunConfig(only_text=True)
        self._logger = get_logger(__name__)

    # ── crawl ─────────────────────────────────────────────────────────────────

    async def crawl(self, url: str) -> CrawlResult:
        self._logger.debug("[crawl4ai] fetching %s", url)
        try:
            async with AsyncWebCrawler(config=self._browser_config) as crawler:
                container = await crawler.arun(url=url, config=self._run_config)
        except Exception as exc:
            self._logger.error("[crawl4ai] crawl raised for %s: %s", url, exc)
            return CrawlResult(
                url=url,
                markdown="",
                success=False,
                error_message=str(exc),
            )

        inner, _ = _unwrap_result(container)
        success = bool(getattr(inner, "success", False))
        error_msg = (
            getattr(inner, "error_message", "") or
            ("" if success else "unknown crawl4ai error")
        )

        result = _build_crawl_result(url=url, inner=inner, success=success)
        result.error_message = error_msg  # override in case inner had empty string

        if success:
            self._logger.info(
                "[crawl4ai] OK — %s — %d chars",
                url,
                result.content_length,
            )
        else:
            self._logger.warning("[crawl4ai] FAIL — %s: %s", url, error_msg)

        return result

    # ── crawl_batch ─────────────────────────────────────────────────────────

    async def crawl_batch(self, urls: list[str]) -> list[CrawlResult]:
        """
        Crawl multiple URLs concurrently using a shared browser instance.

        Uses crawl4ai's native batch runner so all URLs are processed
        in the same browser session.
        """
        if not urls:
            return []

        self._logger.info("[crawl4ai.batch] crawling %d URLs", len(urls))

        async with AsyncWebCrawler(config=self._browser_config) as crawler:
            containers = await crawler.arun_many(urls=urls, config=self._run_config)

        results: list[CrawlResult] = []
        for container in containers:
            inner, container_url = _unwrap_result(container)
            success = bool(getattr(inner, "success", False))
            error_msg = (
                getattr(inner, "error_message", "") or
                ("" if success else "unknown crawl4ai error")
            )
            result = _build_crawl_result(url=container_url, inner=inner, success=success)
            result.error_message = error_msg
            results.append(result)

        ok_count = sum(1 for r in results if r.success)
        self._logger.info(
            "[crawl4ai.batch] done — %d/%d successful",
            ok_count,
            len(results),
        )
        return results


# ─── Coordinator ───────────────────────────────────────────────────────────────

class CrawlFetcher:
    """
    Coordinator that tries configured crawl providers in order and
    returns results from the first one to succeed.

    Usage:
        fetcher = CrawlFetcher()                      # uses crawl4ai by default
        single = await fetcher.fetch("https://…")
        batch  = await fetcher.fetch_batch(["https://…", "https://…"])
    """

    def __init__(self, sources: Optional[list[CrawlerProvider]] = None) -> None:
        self._sources: list[CrawlerProvider] = sources or [Crawl4aiProvider()]

    async def fetch(self, url: str) -> CrawlResult:
        """Crawl a single URL, trying each provider until one succeeds."""
        errors: list[str] = []

        for source in self._sources:
            try:
                result = await source.crawl(url)
                if result.success:
                    logger.info("Crawl provider '%s' succeeded for %s", source.name, url)
                    return result
                errors.append(f"{source.name}: {result.error_message}")
                logger.warning(
                    "Crawl provider '%s' returned failure for %s: %s",
                    source.name,
                    url,
                    result.error_message,
                )
            except Exception as exc:
                errors.append(f"{source.name}: {exc}")
                logger.warning("Crawl provider failed (%s): %s", source.name, exc)

        raise CrawlError(
            f"All crawl providers failed for {url}. Errors: " + " | ".join(errors)
        )

    async def fetch_batch(self, urls: list[str]) -> list[CrawlResult]:
        """Crawl multiple URLs, trying each provider until one succeeds for all."""
        errors: list[str] = []

        for source in self._sources:
            try:
                results = await source.crawl_batch(urls)
                if all(r.success for r in results):
                    logger.info(
                        "Crawl provider '%s' succeeded for all %d URLs",
                        source.name,
                        len(urls),
                    )
                    return results
                failed = [r.url for r in results if not r.success]
                logger.warning(
                    "Crawl provider '%s' partially failed for: %s",
                    source.name,
                    failed,
                )
                errors.append(f"{source.name}: failed for {len(failed)}/{len(urls)} URLs")
            except Exception as exc:
                errors.append(f"{source.name}: {exc}")
                logger.warning("Crawl provider failed (%s): %s", source.name, exc)

        raise CrawlError(
            f"All crawl providers failed for batch of {len(urls)} URLs. Errors: "
            + " | ".join(errors)
        )
