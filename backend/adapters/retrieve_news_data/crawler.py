"""
Crawl provider adapter — crawl4ai implementation + coordinator.

Domain models (CrawlResult, CrawlerProvider ABC, CrawlError) live in
backend/models/search_and_crawl_models.py so they can be imported without
pulling in any I/O dependencies.
"""

import asyncio
from typing import Optional
from urllib.parse import urlsplit

from crawl4ai import (
    AsyncWebCrawler,
    BrowserConfig,
    CrawlerRunConfig,
    SemaphoreDispatcher,
)

from core.logger import get_logger
from core.urls import URL_ERROR_CONTEXT_KEY, UnusableUrl, resolve_crawl_url

from models.search_and_crawl_models import (
    CrawlError,
    CrawlResult,
    CrawlMetadata,
    CrawlerProvider,
)

logger = get_logger(__name__)


# ─── Internal helpers ────────────────────────────────────────────────────────


def _host_waves(
    items: list[tuple[int, str]],
    max_per_host: int,
) -> list[list[tuple[int, str]]]:
    """Split (position, url) pairs into waves capped at `max_per_host` per host.

    The n-th URL of a host lands in wave n // max_per_host, so a wave never
    contains more than `max_per_host` requests to any one host and repeats of a
    host are always separated by a wave boundary. When every URL has a distinct
    host — the usual case — this returns a single wave and changes nothing.
    """
    seen_per_host: dict[str, int] = {}
    waves: list[list[tuple[int, str]]] = []
    for index, url in items:
        host = urlsplit(url).hostname or ""
        position = seen_per_host.get(host, 0)
        seen_per_host[host] = position + 1
        wave_number = position // max_per_host
        while len(waves) <= wave_number:
            waves.append([])
        waves[wave_number].append((index, url))
    return waves


def _rejected_result(url: str, exc: UnusableUrl) -> CrawlResult:
    """A failed CrawlResult for a URL that never reached the browser."""
    result = CrawlResult(
        url=url,
        markdown="",
        success=False,
        error_message=str(exc),
    )
    result.context["requested_url"] = url
    result.context[URL_ERROR_CONTEXT_KEY] = exc.reason
    return result


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

    # crawl4ai 0.8.x may return a StringCompatibleMarkdown wrapper that behaves
    # like text but still contains a non-JSON-serializable generation object.
    raw_markdown = str(raw_markdown)

    effective_success = success and bool(raw_markdown.strip())
    error_message = getattr(inner, "error_message", "") or ""
    if success and not effective_success:
        error_message = "Crawler returned empty Markdown content"

    final_url = (
        getattr(inner, "redirected_url", None)
        or getattr(inner, "url", None)
        or url
    )
    result = CrawlResult(
        url=final_url,
        markdown=raw_markdown,
        html=getattr(inner, "cleaned_html", "") or "",
        metadata=CrawlMetadata(
            title=raw_meta.get("title") if isinstance(raw_meta, dict) else None,
            description=raw_meta.get("description") if isinstance(raw_meta, dict) else None,
            language=raw_meta.get("language") if isinstance(raw_meta, dict) else None,
            author=raw_meta.get("author") if isinstance(raw_meta, dict) else None,
            provider="crawl4ai",
        ),
        success=effective_success,
        error_message=error_message,
    )
    result.context["requested_url"] = url
    return result


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
        *,
        max_per_host: int = 1,
        per_host_delay_seconds: float = 1.0,
    ) -> None:
        self._browser_config = browser_config or BrowserConfig()
        # Default: extract plain text/markdown, skip raw HTML
        self._run_config = run_config or CrawlerRunConfig(only_text=True)
        # A global concurrency cap says nothing about how hard one publisher is
        # hit: ten URLs from one feed are usually ten URLs from one host, which
        # is what trips rate limits and bot filters. These bound per-host load.
        self._max_per_host = max(1, max_per_host)
        self._per_host_delay_seconds = max(0.0, per_host_delay_seconds)
        self._logger = get_logger(__name__)

    # ── crawl ─────────────────────────────────────────────────────────────────

    async def crawl(self, url: str) -> CrawlResult:
        try:
            url = resolve_crawl_url(url)
        except UnusableUrl as exc:
            self._logger.warning("[crawl4ai] REJECT — %s", exc)
            return _rejected_result(url, exc)

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
        if not result.error_message:
            result.error_message = error_msg

        if result.success:
            self._logger.info(
                "[crawl4ai] OK — %s — %d chars",
                url,
                result.content_length,
            )
        else:
            self._logger.warning(
                "[crawl4ai] FAIL — %s: %s", url, result.error_message
            )

        return result

    # ── crawl_batch ─────────────────────────────────────────────────────────

    async def crawl_batch(
        self,
        urls: list[str],
        *,
        max_concurrency: int | None = None,
    ) -> list[CrawlResult]:
        """
        Crawl multiple URLs concurrently using a shared browser instance.

        Uses crawl4ai's native batch runner so all URLs are processed
        in the same browser session.
        """
        if not urls:
            return []

        # Callers zip this list against their own inputs, so it must stay the
        # same length and order as `urls` even when some are never fetched.
        crawlable: list[tuple[int, str]] = []
        completed: dict[int, CrawlResult] = {}
        for index, url in enumerate(urls):
            try:
                crawlable.append((index, resolve_crawl_url(url)))
            except UnusableUrl as exc:
                self._logger.warning("[crawl4ai.batch] REJECT — %s", exc)
                completed[index] = _rejected_result(url, exc)

        if crawlable:
            waves = _host_waves(crawlable, self._max_per_host)
            self._logger.info(
                "[crawl4ai.batch] crawling %d URLs in %d wave(s)",
                len(crawlable),
                len(waves),
            )
            dispatcher = (
                SemaphoreDispatcher(semaphore_count=max_concurrency)
                if max_concurrency is not None
                else None
            )
            async with AsyncWebCrawler(config=self._browser_config) as crawler:
                for wave_number, wave in enumerate(waves):
                    if wave_number and self._per_host_delay_seconds:
                        # Consecutive waves only ever hold repeats of the same
                        # host, so this delay is exactly the per-host politeness
                        # gap and costs nothing when hosts are all distinct.
                        await asyncio.sleep(self._per_host_delay_seconds)
                    containers = await crawler.arun_many(
                        urls=[url for _, url in wave],
                        config=self._run_config,
                        dispatcher=dispatcher,
                    )
                    for (index, _), container in zip(wave, containers):
                        inner, container_url = _unwrap_result(container)
                        success = bool(getattr(inner, "success", False))
                        error_msg = (
                            getattr(inner, "error_message", "") or
                            ("" if success else "unknown crawl4ai error")
                        )
                        result = _build_crawl_result(
                            url=container_url, inner=inner, success=success
                        )
                        if not result.error_message:
                            result.error_message = error_msg
                        completed[index] = result

        results: list[CrawlResult] = []
        for index, url in enumerate(urls):
            result = completed.get(index)
            if result is None:
                # crawl4ai returned fewer containers than it was given; a
                # missing result is still a result the caller must account for.
                result = CrawlResult(
                    url=url,
                    markdown="",
                    success=False,
                    error_message="Crawler returned no result for this URL",
                )
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
