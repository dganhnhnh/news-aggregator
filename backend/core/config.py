"""Environment-backed settings used by pipelines and services."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

from core.html_content import MIN_FULL_CONTENT_CHARS


load_dotenv()


# Identify honestly. Claiming to be a well-known crawler we are not gets us
# blocked by the bot filters in front of many feeds — Slashdot/Feedburner
# answer a Googlebot-shaped User-Agent from a non-Google IP with 403 — and
# those 403s then count towards `--disable-after-failures` until a perfectly
# good feed is disabled. Lives here rather than in the adapter so `Settings`
# can default to it without `core` importing `adapters`.
DEFAULT_USER_AGENT = "news-radar/1.0"


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer, got {raw!r}") from exc


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    try:
        return float(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be a number, got {raw!r}") from exc


@dataclass(frozen=True)
class Settings:
    tavily_api_key: str | None
    log_level: str
    database_url: str | None
    rss_feed_path: Path | None
    max_feeds: int
    max_articles_per_feed: int
    max_concurrent_feeds: int
    max_concurrent_crawls: int
    crawl_batch_size: int
    feed_offset: int
    max_retries_per_run: int
    service_interval_seconds: int
    instant_feed_poll_interval_seconds: int = 30
    feed_fetch_timeout_seconds: float = 8.0
    feed_poll_fast_interval_seconds: int = 300
    feed_poll_interval_seconds: int = 1800
    feed_poll_slow_interval_seconds: int = 21600
    feed_poll_min_gaps: int = 3
    feed_poll_trim_fraction: float = 0.1
    feed_poll_future_tolerance_seconds: float = 300.0
    feed_disable_after_failures: int = 10
    # A feed whose newest entry (as observed in its own body) is older than
    # this is retired outright: a short publication gap between its last few
    # posts otherwise keeps it on a fast adaptive tier forever, because that
    # tier is chosen from gaps between entries, not from gap-to-now.
    feed_stale_after_days: int = 365
    # Per-host politeness. max_concurrent_crawls bounds total browser load;
    # these bound how hard any single publisher is hit, which is what actually
    # trips rate limits and bot filters.
    max_crawls_per_host: int = 1
    crawl_delay_per_host_seconds: float = 1.0
    # Minimum rendered length for a feed's own article body to be stored
    # instead of crawling the page. 0 disables the substitution entirely.
    feed_content_min_chars: int = MIN_FULL_CONTENT_CHARS
    # How we identify to feed hosts. Set this to something with your own
    # contact URL if a publisher needs to reach you about crawl volume.
    feed_user_agent: str = DEFAULT_USER_AGENT
    embed_batch_size: int = 32
    cluster_batch_size: int = 50
    cluster_window_hours: int = 72
    cluster_candidate_limit: int = 20
    cluster_threshold: float = 0.75
    cluster_merge_threshold: float = 0.85
    classify_batch_size: int = 50
    # A run checkpoints (and so heartbeats) after every chunk, so this measures
    # silence between chunks rather than total run length.
    worker_heartbeat_timeout_seconds: int = 900
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    cors_allow_origins: tuple[str, ...] = ()
    api_default_window_days: int = 30
    admin_api_host: str = "0.0.0.0"
    admin_api_port: int = 8001
    openrouter_api_key: str | None = None
    news_agent_model: str = "nvidia/nemotron-3.5-lightning:free"
    openrouter_site_url: str | None = None
    openrouter_timeout_seconds: float = 60.0
    news_agent_run_timeout_seconds: float = 90.0

    @classmethod
    def from_env(cls) -> "Settings":
        feed_path = (
            os.getenv("RSS_FEED_PATH", "").strip()
            or os.getenv("GOOGLE_NEWS_TSV_PATH", "").strip()
        )
        cors_origins = tuple(
            origin.strip()
            for origin in os.getenv("CORS_ALLOW_ORIGINS", "").split(",")
            if origin.strip()
        )
        return cls(
            tavily_api_key=os.getenv("TAVILY_API_KEY") or None,
            log_level=os.getenv("LOG_LEVEL", "INFO").upper(),
            database_url=os.getenv("DATABASE_URL", "").strip() or None,
            rss_feed_path=Path(feed_path) if feed_path else None,
            max_feeds=_env_int("MAX_FEEDS", 0),
            max_articles_per_feed=_env_int("MAX_ARTICLES_PER_FEED", 20),
            max_concurrent_feeds=_env_int("MAX_CONCURRENT_FEEDS", 8),
            max_concurrent_crawls=_env_int("MAX_CONCURRENT_CRAWLS", 6),
            crawl_batch_size=_env_int("CRAWL_BATCH_SIZE", 10),
            feed_offset=_env_int("RSS_FEED_OFFSET", 0),
            max_retries_per_run=_env_int("MAX_RETRIES_PER_RUN", 50),
            service_interval_seconds=_env_int("SERVICE_INTERVAL_SECONDS", 1800),
            instant_feed_poll_interval_seconds=_env_int(
                "INSTANT_FEED_POLL_INTERVAL_SECONDS", 30
            ),
            feed_fetch_timeout_seconds=_env_float(
                "FEED_FETCH_TIMEOUT_SECONDS", 8.0
            ),
            feed_poll_fast_interval_seconds=_env_int(
                "FEED_POLL_FAST_INTERVAL_SECONDS", 300
            ),
            feed_poll_interval_seconds=_env_int("FEED_POLL_INTERVAL_SECONDS", 1800),
            feed_poll_slow_interval_seconds=_env_int(
                "FEED_POLL_SLOW_INTERVAL_SECONDS", 21600
            ),
            feed_poll_min_gaps=_env_int("FEED_POLL_MIN_GAPS", 3),
            feed_poll_trim_fraction=_env_float("FEED_POLL_TRIM_FRACTION", 0.1),
            feed_poll_future_tolerance_seconds=_env_float(
                "FEED_POLL_FUTURE_TOLERANCE_SECONDS", 300.0
            ),
            feed_disable_after_failures=_env_int("FEED_DISABLE_AFTER_FAILURES", 10),
            feed_stale_after_days=_env_int("FEED_STALE_AFTER_DAYS", 365),
            max_crawls_per_host=_env_int("MAX_CRAWLS_PER_HOST", 1),
            crawl_delay_per_host_seconds=_env_float(
                "CRAWL_DELAY_PER_HOST_SECONDS", 1.0
            ),
            feed_content_min_chars=_env_int(
                "FEED_CONTENT_MIN_CHARS", MIN_FULL_CONTENT_CHARS
            ),
            feed_user_agent=(
                os.getenv("FEED_USER_AGENT", "").strip() or DEFAULT_USER_AGENT
            ),
            embed_batch_size=_env_int("EMBED_BATCH_SIZE", 32),
            cluster_batch_size=_env_int("CLUSTER_BATCH_SIZE", 50),
            cluster_window_hours=_env_int("CLUSTER_WINDOW_HOURS", 72),
            cluster_candidate_limit=_env_int("CLUSTER_CANDIDATE_LIMIT", 20),
            cluster_threshold=_env_float("CLUSTER_THRESHOLD", 0.75),
            cluster_merge_threshold=_env_float("CLUSTER_MERGE_THRESHOLD", 0.85),
            classify_batch_size=_env_int("CLASSIFY_BATCH_SIZE", 50),
            worker_heartbeat_timeout_seconds=_env_int(
                "WORKER_HEARTBEAT_TIMEOUT_SECONDS", 900
            ),
            api_host=os.getenv("API_HOST", "0.0.0.0"),
            api_port=_env_int("API_PORT", 8000),
            cors_allow_origins=cors_origins,
            api_default_window_days=_env_int("API_DEFAULT_WINDOW_DAYS", 30),
            admin_api_host=os.getenv("ADMIN_API_HOST", "0.0.0.0"),
            admin_api_port=_env_int("ADMIN_API_PORT", 8001),
            openrouter_api_key=os.getenv("OPENROUTER_API_KEY", "").strip() or None,
            news_agent_model=(
                os.getenv("OPENROUTER_MODEL", "").strip()
                or "nvidia/nemotron-3.5-lightning:free"
            ),
            openrouter_site_url=(
                os.getenv("OPENROUTER_SITE_URL", "").strip() or None
            ),
            openrouter_timeout_seconds=_env_float(
                "OPENROUTER_TIMEOUT_SECONDS", 60.0
            ),
            news_agent_run_timeout_seconds=_env_float(
                "NEWS_AGENT_RUN_TIMEOUT_SECONDS", 90.0
            ),
        )

    @property
    def google_news_tsv_path(self) -> Path | None:
        """Backward-compatible alias for the former TSV-only setting."""
        return self.rss_feed_path
