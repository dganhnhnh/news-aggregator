"""Environment-backed settings used by pipelines and services."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


load_dotenv()


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer, got {raw!r}") from exc


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
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    cors_allow_origins: tuple[str, ...] = ()

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
            max_concurrent_feeds=_env_int("MAX_CONCURRENT_FEEDS", 2),
            max_concurrent_crawls=_env_int("MAX_CONCURRENT_CRAWLS", 2),
            crawl_batch_size=_env_int("CRAWL_BATCH_SIZE", 10),
            feed_offset=_env_int("RSS_FEED_OFFSET", 0),
            max_retries_per_run=_env_int("MAX_RETRIES_PER_RUN", 50),
            service_interval_seconds=_env_int("SERVICE_INTERVAL_SECONDS", 1800),
            api_host=os.getenv("API_HOST", "0.0.0.0"),
            api_port=_env_int("API_PORT", 8000),
            cors_allow_origins=cors_origins,
        )

    @property
    def google_news_tsv_path(self) -> Path | None:
        """Backward-compatible alias for the former TSV-only setting."""
        return self.rss_feed_path
