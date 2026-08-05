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
    google_news_tsv_path: Path | None
    max_articles_per_feed: int
    max_concurrent_crawls: int
    service_interval_seconds: int

    @classmethod
    def from_env(cls) -> "Settings":
        tsv_path = os.getenv("GOOGLE_NEWS_TSV_PATH", "").strip()
        return cls(
            tavily_api_key=os.getenv("TAVILY_API_KEY") or None,
            log_level=os.getenv("LOG_LEVEL", "INFO").upper(),
            google_news_tsv_path=Path(tsv_path) if tsv_path else None,
            max_articles_per_feed=_env_int("MAX_ARTICLES_PER_FEED", 20),
            max_concurrent_crawls=_env_int("MAX_CONCURRENT_CRAWLS", 10),
            service_interval_seconds=_env_int("SERVICE_INTERVAL_SECONDS", 1800),
        )
