import argparse
import asyncio
import pytest

from adapters.storage import StorageError
from core.config import Settings
from models.search_and_crawl_models import CrawlMetadata, CrawlResult
from models.storage_models import CrawlBatch, StorageWriteSummary
from pipelines import news as pipeline_cli
from services import news as service_cli


def _settings(database_url: str | None = None) -> Settings:
    return Settings(
        tavily_api_key=None,
        log_level="INFO",
        database_url=database_url,
        rss_feed_path=None,
        max_feeds=0,
        max_articles_per_feed=20,
        max_concurrent_feeds=2,
        max_concurrent_crawls=2,
        crawl_batch_size=10,
        feed_offset=0,
        max_retries_per_run=50,
        service_interval_seconds=1800,
    )


def test_persistence_and_service_output_flags_are_exposed() -> None:
    pipeline_parser = pipeline_cli.build_parser(_settings())
    crawl_args = pipeline_parser.parse_args(
        ["crawl", "https://example.com", "--persist"]
    )
    rss_args = pipeline_parser.parse_args(
        ["rss", "--feed-path", "feeds.opml", "--persist"]
    )
    service_args = service_cli.build_parser(_settings()).parse_args(
        ["--feed-path", "feeds.opml", "--once", "--emit-jsonl"]
    )

    assert crawl_args.persist is True
    assert rss_args.persist is True
    assert service_args.emit_jsonl is True
    assert service_args.max_concurrent_feeds == 2
    assert service_args.max_concurrent_crawls == 2
    assert service_args.crawl_batch_size == 10
    with pytest.raises(SystemExit):
        pipeline_parser.parse_args(
            ["search", "query", "--persist"]
        )


def test_persist_fails_before_crawl_without_database_url(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    crawled = False

    async def fake_crawl(urls: list[str]) -> CrawlBatch:
        nonlocal crawled
        crawled = True
        return CrawlBatch()

    monkeypatch.setattr(pipeline_cli, "run_crawl_batch", fake_crawl)
    args = argparse.Namespace(
        command="crawl",
        urls=["https://example.com"],
        persist=True,
    )

    with pytest.raises(StorageError, match="DATABASE_URL"):
        asyncio.run(pipeline_cli._dispatch(args, _settings()))
    assert crawled is False


def test_unmigrated_schema_fails_before_crawl(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    crawled = False

    class UnmigratedStorage:
        def __init__(self, database_url: str) -> None:
            pass

        async def check_ready(self) -> None:
            raise StorageError(
                "Database schema is missing; run `uv run alembic upgrade head`"
            )

        async def close(self) -> None:
            pass

    async def fake_crawl(urls: list[str]) -> CrawlBatch:
        nonlocal crawled
        crawled = True
        return CrawlBatch()

    monkeypatch.setattr(pipeline_cli, "PostgresRawStorage", UnmigratedStorage)
    monkeypatch.setattr(pipeline_cli, "run_crawl_batch", fake_crawl)
    args = argparse.Namespace(
        command="crawl",
        urls=["https://example.com"],
        persist=True,
    )

    with pytest.raises(StorageError, match="alembic upgrade head"):
        asyncio.run(pipeline_cli._dispatch(args, _settings("postgresql+asyncpg://db")))
    assert crawled is False


def test_persisted_crawl_creates_and_finishes_run(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    calls: list[object] = []

    class FakeStorage:
        def __init__(self, database_url: str) -> None:
            calls.append(("init", database_url))

        async def check_ready(self) -> None:
            calls.append("ready")

        async def start_run(self, job_type: str) -> int:
            calls.append(("start", job_type))
            return 7

        async def apply_batch(self, *args, **kwargs) -> StorageWriteSummary:
            calls.append("apply")
            return StorageWriteSummary(1, 0, 1, 0, 0)

        async def finish_run(self, run_id, status, stats, error_message=None) -> None:
            calls.append(("finish", run_id, status, stats.articles_written))

        async def close(self) -> None:
            calls.append("close")

    async def fake_crawl(urls: list[str]) -> CrawlBatch:
        return CrawlBatch(
            results=[
                CrawlResult(
                    url=urls[0],
                    markdown="# Article",
                    metadata=CrawlMetadata(title="Article"),
                )
            ],
            articles_discovered=1,
        )

    monkeypatch.setattr(pipeline_cli, "PostgresRawStorage", FakeStorage)
    monkeypatch.setattr(pipeline_cli, "run_crawl_batch", fake_crawl)
    args = argparse.Namespace(
        command="crawl",
        urls=["https://example.com"],
        persist=True,
    )

    asyncio.run(
        pipeline_cli._dispatch(
            args,
            _settings("postgresql+asyncpg://news-radar"),
        )
    )

    assert ("start", "pipeline_crawl") in calls
    assert ("finish", 7, "succeeded", 1) in calls
    assert calls[-1] == "close"
    assert '"url": "https://example.com"' in capsys.readouterr().out


def test_rss_persists_each_chunk_before_later_interruption(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[object] = []

    class FakeStorage:
        def __init__(self, database_url: str) -> None:
            pass

        async def check_ready(self) -> None:
            pass

        async def start_run(self, job_type: str) -> int:
            return 9

        async def apply_batch(self, *args, **kwargs) -> StorageWriteSummary:
            calls.append("apply")
            return StorageWriteSummary(1, 0, 1, 0, 0)

        async def update_run(self, run_id, stats) -> None:
            calls.append(("checkpoint", run_id, stats.articles_written))

        async def finish_run(self, run_id, status, stats, error_message=None) -> None:
            calls.append(("finish", run_id, status, stats.articles_written))

        async def close(self) -> None:
            calls.append("close")

    class InterruptedSearcher:
        def __init__(self, **kwargs) -> None:
            pass

        async def iter_batches(self, **kwargs):
            yield CrawlBatch(
                results=[
                    CrawlResult(
                        url="https://example.com/first",
                        markdown="# First",
                    )
                ],
                articles_discovered=1,
            )
            raise asyncio.CancelledError

    monkeypatch.setattr(pipeline_cli, "PostgresRawStorage", FakeStorage)
    monkeypatch.setattr(pipeline_cli, "RssFeedSearcher", InterruptedSearcher)
    args = argparse.Namespace(
        command="rss",
        feed_path="feeds.opml",
        persist=True,
        max_feeds=0,
        max_articles_per_feed=20,
        max_concurrent_feeds=2,
        max_concurrent_crawls=2,
        crawl_batch_size=10,
        feed_offset=0,
    )

    with pytest.raises(asyncio.CancelledError):
        asyncio.run(
            pipeline_cli._dispatch(
                args,
                _settings("postgresql+asyncpg://news-radar"),
            )
        )

    assert calls[:2] == ["apply", ("checkpoint", 9, 1)]
    assert ("finish", 9, "partial", 1) in calls
    assert calls[-1] == "close"
