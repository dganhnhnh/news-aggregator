import argparse
import asyncio
import pytest

from adapters.storage import StorageError
from core.config import Settings
from models.search_and_crawl_models import CrawlMetadata, CrawlResult
from models.storage_models import CrawlBatch, StorageWriteSummary
from pipelines import news as pipeline_cli
from services import news as service_cli
from workers import cli as worker_cli


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


def test_persistence_and_worker_output_flags_are_exposed() -> None:
    pipeline_parser = pipeline_cli.build_parser(_settings())
    crawl_args = pipeline_parser.parse_args(
        ["crawl", "https://example.com", "--persist"]
    )
    rss_args = pipeline_parser.parse_args(
        ["rss", "--feed-path", "feeds.opml", "--persist"]
    )
    worker_args = worker_cli.build_parser(_settings()).parse_args(
        ["crawl", "--feed-path", "feeds.opml", "--once", "--emit-jsonl"]
    )

    assert crawl_args.persist is True
    assert rss_args.persist is True
    assert worker_args.emit_jsonl is True
    assert worker_args.max_concurrent_feeds == 2
    assert worker_args.max_concurrent_crawls == 2
    assert worker_args.crawl_batch_size == 10
    assert worker_args.feed_fetch_timeout == 8.0
    assert (
        worker_args.poll_fast_interval,
        worker_args.poll_interval,
        worker_args.poll_slow_interval,
    ) == (300, 1800, 21600)
    with pytest.raises(SystemExit):
        pipeline_parser.parse_args(
            ["search", "query", "--persist"]
        )


def _parse(argv: list[str]) -> argparse.Namespace:
    parser = worker_cli.build_parser(_settings())
    args = parser.parse_args(argv)
    worker_cli.validate(args, parser)
    return args


def test_scheduler_flags_are_available_to_every_worker_command() -> None:
    for command in ("crawl", "instant-crawl", "embed", "classify", "cluster"):
        args = _parse(
            [command, "--once", "--exit-busy", "99", "--summary-json", "-",
             "--max-runtime", "25m",
             "--trigger", "scheduled"]
        )
        assert args.exit_busy == 99
        assert args.summary_json == "-"
        assert args.max_runtime == "25m"
        # An explicit trigger is what keeps a scheduled run out of the
        # backfill bucket when the DAG also passes a window.
        assert args.trigger == "scheduled"

    # taxonomy-rebuild takes the same advisory lock, so it needs the same flag.
    assert _parse(["taxonomy-rebuild", "--exit-busy", "99"]).exit_busy == 99


def test_max_runtime_rejects_invalid_values() -> None:
    with pytest.raises(SystemExit):
        _parse(["embed", "--max-runtime", "soon"])


def test_max_runtime_combines_with_interval() -> None:
    # The budget ends one run, not the looping process, which is how the
    # `ingest` service keeps a slow crawl from holding the lock forever.
    args = _parse(["crawl", "--max-runtime", "8m", "--interval", "300"])
    assert (args.max_runtime, args.interval) == ("8m", 300)


def test_adaptive_poll_settings_reject_invalid_order() -> None:
    with pytest.raises(SystemExit):
        _parse(
            [
                "crawl",
                "--poll-fast-interval",
                "1800",
                "--poll-interval",
                "1800",
            ]
        )


def test_instant_crawl_has_fixed_short_poll_default() -> None:
    args = _parse(["instant-crawl", "--interval", "30"])

    assert args.feed_path is None
    assert args.poll_interval == 30


def test_feed_priority_must_be_non_negative() -> None:
    assert _parse(
        ["set-feed-priority", "https://example.com/feed.xml", "0"]
    ).priority == 0
    with pytest.raises(SystemExit):
        _parse(["set-feed-priority", "https://example.com/feed.xml", "-1"])


def test_exit_busy_defaults_to_success_and_rejects_the_failure_code() -> None:
    assert _parse(["embed", "--once"]).exit_busy == 0

    for bad in ("1", "256", "-1"):
        with pytest.raises(SystemExit):
            _parse(["embed", "--once", "--exit-busy", bad])


def test_queue_status_defaults_to_a_human_readable_daily_window() -> None:
    default = _parse(["queue-status"])
    assert default.as_json is False
    assert default.fail_on_alert is False
    assert default.runs_window == 24

    monitoring = _parse(["queue-status", "--json", "--fail-on-alert",
                         "--runs-window", "6"])
    assert (monitoring.as_json, monitoring.fail_on_alert) == (True, True)
    assert monitoring.runs_window == 6

    with pytest.raises(SystemExit):
        _parse(["queue-status", "--runs-window", "0"])


def test_block_page_cleanup_is_dry_run_by_default() -> None:
    parser = worker_cli.build_parser(_settings())

    dry_run = parser.parse_args(["cleanup-block-pages", "--limit", "25"])
    applied = parser.parse_args(["cleanup-block-pages", "--apply"])

    assert dry_run.limit == 25
    assert dry_run.apply is False
    assert applied.apply is True


def test_cluster_command_exposes_mvp_matching_defaults() -> None:
    parser = worker_cli.build_parser(_settings())

    args = parser.parse_args(["cluster", "--once", "--reprocess"])

    assert args.batch_size == 50
    assert args.candidate_limit == 20
    assert args.cluster_threshold == 0.75
    assert args.cluster_window_hours == 72
    assert args.reprocess is True


def test_rss_command_accepts_direct_feed_urls() -> None:
    args = pipeline_cli.build_parser(_settings()).parse_args(
        [
            "rss",
            "https://example.com/feed.xml",
            "https://example.org/atom.xml",
            "--max-articles-per-feed",
            "3",
        ]
    )

    assert args.feed_urls == [
        "https://example.com/feed.xml",
        "https://example.org/atom.xml",
    ]
    assert args.feed_path is None
    assert args.max_articles_per_feed == 3


def test_rss_command_rejects_direct_urls_with_feed_path() -> None:
    parser = pipeline_cli.build_parser(_settings())

    with pytest.raises(SystemExit):
        parser.parse_args(
            [
                "rss",
                "https://example.com/feed.xml",
                "--feed-path",
                "feeds.opml",
            ]
        )


def test_rss_dispatch_passes_direct_urls_as_sources(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[object] = []

    class CapturingSearcher:
        def __init__(self, **kwargs) -> None:
            calls.append(("init", kwargs["feed_path"]))

        async def iter_batches(self, **kwargs):
            calls.append(("sources", kwargs["sources"]))
            if False:
                yield CrawlBatch()

    monkeypatch.setattr(pipeline_cli, "RssFeedSearcher", CapturingSearcher)
    args = pipeline_cli.build_parser(_settings()).parse_args(
        ["rss", "https://example.com/feed.xml"]
    )

    asyncio.run(pipeline_cli._dispatch(args, _settings()))

    assert calls == [
        ("init", None),
        (
            "sources",
            [
                pipeline_cli.RssFeedSource(
                    "https://example.com/feed.xml",
                    "https://example.com/feed.xml",
                )
            ],
        ),
    ]


def test_news_service_alias_translates_legacy_flags() -> None:
    # Bare news-service used to loop; the alias must keep doing so even though
    # bare `news-worker crawl` runs once.
    assert service_cli.translate_argv([], 1800) == [
        "crawl",
        "--interval",
        "1800",
    ]
    assert service_cli.translate_argv(
        ["--feed-path", "feeds.opml", "--interval-seconds", "60"], 1800
    ) == ["crawl", "--feed-path", "feeds.opml", "--interval", "60"]
    assert service_cli.translate_argv(["--interval-seconds=60"], 1800) == [
        "crawl",
        "--interval",
        "60",
    ]
    # --once won over --interval-seconds under the old parser; the new parser
    # rejects the pair, so the interval is dropped rather than passed through.
    assert service_cli.translate_argv(
        ["--once", "--interval-seconds", "60"], 1800
    ) == ["crawl", "--once"]


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

        async def start_run(self, *, worker, trigger, **kwargs) -> int:
            calls.append(("start", worker, trigger, kwargs["params"]["command"]))
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

    assert ("start", "crawl", "manual", "crawl") in calls
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

        async def start_run(self, **kwargs) -> int:
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
        feed_urls=[],
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


def _capturing_provider(seen: list[list[str]]):
    """A Crawl4aiProvider stand-in recording the URLs it was asked to crawl."""

    class FakeProvider:
        async def crawl(self, url: str) -> CrawlResult:
            seen.append([url])
            return CrawlResult(url=url, markdown="# Article", success=True)

        async def crawl_batch(self, urls, **kwargs) -> list[CrawlResult]:
            seen.append(list(urls))
            return [
                CrawlResult(url=url, markdown="# Article", success=True)
                for url in urls
            ]

    return FakeProvider


def test_crawl_command_resolves_a_protocol_relative_url(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # `news-pipeline crawl //host/path` has no containing document, so the CLI
    # supplies the scheme instead of handing the crawler a scheme-less URL.
    seen: list[list[str]] = []
    monkeypatch.setattr(pipeline_cli, "Crawl4aiProvider", _capturing_provider(seen))

    batch = asyncio.run(
        pipeline_cli.run_crawl_batch(
            ["//target.github.io/architecture/modernization_Intro"]
        )
    )

    assert seen == [["https://target.github.io/architecture/modernization_Intro"]]
    assert batch.failures == []


def test_crawl_command_reports_an_unusable_url_as_a_permanent_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seen: list[list[str]] = []
    monkeypatch.setattr(pipeline_cli, "Crawl4aiProvider", _capturing_provider(seen))

    batch = asyncio.run(
        pipeline_cli.run_crawl_batch(
            ["https://example.com/good", "mailto:editor@example.com"]
        )
    )

    assert seen == [["https://example.com/good"]]
    assert len(batch.failures) == 1
    failure = batch.failures[0]
    assert failure.url == "mailto:editor@example.com"
    assert failure.error_type == "UnusableUrl:unsupported_scheme"
    assert failure.permanent is True


def test_crawl_command_with_no_usable_url_never_builds_a_provider(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seen: list[list[str]] = []
    monkeypatch.setattr(pipeline_cli, "Crawl4aiProvider", _capturing_provider(seen))

    batch = asyncio.run(pipeline_cli.run_crawl_batch(["/relative/only"]))

    assert seen == []
    assert [failure.permanent for failure in batch.failures] == [True]


def test_semantic_parses_with_service_defaults() -> None:
    args = _parse(["semantic"])
    assert args.last == "24h"
    assert (args.fallback_interval, args.min_pass_interval) == (300.0, 60.0)
    assert (args.merge_every, args.refresh_every) == ("15m", "15m")
    assert worker_cli.resolve_window(args, worker_cli.build_parser(_settings()))


def test_semantic_rejects_bad_pacing() -> None:
    with pytest.raises(SystemExit):
        _parse(["semantic", "--fallback-interval", "0"])
    with pytest.raises(SystemExit):
        _parse(["semantic", "--merge-every", "soon"])


def test_semantic_stage_args_reuse_each_stage_parser() -> None:
    args = _parse(["semantic", "--last", "6h"])
    stages = worker_cli.semantic_stage_args(args, _settings())
    assert stages.embed.command == "embed" and stages.embed.max_runtime == "30m"
    assert stages.classify.command == "classify"
    assert not stages.classify.refresh_clusters
    assert stages.cluster.command == "cluster" and not stages.cluster.merge_only
    assert stages.merge.merge_only
    assert stages.refresh.command == "classify" and stages.refresh.refresh_clusters
    assert {s.last for s in vars(stages).values()} == {"6h"}


def test_maintenance_steps_all_parse() -> None:
    for argv in worker_cli.MAINTENANCE_STEPS:
        _parse(argv)
    assert worker_cli.MAINTENANCE_STEPS[-1][0] == "queue-status"


def test_maintenance_reports_health_even_after_a_failed_step(monkeypatch) -> None:
    ran: list[str] = []

    async def fake_dispatch(args, settings, window):
        ran.append(args.command)
        if args.command == "cluster":
            raise RuntimeError("repair blew up")
        return worker_cli.EXIT_OK

    monkeypatch.setattr(worker_cli, "_dispatch", fake_dispatch)
    code = asyncio.run(worker_cli._run_maintenance(_settings("postgresql://x")))

    assert ran == ["cluster", "boilerplate-refresh", "cleanup-block-pages", "queue-status"]
    assert code == worker_cli.EXIT_FAILED


def test_maintenance_succeeds_when_every_step_does(monkeypatch) -> None:
    async def fake_dispatch(args, settings, window):
        return worker_cli.EXIT_OK

    monkeypatch.setattr(worker_cli, "_dispatch", fake_dispatch)
    assert asyncio.run(worker_cli._run_maintenance(_settings())) == worker_cli.EXIT_OK
