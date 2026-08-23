from datetime import datetime, timedelta, timezone

import pytest

from core.config import Settings
from workers import cli as worker_cli
from workers.window import build_window, parse_duration, parse_timestamp


NOW = datetime(2026, 8, 17, 12, 0, tzinfo=timezone.utc)


def _settings() -> Settings:
    return Settings(
        tavily_api_key=None,
        log_level="INFO",
        database_url=None,
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


@pytest.mark.parametrize(
    "value,expected",
    [
        ("30m", timedelta(minutes=30)),
        ("12h", timedelta(hours=12)),
        ("3d", timedelta(days=3)),
        ("2w", timedelta(weeks=2)),
    ],
)
def test_parse_duration_accepts_every_unit(value: str, expected: timedelta) -> None:
    assert parse_duration(value) == expected


@pytest.mark.parametrize("value", ["3", "3y", "d3", "", "-1d", "0d"])
def test_parse_duration_rejects_nonsense(value: str) -> None:
    with pytest.raises(ValueError):
        parse_duration(value)


def test_parse_timestamp_reads_naive_values_as_utc() -> None:
    assert parse_timestamp("2026-08-01") == datetime(2026, 8, 1, tzinfo=timezone.utc)
    assert parse_timestamp("2026-08-01T06:30:00") == datetime(
        2026, 8, 1, 6, 30, tzinfo=timezone.utc
    )
    assert parse_timestamp("2026-08-01T06:30:00+07:00") == datetime(
        2026, 8, 1, 6, 30, tzinfo=timezone(timedelta(hours=7))
    )


def test_parse_timestamp_rejects_nonsense() -> None:
    with pytest.raises(ValueError):
        parse_timestamp("last tuesday")


def test_window_defaults_to_the_last_three_days() -> None:
    window = build_window(now=NOW)

    assert window.end == NOW
    assert window.start == NOW - timedelta(days=3)
    assert window.explicit is False
    assert window.is_deep_backfill is False


def test_last_shifts_only_the_start() -> None:
    window = build_window(last="12h", now=NOW)

    assert window.start == NOW - timedelta(hours=12)
    assert window.end == NOW
    assert window.explicit is False


def test_since_without_until_runs_up_to_now() -> None:
    window = build_window(since="2026-08-16", now=NOW)

    assert window.start == datetime(2026, 8, 16, tzinfo=timezone.utc)
    assert window.end == NOW
    assert window.explicit is True


def test_until_requires_since() -> None:
    with pytest.raises(ValueError, match="--until requires --since"):
        build_window(until="2026-08-16", now=NOW)


def test_window_must_move_forwards() -> None:
    with pytest.raises(ValueError, match="must be earlier"):
        build_window(since="2026-08-16", until="2026-08-16", now=NOW)


def test_long_window_is_flagged_as_a_deep_backfill() -> None:
    assert build_window(last="2w", now=NOW).is_deep_backfill is True
    assert build_window(last="7d", now=NOW).is_deep_backfill is False


def test_undated_entries_pass_only_an_implicit_window() -> None:
    implicit = build_window(now=NOW)
    explicit = build_window(since="2026-08-16", now=NOW)

    assert implicit.contains(None) is True
    assert explicit.contains(None) is False
    assert explicit.contains(datetime(2026, 8, 16, 8, 0, tzinfo=timezone.utc)) is True
    # Naive timestamps out of feeds are read as UTC rather than rejected.
    assert explicit.contains(datetime(2026, 8, 16, 8, 0)) is True
    assert explicit.contains(datetime(2026, 8, 1, tzinfo=timezone.utc)) is False


def test_cli_rejects_last_together_with_since() -> None:
    parser = worker_cli.build_parser(_settings())

    with pytest.raises(SystemExit):
        parser.parse_args(["crawl", "--last", "3d", "--since", "2026-08-01"])


def test_cli_replay_requires_failures_or_stage() -> None:
    parser = worker_cli.build_parser(_settings())

    with pytest.raises(SystemExit):
        parser.parse_args(["replay", "--limit", "10"])

    args = parser.parse_args(["replay", "--failures", "--kind", "article"])
    assert args.command == "replay"
    assert args.failures is True
    assert args.kind == "article"
    assert args.limit == 100


def test_cli_replay_accepts_stage() -> None:
    parser = worker_cli.build_parser(_settings())
    args = parser.parse_args(["replay", "--stage", "embed"])

    worker_cli.validate(args, parser)
    assert args.stage == "embed"
    assert args.limit == 100


def test_cli_disable_feed_requires_reason() -> None:
    parser = worker_cli.build_parser(_settings())

    with pytest.raises(SystemExit):
        parser.parse_args(["disable-feed", "https://example.com/feed.xml"])

    args = parser.parse_args(
        ["disable-feed", "https://example.com/feed.xml", "--reason", "manual"]
    )
    assert args.url == "https://example.com/feed.xml"
    assert args.reason == "manual"


def test_cli_enable_feed_takes_only_a_url() -> None:
    parser = worker_cli.build_parser(_settings())
    args = parser.parse_args(["enable-feed", "https://example.com/feed.xml"])

    assert args.command == "enable-feed"
    assert args.url == "https://example.com/feed.xml"
