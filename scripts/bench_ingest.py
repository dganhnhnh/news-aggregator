"""Measure ingest performance and RSS publication cadence without writing anything.

The benchmarks read their inputs from the application database and then talk
to the network directly through the same adapters `CrawlWorker` uses. Nothing
here calls `apply_batch`, `record_feed_poll` or `start_run`: a measurement must
never move `next_poll_at` or leave a `worker_runs` row behind, or the next real
run measures the benchmark instead of production.

    uv run python scripts/bench_ingest.py feeds  --sample 100 --levels 2,8,16,24
    uv run python scripts/bench_ingest.py crawls --sample 24  --levels 2,4,6
    uv run python scripts/bench_ingest.py cadence --sample 0 --concurrency 8

Politeness: each concurrency level re-requests the same sample, so a run of L
levels is L requests per host. The cadence benchmark fetches each feed once.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import random
import statistics
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import feedparser

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "backend"))

from sqlalchemy import select, text  # noqa: E402
from sqlalchemy.ext.asyncio import create_async_engine  # noqa: E402

from adapters.retrieve_news_data.google_news_rss_searcher import (  # noqa: E402
    DEFAULT_USER_AGENT,
    RssFeedSearcher,
    _parse_rss_entry,
)
from adapters.storage.schema import feed_state  # noqa: E402
from core.feed_schedule import (  # noqa: E402
    observe_feed_cadence,
    poll_interval_for_gap,
)
from models.storage_models import FeedPollTarget  # noqa: E402


DEFAULT_URL = "postgresql+asyncpg://news_radar:news_radar@localhost:5433/news_radar"
POLL_TIERS_SECONDS = (300, 1800, 21600)
TIER_LABELS = {300: "5m", 1800: "30m", 21600: "6h"}


def _engine():
    return create_async_engine(os.getenv("DATABASE_URL", DEFAULT_URL))


async def _load_targets(limit: int, seed: int) -> list[FeedPollTarget]:
    """A deterministic random sample of active feeds, with their validators."""
    engine = _engine()
    try:
        async with engine.connect() as connection:
            rows = (
                await connection.execute(
                    select(
                        feed_state.c.feed_url,
                        feed_state.c.source_label,
                        feed_state.c.etag,
                        feed_state.c.last_modified,
                        feed_state.c.resolved_url,
                        feed_state.c.poll_interval_seconds,
                        feed_state.c.cadence_checked_at,
                    ).where(feed_state.c.disabled_at.is_(None))
                )
            ).all()
    finally:
        await engine.dispose()
    targets = [FeedPollTarget(**dict(row._mapping)) for row in rows]
    random.Random(seed).shuffle(targets)
    return targets[:limit] if limit else targets


async def _load_article_urls(limit: int, seed: int) -> list[str]:
    """Recently crawled URLs, at most one per host so the sample is not skewed."""
    engine = _engine()
    try:
        async with engine.connect() as connection:
            rows = (
                await connection.execute(
                    text(
                        "SELECT url FROM raw_articles "
                        "WHERE content_source = 'crawl' "
                        "  AND crawled_at > now() - interval '3 days' "
                        "ORDER BY crawled_at DESC LIMIT 2000"
                    )
                )
            ).all()
    finally:
        await engine.dispose()
    by_host: dict[str, str] = {}
    for (url,) in rows:
        host = urlsplit(url).hostname or ""
        by_host.setdefault(host, url)
    urls = list(by_host.values())
    random.Random(seed).shuffle(urls)
    return urls[:limit] if limit else urls


@dataclass
class PollLevel:
    concurrency: int
    seconds: float
    fetched: int
    not_modified: int
    failed: int
    entries: int


async def _poll_once(targets: list[FeedPollTarget], concurrency: int) -> PollLevel:
    searcher = RssFeedSearcher(max_concurrent_feeds=concurrency)
    fetched = not_modified = failed = entries = 0
    started = time.perf_counter()
    async for poll in searcher.poll_feeds(targets):
        if poll.status == "fetched":
            fetched += 1
            entries += len(poll.entries)
        elif poll.status == "not_modified":
            not_modified += 1
        else:
            failed += 1
    return PollLevel(
        concurrency=concurrency,
        seconds=time.perf_counter() - started,
        fetched=fetched,
        not_modified=not_modified,
        failed=failed,
        entries=entries,
    )


async def bench_feeds(args: argparse.Namespace) -> dict:
    targets = await _load_targets(args.sample, args.seed)
    print(f"# {len(targets)} feeds sampled (seed={args.seed})", file=sys.stderr)

    if args.warmup:
        print("# warmup pass at concurrency 8 (discarded)", file=sys.stderr)
        await _poll_once(targets, 8)

    levels = []
    for concurrency in args.levels:
        level = await _poll_once(targets, concurrency)
        levels.append(level)
        print(
            f"  c={concurrency:>3}  {level.seconds:7.1f}s  "
            f"fetched={level.fetched} 304={level.not_modified} "
            f"failed={level.failed} entries={level.entries}",
            file=sys.stderr,
        )
    return {
        "benchmark": "feeds",
        "sample": len(targets),
        "levels": [level.__dict__ for level in levels],
    }


async def bench_sliding(args: argparse.Namespace) -> dict:
    """Same fetches, but a sliding window instead of `poll_feeds`' gather barrier.

    `poll_feeds` awaits `asyncio.gather` over a whole group of
    `max_concurrent_feeds` before starting the next, so every group costs its
    slowest member -- up to the 20s fetch timeout. This measures what the same
    work costs when a finished slot is refilled immediately, and records each
    feed's own duration so the two can be told apart from a network limit.
    """
    import httpx

    from adapters.retrieve_news_data.google_news_rss_searcher import (
        DEFAULT_USER_AGENT,
        _fetch_rss,
    )

    targets = await _load_targets(args.sample, args.seed)
    print(f"# {len(targets)} feeds sampled (seed={args.seed})", file=sys.stderr)

    levels = []
    durations_by_level: dict[int, list[float]] = {}
    for concurrency in args.levels:
        semaphore = asyncio.Semaphore(concurrency)
        durations: list[float] = []
        counts = {"fetched": 0, "not_modified": 0, "failed": 0}

        async def one(target: FeedPollTarget, client: httpx.AsyncClient) -> None:
            async with semaphore:
                started = time.perf_counter()
                try:
                    fetch = await _fetch_rss(
                        target.fetch_url,
                        client,
                        etag=target.etag,
                        last_modified=target.last_modified,
                    )
                except Exception:
                    counts["failed"] += 1
                else:
                    counts["not_modified" if fetch.not_modified else "fetched"] += 1
                durations.append(time.perf_counter() - started)

        started = time.perf_counter()
        async with httpx.AsyncClient(
            follow_redirects=True, headers={"User-Agent": DEFAULT_USER_AGENT}
        ) as client:
            await asyncio.gather(*[one(target, client) for target in targets])
        seconds = time.perf_counter() - started

        durations_by_level[concurrency] = durations
        ordered = sorted(durations)
        levels.append(
            {
                "concurrency": concurrency,
                "seconds": seconds,
                **counts,
                "feed_seconds_total": round(sum(durations), 1),
                "per_feed_p50": round(ordered[len(ordered) // 2], 2),
                "per_feed_p90": round(ordered[int(len(ordered) * 0.9)], 2),
                "per_feed_max": round(ordered[-1], 2),
                # What the same work would cost with no barrier and no tail:
                "ideal_seconds": round(sum(durations) / concurrency, 1),
                # How many feeds a lower fetch timeout would turn into
                # failures -- the input for picking that timeout.
                "over": {
                    str(cut): sum(1 for value in durations if value > cut)
                    for cut in (3, 5, 8, 10, 15)
                },
            }
        )
        print(
            f"  c={concurrency:>3}  {seconds:7.1f}s  "
            f"(ideal {sum(durations) / concurrency:5.1f}s)  "
            f"per-feed p50={ordered[len(ordered) // 2]:.2f}s "
            f"p90={ordered[int(len(ordered) * 0.9)]:.2f}s "
            f"max={ordered[-1]:.2f}s  "
            f"fetched={counts['fetched']} 304={counts['not_modified']} "
            f"failed={counts['failed']}",
            file=sys.stderr,
        )
    return {"benchmark": "sliding", "sample": len(targets), "levels": levels}


@dataclass
class CrawlLevel:
    concurrency: int
    seconds: float
    ok: int
    failed: int
    per_url_seconds: float


async def bench_crawls(args: argparse.Namespace) -> dict:
    from adapters.retrieve_news_data.crawler import Crawl4aiProvider

    urls = await _load_article_urls(args.sample, args.seed)
    print(f"# {len(urls)} article URLs sampled, one per host", file=sys.stderr)

    levels = []
    for concurrency in args.levels:
        provider = Crawl4aiProvider(
            max_per_host=args.max_per_host,
            per_host_delay_seconds=args.host_delay,
        )
        started = time.perf_counter()
        results = await provider.crawl_batch(urls, max_concurrency=concurrency)
        seconds = time.perf_counter() - started
        ok = sum(1 for result in results if result.success)
        levels.append(
            CrawlLevel(
                concurrency=concurrency,
                seconds=seconds,
                ok=ok,
                failed=len(results) - ok,
                per_url_seconds=seconds / max(1, len(results)),
            )
        )
        print(
            f"  c={concurrency:>3}  {seconds:7.1f}s  ok={ok} "
            f"failed={len(results) - ok}  {seconds / max(1, len(results)):.2f}s/url",
            file=sys.stderr,
        )
    return {
        "benchmark": "crawls",
        "sample": len(urls),
        "levels": [level.__dict__ for level in levels],
    }


async def bench_hosts(args: argparse.Namespace) -> dict:
    """How lumpy the crawl work is per host -- the load-balancing input."""
    engine = _engine()
    try:
        async with engine.connect() as connection:
            rows = (
                await connection.execute(
                    text(
                        "SELECT split_part(split_part(url,'//',2),'/',1) AS host, "
                        "       count(*) AS n "
                        "FROM raw_articles "
                        "WHERE crawled_at > now() - interval '7 days' "
                        "GROUP BY 1 ORDER BY 2 DESC"
                    )
                )
            ).all()
    finally:
        await engine.dispose()
    counts = [int(row.n) for row in rows]
    total = sum(counts)
    return {
        "benchmark": "hosts",
        "hosts": len(counts),
        "articles": total,
        "top": [{"host": row.host, "n": int(row.n)} for row in rows[:10]],
        "p50": statistics.median(counts),
        "max": max(counts),
        "share_top10": round(sum(counts[:10]) / total, 3) if total else 0,
    }


def _cadence_stats(
    observations: list[tuple[str, datetime | None]],
    *,
    fetched_at: datetime,
    min_gaps: int,
    trim_fraction: float,
    future_tolerance_seconds: float,
) -> dict[str, Any]:
    """Summarize gaps between distinct publication instants in one RSS body."""
    cadence = observe_feed_cadence(
        observations,
        observed_at=fetched_at,
        trim_fraction=trim_fraction,
        future_tolerance_seconds=future_tolerance_seconds,
    )
    estimates: dict[str, float | None] = {
        "mean": cadence.mean_gap_seconds,
        "median": cadence.median_gap_seconds,
        "trimmed_mean": cadence.trimmed_mean_gap_seconds,
    }
    tiers = {
        name: poll_interval_for_gap(
            value,
            gap_count=cadence.gap_count,
            min_gaps=min_gaps,
            tiers_seconds=POLL_TIERS_SECONDS,
        )
        for name, value in estimates.items()
    }
    newest_age_seconds = (
        max(0.0, (fetched_at - cadence.latest_published_at).total_seconds())
        if cadence.latest_published_at
        else None
    )
    return {
        "dated_entries": cadence.dated_entries,
        "missing_timestamps": cadence.missing_timestamps,
        "future_timestamps": cadence.future_timestamps,
        "publication_events": cadence.publication_events,
        "same_timestamp_entries": cadence.same_timestamp_entries,
        "gap_count": cadence.gap_count,
        "enough_evidence": cadence.gap_count >= min_gaps,
        "newest_age_seconds": newest_age_seconds,
        "estimates_seconds": estimates,
        "tiers_seconds": tiers,
    }


async def _fetch_cadence_target(
    target: FeedPollTarget,
    client,
    args: argparse.Namespace,
) -> dict[str, Any]:
    """Unconditionally fetch one feed and extract the cadence sample."""
    started = time.perf_counter()
    fetched_at = datetime.now(timezone.utc)
    try:
        # Deliberately omit validators. This command needs an RSS body even
        # when production has an ETag or Last-Modified value for the feed.
        response = await client.get(target.fetch_url, timeout=args.timeout)
        if response.status_code == 304:
            raise RuntimeError("unexpected 304 from unconditional request")
        response.raise_for_status()
        parsed_feed = feedparser.parse(response.text)
        if parsed_feed.bozo and not parsed_feed.entries:
            raise ValueError(f"Malformed RSS/Atom feed: {parsed_feed.bozo_exception}")

        base_url = str(response.url)
        parsed_entries = [
            _parse_rss_entry(entry, base_url) for entry in parsed_feed.entries
        ]
        article_entries = [entry for entry in parsed_entries if entry["feed_entry_url"]]
        cadence = _cadence_stats(
            [
                (entry["feed_entry_url"], entry["published_date"])
                for entry in article_entries
            ],
            fetched_at=fetched_at,
            min_gaps=args.min_gaps,
            trim_fraction=args.trim_fraction,
            future_tolerance_seconds=args.future_tolerance,
        )
        etag = response.headers.get("ETag")
        last_modified = response.headers.get("Last-Modified")
        return {
            "feed_url": target.feed_url,
            "fetch_url": target.fetch_url,
            "status": "fetched",
            "seconds": round(time.perf_counter() - started, 3),
            "response_bytes": len(response.content),
            "has_validator": bool(etag or last_modified),
            "etag": bool(etag),
            "last_modified": bool(last_modified),
            "feed_entries": len(parsed_feed.entries),
            "article_entries": len(article_entries),
            "unique_article_entries": len(
                {entry["feed_entry_url"] for entry in article_entries}
            ),
            "cadence": cadence,
        }
    except Exception as exc:
        return {
            "feed_url": target.feed_url,
            "fetch_url": target.fetch_url,
            "status": "failed",
            "seconds": round(time.perf_counter() - started, 3),
            "error_type": type(exc).__name__,
            "error_message": str(exc),
        }


def _cadence_summary(
    targets: list[FeedPollTarget],
    results: list[dict[str, Any]],
    *,
    stale_after_hours: float,
) -> dict[str, Any]:
    fetched = [result for result in results if result["status"] == "fetched"]
    failed = [result for result in results if result["status"] == "failed"]
    estimators: dict[str, dict[str, Any]] = {}
    for estimator in ("mean", "median", "trimmed_mean"):
        tier_counts = {label: 0 for label in TIER_LABELS.values()}
        successful_polls_per_day = 0.0
        full_body_bytes_per_day = 0.0
        stale_fast = 0
        for result in fetched:
            cadence = result["cadence"]
            interval = cadence["tiers_seconds"][estimator]
            tier_counts[TIER_LABELS[interval]] += 1
            polls_per_day = 86400 / interval
            successful_polls_per_day += polls_per_day
            if not result["has_validator"]:
                full_body_bytes_per_day += result["response_bytes"] * polls_per_day
            newest_age = cadence["newest_age_seconds"]
            if (
                interval == POLL_TIERS_SECONDS[0]
                and newest_age is not None
                and newest_age > stale_after_hours * 3600
            ):
                stale_fast += 1
        # Failed fetches have no cadence evidence. Treating every one as 5m
        # gives the strict-policy request ceiling; production retry backoff
        # will normally make the actual number lower.
        strict_upper = successful_polls_per_day + (
            len(failed) * 86400 / POLL_TIERS_SECONDS[0]
        )
        estimators[estimator] = {
            "tier_counts": tier_counts,
            "successful_feeds_polls_per_day": round(successful_polls_per_day, 1),
            "all_targets_strict_upper_bound_polls_per_day": round(strict_upper, 1),
            "no_validator_full_body_bytes_per_day": round(full_body_bytes_per_day),
            "stale_but_5m_feeds": stale_fast,
        }

    gap_counts = [result["cadence"]["gap_count"] for result in fetched]
    return {
        "targets": len(targets),
        "fetched": len(fetched),
        "failed": len(failed),
        "with_validator": sum(result["has_validator"] for result in fetched),
        "without_validator": sum(not result["has_validator"] for result in fetched),
        "enough_evidence": sum(
            result["cadence"]["enough_evidence"] for result in fetched
        ),
        "insufficient_evidence": sum(
            not result["cadence"]["enough_evidence"] for result in fetched
        ),
        "feeds_with_same_timestamp_batches": sum(
            result["cadence"]["same_timestamp_entries"] > 0 for result in fetched
        ),
        "gap_count_p50": statistics.median(gap_counts) if gap_counts else None,
        "current_fixed_30m_polls_per_day": len(targets) * 48,
        "successful_feeds_fixed_30m_polls_per_day": len(fetched) * 48,
        "estimators": estimators,
        "failure_types": {
            error_type: sum(
                result.get("error_type") == error_type for result in failed
            )
            for error_type in sorted(
                {result.get("error_type", "Unknown") for result in failed}
            )
        },
    }


async def bench_cadence(args: argparse.Namespace) -> dict:
    """Measure feed cadence from fresh, unconditional RSS responses."""
    import httpx

    targets = await _load_targets(args.sample, args.seed)
    print(
        f"# fetching {len(targets)} feeds unconditionally at c={args.concurrency}",
        file=sys.stderr,
    )
    iterator = iter(targets)
    results: list[dict[str, Any]] = []

    async with httpx.AsyncClient(
        follow_redirects=True, headers={"User-Agent": DEFAULT_USER_AGENT}
    ) as client:

        async def worker() -> None:
            for target in iterator:
                result = await _fetch_cadence_target(target, client, args)
                results.append(result)
                if len(results) % 25 == 0:
                    print(f"  completed {len(results)}/{len(targets)}", file=sys.stderr)

        started = time.perf_counter()
        await asyncio.gather(
            *[worker() for _ in range(min(args.concurrency, len(targets)))]
        )
        elapsed = time.perf_counter() - started

    results.sort(key=lambda item: item["feed_url"])
    output = {
        "benchmark": "cadence",
        "sample": len(targets),
        "seed": args.seed,
        "unconditional_fetch": True,
        "seconds": round(elapsed, 1),
        "policy": {
            "tiers_seconds": list(POLL_TIERS_SECONDS),
            "strict_slow_tier_boundary": True,
            "min_gaps": args.min_gaps,
            "trim_fraction": args.trim_fraction,
            "future_tolerance_seconds": args.future_tolerance,
            "stale_after_hours": args.stale_after_hours,
            "publication_event": "distinct timestamp after duplicate URL removal",
        },
        "summary": _cadence_summary(
            targets, results, stale_after_hours=args.stale_after_hours
        ),
    }
    if args.details:
        output["feeds"] = results
    return output


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "benchmark", choices=["feeds", "sliding", "crawls", "hosts", "cadence"]
    )
    parser.add_argument("--sample", type=int, default=100)
    parser.add_argument("--levels", default="2,8,16,24")
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--warmup", action="store_true")
    parser.add_argument("--max-per-host", type=int, default=1)
    parser.add_argument("--host-delay", type=float, default=1.0)
    parser.add_argument("--concurrency", type=int, default=8)
    parser.add_argument("--timeout", type=float, default=20.0)
    parser.add_argument("--min-gaps", type=int, default=3)
    parser.add_argument("--trim-fraction", type=float, default=0.1)
    parser.add_argument("--future-tolerance", type=float, default=300.0)
    parser.add_argument("--stale-after-hours", type=float, default=24.0)
    parser.add_argument("--details", action="store_true")
    args = parser.parse_args()
    args.levels = [int(value) for value in args.levels.split(",") if value.strip()]
    if args.concurrency <= 0:
        parser.error("--concurrency must be positive")
    if args.timeout <= 0:
        parser.error("--timeout must be positive")
    if args.min_gaps <= 0:
        parser.error("--min-gaps must be positive")
    if not 0 <= args.trim_fraction < 0.5:
        parser.error("--trim-fraction must be in [0, 0.5)")
    if args.future_tolerance < 0:
        parser.error("--future-tolerance must not be negative")
    if args.stale_after_hours <= 0:
        parser.error("--stale-after-hours must be positive")

    runner = {
        "feeds": bench_feeds,
        "sliding": bench_sliding,
        "crawls": bench_crawls,
        "hosts": bench_hosts,
        "cadence": bench_cadence,
    }
    print(json.dumps(asyncio.run(runner[args.benchmark](args)), indent=2))


if __name__ == "__main__":
    main()
