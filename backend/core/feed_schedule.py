"""Pure RSS publication-cadence analysis and poll-tier selection."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime, timezone
import statistics


DEFAULT_POLL_TIERS_SECONDS = (300, 1800, 21600)


@dataclass(frozen=True)
class FeedCadence:
    """Cadence evidence extracted from one freshly fetched RSS body."""

    latest_published_at: datetime | None
    dated_entries: int
    publication_events: int
    same_timestamp_entries: int
    missing_timestamps: int
    future_timestamps: int
    gap_count: int
    mean_gap_seconds: float | None
    median_gap_seconds: float | None
    trimmed_mean_gap_seconds: float | None


def _trimmed_mean(values: list[float], fraction: float) -> float:
    ordered = sorted(values)
    trim = int(len(ordered) * fraction)
    if trim and trim * 2 < len(ordered):
        ordered = ordered[trim:-trim]
    return statistics.fmean(ordered)


def observe_feed_cadence(
    observations: Iterable[tuple[str, datetime | None]],
    *,
    observed_at: datetime,
    trim_fraction: float = 0.1,
    future_tolerance_seconds: float = 300.0,
) -> FeedCadence:
    """Measure gaps between distinct publication instants in one RSS body.

    Duplicate entry URLs are one article observation. Entries sharing a
    timestamp are one publication event: their zero-second gaps describe a
    publisher batch, not how often the feed needs polling.
    """
    if not 0 <= trim_fraction < 0.5:
        raise ValueError("trim_fraction must be in [0, 0.5)")
    if future_tolerance_seconds < 0:
        raise ValueError("future_tolerance_seconds must not be negative")
    if observed_at.tzinfo is None:
        observed_at = observed_at.replace(tzinfo=timezone.utc)

    by_entry: dict[str, datetime | None] = {}
    for key, published_at in observations:
        current = by_entry.get(key)
        if key not in by_entry or (current is None and published_at is not None):
            by_entry[key] = published_at

    future_limit = observed_at.timestamp() + future_tolerance_seconds
    timestamps: list[float] = []
    missing = future = 0
    for published_at in by_entry.values():
        if published_at is None:
            missing += 1
            continue
        if published_at.tzinfo is None:
            published_at = published_at.replace(tzinfo=timezone.utc)
        timestamp = published_at.timestamp()
        if timestamp > future_limit:
            future += 1
            continue
        timestamps.append(timestamp)

    timestamps.sort()
    event_timestamps = sorted(set(timestamps))
    gaps = [
        later - earlier
        for earlier, later in zip(event_timestamps, event_timestamps[1:])
    ]
    latest = (
        datetime.fromtimestamp(event_timestamps[-1], timezone.utc)
        if event_timestamps
        else None
    )
    return FeedCadence(
        latest_published_at=latest,
        dated_entries=len(timestamps),
        publication_events=len(event_timestamps),
        same_timestamp_entries=len(timestamps) - len(event_timestamps),
        missing_timestamps=missing,
        future_timestamps=future,
        gap_count=len(gaps),
        mean_gap_seconds=statistics.fmean(gaps) if gaps else None,
        median_gap_seconds=statistics.median(gaps) if gaps else None,
        trimmed_mean_gap_seconds=(
            _trimmed_mean(gaps, trim_fraction) if gaps else None
        ),
    )


def staleness_reason(
    latest_published_at: datetime | None,
    *,
    observed_at: datetime,
    stale_after_seconds: float,
) -> str | None:
    """Describe why a feed should be retired, or None if it is still live.

    `poll_interval_for_gap` tiers a feed by the gap *between* its entries, so
    a feed whose last few posts landed close together keeps a fast tier
    forever even if that burst was years ago. This checks the complementary
    gap: from the newest entry to now. A feed with no dated entries at all is
    left to the entry-gap logic instead of being retired here.
    """
    if latest_published_at is None:
        return None
    if latest_published_at.tzinfo is None:
        latest_published_at = latest_published_at.replace(tzinfo=timezone.utc)
    staleness_seconds = (observed_at - latest_published_at).total_seconds()
    if staleness_seconds <= stale_after_seconds:
        return None
    stale_days = int(stale_after_seconds // 86400)
    return (
        f"No new entries since {latest_published_at.date().isoformat()} "
        f"(older than {stale_days} days)"
    )


def poll_interval_for_gap(
    gap_seconds: float | None,
    *,
    gap_count: int,
    min_gaps: int = 3,
    tiers_seconds: tuple[int, int, int] = DEFAULT_POLL_TIERS_SECONDS,
) -> int:
    """Return the slowest tier strictly shorter than the observed gap."""
    fast, medium, slow = tiers_seconds
    if not 0 < fast < medium < slow:
        raise ValueError("poll tiers must be positive and strictly increasing")
    if min_gaps <= 0:
        raise ValueError("min_gaps must be positive")
    if gap_count < min_gaps or gap_seconds is None:
        return fast
    if gap_seconds > slow:
        return slow
    if gap_seconds > medium:
        return medium
    return fast
