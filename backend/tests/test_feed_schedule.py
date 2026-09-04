from datetime import datetime, timedelta, timezone

from core.feed_schedule import observe_feed_cadence, poll_interval_for_gap


NOW = datetime(2026, 9, 3, 12, 0, tzinfo=timezone.utc)


def test_tier_boundaries_are_strict() -> None:
    assert poll_interval_for_gap(None, gap_count=3) == 300
    assert poll_interval_for_gap(1800, gap_count=3) == 300
    assert poll_interval_for_gap(1800.001, gap_count=3) == 1800
    assert poll_interval_for_gap(21600, gap_count=3) == 1800
    assert poll_interval_for_gap(21600.001, gap_count=3) == 21600


def test_cadence_uses_distinct_instants_and_rejects_future_dates() -> None:
    observations = [
        ("a", NOW - timedelta(hours=8)),
        ("b", NOW - timedelta(hours=8)),
        ("c", NOW - timedelta(hours=6)),
        ("d", NOW - timedelta(hours=4)),
        ("e", NOW - timedelta(hours=2)),
        ("missing", None),
        ("future", NOW + timedelta(hours=1)),
    ]

    result = observe_feed_cadence(
        observations,
        observed_at=NOW,
        trim_fraction=0.1,
        future_tolerance_seconds=300,
    )

    assert result.dated_entries == 5
    assert result.publication_events == 4
    assert result.same_timestamp_entries == 1
    assert result.missing_timestamps == 1
    assert result.future_timestamps == 1
    assert result.gap_count == 3
    assert result.mean_gap_seconds == 7200
    assert result.median_gap_seconds == 7200
    assert result.trimmed_mean_gap_seconds == 7200
    assert poll_interval_for_gap(
        result.trimmed_mean_gap_seconds, gap_count=result.gap_count
    ) == 1800


def test_insufficient_evidence_always_uses_fast_tier() -> None:
    result = observe_feed_cadence(
        [
            ("a", NOW - timedelta(days=2)),
            ("b", NOW - timedelta(days=1)),
        ],
        observed_at=NOW,
        trim_fraction=0.1,
        future_tolerance_seconds=300,
    )

    assert result.gap_count == 1
    assert result.mean_gap_seconds == 86400
    assert poll_interval_for_gap(
        result.trimmed_mean_gap_seconds, gap_count=result.gap_count
    ) == 300


def test_trimmed_mean_discards_one_outlier_from_each_end() -> None:
    gap_hours = [1, 10, 10, 10, 10, 10, 10, 10, 10, 100]
    timestamp = NOW - timedelta(hours=sum(gap_hours))
    observations = [("0", timestamp)]
    for index, gap in enumerate(gap_hours, start=1):
        timestamp += timedelta(hours=gap)
        observations.append((str(index), timestamp))

    result = observe_feed_cadence(
        observations,
        observed_at=NOW,
        trim_fraction=0.1,
    )

    assert result.gap_count == 10
    assert result.trimmed_mean_gap_seconds == 10 * 3600
