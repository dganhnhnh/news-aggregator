"""Compile-level tests for the daily-counts aggregate; no database required.

These pin three contracts an integration test cannot check. A CI box with its
PostgreSQL session TimeZone already set to UTC cannot tell an explicit `'UTC'`
argument from a missing one, and no amount of row fixtures proves the absence
of a `LIMIT`. Both are visible in the compiled SQL.
"""

from __future__ import annotations

from datetime import date, datetime, timezone

from sqlalchemy.dialects import postgresql

from adapters.storage.postgres import PostgresRawStorage, _utc_day_span


def _compile(*, query: str = "quantum", source_slugs=None, topic_slugs=None):
    stmt = PostgresRawStorage._daily_counts_statement(
        since=datetime(2026, 3, 2, tzinfo=timezone.utc),
        until=datetime(2026, 3, 5, tzinfo=timezone.utc),
        query=query,
        source_slugs=source_slugs,
        topic_slugs=topic_slugs,
    )
    # Not `literal_binds=True`: SQLAlchemy has no literal renderer for the
    # REGCONFIG argument of websearch_to_tsquery, so the values are asserted
    # through the params dict instead.
    compiled = stmt.compile(dialect=postgresql.dialect())
    return str(compiled), compiled.params


def test_daily_counts_buckets_in_explicit_utc() -> None:
    sql, params = _compile()
    assert "date_trunc" in sql
    values = set(params.values())
    assert "day" in values
    # The zone must be pinned in the statement. Without it, `date_trunc` on a
    # timestamptz follows the session TimeZone and the same window buckets
    # differently per deployment.
    assert "UTC" in values


def test_daily_counts_keeps_the_query_out_of_the_where_clause() -> None:
    sql, _ = _compile()
    # Anchored past the select list, because the FILTER clause contains a
    # `WHERE` of its own and splitting on the first one lands inside it.
    where = sql.split("FROM raw_articles", 1)[1].split("GROUP BY", 1)[0]
    # Exactly one match predicate, and it belongs to the FILTER rather than
    # the WHERE: that is what makes `articles` a q-independent denominator
    # instead of a second copy of `matches`.
    assert sql.count("@@") == 1
    assert "FILTER (WHERE" in sql
    assert "@@" not in where
    assert "event_at" in where


def test_daily_counts_is_not_capped_like_the_feed() -> None:
    sql, _ = _compile()
    # A histogram counts every matching row. `feed_articles` truncates to
    # `max_per_source` through a LATERAL LIMIT, and inheriting either would
    # silently turn the counts into counts-of-a-page.
    assert "LIMIT" not in sql.upper()
    assert "LATERAL" not in sql.upper()


def test_daily_counts_scopes_sources_without_a_join() -> None:
    sql, _ = _compile(source_slugs=["acme"])
    # `raw_articles.source_id` is nullable, so a join would drop orphan rows
    # from the denominator.
    assert "JOIN" not in sql.upper()
    assert "source_id IN (SELECT" in sql


def test_daily_counts_applies_the_topic_filter() -> None:
    sql, _ = _compile(topic_slugs=["open-source"])
    assert "EXISTS" in sql.upper()
    assert "article_topics" in sql


def test_day_span_covers_whole_days_without_the_exclusive_bound() -> None:
    # `until` is exclusive, so a window ending exactly at midnight must not
    # emit a bucket for the day it stops on -- that bar could only ever be
    # zero, and a permanent trailing zero looks exactly like a crawl gap.
    assert _utc_day_span(
        datetime(2026, 3, 2, tzinfo=timezone.utc),
        datetime(2026, 3, 5, tzinfo=timezone.utc),
    ) == [
        (date(2026, 3, 2), False),
        (date(2026, 3, 3), False),
        (date(2026, 3, 4), False),
    ]


def test_day_span_flags_partial_edges() -> None:
    assert _utc_day_span(
        datetime(2026, 3, 2, 12, tzinfo=timezone.utc),
        datetime(2026, 3, 3, 12, tzinfo=timezone.utc),
    ) == [(date(2026, 3, 2), True), (date(2026, 3, 3), True)]


def test_day_span_is_empty_for_a_collapsed_window() -> None:
    moment = datetime(2026, 3, 2, tzinfo=timezone.utc)
    assert _utc_day_span(moment, moment) == []
    assert _utc_day_span(moment, moment.replace(day=1)) == []


def test_day_span_normalises_non_utc_bounds() -> None:
    # A caller handing in a +09:00 bound must still get UTC buckets.
    tokyo = timezone(__import__("datetime").timedelta(hours=9))
    spans = _utc_day_span(
        datetime(2026, 3, 2, 8, tzinfo=tokyo),  # 2026-03-01T23:00Z
        datetime(2026, 3, 3, 8, tzinfo=tokyo),  # 2026-03-02T23:00Z
    )
    assert [day for day, _ in spans] == [date(2026, 3, 1), date(2026, 3, 2)]
