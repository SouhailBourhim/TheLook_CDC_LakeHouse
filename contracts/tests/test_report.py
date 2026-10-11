# Run from contracts/:  python -m pytest
"""The quality report's decisions (no AWS, no databases)."""

from datetime import datetime, timedelta

from report import BRONZE_LIMIT, counts_match, cut, fresh, lag, parse_ts

T = datetime(2026, 10, 11, 3, 0, 0)


def test_athena_timestamps_are_naive_utc():
    assert parse_ts("2026-10-11 02:58:00.123000 UTC") == datetime(
        2026, 10, 11, 2, 58, 0, 123000
    )
    assert parse_ts(None) is None


def test_lag_is_source_minus_lake_never_negative():
    assert lag(T, T - timedelta(seconds=90)) == timedelta(seconds=90)
    assert lag(T, T + timedelta(milliseconds=5)) == timedelta(0)


def test_an_idle_source_is_not_stale():
    # Measured against the source's newest record, not the clock: a stopped
    # generator does not raise a freshness alert.
    assert fresh(lag(T - timedelta(hours=5), T - timedelta(hours=5)), BRONZE_LIMIT)


def test_a_lake_with_nothing_recent_is_stale():
    assert not fresh(lag(T, None), BRONZE_LIMIT)
    assert not fresh(lag(T, T - timedelta(minutes=11)), BRONZE_LIMIT)


def test_the_cut_is_the_oldest_newest_time_minus_the_margin():
    assert cut([T, T - timedelta(minutes=3)]) == T - timedelta(minutes=13)


def test_the_cut_is_whole_seconds():
    # The same value goes to Athena (seconds) and MongoDB: no microseconds.
    assert cut([T.replace(microsecond=987_000)]).microsecond == 0


def test_counts_must_match_exactly():
    assert counts_match(1000, 1000)
    assert not counts_match(1000, 999)
    assert not counts_match(1000, 1001)


def test_reviews_deleted_at_the_source_are_counted_from_bronze():
    # 42 more in silver, all deleted at the source since silver's cut, and
    # bronze holds every deletion up to the moment the source was counted.
    assert counts_match(25_733, 25_775, pending_deletes=42)
    assert not counts_match(25_733, 25_775, pending_deletes=39)
    # Fewer in silver than at the source is never explained by deletions.
    assert not counts_match(25_733, 25_730, pending_deletes=0)
