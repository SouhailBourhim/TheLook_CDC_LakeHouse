"""Tests for lakehouse.lag: source-to-bronze lag per row (P9, O8)."""

import datetime

from lakehouse.lag import lag_summary, row_lags

T0 = datetime.datetime(2026, 10, 17, 10, 0, 0)


def committed_at(spark, table):
    return {
        r.snapshot_id: r.committed_at
        for r in spark.table(f"{table}.snapshots").collect()
    }


def test_each_row_is_timed_by_the_snapshot_that_added_its_file(spark, lake):
    bronze, _ = lake
    table = f"{bronze}.shop_orders"
    first = spark.createDataFrame([(1, T0)], "id int, source_ts timestamp")
    first.writeTo(table).using("iceberg").create()
    second = spark.createDataFrame(
        [(2, T0 + datetime.timedelta(seconds=30))], "id int, source_ts timestamp"
    )
    second.writeTo(table).append()
    s1, s2 = sorted(committed_at(spark, table).items(), key=lambda kv: kv[1])

    lags = {
        r.source_ts: r.lag_s
        for r in row_lags(
            spark, table, T0, T0 + datetime.timedelta(minutes=1)
        ).collect()
    }

    # Each row's lag is its own snapshot's commit, not the newest one.
    assert lags[T0] == (s1[1] - T0).total_seconds()
    later = T0 + datetime.timedelta(seconds=30)
    assert lags[later] == (s2[1] - later).total_seconds()


def test_rows_outside_the_window_are_left_out(spark, lake):
    bronze, _ = lake
    table = f"{bronze}.web_events"
    rows = [(i, T0 + datetime.timedelta(minutes=i)) for i in range(5)]
    spark.createDataFrame(rows, "id int, source_ts timestamp").writeTo(table).using(
        "iceberg"
    ).create()

    window = row_lags(
        spark,
        table,
        T0 + datetime.timedelta(minutes=1),
        T0 + datetime.timedelta(minutes=3),
    )

    assert window.count() == 2  # minutes 1 and 2; the end is excluded


def test_summary_percentiles(spark):
    lags = spark.createDataFrame([(float(i),) for i in range(1, 101)], "lag_s double")
    s = lag_summary(lags)
    assert s["rows"] == 100
    assert s["max"] == 100.0
    assert round(s["p50"], 1) == 50.5
    assert round(s["p95"], 2) == 95.05
