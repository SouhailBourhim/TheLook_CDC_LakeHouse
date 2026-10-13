"""Tests for the gold backfill (lakehouse.gold.rebuild_days, FR10): a day
range recomputed from silver replaces exactly that range, a rerun changes
nothing, and nothing outside the range moves."""

import datetime
from decimal import Decimal

import pytest
from test_gold_facts import EVENTS, at, item, order, setup, write

from lakehouse.gold import build_facts, rebuild_days

DAY3, DAY4, DAY5 = (datetime.date(2026, 10, d) for d in (3, 4, 5))


@pytest.fixture
def built(spark, lake):
    """Silver with orders on three days (one item each) and gold facts and
    daily marts built from it."""
    silver, gold = setup(
        spark,
        lake,
        [item("i3", "o3"), item("i4", "o4"), item("i5", "o5")],
        [
            order("o3", created=at(11, day=3)),
            order("o4", created=at(11, day=4)),
            order("o5", created=at(11, day=5)),
        ],
    )
    write(
        spark,
        f"{silver}.events",
        [
            (
                "e3",
                "s3",
                "u1",
                "cart",
                at(10, day=3),
                Decimal("5.00"),
                "Email",
                "Chrome",
                at(20),
            ),
            (
                "e4",
                "s4",
                "u1",
                "cart",
                at(10, day=4),
                Decimal("6.00"),
                "Email",
                "Chrome",
                at(20),
            ),
        ],
        EVENTS,
    )
    build_facts(spark, silver, gold)
    rebuild_days(spark, DAY3, DAY5, silver, gold)  # creates the daily marts
    return silver, gold


def snapshot(spark, gold, table, key):
    drop = {"_computed_at"}
    return {
        r[key]: {k: v for k, v in r.asDict().items() if k not in drop}
        for r in spark.table(f"{gold}.{table}").collect()
    }


def revenue(spark, gold):
    return {
        r.date_key: r.gross_revenue
        for r in spark.table(f"{gold}.mart_daily_revenue").collect()
    }


def test_a_rerun_gives_the_same_rows_and_no_duplicates(spark, built):
    silver, gold = built
    before = {
        t: snapshot(spark, gold, t, k)
        for t, k in [
            ("fct_order_items", "order_item_id"),
            ("fct_orders", "order_id"),
            ("fct_sessions", "session_id"),
            ("mart_daily_revenue", "date_key"),
        ]
    }

    rebuild_days(spark, DAY3, DAY5, silver, gold)
    rebuild_days(spark, DAY4, DAY4, silver, gold)

    for (table, key), rows in zip(
        [
            ("fct_order_items", "order_item_id"),
            ("fct_orders", "order_id"),
            ("fct_sessions", "session_id"),
            ("mart_daily_revenue", "date_key"),
        ],
        before.values(),
        strict=True,
    ):
        assert snapshot(spark, gold, table, key) == rows, table
        df = spark.table(f"{gold}.{table}")
        assert df.count() == df.select(key).distinct().count(), table


def test_only_the_range_is_recomputed(spark, built):
    # A logic change shows up as different gold values: simulate it by
    # editing silver for days 4 and 5, then rebuild day 4 only.
    silver, gold = built
    spark.sql(
        f"UPDATE {silver}.order_items SET sale_price = 99.00 WHERE id IN ('i4', 'i5')"
    )

    counts = rebuild_days(spark, DAY4, DAY4, silver, gold)

    assert counts["fct_order_items"] == 1 and counts["fct_orders"] == 1
    prices = {
        r.order_item_id: r.sale_price
        for r in spark.table(f"{gold}.fct_order_items").collect()
    }
    assert prices == {
        "i3": Decimal("10.00"),
        "i4": Decimal("99.00"),
        "i5": Decimal("10.00"),
    }
    assert revenue(spark, gold) == {
        20261003: Decimal("10.00"),
        20261004: Decimal("99.00"),
        20261005: Decimal("10.00"),
    }


def test_rows_silver_no_longer_has_leave_gold_too(spark, built):
    # e.g. a GDPR erasure: the backfill must remove, never bring back.
    silver, gold = built
    spark.sql(f"DELETE FROM {silver}.orders WHERE id = 'o4'")
    spark.sql(f"DELETE FROM {silver}.order_items WHERE id = 'i4'")
    spark.sql(f"DELETE FROM {silver}.events WHERE id = 'e4'")

    rebuild_days(spark, DAY4, DAY4, silver, gold)

    assert "i4" not in snapshot(spark, gold, "fct_order_items", "order_item_id")
    assert "o4" not in snapshot(spark, gold, "fct_orders", "order_id")
    assert "s4" not in snapshot(spark, gold, "fct_sessions", "session_id")
    # The day has no order left: its mart row goes; the others stay.
    assert set(revenue(spark, gold)) == {20261003, 20261005}


def test_an_inverted_range_is_refused(spark, built):
    silver, gold = built
    with pytest.raises(ValueError):
        rebuild_days(spark, DAY5, DAY3, silver, gold)
