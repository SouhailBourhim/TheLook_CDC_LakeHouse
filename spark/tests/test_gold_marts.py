"""Tests for the gold marts: every metric checked against the definitions
written once in ADR 015 (FR6), with numbers worked out by hand."""

import datetime
from decimal import Decimal

from lakehouse.gold import build_marts, daily_revenue, product_ratings, session_funnel

ORDERS = (
    "order_id string, order_date_key int, is_cancelled boolean, gross_amount decimal(12,2), "
    "returned_amount decimal(12,2), net_amount decimal(12,2), cost_amount decimal(12,2), "
    "hours_to_ship double, hours_to_deliver double, _computed_at timestamp"
)
ITEMS = (
    "order_item_id string, order_date_key int, status string, is_returned boolean, "
    "_computed_at timestamp"
)
SESSIONS = (
    "session_id string, session_date_key int, is_ghost boolean, viewed_product boolean, "
    "added_to_cart boolean, purchased boolean, _computed_at timestamp"
)
D1, D2 = 20261004, 20261005
T1, T2 = datetime.datetime(2026, 10, 5, 1, 0), datetime.datetime(2026, 10, 5, 2, 0)


def money(x):
    return Decimal(x)


def day_orders(spark, computed=T1):
    # o1: 25 gross (a 10 item kept, a 15 item returned, a cancelled item
    # excluded), 15 returned, 10 net, cost 4 of the kept item.
    # o2: cancelled. o3: 30 gross, 30 net, cost 12.
    return spark.createDataFrame(
        [
            (
                "o1",
                D1,
                False,
                money("25"),
                money("15"),
                money("10"),
                money("4"),
                2.0,
                30.0,
                computed,
            ),
            (
                "o2",
                D1,
                True,
                money("0"),
                money("0"),
                money("0"),
                money("0"),
                None,
                None,
                computed,
            ),
            (
                "o3",
                D1,
                False,
                money("30"),
                money("0"),
                money("30"),
                money("12"),
                4.0,
                50.0,
                computed,
            ),
        ],
        ORDERS,
    )


def day_items(spark, computed=T1):
    return spark.createDataFrame(
        [
            ("i1", D1, "Delivered", False, computed),
            ("i2", D1, "Returned", True, computed),
            ("i3", D1, "Cancelled", False, computed),
            ("i4", D1, "Delivered", False, computed),
        ],
        ITEMS,
    )


def test_daily_revenue_follows_the_written_definitions(spark):
    (row,) = daily_revenue(day_orders(spark), day_items(spark)).collect()
    assert (row.orders, row.cancelled_orders) == (3, 1)
    assert row.gross_revenue == money("55.00")  # cancelled items never counted
    assert row.returns == money("15.00")
    assert row.net_revenue == money("40.00")  # gross - returns
    assert row.gross_margin == 0.6  # (40 - 16) / 40
    assert row.average_order_value == money("20.00")  # 40 / 2 orders not cancelled
    assert row.cancellation_rate == 0.3333  # 1 / 3
    assert row.return_rate == 0.3333  # 1 returned / 3 items that reached the customer
    assert row.date == datetime.date(2026, 10, 4)


def test_funnel_excludes_ghost_sessions(spark):
    sessions = spark.createDataFrame(
        [
            ("s1", D1, False, True, True, True, T1),
            ("s2", D1, False, True, False, False, T1),
            ("s3", D1, True, False, False, True, T1),  # ghost with a fake purchase
        ],
        SESSIONS,
    )
    (row,) = session_funnel(sessions).collect()
    assert (row.sessions, row.viewed_product, row.added_to_cart, row.purchased) == (
        2,
        2,
        1,
        1,
    )
    assert row.ghost_sessions == 1 and row.conversion_rate == 0.5


def test_product_ratings(spark):
    reviews = spark.createDataFrame(
        [(7, 5, 2, T1), (7, 4, 0, T2), (8, 1, 9, T1)],
        "product_id bigint, rating int, helpful_votes int, updated_at timestamp",
    )
    products = spark.createDataFrame(
        [
            (7, "Shrug", "Brand", "Sweaters", "Women"),
            (8, "Jeans", "B2", "Jeans", "Men"),
        ],
        "product_id bigint, name string, brand string, category string, department string",
    )
    rows = {r.product_id: r for r in product_ratings(reviews, products).collect()}
    assert (rows[7].review_count, rows[7].average_rating, rows[7].helpful_votes) == (
        2,
        4.5,
        2,
    )
    assert rows[7].last_review_at == T2 and rows[8].name == "Jeans"


def test_marts_recompute_only_the_days_touched_since_the_last_run(spark, lake):
    _, gold = lake
    second_day = spark.createDataFrame(
        [
            (
                "o9",
                D2,
                False,
                money("5"),
                money("0"),
                money("5"),
                money("1"),
                1.0,
                2.0,
                T1,
            )
        ],
        ORDERS,
    )
    day_orders(spark).unionByName(second_day).writeTo(f"{gold}.fct_orders").using(
        "iceberg"
    ).create()
    day_items(spark).writeTo(f"{gold}.fct_order_items").using("iceberg").create()
    spark.createDataFrame([("s1", D1, False, True, True, True, T1)], SESSIONS).writeTo(
        f"{gold}.fct_sessions"
    ).using("iceberg").create()
    spark.createDataFrame(
        [(7, 5, 0, T1)],
        "product_id bigint, rating int, helpful_votes int, updated_at timestamp",
    ).writeTo(f"{gold}.reviews").using("iceberg").create()
    spark.createDataFrame(
        [(7, "Shrug", "Brand", "Sweaters", "Women")],
        "product_id bigint, name string, brand string, category string, department string",
    ).writeTo(f"{gold}.dim_product").using("iceberg").create()

    first = build_marts(spark, gold, silver_db=gold)
    assert first["mart_daily_revenue_days"] == 2

    # A later facts run recomputes order o9 only (day D2).
    spark.sql(
        f"UPDATE {gold}.fct_orders SET net_amount = 3, _computed_at = TIMESTAMP "
        f"'2026-10-05 02:00:00' WHERE order_id = 'o9'"
    )
    second = build_marts(spark, gold, silver_db=gold)

    assert (
        second["mart_daily_revenue_days"] == 1
        and second["mart_session_funnel_days"] == 0
    )
    revenue = {
        r.date_key: r.net_revenue
        for r in spark.table(f"{gold}.mart_daily_revenue").collect()
    }
    assert revenue == {D1: money("40.00"), D2: money("3.00")}
