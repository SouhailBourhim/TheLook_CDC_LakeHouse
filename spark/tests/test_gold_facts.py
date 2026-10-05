"""Tests for the gold facts (ADR 015): point-in-time user join, FR6 amount
rules, sessions, and the incremental build on a local Iceberg catalog."""

import datetime
from decimal import Decimal

from lakehouse.gold import (
    OPEN_END,
    build_facts,
    order_facts,
    order_item_facts,
    session_facts,
)

ITEMS = (
    "id string, order_id string, product_id bigint, status string, quantity int, "
    "sale_price decimal(10,2), shipped_at timestamp, delivered_at timestamp, "
    "returned_at timestamp, cancelled_at timestamp, _merged_at timestamp"
)
ORDERS = (
    "id string, user_id string, status string, num_of_items int, created_at timestamp, "
    "shipped_at timestamp, delivered_at timestamp, returned_at timestamp, "
    "cancelled_at timestamp, _merged_at timestamp"
)
EVENTS = (
    "id string, session_id string, user_id string, event_type string, created_at timestamp, "
    "price decimal(10,2), traffic_source string, browser string, _merged_at timestamp"
)
PRODUCTS = "product_id bigint, cost decimal(10,2), distribution_center_id bigint"
USERS = "user_sk bigint, user_id string, valid_from timestamp, valid_to timestamp"


def at(hour, minute=0, day=4):
    return datetime.datetime(2026, 10, day, hour, minute)


# Default arguments are evaluated once, at definition time (ruff B008):
# use constants rather than calls in the signatures below.
ORDERED = at(11)
MERGED = at(20)


def item(
    item_id, order_id="o1", status="Delivered", qty=1, price="10.00", merged=MERGED
):
    shipped = at(14) if status in ("Shipped", "Delivered", "Returned") else None
    return (
        item_id,
        order_id,
        7,
        status,
        qty,
        Decimal(price),
        shipped,
        None,
        None,
        None,
        merged,
    )


def order(order_id, user="u1", created=ORDERED, status="Delivered", merged=MERGED):
    return (order_id, user, status, 1, created, None, None, None, None, merged)


def frames(spark, items, orders, users=None):
    users = users or [
        (100, "u1", at(0, day=1), at(12)),  # version 1 until the address change
        (200, "u1", at(12), OPEN_END),  # version 2
    ]
    return (
        spark.createDataFrame(items, ITEMS),
        spark.createDataFrame(orders, ORDERS),
        spark.createDataFrame([(7, Decimal("4.00"), 3)], PRODUCTS),
        spark.createDataFrame(users, USERS),
    )


# --- pure transforms -------------------------------------------------------------


def test_an_order_joins_the_user_version_valid_when_it_was_placed(spark):
    facts = order_item_facts(
        *frames(
            spark,
            [item("i1", "o1"), item("i2", "o2")],
            [order("o1", created=at(11)), order("o2", created=at(13))],
        )
    )
    sk = {r.order_item_id: r.user_sk for r in facts.collect()}
    assert sk == {"i1": 100, "i2": 200}  # before and after the 12:00 change


def test_line_amounts_flags_and_durations(spark):
    (row,) = order_item_facts(
        *frames(spark, [item("i1", qty=2, price="10.00")], [order("o1")])
    ).collect()
    assert row.gross_amount == Decimal("20.00") and row.cost_amount == Decimal("8.00")
    assert row.distribution_center_id == 3 and row.order_date_key == 20261004
    assert not row.is_cancelled and not row.is_returned
    assert row.hours_to_ship == 3.0  # ordered 11:00, shipped 14:00


def test_no_user_version_points_to_the_unknown_member(spark):
    (row,) = order_item_facts(
        *frames(spark, [item("i1")], [order("o1", user="u-new")])
    ).collect()
    assert row.user_sk == -1  # kept by inner joins, reported as "Unknown"


def test_order_amounts_follow_the_fr6_rules(spark):
    items = [
        item("i1", "o1", "Delivered", 1, "10.00"),
        item("i2", "o1", "Returned", 1, "15.00"),
        item("i3", "o1", "Cancelled", 2, "50.00"),
    ]
    i, o, p, u = frames(spark, items, [order("o1")])
    (row,) = order_facts(o, order_item_facts(i, o, p, u)).collect()
    assert row.item_count == 3
    assert row.gross_amount == Decimal("25.00")  # cancelled item excluded
    assert row.returned_amount == Decimal("15.00")
    assert row.net_amount == Decimal("10.00")  # gross - returns
    assert row.cost_amount == Decimal("4.00")  # kept items only (i1)


def test_sessions_flag_ghosts_and_sum_cart_values(spark):
    events = spark.createDataFrame(
        [
            ("e1", "s1", "u1", "product", at(10, 0), None, "Email", "Chrome", at(20)),
            (
                "e2",
                "s1",
                "u1",
                "cart",
                at(10, 1),
                Decimal("34.99"),
                "Email",
                "Chrome",
                at(20),
            ),
            ("e3", "s1", "u1", "purchase", at(10, 2), None, "Email", "Chrome", at(20)),
            (
                "e4",
                "s2",
                None,
                "purchase",
                at(10, 5),
                None,
                "Organic",
                "Safari",
                at(20),
            ),
        ],
        EVENTS,
    )
    rows = {r.session_id: r for r in session_facts(events).collect()}
    s1, s2 = rows["s1"], rows["s2"]
    assert (s1.is_ghost, s1.purchased, s1.added_to_cart, s1.event_count) == (
        False,
        True,
        True,
        3,
    )
    assert s1.cart_value == Decimal("34.99") and s1.session_date_key == 20261004
    assert s2.is_ghost and s2.purchased  # a fake purchase, excluded by funnels


# --- incremental build on a local Iceberg catalog --------------------------------


def write(spark, table, rows, schema):
    spark.createDataFrame(rows, schema).writeTo(table).using(
        "iceberg"
    ).createOrReplace()


def setup(spark, lake, items, orders, users=None):
    silver, gold = lake  # the fixture's two fresh namespaces
    write(spark, f"{silver}.order_items", items, ITEMS)
    write(spark, f"{silver}.orders", orders, ORDERS)
    write(
        spark,
        f"{silver}.events",
        [
            (
                "e1",
                "s1",
                "u1",
                "cart",
                at(10),
                Decimal("10.00"),
                "Email",
                "Chrome",
                at(20),
            )
        ],
        EVENTS,
    )
    write(spark, f"{gold}.dim_product", [(7, Decimal("4.00"), 3)], PRODUCTS)
    users = users or [(100, "u1", at(0, day=1), OPEN_END)]
    write(spark, f"{gold}.dim_user", users, USERS)
    return silver, gold


def test_second_run_recomputes_only_what_changed(spark, lake):
    silver, gold = setup(
        spark, lake, [item("i1"), item("i2", "o2")], [order("o1"), order("o2")]
    )
    first = build_facts(spark, silver, gold)
    assert first == {
        "fct_order_items_repaired": 0,
        "fct_orders_repaired": 0,
        "fct_order_items_recomputed": 2,
        "fct_orders_recomputed": 2,
        "fct_sessions_recomputed": 1,
    }

    # A later silver run changes item i2 (and its order) only.
    spark.createDataFrame(
        [item("i2", "o2", status="Returned", merged=at(21))], ITEMS
    ).writeTo(f"{silver}.order_items").append()
    spark.createDataFrame(
        [order("o2", status="Returned", merged=at(21))], ORDERS
    ).writeTo(f"{silver}.orders").append()
    spark.sql(
        f"DELETE FROM {silver}.order_items WHERE id = 'i2' AND _merged_at = TIMESTAMP '2026-10-04 20:00:00'"
    )
    spark.sql(
        f"DELETE FROM {silver}.orders WHERE id = 'o2' AND _merged_at = TIMESTAMP '2026-10-04 20:00:00'"
    )
    second = build_facts(spark, silver, gold)

    assert second == {
        "fct_order_items_repaired": 0,
        "fct_orders_repaired": 0,
        "fct_order_items_recomputed": 1,
        "fct_orders_recomputed": 1,
        "fct_sessions_recomputed": 0,
    }
    o2 = spark.table(f"{gold}.fct_orders").where("order_id = 'o2'").first()
    assert o2.is_returned and o2.net_amount == Decimal("0.00")


def test_rows_without_a_user_version_are_retried_until_one_exists(spark, lake):
    silver, gold = setup(spark, lake, [item("i1")], [order("o1", user="u-late")])
    build_facts(spark, silver, gold)
    assert spark.table(f"{gold}.fct_order_items").first().user_sk == -1

    # The user's version appears in a later dim_user rebuild; nothing changed
    # in silver, yet the fact is repaired.
    write(spark, f"{gold}.dim_user", [(300, "u-late", at(0, day=1), OPEN_END)], USERS)
    second = build_facts(spark, silver, gold)

    # Repaired from the fact rows themselves: nothing recomputed from silver.
    assert (second["fct_order_items_repaired"], second["fct_orders_repaired"]) == (1, 1)
    assert second["fct_order_items_recomputed"] == 0
    assert spark.table(f"{gold}.fct_order_items").first().user_sk == 300
    assert spark.table(f"{gold}.fct_orders").first().user_sk == 300


def test_items_built_before_their_order_are_recomputed_when_it_arrives(spark, lake):
    silver, gold = setup(spark, lake, [item("i1", "o9")], [order("o1")])
    build_facts(spark, silver, gold)
    first = spark.table(f"{gold}.fct_order_items").first()
    assert first.created_at is None and first.user_sk == -1  # o9 not in silver

    # The order reaches silver in a later run; the item itself does not change.
    spark.createDataFrame([order("o9", merged=at(21))], ORDERS).writeTo(
        f"{silver}.orders"
    ).append()
    second = build_facts(spark, silver, gold)

    row = spark.table(f"{gold}.fct_order_items").first()
    assert second["fct_order_items_recomputed"] == 1
    assert row.created_at == ORDERED and row.user_sk == 100
