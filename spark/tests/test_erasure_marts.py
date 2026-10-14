"""Tests for the daily marts after an erasure (lakehouse.erasure.erase_lake,
MARTS_STEP): the user's days are recomputed from the facts left, and a
rerun after a crash before that step rebuilds every day."""

from decimal import Decimal

import pytest
from test_gold_facts import EVENTS, OPEN_END, at, item, order, setup, write

from lakehouse import erasure
from lakehouse.erasure import MARTS_STEP, Request, erase_lake
from lakehouse.gold import build_facts, rebuild_days

ALICE = "6f1c2a3b-0d4e-4f5a-8b6c-7d8e9f0a1b2c"  # erased
BOB = "0a1b2c3d-4e5f-4a6b-9c7d-8e9f0a1b2c3d"  # kept
REQUEST = Request("run-1", ALICE)


@pytest.fixture
def built(spark, lake):
    """Alice orders on days 3 and 4, Bob on day 4; one session each, Alice's
    on day 3. Gold facts and daily marts built from it."""
    silver, gold = setup(
        spark,
        lake,
        [item("i3", "o3"), item("i4", "o4"), item("i5", "o5")],
        [
            order("o3", user=ALICE, created=at(11, day=3)),
            order("o4", user=ALICE, created=at(11, day=4)),
            order("o5", user=BOB, created=at(12, day=4)),
        ],
        users=[
            (100, ALICE, at(0, day=1), OPEN_END),
            (200, BOB, at(0, day=1), OPEN_END),
        ],
    )
    price = Decimal("5.00")
    write(
        spark,
        f"{silver}.events",
        [
            (
                "e3",
                "s3",
                ALICE,
                "cart",
                at(10, day=3),
                price,
                "Email",
                "Chrome",
                at(20),
            ),
            ("e4", "s4", BOB, "cart", at(10, day=4), price, "Email", "Chrome", at(20)),
        ],
        EVENTS,
    )
    build_facts(spark, silver, gold)
    rebuild_days(spark, at(0, day=3).date(), at(0, day=4).date(), silver, gold)
    return silver, gold


def marts(spark, gold):
    revenue = {
        r.date_key: r.orders
        for r in spark.table(f"{gold}.mart_daily_revenue").collect()
    }
    funnel = {
        r.date_key: r.sessions
        for r in spark.table(f"{gold}.mart_session_funnel").collect()
    }
    return revenue, funnel


def test_the_users_days_are_recomputed_from_the_facts_left(spark, built):
    silver, gold = built
    assert marts(spark, gold) == (
        {20261003: 1, 20261004: 2},
        {20261003: 1, 20261004: 1},
    )

    report = erase_lake(spark, REQUEST, silver, gold)

    # Day 3 had only Alice: its rows are gone. Day 4 keeps Bob's order.
    assert marts(spark, gold) == ({20261004: 1}, {20261004: 1})
    assert report[MARTS_STEP] == {"mart_daily_revenue": 1, "mart_session_funnel": 0}
    steps = {r.step for r in spark.table(f"{silver}.erasure_audit").collect()}
    assert MARTS_STEP in steps


def test_a_rerun_after_a_crash_before_the_marts_rebuilds_every_day(
    spark, built, monkeypatch
):
    silver, gold = built
    real = erasure.refresh_daily_marts

    def crash(*args, **kwargs):
        raise RuntimeError("network")

    monkeypatch.setattr(erasure, "refresh_daily_marts", crash)
    with pytest.raises(RuntimeError):
        erase_lake(spark, REQUEST, silver, gold)
    # The facts are erased, the marts still count Alice.
    assert marts(spark, gold) == (
        {20261003: 1, 20261004: 2},
        {20261003: 1, 20261004: 1},
    )

    calls = []

    def spy(spark, order_days, session_days, *args):
        calls.append((order_days, session_days))
        return real(spark, order_days, session_days, *args)

    monkeypatch.setattr(erasure, "refresh_daily_marts", spy)
    erase_lake(spark, REQUEST, silver, gold)

    assert calls == [(None, None)]  # the days were lost with the facts
    assert marts(spark, gold) == ({20261004: 1}, {20261004: 1})


def test_a_rerun_after_success_recomputes_nothing(spark, built):
    silver, gold = built
    erase_lake(spark, REQUEST, silver, gold)
    again = erase_lake(spark, REQUEST, silver, gold)

    assert again[MARTS_STEP] == {"mart_daily_revenue": 0, "mart_session_funnel": 0}
    assert marts(spark, gold) == ({20261004: 1}, {20261004: 1})
