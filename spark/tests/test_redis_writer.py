"""Tests for lakehouse.redis_writer on fakeredis: replays, order, expiry.

Redis expires keys on its own clock, so the tests use the real time as
"now" (an expiry in the past would delete the key at once).
"""

import time

import fakeredis
import pytest
from pyspark.sql import Row
from test_features import bronze, event

from lakehouse.features import FAMILIES, HOUR_MS, feature_events, redis_rows
from lakehouse.redis_writer import CHUNK, write

MIN = 60_000


@pytest.fixture
def now():
    return int(time.time() * 1000)


@pytest.fixture
def r():
    # Its own server per test: no state shared between tests.
    return fakeredis.FakeRedis(server=fakeredis.FakeServer())


def zrow(family, key, member, score, expire_at):
    return Row(
        family=family,
        key=key,
        member=member,
        score=float(score),
        value=None,
        expire_at_ms=expire_at,
    )


def state(client):
    """Every key with its content and absolute expiry (ms)."""
    out = {}
    for key in sorted(client.scan_iter("*")):
        if client.type(key) == b"zset":
            content = client.zrange(key, 0, -1, withscores=True)
        else:
            content = sorted(client.hgetall(key).items())
        out[key] = (content, client.pexpiretime(key))
    return out


def batch_rows(spark, docs, now):
    return redis_rows(feature_events(bronze(spark, docs)), now).collect()


# --- idempotency and order ---------------------------------------------------------


def test_same_batch_twice_gives_the_same_state(spark, r, now):
    docs = [
        event("7", "s1", "product", now - 3 * MIN, uri="/product/42"),
        event("7", "s1", "cart", now - 2 * MIN, "/cart", 42, 34.99, "c1"),
        event("7", "s1", "purchase", now - MIN, "/purchase"),
    ]
    rows = batch_rows(spark, docs, now)
    write(r, rows, now)
    once = state(r)
    write(r, rows, now)
    assert state(r) == once
    assert set(once) == {
        b"user:7:viewed",
        b"user:7:events",
        b"user:7:session",
        b"user:7:cart:s1",
    }


def test_batches_in_either_order_give_the_same_state(spark, now):
    early = [
        event("7", "a", "product", now - 30 * MIN, uri=f"/product/{p}")
        for p in range(1, 8)
    ]
    late = [
        event("7", "b", "product", now - (10 - p) * MIN, uri=f"/product/{p}")
        for p in range(5, 10)
    ] + [event("7", "b", "cart", now - MIN, "/cart", 9, 20.0, "c")]
    first, second = batch_rows(spark, early, now), batch_rows(spark, late, now)

    forward = fakeredis.FakeRedis(server=fakeredis.FakeServer())
    write(forward, first, now)
    write(forward, second, now)
    backward = fakeredis.FakeRedis(server=fakeredis.FakeServer())
    write(backward, second, now)
    write(backward, first, now)
    assert state(forward) == state(backward)
    # The newer views of 5-7 won (GT): the session and the order agree.
    assert forward.zrevrange("user:7:session", 0, -1) == [b"b"]
    assert forward.zscore("user:7:viewed", "5") == now - 5 * MIN


def test_an_older_replayed_view_does_not_lower_a_score(r, now):
    key, exp = "user:7:viewed", now + HOUR_MS
    write(r, [zrow("viewed", key, "42", now - MIN, exp)], now)
    write(r, [zrow("viewed", key, "42", now - 10 * MIN, exp)], now)
    assert r.zscore(key, "42") == now - MIN


# --- trims ----------------------------------------------------------------------


def test_viewed_keeps_ten_and_session_keeps_one(r, now):
    exp = now + HOUR_MS
    write(
        r,
        [zrow("viewed", "user:7:viewed", str(p), now - p, exp) for p in range(15)]
        + [
            zrow("session", "user:7:session", s, now - i, exp)
            for i, s in enumerate("abc")
        ],
        now,
    )
    assert r.zcard("user:7:viewed") == FAMILIES["viewed"].keep
    assert r.zrange("user:7:viewed", 0, -1) == [
        str(p).encode() for p in range(9, -1, -1)
    ]
    assert r.zrange("user:7:session", 0, -1) == [b"a"]


def test_events_window_drops_members_older_than_an_hour(r, now):
    key, exp = "user:7:events", now + HOUR_MS
    write(r, [zrow("events", key, "old", now - HOUR_MS - 1, exp)], now - 2 * MIN)
    write(r, [zrow("events", key, "new", now - MIN, exp)], now)
    assert r.zrange(key, 0, -1) == [b"new"]


# --- expiry ------------------------------------------------------------------------


def test_every_key_gets_the_expiry_of_its_rows(spark, r, now):
    docs = [
        event("7", "s", "product", now - 2 * MIN, uri="/product/1"),
        event("7", "s", "cart", now - MIN, "/cart", 1, 9.5, "c"),
    ]
    rows = batch_rows(spark, docs, now)
    write(r, rows, now)
    expected = {row.key.encode(): row.expire_at_ms for row in rows}
    assert {k: v[1] for k, v in state(r).items()} == expected
    assert expected[b"user:7:events"] == now - MIN + HOUR_MS


def test_expiry_is_extended_by_newer_rows_never_shortened(r, now):
    key = "user:7:viewed"
    write(r, [zrow("viewed", key, "1", now, now + 2 * HOUR_MS)], now)
    write(r, [zrow("viewed", key, "2", now, now + HOUR_MS)], now)  # older replay
    assert r.pexpiretime(key) == now + 2 * HOUR_MS
    write(r, [zrow("viewed", key, "3", now, now + 3 * HOUR_MS)], now)
    assert r.pexpiretime(key) == now + 3 * HOUR_MS


def test_gt_alone_never_sets_an_expiry_on_a_new_key(r, now):
    # Why the writer sends NX first: Redis treats a key without an expiry as
    # expiring never, and no time is greater than never.
    r.zadd("k", {"m": 1})
    assert r.pexpireat("k", now + HOUR_MS, gt=True) == 0
    assert r.pttl("k") == -1  # no expiry
    assert r.pexpireat("k", now + HOUR_MS, nx=True) == 1
    assert r.pttl("k") > 0


# --- chunking ----------------------------------------------------------------------


def test_more_rows_than_a_pipeline_chunk(r, now):
    exp = now + HOUR_MS
    rows = [zrow("events", f"user:{u}:events", "e", now, exp) for u in range(CHUNK + 5)]
    assert write(r, rows, now) == CHUNK + 5
    assert r.dbsize() == CHUNK + 5


def test_a_new_view_trims_views_older_than_72_hours(r, now):
    key, exp = "user:7:viewed", now + HOUR_MS
    write(r, [zrow("viewed", key, "old", now - 73 * HOUR_MS, exp)], now - 2 * HOUR_MS)
    write(r, [zrow("viewed", key, "new", now - MIN, exp)], now)
    assert r.zrange(key, 0, -1) == [b"new"]
