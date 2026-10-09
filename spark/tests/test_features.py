"""Tests for lakehouse.features: web.events change events -> Redis writes.

Input rows have the bronze shape (op, after as MongoDB Extended JSON); the
decoding that produces them is tested in test_cdc.py.
"""

import json
import uuid

from chispa import assert_df_equality

from lakehouse.features import FAMILIES, HOUR_MS, feature_events, redis_rows

NOW = 1_760_000_000_000  # epoch ms
MIN = 60_000
DAY = 24 * HOUR_MS


def event(
    user,
    session,
    event_type,
    ms,
    uri="/",
    product_id=None,
    price=None,
    event_id=None,
):
    doc = {
        "_id": event_id or str(uuid.uuid4()),
        "session_id": session,
        "sequence_number": 1,
        "event_type": event_type,
        "uri": uri,
        "created_at": {"$date": ms},
    }
    if user is not None:
        doc["user_id"] = user
    if product_id is not None:
        doc["product_id"] = product_id
    if price is not None:
        doc["price"] = price
    return json.dumps(doc)


def bronze(spark, docs, op="c"):
    return spark.createDataFrame([(op, d) for d in docs], "op string, after string")


def rows_of(spark, docs, now=NOW):
    return redis_rows(feature_events(bronze(spark, docs)), now)


def written(df, family=None):
    """{(key, member): (score, value, expire_at_ms)} for one family."""
    return {
        (r.key, r.member): (r.score, r.value, r.expire_at_ms)
        for r in df.collect()
        if family is None or r.family == family
    }


# --- feature_events --------------------------------------------------------------


def test_typed_event_with_product_from_uri_or_field(spark):
    docs = [
        event("7", "s1", "product", NOW, uri="/product/42", event_id="e1"),
        event("7", "s1", "cart", NOW + 1, "/cart", 42, 34.9900016784668, "e2"),
        event("7", "s1", "department", NOW + 2, "/department/men", event_id="e3"),
    ]
    expected = spark.createDataFrame(
        [
            ("7", "e1", "s1", "product", NOW, 42, None),
            ("7", "e2", "s1", "cart", NOW + 1, 42, "34.99"),
            ("7", "e3", "s1", "department", NOW + 2, None, None),
        ],
        "user_id string, event_id string, session_id string, event_type string, "
        "event_ms long, product_id long, price string",
    )
    assert_df_equality(
        feature_events(bronze(spark, docs)),
        expected,
        ignore_row_order=True,
        ignore_nullable=True,
    )


def test_ghost_sessions_are_skipped(spark):
    docs = [event(None, "ghost", "product", NOW, uri="/product/1")]
    assert feature_events(bronze(spark, docs)).count() == 0


def test_only_inserts_and_snapshot_reads_count(spark):
    doc = event("7", "s1", "home", NOW)
    kept = [
        op
        for op in ("c", "r", "u", "d")
        if feature_events(bronze(spark, [doc], op)).count()
    ]
    assert kept == ["c", "r"]


def test_unparseable_document_is_dropped(spark):
    assert feature_events(bronze(spark, ["not json", '{"no_id": 1}'])).count() == 0


def test_product_uri_that_does_not_match_gives_no_product(spark):
    docs = [event("7", "s1", "product", NOW, uri="/product/abc")]
    assert feature_events(bronze(spark, docs)).first().product_id is None


# --- viewed ----------------------------------------------------------------------


def test_viewed_keeps_last_view_per_product(spark):
    docs = [
        event("7", "s1", "product", NOW - 5 * MIN, uri="/product/42"),
        event("7", "s2", "product", NOW - MIN, uri="/product/42"),
    ]
    viewed = written(rows_of(spark, docs), "viewed")
    assert viewed == {
        ("user:7:viewed", "42"): (NOW - MIN, None, NOW - MIN + 72 * HOUR_MS)
    }


def test_viewed_keeps_ten_newest_ties_broken_like_redis(spark):
    # 11 products; the two oldest share a time: Redis orders equal scores by
    # member bytes ("10" < "11") and trims the lowest rank, so "10" goes.
    docs = [
        event("7", "s", "product", NOW - (20 - p) * MIN, uri=f"/product/{p}")
        for p in range(1, 10)
    ] + [
        event("7", "s", "product", NOW - 30 * MIN, uri="/product/10"),
        event("7", "s", "product", NOW - 30 * MIN, uri="/product/11"),
    ]
    members = {m for _, m in written(rows_of(spark, docs), "viewed")}
    assert len(members) == FAMILIES["viewed"].keep
    assert members == {str(p) for p in range(1, 10)} | {"11"}


# --- events (last hour) -------------------------------------------------------------


def test_events_are_one_member_each_within_the_hour(spark):
    docs = [
        event("7", "s1", "home", NOW - 10 * MIN, event_id="new"),
        event("7", "s1", "home", NOW - 2 * HOUR_MS, event_id="old"),
    ]
    events = written(rows_of(spark, docs), "events")
    # Key expires one hour after its newest event.
    assert events == {
        ("user:7:events", "new"): (NOW - 10 * MIN, None, NOW - 10 * MIN + HOUR_MS)
    }


# --- session and cart -----------------------------------------------------------------


def test_session_is_the_newest_one(spark):
    docs = [
        event("7", "older", "home", NOW - 10 * MIN),
        event("7", "newer", "home", NOW - 9 * MIN),
        event("7", "newer", "product", NOW - 8 * MIN, uri="/product/3"),
    ]
    assert written(rows_of(spark, docs), "session") == {
        ("user:7:session", "newer"): (NOW - 8 * MIN, None, NOW - 8 * MIN + 72 * HOUR_MS)
    }


def test_cart_items_and_purchase_expire_with_their_session(spark):
    docs = [
        event("7", "s1", "product", NOW - 3 * MIN, uri="/product/42"),
        event("7", "s1", "cart", NOW - 2 * MIN, "/cart", 42, 34.99, "c1"),
        event("7", "s1", "purchase", NOW - MIN, "/purchase"),
    ]
    expire = NOW - MIN + 72 * HOUR_MS  # the session's newest event, a purchase
    assert written(rows_of(spark, docs), "cart") == {
        ("user:7:cart:s1", "item:c1"): (None, "34.99", expire),
        ("user:7:cart:s1", "purchased"): (None, "1", expire),
    }


def test_session_without_cart_or_purchase_writes_no_cart(spark):
    docs = [event("7", "s1", "product", NOW, uri="/product/1")]
    assert written(rows_of(spark, docs), "cart") == {}


# --- expiry ------------------------------------------------------------------------


def test_every_row_of_a_key_carries_the_key_expiry(spark):
    docs = [
        event("7", "s", "product", NOW - 5 * MIN, uri="/product/1"),
        event("7", "s", "product", NOW - MIN, uri="/product/2"),
    ]
    expiries = {
        r.expire_at_ms for r in rows_of(spark, docs).collect() if r.family == "viewed"
    }
    assert expiries == {NOW - MIN + 72 * HOUR_MS}


def test_nothing_already_expired_is_written(spark):
    # 4 days old: every family's TTL (72 h at most) has passed.
    docs = [
        event("7", "s", "product", NOW - 4 * DAY, uri="/product/1"),
        event("7", "s", "cart", NOW - 4 * DAY, "/cart", 1, 9.5),
    ]
    assert rows_of(spark, docs).count() == 0


def test_values_do_not_depend_on_now(spark):
    # now only filters: a replay later writes the same values for what is
    # still alive (here, everything but the two events-family members).
    docs = [
        event("7", "s", "product", NOW - 10 * MIN, uri="/product/1", event_id="e"),
        event("7", "s", "cart", NOW - 9 * MIN, "/cart", 1, 9.5, "c"),
    ]
    first = written(rows_of(spark, docs, NOW))
    later = written(rows_of(spark, docs, NOW + 2 * HOUR_MS))
    assert set(first) - set(later) == {("user:7:events", "e"), ("user:7:events", "c")}
    assert {k: first[k] for k in later} == later


def test_views_older_than_72_hours_are_dropped_even_in_a_live_key(spark):
    # The key lives on thanks to the recent view; the old view must not
    # (personal data bounded per view; no longer in Kafka to rebuild it).
    docs = [
        event("7", "s1", "product", NOW - 73 * HOUR_MS, uri="/product/1"),
        event("7", "s2", "product", NOW - MIN, uri="/product/2"),
    ]
    assert {m for _, m in written(rows_of(spark, docs), "viewed")} == {"2"}
