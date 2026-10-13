"""Tests for lakehouse.erasure: request parsing, and the bronze purge on a
local Iceberg catalog (rows gone, others kept, replay, expiry)."""

import json
import os

import pytest

from lakehouse import erasure
from lakehouse.bronze import already_committed, snapshot_summaries
from lakehouse.erasure import Request, apply_requests, parse_request, user_hash

ALICE = "6f1c2a3b-0d4e-4f5a-8b6c-7d8e9f0a1b2c"  # erased
BOB = "0a1b2c3d-4e5f-4a6b-9c7d-8e9f0a1b2c3d"  # kept

PG = "op string, before struct<{0}>, after struct<{0}>"


def request(**body) -> bytes:
    return json.dumps(body).encode()


def test_a_valid_request_is_parsed():
    assert parse_request(request(request_id="run-1", user_id=ALICE)) == Request(
        "run-1", ALICE
    )


@pytest.mark.parametrize(
    "value",
    [
        b"not json",
        None,
        request(user_id=ALICE),  # no request id
        request(request_id="run-1"),  # no user id
        request(request_id="run-1", user_id="42"),
        request(request_id="run-1", user_id=ALICE.upper()),  # not as stored
        request(request_id="run-1", user_id=f"{ALICE}' OR '1'='1"),
        request(request_id="", user_id=ALICE),
    ],
)
def test_anything_else_is_refused(value):
    assert parse_request(value) is None


def test_the_hash_is_stable_and_hides_the_id():
    assert user_hash(ALICE) == user_hash(ALICE) != user_hash(BOB)
    assert ALICE not in user_hash(ALICE)


def create(spark, table, schema, rows):
    spark.createDataFrame(rows, schema).writeTo(table).using("iceberg").tableProperty(
        "format-version", "2"
    ).create()


@pytest.fixture
def bronze_db(spark, lake):
    """Bronze-like tables: two users, Alice with orders, items, events and a
    review, Bob with one of each; Debezium deletes carry only the key."""
    db, _ = lake
    user = PG.format("id string, email string")
    create(
        spark,
        f"{db}.shop_users",
        user,
        [
            ("r", None, (ALICE, "alice@example.com")),
            ("u", None, (ALICE, "alice@new.example.com")),
            ("d", (ALICE, None), None),
            ("r", None, (BOB, "bob@example.com")),
        ],
    )
    order = PG.format("id string, user_id string")
    create(
        spark,
        f"{db}.shop_orders",
        order,
        [
            ("c", None, ("o-a1", ALICE)),
            ("u", None, ("o-a1", ALICE)),
            ("d", ("o-a1", None), None),  # the delete: no user_id
            ("c", None, ("o-a2", ALICE)),
            ("c", None, ("o-b1", BOB)),
        ],
    )
    item = PG.format("id string, order_id string")
    create(
        spark,
        f"{db}.shop_order_items",
        item,
        [
            ("c", None, ("i-a1", "o-a1")),
            ("d", ("i-a1", None), None),  # the delete: no order_id
            ("c", None, ("i-a2", "o-a2")),
            ("c", None, ("i-b1", "o-b1")),
        ],
    )
    mongo = "op string, doc_id string, after string"
    create(
        spark,
        f"{db}.web_events",
        mongo,
        [
            ("c", "e-a1", json.dumps({"user_id": ALICE, "ip": "10.0.0.1"})),
            ("d", "e-a1", None),  # the delete: key only
            ("c", "e-b1", json.dumps({"user_id": BOB})),
            # Alice's id as text, but not as user_id: kept.
            ("c", "e-b2", json.dumps({"user_id": BOB, "referrer": ALICE})),
            ("c", "e-ghost", json.dumps({"ip": "10.0.0.9"})),  # no user
        ],
    )
    create(
        spark,
        f"{db}.web_reviews",
        mongo,
        [("c", "rv-b1", json.dumps({"user_id": BOB, "text": "fine"}))],
    )
    return db


def ids(spark, table):
    rows = spark.sql(f"SELECT * FROM {table}").collect()
    return sorted(r.doc_id if "doc_id" in r else (r.after or r.before).id for r in rows)


def test_user_keys_follow_orders_to_their_items(spark, bronze_db):
    assert erasure.user_keys(spark, ALICE, bronze_db) == {
        "shop_users": [ALICE],
        "shop_orders": ["o-a1", "o-a2"],
        "shop_order_items": ["i-a1", "i-a2"],
        "web_events": ["e-a1"],
        "web_reviews": [],
    }


def test_an_unknown_user_has_no_keys(spark, bronze_db):
    unknown = "00000000-0000-4000-8000-000000000000"

    assert erasure.user_keys(spark, unknown, bronze_db) == {
        name: [] for name in erasure.USER_TABLES
    }


def test_purge_removes_every_image_of_the_user_and_nothing_else(spark, bronze_db):
    expired = apply_requests(spark, [Request("run-1", ALICE)], "q1", 9, bronze_db)

    assert ids(spark, f"{bronze_db}.shop_users") == [BOB]
    assert ids(spark, f"{bronze_db}.shop_orders") == ["o-b1"]
    assert ids(spark, f"{bronze_db}.shop_order_items") == ["i-b1"]
    assert ids(spark, f"{bronze_db}.web_events") == ["e-b1", "e-b2", "e-ghost"]
    assert ids(spark, f"{bronze_db}.web_reviews") == ["rv-b1"]
    # Every user table loses its history, rows deleted now or not.
    assert sorted(expired) == sorted(erasure.USER_TABLES)
    (row,) = spark.table(f"{bronze_db}.erasure_log").collect()
    assert row.request_id == "run-1"
    assert row.user_sha256 == user_hash(ALICE)
    assert row.rows_deleted == {
        "shop_users": 3,
        "shop_orders": 4,
        "shop_order_items": 3,
        "web_events": 2,
        "web_reviews": 0,
    }


def test_a_replayed_batch_purges_and_logs_nothing_more(spark, bronze_db):
    apply_requests(spark, [Request("run-1", ALICE)], "q1", 9, bronze_db)

    assert apply_requests(spark, [Request("run-1", ALICE)], "q1", 9, bronze_db) is None
    assert spark.table(f"{bronze_db}.erasure_log").count() == 1


def test_a_purge_interrupted_before_its_log_row_can_run_again(spark, bronze_db):
    # The batch failed after the deletes: the replay finds no row to delete.
    erasure.purge_user(spark, ALICE, bronze_db)

    apply_requests(spark, [Request("run-1", ALICE)], "q1", 9, bronze_db)

    (row,) = spark.table(f"{bronze_db}.erasure_log").collect()
    assert set(row.rows_deleted.values()) == {0}
    assert ids(spark, f"{bronze_db}.shop_users") == [BOB]
    # The replay deleted nothing, yet the snapshots that still held the rows
    # (before the interrupted attempt's DELETEs) are gone too.
    for name in erasure.USER_TABLES:
        assert spark.table(f"{bronze_db}.{name}.snapshots").count() == 1, name


def test_old_snapshots_and_their_files_are_gone(spark, bronze_db):
    table = f"{bronze_db}.shop_users"
    before = spark.sql(f"SELECT snapshot_id FROM {table}.snapshots").first()[0]
    old_files = [
        r.file_path.removeprefix("file:")
        for r in spark.sql(f"SELECT file_path FROM {table}.files").collect()
    ]

    apply_requests(spark, [Request("run-1", ALICE)], "q1", 9, bronze_db)

    assert spark.table(f"{table}.snapshots").count() == 1
    with pytest.raises(Exception, match="snapshot"):
        spark.sql(f"SELECT * FROM {table} VERSION AS OF {before}").collect()
    # The files that held Alice's rows are deleted, not only unreferenced.
    assert old_files and not any(os.path.exists(f) for f in old_files)


def test_the_batch_mark_survives_the_expiry(spark, bronze_db):
    # The append of batch 9 left its mark; the expiry removes that snapshot.
    # Without the stamp, a replay of batch 9 would append its rows again.
    apply_requests(spark, [Request("run-1", ALICE)], "q1", 9, bronze_db)

    for name in ("shop_users", "web_events"):
        summaries = snapshot_summaries(spark, f"{bronze_db}.{name}")
        assert already_committed(summaries, "q1", 9)


def test_tables_without_the_user_lose_their_history_but_no_row(spark, bronze_db):
    # Alice has no review: nothing is deleted there, but the history goes
    # all the same (a table cannot tell an earlier interrupted attempt).
    reviews = f"{bronze_db}.web_reviews"

    apply_requests(spark, [Request("run-1", ALICE)], "q1", 9, bronze_db)

    assert ids(spark, reviews) == ["rv-b1"]
    assert spark.table(f"{reviews}.snapshots").count() == 1


def test_two_requests_in_one_batch_get_one_log_row_each(spark, bronze_db):
    apply_requests(
        spark, [Request("run-1", ALICE), Request("run-2", BOB)], "q1", 9, bronze_db
    )

    assert spark.table(f"{bronze_db}.shop_users").count() == 0
    log = {r.request_id: r for r in spark.table(f"{bronze_db}.erasure_log").collect()}
    assert log["run-2"].rows_deleted["web_reviews"] == 1
    assert log["run-1"].user_sha256 != log["run-2"].user_sha256


def test_a_blip_repeats_one_step_not_the_purge(spark, bronze_db):
    # A network error on the first DELETE: only that DELETE runs again; the
    # key lookups before it are not repeated.
    calls = {"lookups": 0, "failed": False}
    real_keys = erasure._keys

    def counting_keys(*args):
        calls["lookups"] += 1
        return real_keys(*args)

    def flaky_retry(step):
        try:
            return step()
        except ConnectionError:
            return step()

    real_delete = erasure.delete_keys

    def flaky_delete(spark, table, keys):
        if keys and not calls["failed"]:
            calls["failed"] = True
            raise ConnectionError("Unable to execute HTTP request")
        return real_delete(spark, table, keys)

    erasure._keys, erasure.delete_keys = counting_keys, flaky_delete
    try:
        apply_requests(
            spark, [Request("run-1", ALICE)], "q1", 9, bronze_db, retry=flaky_retry
        )
    finally:
        erasure._keys, erasure.delete_keys = real_keys, real_delete

    assert calls["lookups"] == len(erasure.USER_TABLES)  # each table once
    assert ids(spark, f"{bronze_db}.shop_users") == [BOB]
    (row,) = spark.table(f"{bronze_db}.erasure_log").collect()
    assert row.rows_deleted["shop_users"] == 3
