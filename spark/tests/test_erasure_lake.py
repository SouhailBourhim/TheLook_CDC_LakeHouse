"""Tests for the silver and gold purge (lakehouse.erasure, jobs/erase.py)
on a local Iceberg catalog: rows gone from every table and every file on
disk, others kept, time travel gone, audit kept on a rerun."""

import json
import os
import uuid

import pytest
from pyspark.sql import functions as F

from lakehouse import erasure
from lakehouse.erasure import Request, erase_lake, user_hash

ALICE = "6f1c2a3b-0d4e-4f5a-8b6c-7d8e9f0a1b2c"  # erased
BOB = "0a1b2c3d-4e5f-4a6b-9c7d-8e9f0a1b2c3d"  # kept
REQUEST = Request("run-1", ALICE)

MERGE_ON_READ = {
    "format-version": "2",
    "write.delete.mode": "merge-on-read",
    "write.update.mode": "merge-on-read",
    "write.merge.mode": "merge-on-read",
}


def table_of(spark, table, schema, commits, mor=True, partition=None):
    """One file per commit (several rows each: a DELETE of a file's only
    row drops the file instead of writing a delete file)."""
    for i, rows in enumerate(commits):
        df = spark.createDataFrame(rows, schema).coalesce(1)
        if i:
            df.writeTo(table).append()
            continue
        writer = df.writeTo(table).using("iceberg")
        if partition:
            from pyspark.sql.functions import partitioning

            writer = writer.partitionedBy(partitioning.days(partition))
        for key, value in (MERGE_ON_READ if mor else {"format-version": "2"}).items():
            writer = writer.tableProperty(key, value)
        writer.create()


@pytest.fixture
def dbs(spark, lake):
    _, silver = lake
    gold = f"local.gold_{uuid.uuid4().hex[:8]}"
    spark.sql(f"CREATE NAMESPACE {gold}")
    s, g = f"{silver}.", f"{gold}."
    table_of(
        spark,
        s + "users",
        "id string, email string",
        [[(ALICE, "alice@example.com"), (BOB, "bob@example.com")]],
    )
    table_of(
        spark,
        s + "orders",
        "id string, user_id string",
        [[("o-a1", ALICE), ("o-b1", BOB)], [("o-a2", ALICE), ("o-b2", BOB)]],
    )
    # The source deletion already reached silver for o-a2: the MERGE deleted
    # it (a delete file), its row is still inside a data file.
    spark.sql(f"DELETE FROM {s}orders WHERE id = 'o-a2'")
    table_of(
        spark,
        s + "order_items",
        "id string, order_id string",
        [[("i-a1", "o-a1"), ("i-a2", "o-a2"), ("i-b1", "o-b1")]],
    )
    events = spark.createDataFrame(
        [
            ("e-a1", ALICE, "10.0.0.1", "2026-10-01 10:00:00"),
            ("e-b1", BOB, "10.0.0.2", "2026-10-01 11:00:00"),
            ("e-g1", None, "10.0.0.9", "2026-10-01 12:00:00"),
        ],
        "id string, user_id string, ip_address string, created_at string",
    ).withColumn("created_at", F.to_timestamp("created_at"))
    table_of(
        spark,
        s + "events",
        events.schema,
        [events.collect()],
        partition="created_at",
    )
    table_of(
        spark,
        s + "reviews",
        "id string, user_id string, text string",
        [[("rv-a1", ALICE, "great"), ("rv-b1", BOB, "fine")]],
    )
    table_of(
        spark,
        s + "dead_letters",
        "source_table string, record_key string, payload string",
        [
            [
                ("events", "e-a9", json.dumps({"user_id": ALICE, "ip": "1.2.3.4"})),
                ("events", "e-b9", json.dumps({"user_id": BOB, "ref": ALICE})),
            ]
        ],
    )
    # dim_user is rebuilt with REPLACE TABLE (copy-on-write); two versions.
    table_of(
        spark,
        g + "dim_user",
        "user_sk bigint, user_id string, city string",
        [[(1, ALICE, "Lyon"), (2, ALICE, "Paris"), (3, BOB, "Rome")]],
        mor=False,
    )
    table_of(
        spark,
        g + "fct_order_items",
        "order_item_id string, order_id string, user_id string, order_date_key int",
        [
            [
                ("i-a1", "o-a1", ALICE, 20261001),
                ("i-a2", "o-a2", ALICE, 20261002),
                ("i-b1", "o-b1", BOB, 20261001),
            ]
        ],
    )
    table_of(
        spark,
        g + "fct_orders",
        "order_id string, user_id string, order_date_key int",
        [
            [
                ("o-a1", ALICE, 20261001),
                ("o-a2", ALICE, 20261002),
                ("o-b1", BOB, 20261001),
            ]
        ],
    )
    table_of(
        spark,
        g + "fct_sessions",
        "session_id string, user_id string, session_date_key int",
        [
            [
                ("s-a", ALICE, 20261001),
                ("s-b", BOB, 20261001),
                ("s-ghost", None, 20261001),
            ]
        ],
    )
    return silver, gold


def tables(dbs):
    silver, gold = dbs
    return {step: erasure._table(step, silver, gold) for step in erasure.LAKE_TABLES}


def parquet_files(spark, table):
    """Every Parquet file under the table's data folder, referenced or not."""
    location = (
        spark.sql(f"DESCRIBE TABLE EXTENDED {table}")
        .where("col_name = 'Location'")
        .first()["data_type"]
    )
    folder = location.removeprefix("file:") + "/data"
    return [
        os.path.join(root, name)
        for root, _, names in os.walk(folder)
        for name in names
        if name.endswith(".parquet")
    ]


def holds(spark, path, text):
    df = spark.read.parquet(path)
    row = F.concat_ws("|", *[F.col(c).cast("string") for c in df.columns])
    return df.where(row.contains(text)).count() > 0


def test_the_user_is_gone_from_every_table_and_others_stay(spark, dbs):
    erase_lake(spark, REQUEST, *dbs)

    t = tables(dbs)
    assert [r.id for r in spark.table(t["silver.users"]).collect()] == [BOB]
    assert sorted(r.id for r in spark.table(t["silver.orders"]).collect()) == [
        "o-b1",
        "o-b2",
    ]
    # i-a2 belongs to o-a2, found through gold although silver lost it.
    assert [r.id for r in spark.table(t["silver.order_items"]).collect()] == ["i-b1"]
    assert sorted(r.id for r in spark.table(t["silver.events"]).collect()) == [
        "e-b1",
        "e-g1",
    ]
    assert [r.record_key for r in spark.table(t["silver.dead_letters"]).collect()] == [
        "e-b9"
    ]
    assert [r.user_id for r in spark.table(t["gold.dim_user"]).collect()] == [BOB]
    assert sorted(
        r.session_id for r in spark.table(t["gold.fct_sessions"]).collect()
    ) == ["s-b", "s-ghost"]


def test_no_file_on_disk_holds_the_user(spark, dbs):
    erase_lake(spark, REQUEST, *dbs)

    for step, table in tables(dbs).items():
        # Bob's dead letter mentions Alice's id and is kept: look for her
        # own record there.
        probe = "e-a9" if step == "silver.dead_letters" else ALICE
        files = parquet_files(spark, table)
        assert files, step
        assert not [f for f in files if holds(spark, f, probe)], step


def test_time_travel_to_before_the_purge_fails(spark, dbs):
    orders = tables(dbs)["silver.orders"]
    first = spark.sql(f"SELECT snapshot_id FROM {orders}.snapshots").first()[0]

    erase_lake(spark, REQUEST, *dbs)

    assert spark.table(f"{orders}.snapshots").count() == 1
    with pytest.raises(Exception, match="snapshot"):
        spark.sql(f"SELECT * FROM {orders} VERSION AS OF {first}").collect()


def test_audit_has_one_row_per_table_and_a_rerun_keeps_it(spark, dbs):
    silver, _ = dbs
    report = erase_lake(spark, REQUEST, *dbs)
    again = erase_lake(spark, REQUEST, *dbs)

    audit = {r.step: r for r in spark.table(f"{silver}.erasure_audit").collect()}
    assert sorted(audit) == sorted(erasure.LAKE_TABLES)
    assert {r.user_sha256 for r in audit.values()} == {user_hash(ALICE)}
    assert {r.request_id for r in audit.values()} == {"run-1"}
    # o-a2 was already deleted by silver's MERGE: one row left to delete.
    assert audit["silver.orders"].rows_deleted == 1
    assert audit["gold.dim_user"].rows_deleted == 2
    assert audit["silver.orders"].files_rewritten >= 1
    assert report["silver.order_items"]["rows_deleted"] == 2
    # The rerun found nothing to do, and the audit kept the first counts.
    assert {r["rows_deleted"] for r in again.values()} == {0}
    assert audit["gold.dim_user"].rows_deleted == 2


def test_a_malformed_user_id_is_refused(spark, dbs):
    with pytest.raises(ValueError):
        erase_lake(spark, Request("run-1", "x' OR '1'='1"), *dbs)


def test_upstream_steps_are_audited_too(spark, dbs):
    silver, _ = dbs
    erase_lake(spark, REQUEST, *dbs, upstream={"postgres.orders": 2, "redis.keys": 5})

    audit = {r.step: r for r in spark.table(f"{silver}.erasure_audit").collect()}
    assert audit["postgres.orders"].rows_deleted == 2
    assert audit["redis.keys"].rows_deleted == 5
    assert audit["redis.keys"].files_rewritten is None
