"""Tests for lakehouse.silver (ADR 014): latest event per key, typed rows,
and the incremental MERGE on a local Iceberg catalog."""

import datetime
import json
from decimal import Decimal

from pyspark.sql import Row
from pyspark.sql import functions as F

from lakehouse.silver import (
    SPECS,
    WATERMARK,
    latest_per_key,
    merge_sql,
    process_table,
    silver_rows,
)

ITEM = (
    "struct<id:string,order_id:string,product_id:bigint,status:string,quantity:int,"
    "sale_price:double,created_at:bigint,updated_at:bigint,shipped_at:bigint,"
    "delivered_at:bigint,returned_at:bigint,cancelled_at:bigint>"
)
PG_BRONZE = (
    "op string, source_ts_ms bigint, source_ts timestamp, ts_ms bigint, snapshot string, "
    "kafka_topic string, kafka_partition int, kafka_offset bigint, kafka_timestamp timestamp, "
    f"schema_id int, ingested_at timestamp, lsn bigint, tx_id bigint, before {ITEM}, after {ITEM}"
)
MONGO_BRONZE = (
    "op string, source_ts_ms bigint, source_ts timestamp, ts_ms bigint, snapshot string, "
    "kafka_topic string, kafka_partition int, kafka_offset bigint, kafka_timestamp timestamp, "
    "schema_id int, ingested_at timestamp, doc_id string, ord int, after string"
)
T0 = datetime.datetime(2026, 10, 4, 12, 0)
CREATED_US = 1791082150876397  # microseconds, as Debezium sends TIMESTAMP


def item(item_id, status="Processing", price=34.9900016784668):
    return Row(
        id=item_id, order_id="o-1", product_id=1281, status=status, quantity=1,
        sale_price=price, created_at=CREATED_US, updated_at=CREATED_US,
        shipped_at=None, delivered_at=None, returned_at=None, cancelled_at=None,
    )  # fmt: skip


def pg(op, item_id, lsn, offset, status="Processing", price=34.9900016784668):
    image = item(item_id, status, price)
    before = Row(**{k: (item_id if k == "id" else None) for k in image.asDict()})
    return (
        op, lsn // 1000, T0, lsn // 1000, "true" if op == "r" else "false",
        "thelook.shop.order_items", 0, offset, T0, 3, T0, lsn, 1,
        before if op == "d" else None, None if op == "d" else image,
    )  # fmt: skip


def mongo(op, doc_id, ts_ms, ord_, offset, doc=None):
    return (
        op, ts_ms, T0, ts_ms, "false", "thelook_mongo.web.reviews", 0, offset, T0, 21, T0,
        doc_id, ord_, None if op == "d" else json.dumps(doc),
    )  # fmt: skip


def review(doc_id, rating=5, **extra):
    return {
        "_id": doc_id, "order_item_id": f"oi-{doc_id}", "product_id": 17740, "user_id": "u-1",
        "rating": rating, "title": "t", "text": "x", "tags": ["fit", "value"],
        "helpful_votes": 0, "created_at": {"$date": 1791055317710},
        "updated_at": {"$date": 1791055317710}, **extra,
    }  # fmt: skip


# --- latest event per key ------------------------------------------------------


def test_highest_lsn_wins(spark):
    df = spark.createDataFrame(
        [pg("c", "a", 100, 1), pg("u", "a", 300, 3, "Shipped"), pg("u", "a", 200, 2)],
        PG_BRONZE,
    )
    (row,) = latest_per_key(df, "postgres").collect()
    assert (row._position, row._image.status) == (300, "Shipped")


def test_streamed_event_beats_snapshot_row_on_the_same_lsn(spark):
    # The P3 tie: a blocking snapshot's rows carry the pause LSN, and the
    # first event streamed after the resume can have the same LSN.
    df = spark.createDataFrame(
        [pg("r", "a", 500, 10), pg("u", "a", 500, 9, "Shipped")], PG_BRONZE
    )
    (row,) = latest_per_key(df, "postgres").collect()
    assert row._op == "u"


def test_kafka_offset_breaks_the_last_ties(spark):
    df = spark.createDataFrame(
        [pg("u", "a", 500, 9, "Shipped"), pg("u", "a", 500, 11, "Delivered")], PG_BRONZE
    )
    (row,) = latest_per_key(df, "postgres").collect()
    assert row._image.status == "Delivered"


def test_delete_takes_its_key_from_before(spark):
    df = spark.createDataFrame([pg("c", "a", 100, 1), pg("d", "a", 200, 2)], PG_BRONZE)
    (row,) = latest_per_key(df, "postgres").collect()
    assert (row._key, row._op, row._image) == ("a", "d", None)


def test_mongo_order_is_commit_time_then_ord(spark):
    df = spark.createDataFrame(
        [
            mongo("u", "r1", 1_000, 7, 2, review("r1", rating=3)),
            mongo(
                "u", "r1", 1_000, 9, 1, review("r1", rating=4)
            ),  # same ms, higher ord
            mongo("c", "r1", 999, 50, 3, review("r1", rating=5)),
        ],
        MONGO_BRONZE,
    )
    (row,) = latest_per_key(df, "mongo").collect()
    assert json.loads(row._image)["rating"] == 4
    assert row._position == 1_000 * 1_000_000 + 9


# --- typed rows ----------------------------------------------------------------


def test_postgres_types_timestamps_and_money(spark):
    df = spark.createDataFrame([pg("c", "a", 100, 1)], PG_BRONZE)
    rows, _ = silver_rows(latest_per_key(df, "postgres"), SPECS["order_items"])
    (row,) = rows.collect()
    assert row.sale_price == Decimal("34.99")  # float artefact removed
    assert isinstance(row.created_at, datetime.datetime)
    assert row.created_at == datetime.datetime(2026, 10, 4, 2, 49, 10, 876397)
    assert len(row._row_hash) == 64


def test_postgres_delete_row_keeps_its_id(spark):
    df = spark.createDataFrame([pg("d", "a", 200, 2)], PG_BRONZE)
    rows, _ = silver_rows(latest_per_key(df, "postgres"), SPECS["order_items"])
    (row,) = rows.collect()
    assert (row.id, row._op, row.status) == ("a", "d", None)


def test_mongo_json_is_parsed_and_bad_json_is_set_aside(spark):
    df = spark.createDataFrame(
        [
            mongo(
                "c", "r1", 1_000, 1, 1, review("r1", edited_at={"$date": 1791055400000})
            ),
            (
                "c",
                1_001,
                T0,
                1_001,
                "false",
                "t",
                0,
                2,
                T0,
                21,
                T0,
                "r2",
                1,
                "{not json",
            ),
        ],
        MONGO_BRONZE,
    )
    rows, bad = silver_rows(latest_per_key(df, "mongo"), SPECS["reviews"])
    (row,) = rows.collect()
    assert row.id == "r1" and row.tags == ["fit", "value"] and row.rating == 5
    assert row.created_at == datetime.datetime(2026, 10, 3, 19, 21, 57, 710000)
    assert row.edited_at is not None
    assert bad.count() == 1


def test_events_merge_reads_only_recent_partitions():
    sql = merge_sql(
        "t", "s", ["id", "_position"], prune=("created_at", "2026-10-04 00:00:00")
    )
    assert "t.created_at >= TIMESTAMP '2026-10-04 00:00:00'" in sql


# --- incremental MERGE on a local Iceberg catalog -------------------------------


def append(spark, table, rows, schema):
    """Append rows to a local bronze table (creating it the first time):
    each call makes one Iceberg snapshot, like one stream micro-batch."""
    df = spark.createDataFrame(rows, schema)
    if spark.catalog.tableExists(table):
        df.writeTo(table).append()
    else:
        df.writeTo(table).using("iceberg").create()


def silver_state(spark, table):
    return {r.id: r for r in spark.table(table).collect()}


def test_first_run_reads_everything_then_only_new_snapshots(spark, lake):
    bronze_db, silver_db = lake
    src, dst = f"{bronze_db}.shop_order_items", f"{silver_db}.order_items"
    spec = SPECS["order_items"]
    append(spark, src, [pg("r", "a", 100, 1), pg("r", "b", 100, 2)], PG_BRONZE)

    first = process_table(spark, spec, bronze_db, silver_db)
    assert first["full_read"] and first["keys"] == 2
    assert set(silver_state(spark, dst)) == {"a", "b"}

    append(
        spark,
        src,
        [pg("u", "a", 200, 3, "Shipped"), pg("d", "b", 210, 4), pg("c", "c", 220, 5)],
        PG_BRONZE,
    )
    second = process_table(spark, spec, bronze_db, silver_db)
    state = silver_state(spark, dst)

    assert not second["full_read"] and second["keys"] == 3  # only the new snapshot
    assert set(state) == {"a", "c"}  # b deleted, c inserted
    assert state["a"].status == "Shipped" and state["a"]._position == 200
    assert process_table(spark, spec, bronze_db, silver_db)["status"] == "up to date"


def test_an_older_event_replayed_later_cannot_overwrite_newer_state(spark, lake):
    bronze_db, silver_db = lake
    src, dst = f"{bronze_db}.shop_order_items", f"{silver_db}.order_items"
    spec = SPECS["order_items"]
    append(
        spark, src, [pg("c", "a", 100, 1), pg("u", "a", 300, 2, "Delivered")], PG_BRONZE
    )
    process_table(spark, spec, bronze_db, silver_db)

    # e.g. after a lost checkpoint, bronze receives an old event again.
    append(spark, src, [pg("u", "a", 200, 3, "Shipped")], PG_BRONZE)
    process_table(spark, spec, bronze_db, silver_db)

    assert silver_state(spark, dst)["a"].status == "Delivered"


def test_a_no_op_update_moves_the_position_forward(spark, lake):
    # Keeping _position current is what makes the replay guard safe (ADR 014).
    bronze_db, silver_db = lake
    src, dst = f"{bronze_db}.shop_order_items", f"{silver_db}.order_items"
    spec = SPECS["order_items"]
    append(spark, src, [pg("c", "a", 100, 1)], PG_BRONZE)
    process_table(spark, spec, bronze_db, silver_db)
    hash_before = silver_state(spark, dst)["a"]._row_hash

    append(spark, src, [pg("u", "a", 400, 2)], PG_BRONZE)  # same values
    process_table(spark, spec, bronze_db, silver_db)
    row = silver_state(spark, dst)["a"]

    assert row._position == 400 and row._row_hash == hash_before


def test_rerunning_the_same_increment_changes_nothing(spark, lake):
    bronze_db, silver_db = lake
    src, dst = f"{bronze_db}.shop_order_items", f"{silver_db}.order_items"
    spec = SPECS["order_items"]
    append(spark, src, [pg("c", "a", 100, 1), pg("c", "b", 110, 2)], PG_BRONZE)
    process_table(spark, spec, bronze_db, silver_db)
    append(
        spark, src, [pg("u", "a", 200, 3, "Shipped"), pg("d", "b", 210, 4)], PG_BRONZE
    )
    process_table(spark, spec, bronze_db, silver_db)
    expected = {k: (v.status, v._position) for k, v in silver_state(spark, dst).items()}

    # Simulate a crash after the MERGE but before the watermark: rewind it.
    first = spark.sql(
        f"SELECT snapshot_id FROM {src}.snapshots ORDER BY committed_at"
    ).first()[0]
    spark.sql(f"ALTER TABLE {dst} SET TBLPROPERTIES ('{WATERMARK}' = '{first}')")
    process_table(spark, spec, bronze_db, silver_db)

    assert {
        k: (v.status, v._position) for k, v in silver_state(spark, dst).items()
    } == expected


def test_a_new_source_column_becomes_a_silver_column(spark, lake):
    bronze_db, silver_db = lake
    src, dst = f"{bronze_db}.shop_order_items", f"{silver_db}.order_items"
    spec = SPECS["order_items"]
    append(spark, src, [pg("c", "a", 100, 1)], PG_BRONZE)
    process_table(spark, spec, bronze_db, silver_db)

    # A nullable column added at the source arrives in bronze's after struct.
    newer = spark.createDataFrame([pg("c", "b", 200, 2)], PG_BRONZE).withColumn(
        "after", F.col("after").withField("gift_wrap", F.lit(True))
    )
    spark.sql(
        f"ALTER TABLE {src} SET TBLPROPERTIES ('write.spark.accept-any-schema'='true')"
    )
    newer.writeTo(src).option("mergeSchema", "true").append()
    result = process_table(spark, spec, bronze_db, silver_db)

    assert result["new_columns"] == ["gift_wrap"]
    state = silver_state(spark, dst)
    assert state["b"].gift_wrap is True and state["a"].gift_wrap is None
