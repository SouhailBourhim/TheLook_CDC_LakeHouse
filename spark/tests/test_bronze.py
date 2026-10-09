"""Tests for lakehouse.bronze: table names, the replay guard, the schema cache,
and routing each topic to the right parser."""

import datetime
import io
import json
import os
import time

import pytest
from pyspark.sql import functions as F

from lakehouse import bronze, upkeep
from lakehouse.bronze import (
    BATCH_ID,
    QUERY_ID,
    TOPICS,
    SchemaRegistry,
    already_committed,
    bronze_rows,
    table_for,
)


@pytest.mark.parametrize(
    ("topic", "table"),
    [
        ("thelook.shop.users", "lake.thelook_bronze.shop_users"),
        ("thelook.shop.order_items", "lake.thelook_bronze.shop_order_items"),
        ("thelook_mongo.web.events", "lake.thelook_bronze.web_events"),
    ],
)
def test_table_for(topic, table):
    assert table_for(topic) == table


def test_seven_topics_seven_distinct_tables():
    assert len({table_for(t) for t in TOPICS}) == len(TOPICS) == 7


def test_heartbeat_topic_is_not_ingested():
    assert "thelook.shop.heartbeat" not in TOPICS


def snap(query_id, batch_id):
    return {QUERY_ID: query_id, BATCH_ID: str(batch_id), "added-records": "10"}


def test_replayed_batch_is_skipped():
    summaries = [snap("q1", 4), snap("q1", 5)]
    assert already_committed(summaries, "q1", 5)  # replay of the last batch
    assert already_committed(summaries, "q1", 3)  # older batch, also done


def test_next_batch_is_written():
    assert not already_committed([snap("q1", 5)], "q1", 6)


def test_batches_of_another_query_do_not_count():
    # A new checkpoint is a new query whose batch ids restart at 0.
    assert not already_committed([snap("old-query", 900)], "new-query", 0)


def test_snapshots_without_marks_are_ignored():
    # e.g. a compaction or maintenance commit (no query/batch id).
    assert not already_committed([{"operation": "replace"}], "q1", 0)


def test_schema_registry_fetches_each_id_once(monkeypatch):
    calls = []

    def fake_urlopen(url, timeout):
        calls.append(url)
        return io.BytesIO(json.dumps({"schema": '"long"'}).encode())

    monkeypatch.setattr(bronze.urllib.request, "urlopen", fake_urlopen)
    registry = SchemaRegistry("http://schema-registry:8081/")

    assert registry(7) == registry(7) == '"long"'
    assert calls == ["http://schema-registry:8081/schemas/ids/7"]


def test_bronze_rows_routes_each_topic_to_its_parser(kafka_df, captured, schemas):
    users = captured["thelook.shop.users"]
    reviews = captured["thelook_mongo.web.reviews"]
    batch = kafka_df([users["c"], reviews["c"], reviews["d"], reviews["tombstone"]])
    now = F.current_timestamp()

    pg = bronze_rows(batch, "thelook.shop.users", schemas.get, now)
    mongo = bronze_rows(batch, "thelook_mongo.web.reviews", schemas.get, now)

    assert "lsn" in pg.columns and "doc_id" not in pg.columns
    assert "doc_id" in mongo.columns and "lsn" not in mongo.columns
    assert pg.count() == 1
    assert sorted(r.op for r in mongo.collect()) == ["c", "d"]  # tombstone dropped


# --- retries (seen in P3: a Glue blip stopped the stream) ----------------------

GLUE_BLIP = Exception(
    "software.amazon.awssdk.core.exception.SdkClientException: Unable to execute "
    "HTTP request: Connect to https://glue.us-east-1.amazonaws.com:443 failed: "
    "Connection refused (SDK Attempt Count: 3)"
)


def test_bronze_rows_is_none_when_nothing_decodes(kafka_df, captured):
    # Only a tombstone, or no record of the topic at all (an empty batch:
    # offsets skipped after retention deleted them). Before the fix both
    # failed at analysis: "Can't extract a value from event ... VOID".
    reviews = captured["thelook_mongo.web.reviews"]
    batch = kafka_df([reviews["tombstone"]])
    now = F.current_timestamp()
    assert bronze_rows(batch, "thelook_mongo.web.reviews", None, now) is None
    assert bronze_rows(batch, "thelook.shop.users", None, now) is None


def test_network_errors_are_transient_and_permissions_are_not():
    assert bronze.is_transient(GLUE_BLIP)
    assert not bronze.is_transient(Exception("AccessDeniedException: not authorized"))
    assert not bronze.is_transient(Exception("Cannot write incompatible data"))
    s3_open_failed = Exception(
        "[INTERNAL_ERROR] The Spark SQL phase optimization failed ... "
        'NullPointerException: Cannot invoke "java.io.InputStream.read(byte[], int, '
        'int)" because "this.stream" is null'
    )
    assert bronze.is_transient(s3_open_failed)


def test_with_retries_recovers_from_a_blip():
    calls, waits = [], []

    def step():
        calls.append(1)
        if len(calls) < 3:
            raise GLUE_BLIP
        return True

    assert bronze.with_retries(step, sleep=waits.append) is True
    assert len(calls) == 3
    assert waits == [5.0, 10.0]  # doubling delays


def test_with_retries_gives_up_after_the_last_attempt():
    waits = []

    def step():
        raise GLUE_BLIP

    with pytest.raises(Exception, match="Connection refused"):
        bronze.with_retries(step, attempts=3, sleep=waits.append)
    assert waits == [5.0, 10.0]


def test_with_retries_does_not_retry_permanent_errors():
    waits = []

    def step():
        raise Exception("AccessDeniedException")

    with pytest.raises(Exception, match="AccessDenied"):
        bronze.with_retries(step, sleep=waits.append)
    assert waits == []


def test_append_once_skips_a_batch_already_in_the_table(monkeypatch):
    # e.g. a retry after a commit that reached Glue but whose reply was lost.
    writes = []
    monkeypatch.setattr(bronze, "snapshot_summaries", lambda spark, t: [snap("q1", 7)])
    monkeypatch.setattr(bronze, "write_bronze", lambda *args: writes.append(args))

    assert bronze.append_once(None, None, "t", "q1", 7) is False
    assert bronze.append_once(None, None, "t", "q1", 8) is True
    assert len(writes) == 1


def test_ledger_row_records_every_table_once(spark, lake):
    bronze_db, _ = lake
    ledger = f"{bronze_db}.stream_batches"
    users, orders = f"{bronze_db}.shop_users", f"{bronze_db}.shop_orders"
    not_yet = f"{bronze_db}.shop_products"  # no batch has written it yet
    for table in (users, orders):
        spark.createDataFrame([(1,)], "id int").writeTo(table).using("iceberg").create()

    assert bronze.record_batch(spark, "q1", 7, [users, orders, not_yet], ledger)
    # A replayed batch finds its row and does not add a second one.
    assert not bronze.record_batch(spark, "q1", 7, [users, orders, not_yet], ledger)

    (row,) = spark.table(ledger).collect()
    assert (row.query_id, row.batch_id) == ("q1", 7)
    assert row.snapshots == {
        "shop_users": bronze.current_snapshot(spark, users),
        "shop_orders": bronze.current_snapshot(spark, orders),
    }


def test_maintenance_expires_old_snapshots_but_keeps_every_row(spark, lake):
    bronze_db, _ = lake
    table = f"{bronze_db}.shop_users"
    for i in range(8):  # eight micro-batches, eight snapshots
        df = spark.createDataFrame([(i,)], "id int")
        if i == 0:
            df.writeTo(table).using("iceberg").create()
        else:
            df.writeTo(table).append()
    later = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(days=1)

    expired = bronze.maintain_bronze(spark, [table], later)

    assert expired == {"shop_users": 8 - bronze.KEEP_LAST_SNAPSHOTS}
    assert spark.table(table).count() == 8  # an append-only log loses no row
    props = {r.key: r.value for r in spark.sql(f"SHOW TBLPROPERTIES {table}").collect()}
    assert props["write.metadata.delete-after-commit.enabled"] == "true"


def test_recent_snapshots_are_kept(spark, lake):
    bronze_db, _ = lake
    table = f"{bronze_db}.shop_orders"
    spark.createDataFrame([(1,)], "id int").writeTo(table).using("iceberg").create()
    for i in range(6):
        spark.createDataFrame([(i,)], "id int").writeTo(table).append()
    now = datetime.datetime.now(datetime.timezone.utc)

    assert bronze.maintain_bronze(spark, [table], now) == {"shop_orders": 0}


def test_ledger_compaction_merges_its_small_files(spark, lake):
    bronze_db, _ = lake
    ledger = f"{bronze_db}.stream_batches"
    for batch_id in range(6):  # one tiny file per batch
        bronze.record_batch(spark, "q1", batch_id, [], ledger)

    assert upkeep.compact(spark, ledger) == 6
    assert spark.table(f"{ledger}.files").count() == 1
    assert spark.table(ledger).count() == 6


TODAY = datetime.datetime(2026, 10, 5, 12, 0, tzinfo=datetime.timezone.utc)
YESTERDAY = datetime.datetime(2026, 10, 4, 12, 0)


def day_table(spark, table, batches):
    """A bronze-like table partitioned by day: one file per (batch, day)."""
    from pyspark.sql.functions import partitioning

    for i, when in enumerate(batches):
        df = spark.createDataFrame([(i, when)], "id int, source_ts timestamp")
        if i == 0:
            df.writeTo(table).using("iceberg").partitionedBy(
                partitioning.days("source_ts")
            ).create()
        else:
            df.writeTo(table).append()


def files_per_day(spark, table):
    rows = spark.sql(f"SELECT partition.source_ts_day AS day FROM {table}.files")
    return {day.isoformat(): n for day, n in rows.groupBy("day").count().collect()}


def test_compaction_rewrites_past_days_one_table_per_run(spark, lake):
    bronze_db, _ = lake
    first, second = f"{bronze_db}.shop_users", f"{bronze_db}.shop_orders"
    for table in (first, second):
        # Six files on each day: enough for Iceberg to compact (groups of 5+),
        # so only the "before today" filter keeps today's files untouched.
        day_table(spark, table, [YESTERDAY] * 6 + [TODAY.replace(tzinfo=None)] * 6)

    done = bronze.heavy_upkeep(spark, [first, second], TODAY)

    # Neither was ever compacted: ties go by name.
    assert done.startswith("compacted shop_orders")
    assert files_per_day(spark, second) == {"2026-10-04": 1, "2026-10-05": 6}
    assert files_per_day(spark, first) == {"2026-10-04": 6, "2026-10-05": 6}
    assert spark.table(second).count() == 12
    # The next run takes the other table, then nothing is due for a day.
    assert bronze.heavy_upkeep(spark, [first, second], TODAY).startswith(
        "compacted shop_users"
    )
    assert not bronze.heavy_upkeep(spark, [first, second], TODAY).startswith(
        "compacted"
    )


def test_orphan_removal_deletes_only_old_unreferenced_files(spark, lake):
    bronze_db, _ = lake
    table = f"{bronze_db}.shop_users"
    day_table(spark, table, [YESTERDAY] * 2)
    data_file = spark.sql(f"SELECT file_path FROM {table}.files").first()[0]
    folder = data_file.removeprefix("file:").rsplit("/", 1)[0]
    old, young = (
        f"{folder}/failed-write-old.parquet",
        f"{folder}/failed-write-new.parquet",
    )
    for path in (old, young):
        with open(path, "wb") as f:
            f.write(b"not referenced by any snapshot")
    five_days_ago = time.time() - 5 * 86400
    os.utime(old, (five_days_ago, five_days_ago))

    # Only the file older than ORPHAN_MIN_AGE goes; the young one may belong
    # to a write in progress. (Iceberg refuses a cutoff under 24 hours.)
    assert upkeep.remove_orphans(spark, table, datetime.datetime.now()) == 1
    assert not os.path.exists(old) and os.path.exists(young)
    assert spark.table(table).count() == 2


def test_current_snapshot_sees_commits_made_behind_the_cache(spark, lake):
    # The maintenance thread commits through its own table object; Spark's
    # cached copy, kept alive while the stream uses it, can lag behind. The
    # ledger once recorded the previous batch's snapshot this way.
    bronze_db, _ = lake
    table = f"{bronze_db}.shop_users"
    spark.createDataFrame([(1,)], "id int").writeTo(table).using("iceberg").create()
    bronze.current_snapshot(spark, table)  # Spark now holds a cached copy
    jvm = spark._jvm
    location = jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(
        spark._jsparkSession, table
    ).location()
    other = jvm.org.apache.iceberg.hadoop.HadoopTables(
        spark._jsc.hadoopConfiguration()
    ).load(location)
    other.newAppend().commit()  # a commit the cached copy does not know about

    assert bronze.current_snapshot(spark, table) == other.currentSnapshot().snapshotId()
