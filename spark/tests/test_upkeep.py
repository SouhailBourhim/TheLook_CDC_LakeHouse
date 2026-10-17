"""Tests for lakehouse.upkeep (ADR 017): the daily upkeep of silver and gold
on a local Iceberg catalog."""

import datetime

from lakehouse import upkeep
from lakehouse.bronze import with_retries

MERGE_ON_READ = {
    "format-version": "2",
    "write.delete.mode": "merge-on-read",
    "write.update.mode": "merge-on-read",
    "write.merge.mode": "merge-on-read",
}


def silver_like(spark, table, commits=6):
    """A merge-on-read table with one small file per commit, then a delete
    (which writes a position-delete file instead of rewriting a data file).
    The first file also holds row 100: had row 0 been alone in its file,
    Iceberg would drop the whole file instead of writing a delete file."""
    for i in range(commits):
        rows = [(0, "v0"), (100, "v100")] if i == 0 else [(i, f"v{i}")]
        # One partition, so both rows land in the same file.
        df = spark.createDataFrame(rows, "id int, value string").coalesce(1)
        if i == 0:
            writer = df.writeTo(table).using("iceberg")
            for key, value in MERGE_ON_READ.items():
                writer = writer.tableProperty(key, value)
            writer.create()
        else:
            df.writeTo(table).append()
    spark.sql(f"DELETE FROM {table} WHERE id = 0")


def test_daily_upkeep_compacts_and_expires_without_losing_rows(spark, lake):
    silver, _ = lake
    table = f"{silver}.orders"
    silver_like(spark, table)
    assert spark.table(f"{table}.delete_files").count() == 1
    two_days_later = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(
        days=2
    )

    report = upkeep.maintain_tables(spark, [table], two_days_later)["orders"]

    assert report["files_rewritten"] == 6  # six small files, deletes applied
    assert report["snapshots_expired"] > 0
    assert sorted(r.id for r in spark.table(table).collect()) == [1, 2, 3, 4, 5, 100]
    assert spark.table(f"{table}.data_files").count() == 1
    assert spark.table(f"{table}.delete_files").count() == 0  # none left dangling


def test_orphan_removal_is_limited_to_a_few_tables_per_run(spark, lake):
    silver, gold = lake
    tables = [f"{silver}.users", f"{silver}.orders", f"{gold}.fct_orders"]
    for table in tables:
        silver_like(spark, table, commits=1)
    now = datetime.datetime.now(datetime.timezone.utc)

    report = upkeep.maintain_tables(spark, tables, now, orphan_tables=2)

    cleaned = [name for name, done in report.items() if "orphans_deleted" in done]
    assert len(cleaned) == 2
    # The next run starts with the table left out.
    assert upkeep.due(
        spark, tables, upkeep.ORPHANS_REMOVED_AT, datetime.timedelta(days=7), now
    ) == [f"{gold}.fct_orders"]


def test_apply_deletes_rewrites_a_file_that_compaction_leaves(spark, lake):
    # GDPR erasure (ADR 021): one deleted row in a file of ten is under
    # Iceberg's thresholds, so compact() keeps the file, the row inside it.
    _, silver_db = lake
    table = f"{silver_db}.one_delete"
    df = spark.createDataFrame([(i,) for i in range(10)], "id int").coalesce(1)
    writer = df.writeTo(table).using("iceberg")
    for key, value in MERGE_ON_READ.items():
        writer = writer.tableProperty(key, value)
    writer.create()
    spark.sql(f"DELETE FROM {table} WHERE id = 3")

    assert upkeep.compact(spark, table) == 0
    assert upkeep.apply_deletes(spark, table) == 1
    assert spark.table(f"{table}.delete_files").count() == 0
    assert spark.table(table).count() == 9


def no_wait_retries(step):
    return with_retries(step, sleep=lambda _: None)


def test_a_blip_repeats_one_operation_and_the_report_keeps_the_others(
    spark, lake, monkeypatch
):
    # Seen in P6: one DNS blip re-ran a whole database, and the report showed
    # 0 files rewritten for the tables compacted by the failed attempt.
    silver, _ = lake
    table = f"{silver}.orders"
    silver_like(spark, table)
    real_expire, calls = upkeep.expire, []

    def flaky_expire(*args, **kwargs):
        calls.append(1)
        if len(calls) == 1:
            raise RuntimeError("SdkClientException: Unable to execute HTTP request")
        return real_expire(*args, **kwargs)

    monkeypatch.setattr(upkeep, "expire", flaky_expire)
    later = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(days=2)

    report = upkeep.maintain_tables(spark, [table], later, retry=no_wait_retries)

    assert report["orders"]["files_rewritten"] == 6  # not redone, not lost
    assert report["orders"]["snapshots_expired"] > 0
    assert report["orders"]["retries"] == 1
    assert "error" not in report["orders"]


def test_a_table_that_keeps_failing_does_not_stop_the_others(spark, lake, monkeypatch):
    silver, _ = lake
    broken, fine = f"{silver}.users", f"{silver}.orders"
    for table in (broken, fine):
        silver_like(spark, table)
    real_compact = upkeep.compact

    def compact(spark, table, where=None):
        if table == broken:
            raise RuntimeError("AccessDenied: not retried")
        return real_compact(spark, table, where)

    monkeypatch.setattr(upkeep, "compact", compact)
    later = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(days=2)

    report = upkeep.maintain_tables(spark, [broken, fine], later, retry=no_wait_retries)

    assert report["users"]["error"].startswith("AccessDenied")
    assert report["users"]["retries"] == 0  # not transient: not retried
    assert "orphans_deleted" not in report["users"]  # skipped after a failure
    assert report["orders"]["files_rewritten"] == 6
    assert "error" not in report["orders"]


def test_a_sorted_table_is_compacted_in_its_sort_order(spark, lake):
    # P9: bin-packing merged day-grouped files into one file spanning every
    # day; with a sort order, compaction keeps each file on a narrow range.
    _, silver = lake
    table = f"{silver}.facts"
    days = [20261001 + d for d in range(10)]
    first = True
    for day in reversed(days):  # files written in reverse order of days
        df = spark.createDataFrame(
            [(day, i) for i in range(200)], "day int, n int"
        ).coalesce(1)
        if first:
            df.writeTo(table).using("iceberg").create()
            first = False
        else:
            df.writeTo(table).append()
    spark.sql(f"ALTER TABLE {table} WRITE ORDERED BY day")
    # Tiny files, so the sorted rewrite produces several, each on its days.
    spark.sql(
        f"ALTER TABLE {table} SET TBLPROPERTIES ('write.target-file-size-bytes' = '2000')"
    )
    assert upkeep.sorted_table(spark, table)

    upkeep.compact(spark, table)

    ranges = sorted(
        (r.lower_bounds[1], r.upper_bounds[1])
        for r in spark.sql(
            f"SELECT lower_bounds, upper_bounds FROM {table}.data_files"
        ).collect()
    )
    assert len(ranges) > 1
    # Sorted output: no file's range overlaps the next one's.
    lows = [int.from_bytes(lo, "little") for lo, _ in ranges]
    highs = [int.from_bytes(hi, "little") for _, hi in ranges]
    assert all(highs[i] <= lows[i + 1] for i in range(len(ranges) - 1))
    assert spark.table(table).count() == 2000
