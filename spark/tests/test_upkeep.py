"""Tests for lakehouse.upkeep (ADR 017): the daily upkeep of silver and gold
on a local Iceberg catalog."""

import datetime

from lakehouse import upkeep

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
