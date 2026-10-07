"""Iceberg table upkeep shared by every layer (ADR 017): snapshot expiry,
compaction, orphan removal, and when each last ran.

Bronze is maintained by the stream (lakehouse.bronze, jobs/bronze_stream.py),
silver and gold by the daily maintenance job (jobs/maintenance.py), each with
its writer's identity. Procedures get full table names: given "db.table",
Iceberg first tries "db" as a catalog and logs a warning with a stack trace.
"""

import datetime

from pyspark.sql import SparkSession

# Every commit writes a new metadata.json; Iceberg deletes the oldest ones at
# commit time instead of letting them pile up.
METADATA_CLEANUP = {
    "write.metadata.delete-after-commit.enabled": "true",
    "write.metadata.previous-versions-max": "20",
}
# When a table last had a task: table properties, so restarts reset nothing.
COMPACTED_AT = "thelook.compacted-at"
ORPHANS_REMOVED_AT = "thelook.orphans-removed-at"
# Files younger than this are never orphans: a write may still be using them
# (Iceberg itself refuses less than 24 hours).
ORPHAN_MIN_AGE = datetime.timedelta(days=3)


def expire(
    spark: SparkSession, table: str, older_than: datetime.datetime, keep_last: int
) -> int:
    """Expire the table's snapshots older than `older_than`, keeping at least
    the last `keep_last`, and make Iceberg delete old metadata files at
    commit; returns how many snapshots were expired.

    Uses Iceberg's Java API (through PySpark's gateway to the JVM), not the
    `expire_snapshots` SQL procedure: the procedure finds unreferenced files
    by reading every manifest as Spark tasks, a fixed ~2.5 min per table
    here (measured: 15.6 min for 36 snapshots on each of 8 tables). The Java
    API's incremental cleanup, chosen automatically when only the main
    branch exists, reads only what the expired snapshots referenced (36 s).
    """
    props = ", ".join(f"'{k}' = '{v}'" for k, v in METADATA_CLEANUP.items())
    spark.sql(f"ALTER TABLE {table} SET TBLPROPERTIES ({props})")
    jvm = spark._jvm
    size = jvm.org.apache.iceberg.relocated.com.google.common.collect.Iterables.size
    iceberg = jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(
        spark._jsparkSession, table
    )
    before = size(iceberg.snapshots())
    (
        iceberg.expireSnapshots()
        .expireOlderThan(int(older_than.timestamp() * 1000))
        .retainLast(keep_last)
        .commit()
    )
    iceberg.refresh()
    # Spark caches loaded tables: forget this one so the next write starts
    # from the new metadata.
    spark.catalog.refreshTable(table)
    return before - size(iceberg.snapshots())


def compact(spark: SparkSession, table: str, where: str | None = None) -> int:
    """Rewrite small data files (only those matching `where`, an SQL
    condition, if given) into larger ones, applying any delete files on the
    way; returns how many files were rewritten. Iceberg's defaults: groups
    of at least 5 small files, or files with many deleted rows.

    Delete files whose rows are all in rewritten files are then dangling;
    readers would still open them (deletes are matched by partition and
    sequence number, not by file), so they are removed too."""
    catalog = table.split(".", 1)[0]
    condition = "" if where is None else f', where => "{where}"'
    return spark.sql(
        f"CALL {catalog}.system.rewrite_data_files(table => '{table}'{condition}, "
        "options => map('remove-dangling-deletes', 'true'))"
    ).first()["rewritten_data_files_count"]


def compact_deletes(spark: SparkSession, table: str) -> int:
    """Merge-on-read tables: rewrite small position-delete files into larger
    ones (and drop those whose rows are gone); returns how many."""
    catalog = table.split(".", 1)[0]
    return spark.sql(
        f"CALL {catalog}.system.rewrite_position_delete_files(table => '{table}')"
    ).first()["rewritten_delete_files_count"]


def remove_orphans(spark: SparkSession, table: str, now: datetime.datetime) -> int:
    """Delete files under the table's folder that no snapshot references
    (left by failed writes, or old metadata files), if older than
    ORPHAN_MIN_AGE; returns how many. Costs a full listing and a read of
    every manifest: run it rarely.

    prefix_listing: list through the table's FileIO (S3FileIO here), as for
    every other Iceberg access. The default lists through Hadoop's
    FileSystem, which has no "s3" scheme in our image (no hadoop-aws)."""
    catalog = table.split(".", 1)[0]
    older_than = (now - ORPHAN_MIN_AGE).strftime("%Y-%m-%d %H:%M:%S")
    return len(
        spark.sql(
            f"CALL {catalog}.system.remove_orphan_files(table => '{table}', "
            f"older_than => TIMESTAMP '{older_than}', prefix_listing => true)"
        ).collect()
    )


def last_run(spark: SparkSession, table: str, key: str) -> datetime.datetime | None:
    props = {r.key: r.value for r in spark.sql(f"SHOW TBLPROPERTIES {table}").collect()}
    return datetime.datetime.fromisoformat(props[key]) if key in props else None


def mark_run(spark: SparkSession, table: str, key: str, now: datetime.datetime) -> None:
    spark.sql(f"ALTER TABLE {table} SET TBLPROPERTIES ('{key}' = '{now.isoformat()}')")


def due(
    spark: SparkSession,
    tables: list[str],
    key: str,
    every: datetime.timedelta,
    now: datetime.datetime,
    limit: int = 1,
) -> list[str]:
    """Up to `limit` tables whose task `key` ran at least `every` ago, the
    longest ago first (never run: first of all; ties by name)."""
    found = []
    for table in tables:
        if not spark.catalog.tableExists(table):
            continue
        last = last_run(spark, table, key)
        if last is None or now - last >= every:
            found.append((last is not None, last, table))
    return [table for _, _, table in sorted(found)[:limit]]


def maintain_tables(
    spark: SparkSession,
    tables: list[str],
    now: datetime.datetime,
    keep_for: datetime.timedelta = datetime.timedelta(days=1),
    keep_last: int = 5,
    orphans_every: datetime.timedelta = datetime.timedelta(days=7),
    orphan_tables: int = 3,
) -> dict:
    """Daily upkeep of silver and gold (ADR 017), table by table: compact
    delete files, then data files (applying the deletes), expire snapshots
    older than `keep_for` (keeping `keep_last`); then remove the
    orphan files of at most `orphan_tables` tables due (each costs minutes:
    the run holds the Airflow pool that transform needs, and O2 is 1 hour).
    Returns what was done per table."""
    report = {}
    for table in tables:
        # Delete files first: in an unpartitioned table every delete file
        # applies to every data file, so a data compaction reads all of them
        # once per data file (measured: one fact table's compaction, one
        # Spark task, took 20 minutes after a day of MERGEs).
        deletes = compact_deletes(spark, table)
        report[table.rsplit(".", 1)[1]] = {
            "delete_files_rewritten": deletes,
            "files_rewritten": compact(spark, table),
            "snapshots_expired": expire(spark, table, now - keep_for, keep_last),
        }
    for table in due(
        spark, tables, ORPHANS_REMOVED_AT, orphans_every, now, orphan_tables
    ):
        report[table.rsplit(".", 1)[1]]["orphans_deleted"] = remove_orphans(
            spark, table, now
        )
        mark_run(spark, table, ORPHANS_REMOVED_AT, now)
    return report
