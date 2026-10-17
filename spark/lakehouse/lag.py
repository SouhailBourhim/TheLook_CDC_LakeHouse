"""Source-to-bronze lag per row, for the load test (P9, O8).

A row is queryable in bronze when the Iceberg snapshot that added its data
file commits. Its lag is that commit time minus `source_ts` (the source's
commit, from Debezium). Row -> `_file` (Iceberg's metadata column) ->
`entries` (the snapshot that added the file) -> `snapshots.committed_at`.

`ingested_at` is not used: it is set when a table's rows are built, before
the write and the commit, and a batch commits its tables one after the
other, so it would hide the time spent waiting for the earlier tables.

Snapshots are read from the `snapshots` table, so the lag can only be
computed while the adding snapshot is still there: bronze keeps 1 to 2
hours of them (lakehouse.bronze.KEEP_SNAPSHOTS_FOR). Files rewritten by a
compaction would point at the compaction's snapshot: bronze compacts only
the days before today.
"""

import datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def row_lags(
    spark: SparkSession,
    table: str,
    start: datetime.datetime,
    end: datetime.datetime,
) -> DataFrame:
    """One row per bronze row with `source_ts` in [start, end): its
    `source_ts` and `lag_s`, seconds from the source commit to the commit
    of the snapshot that made it queryable. Rows whose snapshot has expired
    are dropped (an inner join): count them against the expected total."""
    rows = (
        spark.table(table)
        .where((F.col("source_ts") >= F.lit(start)) & (F.col("source_ts") < F.lit(end)))
        .select("source_ts", F.col("_file").alias("file_path"))
    )
    added_by = spark.table(f"{table}.entries").select(
        F.col("data_file.file_path").alias("file_path"), "snapshot_id"
    )
    committed = spark.table(f"{table}.snapshots").select("snapshot_id", "committed_at")
    return (
        rows.join(added_by, "file_path")
        .join(committed, "snapshot_id")
        .select(
            "source_ts",
            (F.unix_micros("committed_at") - F.unix_micros("source_ts")).alias(
                "lag_us"
            ),
        )
        .select("source_ts", (F.col("lag_us") / 1e6).alias("lag_s"))
    )


def lag_summary(lags: DataFrame) -> dict:
    """rows, p50, p95 and max of `lag_s` (exact percentiles)."""
    r = lags.agg(
        F.count("*").alias("rows"),
        F.percentile("lag_s", 0.5).alias("p50"),
        F.percentile("lag_s", 0.95).alias("p95"),
        F.max("lag_s").alias("max"),
    ).first()
    return r.asDict()
