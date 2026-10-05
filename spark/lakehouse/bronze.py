"""One streaming micro-batch -> appends to the bronze Iceberg tables.

Used by jobs/bronze_stream.py (ADR 007, ADR 012). Each micro-batch holds
records of several topics; each topic goes to its own table, so a batch
makes one Iceberg commit per table it touches.

Exactly-once per table: every commit records the streaming query's id and
the batch id in the Iceberg snapshot summary. When a batch fails after
committing some of its tables, Spark replays it with the same batch id;
already_committed() then skips the tables that already have it, so nothing
is appended twice. The query id comes from the checkpoint: a new checkpoint
means a new query, which starts again from the earliest offsets (bronze
would then hold duplicates, which silver removes by log position and the
Kafka coordinates make visible).
"""

import datetime
import json
import logging
import time
import urllib.request
from collections.abc import Callable

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import partitioning

from lakehouse.cdc import decode_by_schema, mongo_bronze, postgres_bronze, split_frames

POSTGRES_TOPICS = [
    f"thelook.shop.{t}"
    for t in ("users", "orders", "order_items", "products", "dist_centers")
]
MONGO_TOPICS = [f"thelook_mongo.web.{c}" for c in ("events", "reviews")]
TOPICS = POSTGRES_TOPICS + MONGO_TOPICS
DATABASE = "lake.thelook_bronze"
# One row per finished micro-batch: every bronze table's snapshot id at the
# end of that batch (ADR 012). Silver reads the newest row (ADR 014).
LEDGER = f"{DATABASE}.stream_batches"

QUERY_ID = "thelook.query-id"
BATCH_ID = "thelook.batch-id"

log = logging.getLogger("bronze")

# Signs of a network or service blip on the way to S3 or Glue, as they
# appear in the Java exception text that py4j hands to Python. Seen in P3:
# "Unable to execute HTTP request: Connect to https://glue... failed:
# Connection refused" after the AWS SDK's own 3 attempts.
TRANSIENT_MARKERS = (
    "Unable to execute HTTP request",
    "Connection refused",
    "Connection reset",
    "Read timed out",
    "SdkClientException",
    "ThrottlingException",
    "SlowDown",
    "ServiceUnavailable",
)


def table_for(topic: str) -> str:
    """thelook.shop.users -> lake.thelook_bronze.shop_users (ADR 012)."""
    _prefix, database, name = topic.split(".")
    return f"{DATABASE}.{database}_{name}"


class SchemaRegistry:
    """schema id -> Avro schema JSON, fetched once per id. A schema id never
    changes meaning in the registry, so the cache never goes stale."""

    def __init__(self, url: str):
        self.url = url.rstrip("/")
        self.cache: dict[int, str] = {}

    def __call__(self, schema_id: int) -> str:
        if schema_id not in self.cache:
            with urllib.request.urlopen(
                f"{self.url}/schemas/ids/{schema_id}", timeout=10
            ) as r:
                self.cache[schema_id] = json.load(r)["schema"]
        return self.cache[schema_id]


def already_committed(summaries: list[dict], query_id: str, batch_id: int) -> bool:
    """True if a snapshot of the table already holds this query's batch_id
    (or a later one: batches of one query commit in order)."""
    return any(
        s.get(QUERY_ID) == query_id and int(s.get(BATCH_ID, "-1")) >= batch_id
        for s in summaries
    )


def snapshot_summaries(spark: SparkSession, table: str) -> list[dict]:
    if not spark.catalog.tableExists(table):
        return []
    return [
        r.summary for r in spark.sql(f"SELECT summary FROM {table}.snapshots").collect()
    ]


def bronze_rows(
    batch: DataFrame, topic: str, schema_for, ingested_at: Column
) -> DataFrame:
    """The batch's records of one topic as bronze rows."""
    decoded = decode_by_schema(
        split_frames(batch.where(F.col("topic") == topic)), schema_for
    )
    if topic in MONGO_TOPICS:
        decoded = decode_by_schema(
            decoded, schema_for, "key_schema_id", "key_payload", "key_event"
        )
        return mongo_bronze(decoded, ingested_at)
    return postgres_bronze(decoded, ingested_at)


def current_snapshot(spark: SparkSession, table: str) -> int | None:
    rows = spark.sql(
        f"SELECT snapshot_id FROM {table}.refs WHERE name = 'main'"
    ).collect()
    return rows[0][0] if rows else None


def _marks(query_id: str, batch_id: int) -> dict:
    """Write options that stamp the commit's snapshot summary (replay guard)."""
    return {
        f"snapshot-property.{QUERY_ID}": query_id,
        f"snapshot-property.{BATCH_ID}": str(batch_id),
    }


def write_bronze(
    spark: SparkSession, rows: DataFrame, table: str, query_id: str, batch_id: int
) -> None:
    """Append rows, creating the table on its first batch (ADR 006: the
    writer creates and evolves bronze tables)."""
    marks = _marks(query_id, batch_id)
    if not spark.catalog.tableExists(table):
        (
            rows.writeTo(table)
            .using("iceberg")
            # Hidden partitioning by commit day (ADR 012).
            .partitionedBy(partitioning.days("source_ts"))
            .tableProperty("format-version", "2")
            # Lets an append with mergeSchema add new (nullable) columns.
            .tableProperty("write.spark.accept-any-schema", "true")
            .options(**marks)
            .create()
        )
        return
    rows.writeTo(table).options(**marks).option("mergeSchema", "true").append()


def is_transient(error: Exception) -> bool:
    """True for errors worth retrying (network, throttling), False for
    errors a retry cannot fix (permissions, bad data, schema conflicts)."""
    text = str(error)
    return "AccessDenied" not in text and any(m in text for m in TRANSIENT_MARKERS)


def with_retries(
    step: Callable[[], object],
    attempts: int = 5,
    first_delay: float = 5.0,
    sleep: Callable[[float], None] = time.sleep,
) -> object:
    """Run step(), retrying transient errors with doubling delays
    (5, 10, 20, 40 s by default). Other errors, and the last attempt's
    error, are raised: the micro-batch fails and Spark stops the query."""
    delay = first_delay
    for attempt in range(1, attempts + 1):
        try:
            return step()
        except Exception as error:
            if attempt == attempts or not is_transient(error):
                raise
            log.warning(
                "transient error (attempt %s/%s), retry in %.0f s: %s",
                attempt,
                attempts,
                delay,
                str(error).splitlines()[0][:200],
            )
            sleep(delay)
            delay *= 2
    raise AssertionError("unreachable")


def append_once(
    spark: SparkSession,
    rows: DataFrame,
    table: str,
    query_id: str,
    batch_id: int,
) -> bool:
    """Append this batch's rows to the table unless a snapshot already holds
    this query's batch id. Returns False when skipped.

    The check runs again on every retry: if a commit reached Glue but its
    reply was lost (an ambiguous failure), the retry finds the batch mark in
    the table's snapshots and does not append a second time.
    """
    if already_committed(snapshot_summaries(spark, table), query_id, batch_id):
        return False
    write_bronze(spark, rows, table, query_id, batch_id)
    return True


def record_batch(
    spark: SparkSession,
    query_id: str,
    batch_id: int,
    tables: list[str],
    ledger: str = LEDGER,
) -> bool:
    """The micro-batch's last step: one ledger row holding every bronze
    table's current snapshot id. A row for batch N therefore means all of
    batch N's appends have committed, so silver can read every table as of
    the same batch (ADR 014). Returns False when a replay finds the row.
    """
    if already_committed(snapshot_summaries(spark, ledger), query_id, batch_id):
        return False
    snapshots = {
        table.rsplit(".", 1)[1]: current_snapshot(spark, table)
        for table in tables
        if spark.catalog.tableExists(table)
    }
    row = spark.createDataFrame(
        [(query_id, batch_id, snapshots)],
        "query_id string, batch_id bigint, snapshots map<string, bigint>",
    ).withColumn("committed_at", F.current_timestamp())
    writer = row.writeTo(ledger).options(**_marks(query_id, batch_id))
    if spark.catalog.tableExists(ledger):
        writer.append()
    else:
        writer.using("iceberg").tableProperty("format-version", "2").create()
    return True


# --- maintenance (ADR 017): the stream maintains its own tables ----------------

# Bronze is an append-only log: its history is in the rows ("bronze as of T"
# is a filter on ingested_at), so an hour of snapshots is enough; the replay
# guard needs only the newest one, silver only the ledger's cut (ADR 014).
KEEP_SNAPSHOTS_FOR = datetime.timedelta(hours=1)
KEEP_LAST_SNAPSHOTS = 5
# Every commit writes a new metadata.json; Iceberg deletes the oldest ones at
# commit time instead of letting them pile up.
METADATA_CLEANUP = {
    "write.metadata.delete-after-commit.enabled": "true",
    "write.metadata.previous-versions-max": "20",
}


def maintain_bronze(
    spark: SparkSession, tables: list[str], now: datetime.datetime
) -> dict:
    """Expire snapshots older than KEEP_SNAPSHOTS_FOR on each table (keeping
    the last KEEP_LAST_SNAPSHOTS), and make Iceberg delete old metadata
    files at commit; returns how many snapshots each table lost.

    Append-only tables: every data file stays referenced by the newest
    snapshot, so expiry deletes only old manifest lists and manifests.

    Uses Iceberg's Java API (through PySpark's gateway to the JVM), not the
    `expire_snapshots` SQL procedure: the procedure finds unreferenced files
    by reading every manifest as Spark tasks, a fixed ~2.5 min per table
    here (measured: 15.6 min an hour for 36 snapshots a table). The Java
    API's incremental cleanup, chosen automatically when only the main
    branch exists, reads only what the expired snapshots referenced.
    """
    older_than_ms = int((now - KEEP_SNAPSHOTS_FOR).timestamp() * 1000)
    jvm = spark._jvm
    size = jvm.org.apache.iceberg.relocated.com.google.common.collect.Iterables.size
    expired = {}
    for table in tables:
        if not spark.catalog.tableExists(table):
            continue
        props = ", ".join(f"'{k}' = '{v}'" for k, v in METADATA_CLEANUP.items())
        spark.sql(f"ALTER TABLE {table} SET TBLPROPERTIES ({props})")
        iceberg = jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(
            spark._jsparkSession, table
        )
        before = size(iceberg.snapshots())
        (
            iceberg.expireSnapshots()
            .expireOlderThan(older_than_ms)
            .retainLast(KEEP_LAST_SNAPSHOTS)
            .commit()
        )
        iceberg.refresh()
        expired[table.rsplit(".", 1)[1]] = before - size(iceberg.snapshots())
        # Spark caches loaded tables: forget this one so the next append
        # starts from the new metadata.
        spark.catalog.refreshTable(table)
    return expired


def compact(spark: SparkSession, table: str) -> int:
    """Rewrite small data files into larger ones; returns how many files
    were rewritten (Iceberg's default: groups of at least 5 small files)."""
    # The full name: given "db.table", Iceberg first tries "db" as a catalog
    # and logs a warning with a stack trace.
    catalog = table.split(".", 1)[0]
    result = spark.sql(
        f"CALL {catalog}.system.rewrite_data_files(table => '{table}')"
    ).first()
    return result.rewritten_data_files_count


# Heavier upkeep, at most one table per hourly run so that a pause stays well
# under O1's 5 minutes (ADR 017). When a table last had each task is a table
# property, so a restart of the stream does not reset it.
COMPACTED_AT = "thelook.compacted-at"
ORPHANS_REMOVED_AT = "thelook.orphans-removed-at"
COMPACT_EVERY = datetime.timedelta(days=1)
REMOVE_ORPHANS_EVERY = datetime.timedelta(days=7)
# Files younger than this are never orphans: a write may still be using them.
ORPHAN_MIN_AGE = datetime.timedelta(days=3)


def _last_run(spark: SparkSession, table: str, key: str) -> datetime.datetime | None:
    props = {r.key: r.value for r in spark.sql(f"SHOW TBLPROPERTIES {table}").collect()}
    return datetime.datetime.fromisoformat(props[key]) if key in props else None


def _due(
    spark: SparkSession,
    tables: list[str],
    key: str,
    every: datetime.timedelta,
    now: datetime.datetime,
) -> str | None:
    """The table whose task `key` ran longest ago (never: first; ties by
    name), if that was at least `every` ago."""
    due = []
    for table in tables:
        if not spark.catalog.tableExists(table):
            continue
        last = _last_run(spark, table, key)
        if last is None or now - last >= every:
            due.append((last is not None, last, table))
    return min(due)[2] if due else None


def compact_past_days(spark: SparkSession, table: str, now: datetime.datetime) -> int:
    """Compact the partitions before today (the stream only adds to today's);
    returns how many files were rewritten. The small files it replaces are
    deleted by a later expiry."""
    catalog = table.split(".", 1)[0]
    today = now.strftime("%Y-%m-%d")
    return spark.sql(
        f"CALL {catalog}.system.rewrite_data_files(table => '{table}', "
        f"where => \"source_ts < TIMESTAMP '{today} 00:00:00'\")"
    ).first()["rewritten_data_files_count"]


def remove_orphans(spark: SparkSession, table: str, now: datetime.datetime) -> int:
    """Delete files under the table's folder that no snapshot references
    (left by failed writes, or old metadata files), if older than
    ORPHAN_MIN_AGE; returns how many."""
    catalog = table.split(".", 1)[0]
    older_than = (now - ORPHAN_MIN_AGE).strftime("%Y-%m-%d %H:%M:%S")
    return len(
        spark.sql(
            f"CALL {catalog}.system.remove_orphan_files(table => '{table}', "
            f"older_than => TIMESTAMP '{older_than}')"
        ).collect()
    )


def heavy_upkeep(
    spark: SparkSession, tables: list[str], now: datetime.datetime
) -> str | None:
    """One task on one table: compaction if one is due (daily), otherwise
    orphan removal if one is due (weekly). Returns what was done, if anything."""
    stamp = now.isoformat()
    table = _due(spark, tables, COMPACTED_AT, COMPACT_EVERY, now)
    if table is not None:
        files = compact_past_days(spark, table, now)
        spark.sql(
            f"ALTER TABLE {table} SET TBLPROPERTIES ('{COMPACTED_AT}' = '{stamp}')"
        )
        return f"compacted {table.rsplit('.', 1)[1]}: {files} files rewritten"
    table = _due(spark, tables, ORPHANS_REMOVED_AT, REMOVE_ORPHANS_EVERY, now)
    if table is not None:
        files = remove_orphans(spark, table, now)
        spark.sql(
            f"ALTER TABLE {table} SET TBLPROPERTIES ('{ORPHANS_REMOVED_AT}' = '{stamp}')"
        )
        return f"orphans of {table.rsplit('.', 1)[1]}: {files} files deleted"
    return None
