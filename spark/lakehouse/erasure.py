"""GDPR erasure in bronze, applied by the bronze stream (ADR 021).

The gdpr_erasure DAG cannot delete from bronze (ADR 017: no key that can
delete bronze in Airflow). It publishes a request to ERASURE_TOPIC; the
stream, bronze's only writer, reads it in its normal micro-batches and calls
apply_requests() after the CDC appends and before the ledger row.

Every past image of the user is found by key, then deleted by key:

  shop_users        the user id itself
  shop_orders       ids of the orders whose user_id is the user
  shop_order_items  ids of the items whose order_id is one of those orders
  web_events,       doc_id of the documents whose JSON has the user's
  web_reviews       $.user_id

Deleting by key, not by user_id, also removes the rows that do not carry the
user_id: Debezium delete events (REPLICA IDENTITY DEFAULT: only the primary
key in `before`), and MongoDB deletes (only the key). Every key is found
before anything is deleted: the items are found through the orders.

A DELETE alone is not erasure: older snapshots still reference the files
that hold the rows. Bronze tables are copy-on-write (Iceberg's default), so
the DELETE rewrites the affected files without the user's rows; expiring
every older snapshot then lets Iceberg delete the old files.
"""

import datetime
import hashlib
import json
import logging
import uuid
from dataclasses import dataclass

from pyspark.sql import Column, SparkSession
from pyspark.sql import functions as F

from lakehouse import upkeep
from lakehouse.bronze import (
    BATCH_ID,
    DATABASE,
    QUERY_ID,
    _marks,
    already_committed,
    snapshot_summaries,
)

ERASURE_TOPIC = "thelook.erasure-requests"
# One row per request applied: what was removed, never who (ADR 021).
LOG_NAME = "erasure_log"
ERASURE_LOG = f"{DATABASE}.{LOG_NAME}"

log = logging.getLogger("erasure")


@dataclass(frozen=True)
class Request:
    request_id: str
    user_id: str


def parse_request(value: bytes | None) -> Request | None:
    """A request record's value -> Request, or None if it is not one.

    The value is JSON: {"request_id": "...", "user_id": "<uuid>"}. The user
    id must be a UUID in its canonical lowercase form, as the source writes
    it: anything else cannot match a row, and is refused rather than
    searched for. A bad request is skipped, never raised: a raise would fail
    the batch, and the replay would fail again on the same record, forever.
    The DAG notices the missing acknowledgement and fails instead.
    """
    try:
        body = json.loads(value)
        request_id, user_id = str(body["request_id"]), str(body["user_id"])
        canonical = str(uuid.UUID(user_id))
    except (TypeError, ValueError, KeyError):
        return None
    if user_id != canonical or not 0 < len(request_id) <= 200:
        return None
    return Request(request_id, user_id)


def user_hash(user_id: str) -> str:
    """sha256 of the user id: the audit proves a request was applied and can
    be matched to it, without keeping the identifier."""
    return hashlib.sha256(user_id.encode()).hexdigest()


def _pg(field: str) -> Column:
    """A PostgreSQL table's column from whichever image the event has:
    `after` for inserts, updates and snapshot reads, `before` for deletes."""
    return F.coalesce(F.col(f"after.{field}"), F.col(f"before.{field}"))


def _json_user_is(user_id: str) -> Column:
    """The MongoDB document's user_id is `user_id` (bronze keeps documents
    as JSON). A plain substring test first: Spark evaluates AND lazily, so
    only the few documents holding the id as text are parsed (measured on
    web_events, 6.5 million rows: 586 s -> 448 s; most of the rest is
    reading the JSON column from S3)."""
    return F.col("after").contains(user_id) & (
        F.get_json_object(F.col("after"), "$.user_id") == F.lit(user_id)
    )


# Bronze tables holding user data, and the key each is deleted by. SQL text,
# not Columns: a Column needs a running Spark session, and the DELETE is SQL.
PG_KEY = "coalesce(after.id, before.id)"
USER_TABLES = {
    "shop_users": PG_KEY,
    "shop_orders": PG_KEY,
    "shop_order_items": PG_KEY,
    "web_events": "doc_id",
    "web_reviews": "doc_id",
}


def _keys(spark: SparkSession, table: str, where: Column) -> list[str]:
    """Distinct keys of the table's rows matching `where`."""
    if not spark.catalog.tableExists(table):
        return []
    key = F.expr(USER_TABLES[table.rsplit(".", 1)[1]])
    rows = spark.table(table).where(where).select(key.alias("k")).distinct()
    return sorted(r.k for r in rows.collect() if r.k is not None)


def user_keys(
    spark: SparkSession, user_id: str, database: str = DATABASE
) -> dict[str, list[str]]:
    """Every bronze key that holds the user's data, per table."""
    t = {name: f"{database}.{name}" for name in USER_TABLES}
    user = F.lit(user_id)
    orders = _keys(spark, t["shop_orders"], _pg("user_id") == user)
    items = _keys(spark, t["shop_order_items"], _pg("order_id").isin(orders))
    return {
        "shop_users": _keys(spark, t["shop_users"], _pg("id") == user),
        "shop_orders": orders,
        "shop_order_items": items,
        "web_events": _keys(spark, t["web_events"], _json_user_is(user_id)),
        "web_reviews": _keys(spark, t["web_reviews"], _json_user_is(user_id)),
    }


def delete_keys(spark: SparkSession, table: str, keys: list[str]) -> int:
    """Delete every row of the table whose key is in `keys` (keys found in
    this table by user_keys, so at least one row matches); returns how many
    rows were deleted.

    The keys go in as a DataFrame that the DELETE reads as a subquery
    (PySpark registers it as a temporary view for the statement), never as
    text inside the SQL: a key cannot change the statement.

    The count comes from the snapshot summaries (total records before and
    after the DELETE), not from a count before it: each scan of a bronze table costs minutes (P8), and no one
    else commits meanwhile (the stream is bronze's only writer, and its
    upkeep thread is held off during the purge).
    """
    if not keys:
        return 0
    key_sql = USER_TABLES[table.rsplit(".", 1)[1]]
    spark.sql(
        f"DELETE FROM {table} WHERE {key_sql} IN (SELECT k FROM {{keys}})",
        keys=spark.createDataFrame([(k,) for k in keys], "k string"),
    )
    iceberg = spark._jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(
        spark._jsparkSession, table
    )
    iceberg.refresh()
    # Not "deleted-records": copy-on-write counts every record of the files
    # it replaced, including the other users' rows it wrote back.
    after = iceberg.currentSnapshot()
    before = iceberg.snapshot(after.parentId())
    return _total(before) - _total(after)


def _total(snapshot) -> int:
    return int(snapshot.summary().get("total-records"))


def purge_user(
    spark: SparkSession, user_id: str, database: str = DATABASE
) -> dict[str, int]:
    """Delete every bronze row holding the user's data; returns the rows
    deleted per table. Idempotent: run again, it finds nothing."""
    keys = user_keys(spark, user_id, database)
    return {
        name: delete_keys(spark, f"{database}.{name}", keys[name])
        for name in USER_TABLES
    }


def stamp(spark: SparkSession, table: str, query_id: str, batch_id: int) -> None:
    """An empty commit carrying the batch's marks (lakehouse.bronze).

    Expiry keeps only the newest snapshot, and the DELETE's snapshot has no
    marks (SQL statements cannot set snapshot properties from PySpark). The
    batch's append snapshot, the one with the marks, is about to go: without
    a newer marked snapshot, a replay of this batch would find no mark and
    append its rows a second time.
    """
    iceberg = spark._jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(
        spark._jsparkSession, table
    )
    iceberg.refresh()
    iceberg.newAppend().set(QUERY_ID, query_id).set(BATCH_ID, str(batch_id)).commit()
    spark.catalog.refreshTable(table)


def apply_requests(
    spark: SparkSession,
    requests: list[Request],
    query_id: str,
    batch_id: int,
    database: str = DATABASE,
) -> dict[str, int] | None:
    """Apply a micro-batch's erasure requests to bronze; returns how many
    snapshots each purged table lost, or None when a replay of the batch
    finds its log rows (already applied).

    Order, each step safe to replay:
      1. delete every user's rows (idempotent);
      2. on each table that lost rows, stamp the batch's marks, then expire
         every snapshot but the newest: the old files that held the rows are
         deleted, and time travel to before the purge fails;
      3. one log row per request, stamped with the batch: written last, so
         a replay before it purges again (finding nothing) and logs then.
    The caller writes the ledger row only after this returns, so the ledger
    never points at an expired snapshot.
    """
    log_table = f"{database}.{LOG_NAME}"
    if already_committed(snapshot_summaries(spark, log_table), query_id, batch_id):
        return None
    deleted = [purge_user(spark, r.user_id, database) for r in requests]
    purged = sorted({name for counts in deleted for name, n in counts.items() if n})
    now = datetime.datetime.now(datetime.timezone.utc)
    expired = {}
    for name in purged:
        table = f"{database}.{name}"
        stamp(spark, table, query_id, batch_id)
        # Every snapshot older than now is pre-stamp; the current one (the
        # stamp) is always kept.
        expired[name] = upkeep.expire(spark, table, now, keep_last=1)
    rows = spark.createDataFrame(
        [
            (r.request_id, user_hash(r.user_id), counts)
            for r, counts in zip(requests, deleted, strict=True)
        ],
        "request_id string, user_sha256 string, rows_deleted map<string, bigint>",
    ).withColumn("purged_at", F.current_timestamp())
    writer = rows.writeTo(log_table).options(**_marks(query_id, batch_id))
    if spark.catalog.tableExists(log_table):
        writer.append()
    else:
        writer.using("iceberg").tableProperty("format-version", "2").create()
    return expired
