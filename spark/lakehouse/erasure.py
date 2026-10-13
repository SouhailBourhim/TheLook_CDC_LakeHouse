"""GDPR erasure in bronze (by the bronze stream) and in silver and gold
(by jobs/erase.py), ADR 021.

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
    except (TypeError, ValueError, KeyError):
        return None
    if not is_user_id(user_id) or not 0 < len(request_id) <= 200:
        return None
    return Request(request_id, user_id)


def is_user_id(user_id: str) -> bool:
    """A UUID in the canonical lowercase form the source writes."""
    try:
        return str(uuid.UUID(user_id)) == user_id
    except ValueError:
        return False


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


def _once(step):
    return step()


def user_keys(
    spark: SparkSession, user_id: str, database: str = DATABASE, retry=_once
) -> dict[str, list[str]]:
    """Every bronze key that holds the user's data, per table. Each table's
    lookup is retried on its own (`retry`): one scan of web_events takes up
    to 18 minutes (P8), not worth redoing for a blip on another table."""
    t = {name: f"{database}.{name}" for name in USER_TABLES}
    user = F.lit(user_id)

    def keys(name, where):
        return retry(lambda: _keys(spark, t[name], where))

    orders = keys("shop_orders", _pg("user_id") == user)
    items = keys("shop_order_items", _pg("order_id").isin(orders))
    return {
        "shop_users": keys("shop_users", _pg("id") == user),
        "shop_orders": orders,
        "shop_order_items": items,
        "web_events": keys("web_events", _json_user_is(user_id)),
        "web_reviews": keys("web_reviews", _json_user_is(user_id)),
    }


def delete_keys(spark: SparkSession, table: str, keys: list[str]) -> int:
    """Delete every row of the table whose key is in `keys` (keys found in
    this table by user_keys, so at least one row matches); returns how many
    rows were deleted.

    The keys go in as a DataFrame that the DELETE reads as a subquery
    (PySpark registers it as a temporary view for the statement), never as
    text inside the SQL: a key cannot change the statement.

    The count comes from the snapshot summaries (total records before and
    after the DELETE), not from a count before it: each scan of a bronze
    table costs minutes (P8), and no one else commits meanwhile (the stream
    is bronze's only writer, and its upkeep thread is held off during the
    purge). A DELETE that commits nothing (a retry after a commit whose
    reply was lost finds no row left) counts 0.
    """
    if not keys:
        return 0
    key_sql = USER_TABLES[table.rsplit(".", 1)[1]]
    iceberg = spark._jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(
        spark._jsparkSession, table
    )
    iceberg.refresh()
    before = iceberg.currentSnapshot()
    spark.sql(
        f"DELETE FROM {table} WHERE {key_sql} IN (SELECT k FROM {{keys}})",
        keys=spark.createDataFrame([(k,) for k in keys], "k string"),
    )
    iceberg.refresh()
    after = iceberg.currentSnapshot()
    if after.snapshotId() == before.snapshotId():
        return 0
    # Not "deleted-records": copy-on-write counts every record of the files
    # it replaced, including the other users' rows it wrote back.
    return _total(before) - _total(after)


def _total(snapshot) -> int:
    return int(snapshot.summary().get("total-records"))


def purge_user(
    spark: SparkSession, user_id: str, database: str = DATABASE, retry=_once
) -> dict[str, int]:
    """Delete every bronze row holding the user's data; returns the rows
    deleted per table. Idempotent: run again, it finds nothing. Each lookup
    and each DELETE is retried on its own (`retry`)."""
    keys = user_keys(spark, user_id, database, retry)
    return {
        name: retry(
            lambda name=name: delete_keys(spark, f"{database}.{name}", keys[name])
        )
        for name in USER_TABLES
    }


def apply_requests(
    spark: SparkSession,
    requests: list[Request],
    query_id: str,
    batch_id: int,
    database: str = DATABASE,
    retry=_once,
) -> dict[str, int] | None:
    """Apply a micro-batch's erasure requests to bronze; returns how many
    snapshots each purged table lost, or None when a replay of the batch
    finds its log rows (already applied). `retry` wraps each step (the
    stream passes with_retries): a network blip repeats one step, not the
    whole purge (a first version retried it all: 25 more minutes, P8).

    Order, each step safe to replay:
      1. delete every user's rows (idempotent);
      2. on every table that can hold user data, whatever this attempt
         deleted, an empty commit carrying the batch's marks, then expiry
         of every other snapshot (upkeep.drop_history). Not only the tables
         that lost rows now: an interrupted attempt may have deleted rows
         and failed before this step; its replay deletes nothing there, and
         the old snapshots would keep the rows (found in P8 step 6):
         the old files that held the rows are deleted, and time travel to
         before the purge fails. The marks matter: the expiry removes the
         append snapshot that held them (the DELETE's snapshot has none:
         PySpark cannot set snapshot properties on an SQL statement), and
         without them a replay of the batch would append its rows twice;
      3. one log row per request, stamped with the batch: written last, so
         a replay before it purges again (finding nothing) and logs then.
    The caller writes the ledger row only after this returns, so the ledger
    never points at an expired snapshot.
    """
    log_table = f"{database}.{LOG_NAME}"

    def logged() -> bool:
        return already_committed(
            snapshot_summaries(spark, log_table), query_id, batch_id
        )

    if retry(logged):
        return None
    deleted = [purge_user(spark, r.user_id, database, retry) for r in requests]
    purged = [
        name for name in USER_TABLES if spark.catalog.tableExists(f"{database}.{name}")
    ]
    marks = {QUERY_ID: query_id, BATCH_ID: str(batch_id)}
    expired = {
        name: retry(
            lambda name=name: upkeep.drop_history(spark, f"{database}.{name}", marks)
        )
        for name in purged
    }
    rows = spark.createDataFrame(
        [
            (r.request_id, user_hash(r.user_id), counts)
            for r, counts in zip(requests, deleted, strict=True)
        ],
        "request_id string, user_sha256 string, rows_deleted map<string, bigint>",
    ).withColumn("purged_at", F.current_timestamp())

    def write_log() -> None:
        # Checked again on each attempt: a commit whose reply was lost must
        # not be written twice.
        if logged():
            return
        writer = rows.writeTo(log_table).options(**_marks(query_id, batch_id))
        if spark.catalog.tableExists(log_table):
            writer.append()
        else:
            writer.using("iceberg").tableProperty("format-version", "2").create()

    retry(write_log)
    return expired


# --- silver and gold (jobs/erase.py, batch user) ---------------------------------

SILVER_DB = "lake.thelook_silver"
GOLD_DB = "lake.thelook_gold"
# One row per request and table: what was done, never who (ADR 021).
AUDIT_NAME = "erasure_audit"
AUDIT_SCHEMA = (
    "request_id string, user_sha256 string, step string, rows_deleted bigint, "
    "files_rewritten bigint, snapshots_expired bigint, done_at timestamp"
)

# The user's rows in each table, as an SQL condition: the user id is the
# named parameter :uid, the user's order ids the DataFrame {orders}. Neither
# is ever text inside the SQL. The marts are aggregates and are kept (FR9).
_JSON_USER = "contains({c}, :uid) AND get_json_object({c}, '$.user_id') = :uid"
LAKE_TABLES = {
    "silver.users": "id = :uid",
    "silver.orders": "user_id = :uid",
    # No user_id in order items: through the user's orders.
    "silver.order_items": "order_id IN (SELECT k FROM {orders})",
    "silver.events": "user_id = :uid",
    "silver.reviews": "user_id = :uid",
    # Raw MongoDB documents that did not fit their table (ADR 020).
    "silver.dead_letters": _JSON_USER.format(c="payload"),
    "gold.dim_user": "user_id = :uid",
    "gold.fct_order_items": "user_id = :uid",
    "gold.fct_orders": "user_id = :uid",
    "gold.fct_sessions": "user_id = :uid",
}


def _table(step: str, silver_db: str, gold_db: str) -> str:
    layer, name = step.split(".")
    return f"{silver_db if layer == 'silver' else gold_db}.{name}"


def user_orders(
    spark: SparkSession, user_id: str, silver_db: str, gold_db: str
) -> list[str]:
    """The user's order ids, from silver and from gold: silver may already
    have deleted the orders (the source deletion came first), the facts keep
    them until this job runs."""
    found: set[str] = set()
    for table, key in (
        (f"{silver_db}.orders", "id"),
        (f"{gold_db}.fct_orders", "order_id"),
    ):
        if spark.catalog.tableExists(table):
            rows = spark.table(table).where(F.col("user_id") == F.lit(user_id))
            found |= {r[0] for r in rows.select(key).collect()}
    return sorted(found)


def erase_table(
    spark: SparkSession, table: str, condition: str, user_id: str, orders: list[str]
) -> dict[str, int]:
    """Delete the user's rows, rewrite every file that has deletes (the
    rows deleted now, and those an earlier MERGE already deleted when the
    source deletion reached silver), then expire every older snapshot.
    Idempotent: run again, it deletes and rewrites nothing."""
    sql_args = {"args": {"uid": user_id}}
    if "{orders}" in condition:
        sql_args["orders"] = spark.createDataFrame([(k,) for k in orders], "k string")
    deleted = spark.sql(
        f"SELECT count(*) AS n FROM {table} WHERE {condition}", **sql_args
    ).first()["n"]
    if deleted:
        spark.sql(f"DELETE FROM {table} WHERE {condition}", **sql_args)
    files = upkeep.apply_deletes(spark, table)
    return {
        "rows_deleted": deleted,
        "files_rewritten": files,
        "snapshots_expired": upkeep.drop_history(spark, table),
    }


def _audit(
    spark: SparkSession, audit: str, request: Request, step: str, done: dict
) -> None:
    """One audit row for a step, unless the request already has one: a
    rerun after a failure keeps the first run's counts, the real ones."""
    if not spark.catalog.tableExists(audit):
        spark.createDataFrame([], AUDIT_SCHEMA).writeTo(audit).using(
            "iceberg"
        ).tableProperty("format-version", "2").create()
    counts = [
        done.get(k) for k in ("rows_deleted", "files_rewritten", "snapshots_expired")
    ]
    row = spark.createDataFrame(
        [(request.request_id, user_hash(request.user_id), step, *counts)],
        "request_id string, user_sha256 string, step string, rows_deleted bigint, "
        "files_rewritten bigint, snapshots_expired bigint",
    ).withColumn("done_at", F.current_timestamp())
    spark.sql(
        f"MERGE INTO {audit} t USING {{row}} s "
        "ON t.request_id = s.request_id AND t.step = s.step "
        "WHEN NOT MATCHED THEN INSERT *",
        row=row,
    )


def erase_lake(
    spark: SparkSession,
    request: Request,
    silver_db: str = SILVER_DB,
    gold_db: str = GOLD_DB,
    retry=lambda step: step(),
    upstream: dict[str, int] | None = None,
) -> dict[str, dict[str, int]]:
    """Erase the user from every silver and gold table of the map, table by
    table, each followed by its audit row; returns what was done per table.
    `retry` wraps each table's work (the job passes with_retries).

    `upstream`: rows removed by the DAG's earlier steps (sources, bronze,
    Redis), {step: rows}, audited first: one writer for the audit table."""
    if not is_user_id(request.user_id):
        raise ValueError("the user id must be a canonical lowercase UUID")
    audit = f"{silver_db}.{AUDIT_NAME}"
    for step, rows in (upstream or {}).items():
        _audit(spark, audit, request, step, {"rows_deleted": rows})
    orders = user_orders(spark, request.user_id, silver_db, gold_db)
    report = {}
    for step, condition in LAKE_TABLES.items():
        table = _table(step, silver_db, gold_db)
        if not spark.catalog.tableExists(table):
            continue
        report[step] = retry(
            lambda table=table, condition=condition: erase_table(
                spark, table, condition, request.user_id, orders
            )
        )
        _audit(spark, audit, request, step, report[step])
        # Counts and the request id, never the user id.
        log.info("%s %s: %s", request.request_id, step, report[step])
    return report
