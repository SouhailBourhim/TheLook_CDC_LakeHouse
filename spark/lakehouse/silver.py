"""Bronze change events -> silver current state (ADR 014).

For one table, each run:
  1. reads the bronze snapshots added since the last run (incremental read),
     up to the stream's newest finished batch (consistent_cut);
  2. keeps the latest event per key (latest_per_key);
  3. types the columns (timestamps, money, MongoDB JSON) (silver_rows);
  4. MERGEs into the silver table with a position guard (merge_sql);
  5. records the bronze snapshot it reached as a table property.

Steps 4 and 5 are not atomic, and do not need to be: if the job dies
between them, the next run reads the same increment again, and the MERGE
is idempotent (no event is newer than itself), so the result is the same.
"""

from dataclasses import dataclass

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from lakehouse.bronze import LEDGER, current_snapshot

WATERMARK = "thelook.bronze-snapshot"
MONEY = "decimal(10,2)"
METADATA = ["_position", "_source_ts", "_row_hash", "_merged_at"]

# MongoDB Extended JSON (legacy mode) writes dates as {"$date": <epoch ms>}.
_DATE = StructType([StructField("$date", LongType())])


@dataclass(frozen=True)
class TableSpec:
    name: str  # silver table name, e.g. "orders"
    bronze: str  # bronze table name, e.g. "shop_orders"
    source: str  # "postgres" or "mongo"
    money: tuple[str, ...] = ()
    json_schema: StructType | None = None  # MongoDB documents only
    partition_day: str | None = None  # days(<column>) partitioning
    prune_on: str | None = None  # MERGE reads only target rows >= batch min


EVENTS_JSON = StructType(
    [
        StructField("_id", StringType()),
        StructField("user_id", StringType()),
        StructField("sequence_number", IntegerType()),
        StructField("session_id", StringType()),
        StructField("ip_address", StringType()),
        StructField("city", StringType()),
        StructField("state", StringType()),
        StructField("postal_code", StringType()),
        StructField("browser", StringType()),
        StructField("traffic_source", StringType()),
        StructField("uri", StringType()),
        StructField("event_type", StringType()),
        StructField("created_at", _DATE),
        StructField("product_id", LongType()),
        StructField("price", DoubleType()),
    ]
)

REVIEWS_JSON = StructType(
    [
        StructField("_id", StringType()),
        StructField("order_item_id", StringType()),
        StructField("product_id", LongType()),
        StructField("user_id", StringType()),
        StructField("rating", IntegerType()),
        StructField("title", StringType()),
        StructField("text", StringType()),
        StructField("tags", ArrayType(StringType())),
        StructField("helpful_votes", IntegerType()),
        StructField("created_at", _DATE),
        StructField("updated_at", _DATE),
        StructField("edited_at", _DATE),
    ]
)

SPECS = {
    s.name: s
    for s in [
        TableSpec("users", "shop_users", "postgres"),
        TableSpec("orders", "shop_orders", "postgres"),
        TableSpec("order_items", "shop_order_items", "postgres", money=("sale_price",)),
        TableSpec(
            "products", "shop_products", "postgres", money=("cost", "retail_price")
        ),
        TableSpec("dist_centers", "shop_dist_centers", "postgres"),
        TableSpec(
            "events",
            "web_events",
            "mongo",
            money=("price",),
            json_schema=EVENTS_JSON,
            partition_day="created_at",
            prune_on="created_at",
        ),
        TableSpec("reviews", "web_reviews", "mongo", json_schema=REVIEWS_JSON),
    ]
}


# --- 2. latest event per key ---------------------------------------------------


def latest_per_key(bronze: DataFrame, source: str) -> DataFrame:
    """One row per key: the newest event, as _key, _op, _position,
    _source_ts and _image (the after image: a struct for PostgreSQL, a JSON
    string for MongoDB; null for a delete).

    Order, highest first: log position, then streamed before snapshot (`r`)
    rows (a blocking snapshot's rows carry the pause LSN, which the first
    streamed event can share, P3), then the Kafka offset.
    """
    if source == "postgres":
        # A delete carries the key only in `before` (REPLICA IDENTITY DEFAULT).
        key = F.coalesce(F.col("after.id"), F.col("before.id"))
        position = F.col("lsn")
    else:
        key = F.col("doc_id")
        # (source_ts_ms, ord) as one comparable number (E1); ord counts
        # events within one cluster-time second, far below 1,000,000.
        position = F.col("source_ts_ms") * F.lit(1_000_000) + F.col("ord")
    events = bronze.select(
        key.alias("_key"),
        F.col("op").alias("_op"),
        position.alias("_position"),
        F.col("source_ts").alias("_source_ts"),
        F.col("after").alias("_image"),
        F.col("kafka_offset"),
    )
    newest_first = Window.partitionBy("_key").orderBy(
        F.col("_position").desc(),
        (F.col("_op") == "r").asc(),  # False (streamed) sorts before True
        F.col("kafka_offset").desc(),
    )
    return (
        events.withColumn("_rank", F.row_number().over(newest_first))
        .where("_rank = 1")
        .drop("_rank", "kafka_offset")
    )


# --- 3. typed rows -------------------------------------------------------------


def _money(column: Column) -> Column:
    return F.round(column, 2).cast(MONEY)


def _postgres_columns(latest: DataFrame, spec: TableSpec) -> list[Column]:
    image = latest.schema["_image"].dataType
    columns = []
    for f in image.fields:
        value = F.col("_image")[f.name]
        if f.name == "id":
            value = F.col("_key")  # also set for deletes (no after image)
        elif f.name.endswith("_at") and isinstance(f.dataType, LongType):
            # io.debezium.time.MicroTimestamp: microseconds since the epoch.
            value = F.timestamp_micros(value)
        elif f.name in spec.money:
            value = _money(value)
        columns.append(value.alias(f.name))
    return columns


def _mongo_columns(spec: TableSpec) -> list[Column]:
    doc = F.col("_doc")
    columns = [F.col("_key").alias("id")]
    for f in spec.json_schema.fields:
        if f.name == "_id":
            continue
        value = doc[f.name]
        if f.dataType == _DATE:
            value = F.timestamp_millis(value["$date"])
        elif f.name in spec.money:
            value = _money(value)
        columns.append(value.alias(f.name))
    return columns


def silver_rows(latest: DataFrame, spec: TableSpec) -> tuple[DataFrame, DataFrame]:
    """Typed silver rows (with _op, kept only for the MERGE), and the rows
    whose MongoDB JSON did not parse (for the dead-letter area, P7)."""
    if spec.source == "postgres":
        typed = latest.select("*", *_postgres_columns(latest, spec))
        bad = latest.limit(0)
        business = [f.name for f in latest.schema["_image"].dataType.fields]
    else:
        parsed = latest.withColumn("_doc", F.from_json("_image", spec.json_schema))
        # Not a delete, yet nothing parsed: not valid JSON for this schema.
        bad = parsed.where(F.col("_image").isNotNull() & F.col("_doc._id").isNull())
        good = parsed.where(F.col("_image").isNull() | F.col("_doc._id").isNotNull())
        typed = good.select("*", *_mongo_columns(spec))
        business = ["id"] + [f.name for f in spec.json_schema.fields if f.name != "_id"]
    row_hash = F.sha2(F.to_json(F.struct(*[F.col(c) for c in business])), 256)
    rows = typed.select(
        *business,
        "_position",
        "_source_ts",
        row_hash.alias("_row_hash"),
        F.current_timestamp().alias("_merged_at"),
        "_op",
    )
    return rows, bad


# --- 4. MERGE ------------------------------------------------------------------


def merge_sql(
    target: str,
    source_view: str,
    columns: list[str],
    prune: tuple[str, str] | None = None,
) -> str:
    """MERGE with a position guard: an event changes a row only if it is
    newer than what the row already reflects. Every newer event updates,
    no-ops included, so _position stays current (ADR 014)."""
    on = "t.id = s.id"
    if prune:
        # Lets Iceberg skip the partitions older than the batch.
        column, lower_bound = prune
        on += f" AND t.{column} >= TIMESTAMP '{lower_bound}'"
    sets = ", ".join(f"t.{c} = s.{c}" for c in columns)
    names = ", ".join(columns)
    values = ", ".join(f"s.{c}" for c in columns)
    return (
        f"MERGE INTO {target} t USING {source_view} s ON {on} "
        f"WHEN MATCHED AND s._op = 'd' AND s._position > t._position THEN DELETE "
        f"WHEN MATCHED AND s._op <> 'd' AND s._position > t._position THEN UPDATE SET {sets} "
        f"WHEN NOT MATCHED AND s._op <> 'd' THEN INSERT ({names}) VALUES ({values})"
    )


# --- 1 and 5. snapshots, watermark, table creation ------------------------------


def consistent_cut(spark: SparkSession, ledger: str = LEDGER) -> dict[str, int]:
    """Every bronze table's snapshot id at the end of the newest finished
    stream batch (the stream's ledger, ADR 012). Reading all tables up to
    the same batch keeps silver consistent across tables: an order item is
    never merged while its order, written in the same batch, is not."""
    if not spark.catalog.tableExists(ledger):
        raise RuntimeError(f"{ledger} does not exist: has the bronze stream run?")
    newest = spark.table(ledger).orderBy(F.col("committed_at").desc()).first()
    if newest is None:
        raise RuntimeError(f"{ledger} is empty: has the bronze stream run?")
    return dict(newest.snapshots)


def watermark(spark: SparkSession, table: str) -> int | None:
    if not spark.catalog.tableExists(table):
        return None
    props = {r.key: r.value for r in spark.sql(f"SHOW TBLPROPERTIES {table}").collect()}
    return int(props[WATERMARK]) if WATERMARK in props else None


def read_bronze(
    spark: SparkSession, table: str, after: int | None, upto: int
) -> DataFrame:
    """Bronze rows committed after snapshot `after` up to snapshot `upto`
    (all of them as of `upto` when there is no watermark yet)."""
    reader = spark.read.format("iceberg")
    if after is None:
        # Time travel to one snapshot: Spark's own option since Iceberg on
        # Spark 4 (Iceberg's former `snapshot-id` option is rejected).
        return reader.option("versionAsOf", upto).load(table)
    return (
        reader.option("start-snapshot-id", after)
        .option("end-snapshot-id", upto)
        .load(table)
    )


def create_table(
    spark: SparkSession, table: str, rows: DataFrame, spec: TableSpec
) -> None:
    writer = rows.drop("_op").limit(0).writeTo(table).using("iceberg")
    if spec.partition_day:
        from pyspark.sql.functions import partitioning

        writer = writer.partitionedBy(partitioning.days(spec.partition_day))
    (
        writer.tableProperty("format-version", "2")
        # Merge-on-read: matched rows go to delete files instead of
        # rewriting (and re-downloading) whole data files on each run.
        .tableProperty("write.delete.mode", "merge-on-read")
        .tableProperty("write.update.mode", "merge-on-read")
        .tableProperty("write.merge.mode", "merge-on-read")
        .create()
    )


def add_new_columns(spark: SparkSession, table: str, rows: DataFrame) -> list[str]:
    """A new nullable source column (schema evolution) becomes a silver
    column before the MERGE needs it."""
    existing = set(spark.table(table).columns)
    added = []
    for f in rows.drop("_op").schema.fields:
        if f.name not in existing:
            spark.sql(
                f"ALTER TABLE {table} ADD COLUMN {f.name} {f.dataType.simpleString()}"
            )
            added.append(f.name)
    return added


def process_table(
    spark: SparkSession,
    spec: TableSpec,
    bronze_db: str = "lake.thelook_bronze",
    silver_db: str = "lake.thelook_silver",
    cut: dict[str, int] | None = None,
) -> dict:
    """One incremental run for one table, up to its snapshot in `cut` (the
    job passes consistent_cut(); without one, the table's current snapshot,
    for tests and one-off runs); returns what it did."""
    bronze, target = f"{bronze_db}.{spec.bronze}", f"{silver_db}.{spec.name}"
    upto = current_snapshot(spark, bronze) if cut is None else cut.get(spec.bronze)
    after = watermark(spark, target)
    if upto is None or upto == after:
        return {"table": spec.name, "status": "up to date"}
    latest = latest_per_key(read_bronze(spark, bronze, after, upto), spec.source)
    rows, bad = silver_rows(latest, spec)
    rows = rows.persist()
    try:
        if not spark.catalog.tableExists(target):
            create_table(spark, target, rows, spec)
        added = add_new_columns(spark, target, rows)
        prune = None
        if spec.prune_on and rows.where("_op = 'd'").isEmpty():
            oldest = rows.agg(F.min(spec.prune_on)).first()[0]
            if oldest is not None:
                prune = (spec.prune_on, oldest.strftime("%Y-%m-%d %H:%M:%S"))
        rows.createOrReplaceTempView("silver_source")
        columns = [c for c in rows.columns if c != "_op"]
        spark.sql(merge_sql(target, "silver_source", columns, prune))
        spark.sql(f"ALTER TABLE {target} SET TBLPROPERTIES ('{WATERMARK}' = '{upto}')")
        return {
            "table": spec.name,
            "status": "merged",
            "keys": rows.count(),
            "bad_json": bad.count(),
            "new_columns": added,
            "bronze_snapshot": upto,
            "full_read": after is None,
        }
    finally:
        rows.unpersist()
