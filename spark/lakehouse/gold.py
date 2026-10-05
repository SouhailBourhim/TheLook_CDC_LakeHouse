"""Silver (and bronze history) -> gold star schema (ADR 015).

Dimensions are small and rebuilt on every run:
  dim_date, dim_product, dim_distribution_center (from silver), and
  dim_user, a type 2 slowly changing dimension built from the full
  bronze history of shop_users (one row per version of a user).
"""

import datetime

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

# The open end of the current version: a sentinel instead of NULL, so a
# point-in-time join is just `ts >= valid_from AND ts < valid_to`.
OPEN_END = datetime.datetime(9999, 12, 31)

# Kimball's "unknown member": facts whose user version does not exist (yet)
# point here instead of holding NULL, so an inner join to dim_user keeps them
# (reported as "Unknown") and the retry finds them by this key.
UNKNOWN_USER_SK = -1

# Columns whose change makes a new version of a user. updated_at is left
# out: the generator does not bump it on address changes (source-schema.md).
USER_COLUMNS = [
    "first_name",
    "last_name",
    "email",
    "age",
    "gender",
    "street_address",
    "postal_code",
    "city",
    "state",
    "country",
    "latitude",
    "longitude",
    "traffic_source",
]


def dim_date(
    spark: SparkSession, start: str = "2020-01-01", end: str = "2030-12-31"
) -> DataFrame:
    days = spark.sql(
        f"SELECT explode(sequence(DATE '{start}', DATE '{end}', INTERVAL 1 DAY)) AS date"
    )
    d = F.col("date")
    return days.select(
        F.date_format(d, "yyyyMMdd").cast("int").alias("date_key"),
        d,
        F.year(d).alias("year"),
        F.quarter(d).alias("quarter"),
        F.month(d).alias("month"),
        F.date_format(d, "MMMM").alias("month_name"),
        F.dayofmonth(d).alias("day"),
        F.weekofyear(d).alias("iso_week"),
        # Spark's dayofweek: 1 = Sunday ... 7 = Saturday.
        F.date_format(d, "EEEE").alias("day_name"),
        F.dayofweek(d).isin(1, 7).alias("is_weekend"),
    )


def dim_product(products: DataFrame) -> DataFrame:
    return products.select(
        F.col("id").alias("product_id"),
        "name",
        "brand",
        "category",
        "department",
        "sku",
        "cost",
        "retail_price",
        "distribution_center_id",
    )


def dim_distribution_center(dist_centers: DataFrame) -> DataFrame:
    return dist_centers.select(
        F.col("id").alias("distribution_center_id"), "name", "latitude", "longitude"
    )


def dim_user(bronze_users: DataFrame) -> DataFrame:
    """One row per version of each user (SCD2, FR4), from every shop_users
    change event in bronze.

    - Events are ordered by log position: lsn, then snapshot rows before
      streamed events (the reverse of silver's "newest first", so the same
      event is the latest in both), then the Kafka offset.
    - An event whose business columns hash like the previous event's starts
      no version: no-op updates, and snapshot rows repeating the state.
    - valid_to = the next version's valid_from (or the delete time);
      OPEN_END for the current version.
    - The first version starts at the user's created_at: history before
      bronze began was lost to Kafka retention (ADR 015).
    - user_sk = xxhash64(user_id, lsn of the version's first event): unique
      and identical on every rebuild, so facts keep matching.
    """
    image = F.coalesce(F.col("after"), F.col("before"))
    events = bronze_users.select(
        image["id"].alias("user_id"),
        "op",
        "lsn",
        "kafka_offset",
        F.col("source_ts").alias("changed_at"),
        *[F.col("after")[c].alias(c) for c in USER_COLUMNS],
        F.timestamp_micros(F.col("after.created_at")).alias("created_at"),
    ).withColumn(
        "version_hash",
        F.when(
            F.col("op") != "d",
            F.sha2(F.to_json(F.struct(*[F.col(c) for c in USER_COLUMNS])), 256),
        ),
    )
    in_order = Window.partitionBy("user_id").orderBy(
        F.col("lsn").asc(), (F.col("op") == "r").desc(), F.col("kafka_offset").asc()
    )
    changes = (
        events.withColumn("previous_hash", F.lag("version_hash").over(in_order))
        .where(
            (F.col("op") == "d")
            | F.col("previous_hash").isNull()
            | (F.col("version_hash") != F.col("previous_hash"))
        )
        # A delete right after a delete (a replay) closes nothing new.
        .where(~((F.col("op") == "d") & F.col("previous_hash").isNull()))
    )
    kept = Window.partitionBy("user_id").orderBy(
        F.col("lsn").asc(), (F.col("op") == "r").desc(), F.col("kafka_offset").asc()
    )
    versions = (
        changes.withColumn("valid_to", F.lead("changed_at").over(kept))
        .withColumn("version", F.row_number().over(kept))
        .where(F.col("op") != "d")
    )
    first_valid_from = F.least(F.col("created_at"), F.col("changed_at"))
    known = versions.select(
        F.xxhash64("user_id", "lsn").alias("user_sk"),
        "user_id",
        *USER_COLUMNS,
        "created_at",
        F.when(F.col("version") == 1, first_valid_from)
        .otherwise(F.col("changed_at"))
        .alias("valid_from"),
        F.coalesce(F.col("valid_to"), F.lit(OPEN_END)).alias("valid_to"),
        F.col("valid_to").isNull().alias("is_current"),
        "version",
    )
    return known.unionByName(_unknown_user(known))


def _unknown_user(known: DataFrame) -> DataFrame:
    """The single "unknown member" row: valid for all time, never current,
    text attributes "Unknown", the rest NULL."""
    values = {
        "user_sk": UNKNOWN_USER_SK,
        "valid_from": datetime.datetime(1900, 1, 1),
        "valid_to": OPEN_END,
        "is_current": False,
        "version": 0,
    }
    row = []
    for f in known.schema.fields:
        if f.name in values:
            row.append(values[f.name])
        elif f.name in USER_COLUMNS and isinstance(f.dataType, StringType):
            row.append("Unknown")
        else:
            row.append(None)
    return known.sparkSession.createDataFrame([tuple(row)], known.schema)


def replace_table(df: DataFrame, table: str) -> None:
    """Atomically replace a small gold table (Iceberg REPLACE TABLE: readers
    see the old or the new version, never a half-written one; the table is
    never dropped, which the batch user could not do anyway)."""
    df.writeTo(table).using("iceberg").tableProperty(
        "format-version", "2"
    ).createOrReplace()


def build_dimensions(
    spark: SparkSession,
    bronze_db: str = "lake.thelook_bronze",
    silver_db: str = "lake.thelook_silver",
    gold_db: str = "lake.thelook_gold",
) -> dict:
    """Rebuild the four dimensions; returns their row counts."""
    users = spark.table(f"{bronze_db}.shop_users").select(
        "op", "lsn", "kafka_offset", "source_ts", "before", "after"
    )
    dims = {
        "dim_date": dim_date(spark),
        "dim_product": dim_product(spark.table(f"{silver_db}.products")),
        "dim_distribution_center": dim_distribution_center(
            spark.table(f"{silver_db}.dist_centers")
        ),
        "dim_user": dim_user(users),
    }
    counts = {}
    for name, df in dims.items():
        replace_table(df, f"{gold_db}.{name}")
        counts[name] = spark.table(f"{gold_db}.{name}").count()
    return counts


# --- facts (incremental, ADR 015) ---------------------------------------------

AMOUNT = "decimal(12,2)"
WATERMARK = "thelook.silver-merged-at"


def _hours(start: str, end: str):
    return (F.unix_timestamp(end) - F.unix_timestamp(start)) / 3600.0


def order_item_facts(
    items: DataFrame, orders: DataFrame, products: DataFrame, users: DataFrame
) -> DataFrame:
    """fct_order_items: one row per order item.

    user_sk is the dim_user version valid when the order was placed (a
    point-in-time join, FR4); the unknown member (-1) if no version covers
    that moment yet (the run retries those rows until one does).
    """
    o = orders.select(
        F.col("id").alias("order_id"),
        F.col("user_id"),
        F.col("created_at").alias("order_created_at"),
    )
    p = products.select(
        F.col("product_id"), F.col("cost").alias("unit_cost"), "distribution_center_id"
    )
    u = users.select(
        "user_sk", F.col("user_id").alias("u_user_id"), "valid_from", "valid_to"
    )
    joined = (
        items.join(o, "order_id", "left")
        .join(p, "product_id", "left")
        .join(
            u,
            (F.col("user_id") == F.col("u_user_id"))
            & (F.col("order_created_at") >= F.col("valid_from"))
            & (F.col("order_created_at") < F.col("valid_to")),
            "left",
        )
    )
    return joined.select(
        F.col("id").alias("order_item_id"),
        "order_id",
        "user_id",
        F.coalesce("user_sk", F.lit(UNKNOWN_USER_SK)).alias("user_sk"),
        "product_id",
        "distribution_center_id",
        F.date_format("order_created_at", "yyyyMMdd")
        .cast("int")
        .alias("order_date_key"),
        "status",
        "quantity",
        "sale_price",
        "unit_cost",
        (F.col("sale_price") * F.col("quantity")).cast(AMOUNT).alias("gross_amount"),
        (F.col("unit_cost") * F.col("quantity")).cast(AMOUNT).alias("cost_amount"),
        (F.col("status") == "Cancelled").alias("is_cancelled"),
        (F.col("status") == "Returned").alias("is_returned"),
        F.col("order_created_at").alias("created_at"),
        "shipped_at",
        "delivered_at",
        "returned_at",
        "cancelled_at",
        _hours("order_created_at", "shipped_at").alias("hours_to_ship"),
        _hours("order_created_at", "delivered_at").alias("hours_to_deliver"),
    )


def order_facts(orders: DataFrame, item_facts: DataFrame) -> DataFrame:
    """fct_orders: one row per order, amounts summed from its items with the
    FR6 rules: gross excludes cancelled items, net = gross - returns."""
    kept = ~F.col("is_cancelled")
    totals = item_facts.groupBy("order_id").agg(
        F.count("*").alias("item_count"),
        F.max("user_sk").alias("user_sk"),
        F.sum(F.when(kept, F.col("gross_amount")).otherwise(0))
        .cast(AMOUNT)
        .alias("gross_amount"),
        F.sum(F.when(F.col("is_returned"), F.col("gross_amount")).otherwise(0))
        .cast(AMOUNT)
        .alias("returned_amount"),
        F.sum(F.when(kept & ~F.col("is_returned"), F.col("cost_amount")).otherwise(0))
        .cast(AMOUNT)
        .alias("cost_amount"),
    )
    return orders.join(totals, orders["id"] == totals["order_id"], "left").select(
        F.col("id").alias("order_id"),
        "user_id",
        "user_sk",
        F.date_format("created_at", "yyyyMMdd").cast("int").alias("order_date_key"),
        "status",
        "num_of_items",
        F.coalesce("item_count", F.lit(0)).alias("item_count"),
        F.coalesce("gross_amount", F.lit(0).cast(AMOUNT)).alias("gross_amount"),
        F.coalesce("returned_amount", F.lit(0).cast(AMOUNT)).alias("returned_amount"),
        (F.coalesce("gross_amount", F.lit(0)) - F.coalesce("returned_amount", F.lit(0)))
        .cast(AMOUNT)
        .alias("net_amount"),
        F.coalesce("cost_amount", F.lit(0).cast(AMOUNT)).alias("cost_amount"),
        (F.col("status") == "Cancelled").alias("is_cancelled"),
        (F.col("status") == "Returned").alias("is_returned"),
        "created_at",
        "shipped_at",
        "delivered_at",
        "returned_at",
        "cancelled_at",
        _hours("created_at", "shipped_at").alias("hours_to_ship"),
        _hours("created_at", "delivered_at").alias("hours_to_deliver"),
    )


def session_facts(events: DataFrame) -> DataFrame:
    """fct_sessions: one row per session. Ghost sessions (no user) are kept
    and flagged: their "purchase" events have no order (source-schema.md),
    so funnels exclude them."""
    has = lambda kind: F.max(F.col("event_type") == kind)  # noqa: E731
    return (
        events.groupBy("session_id")
        .agg(
            F.max("user_id").alias("user_id"),
            F.max("user_id").isNull().alias("is_ghost"),
            F.min("created_at").alias("started_at"),
            F.max("created_at").alias("ended_at"),
            F.count("*").alias("event_count"),
            has("product").alias("viewed_product"),
            has("cart").alias("added_to_cart"),
            has("purchase").alias("purchased"),
            F.sum(F.when(F.col("event_type") == "cart", F.col("price")))
            .cast(AMOUNT)
            .alias("cart_value"),
            F.first("traffic_source", ignorenulls=True).alias("traffic_source"),
            F.first("browser", ignorenulls=True).alias("browser"),
        )
        .withColumn(
            "session_date_key", F.date_format("started_at", "yyyyMMdd").cast("int")
        )
    )


def fact_watermark(spark: SparkSession, table: str) -> datetime.datetime | None:
    if not spark.catalog.tableExists(table):
        return None
    props = {r.key: r.value for r in spark.sql(f"SHOW TBLPROPERTIES {table}").collect()}
    return (
        datetime.datetime.fromisoformat(props[WATERMARK])
        if WATERMARK in props
        else None
    )


def merge_facts(spark: SparkSession, table: str, rows: DataFrame, key: str) -> None:
    """Upsert recomputed fact rows on their grain key (idempotent)."""
    if not spark.catalog.tableExists(table):
        (
            rows.limit(0)
            .writeTo(table)
            .using("iceberg")
            .tableProperty("format-version", "2")
            .tableProperty("write.merge.mode", "merge-on-read")
            .tableProperty("write.update.mode", "merge-on-read")
            .tableProperty("write.delete.mode", "merge-on-read")
            .create()
        )
    rows.createOrReplaceTempView("fact_source")
    spark.sql(
        f"MERGE INTO {table} t USING fact_source s ON t.{key} = s.{key} "
        "WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *"
    )


def _set_watermark(spark: SparkSession, table: str, value) -> None:
    if value is not None:
        spark.sql(
            f"ALTER TABLE {table} SET TBLPROPERTIES ('{WATERMARK}' = '{value.isoformat()}')"
        )


def _changed(df: DataFrame, since) -> DataFrame:
    # Every silver run writes new files stamped with one _merged_at: with
    # Iceberg's per-file min/max statistics, older files are skipped unread.
    return df if since is None else df.where(F.col("_merged_at") > F.lit(since))


def _unresolved_ids(spark: SparkSession, table: str, key: str) -> DataFrame | None:
    """Fact rows still pointing to the unknown member (or NULL, the
    convention before the unknown member existed): retried every run."""
    if not spark.catalog.tableExists(table):
        return None
    unresolved = F.col("user_sk").isNull() | (F.col("user_sk") == UNKNOWN_USER_SK)
    return spark.table(table).where(unresolved).select(F.col(key).alias("_retry"))


def build_facts(
    spark: SparkSession,
    silver_db: str = "lake.thelook_silver",
    gold_db: str = "lake.thelook_gold",
) -> dict:
    """Recompute and MERGE the fact rows that changed since the last run;
    returns how many rows *this call* recomputed (after a retry, rows the
    first attempt already merged are not counted again).

    Assumes silver is not being written meanwhile (the DAG runs silver, then
    gold, one run at a time): otherwise a silver commit could slip behind
    the watermark.
    """
    items, orders, events = (
        spark.table(f"{silver_db}.{t}") for t in ("order_items", "orders", "events")
    )
    products = spark.table(f"{gold_db}.dim_product")
    users = spark.table(f"{gold_db}.dim_user")
    counts = {}

    # fct_order_items: changed items, plus items still waiting for a user.
    t_items = f"{gold_db}.fct_order_items"
    since = fact_watermark(spark, t_items)
    changed = _changed(items, since)
    retry = _unresolved_ids(spark, t_items, "order_item_id")
    affected = changed
    if retry is not None:
        affected = affected.unionByName(
            items.join(retry, items["id"] == retry["_retry"], "left_semi")
        ).dropDuplicates(["id"])
    affected = affected.persist()
    item_rows = order_item_facts(affected, orders, products, users)
    merge_facts(spark, t_items, item_rows, "order_item_id")
    _set_watermark(spark, t_items, changed.agg(F.max("_merged_at")).first()[0])
    counts["fct_order_items_recomputed"] = affected.count()

    # fct_orders: changed orders, orders of changed items, orders without a user.
    t_orders = f"{gold_db}.fct_orders"
    since = fact_watermark(spark, t_orders)
    changed_orders = _changed(orders, since)
    ids = changed_orders.select(F.col("id").alias("_id")).unionByName(
        affected.select(F.col("order_id").alias("_id"))
    )
    retry = _unresolved_ids(spark, t_orders, "order_id")
    if retry is not None:
        ids = ids.unionByName(retry.select(F.col("_retry").alias("_id")))
    ids = ids.distinct().persist()
    order_rows = order_facts(
        orders.join(ids, orders["id"] == ids["_id"], "left_semi"),
        spark.table(t_items).join(ids, F.col("order_id") == ids["_id"], "left_semi"),
    )
    merge_facts(spark, t_orders, order_rows, "order_id")
    _set_watermark(spark, t_orders, changed_orders.agg(F.max("_merged_at")).first()[0])
    counts["fct_orders_recomputed"] = ids.count()

    # fct_sessions: sessions with new events, recomputed from recent partitions.
    t_sessions = f"{gold_db}.fct_sessions"
    since = fact_watermark(spark, t_sessions)
    new_events = _changed(events, since).persist()
    bounds = new_events.agg(F.min("created_at"), F.max("_merged_at")).first()
    if bounds[0] is not None:
        sessions = new_events.select("session_id").distinct()
        # A session spans minutes (events are back-dated): one hour of margin
        # keeps all its events while reading only recent day partitions.
        window = events.where(
            F.col("created_at") >= F.lit(bounds[0] - datetime.timedelta(hours=1))
        )
        merge_facts(
            spark,
            t_sessions,
            session_facts(window.join(sessions, "session_id", "left_semi")),
            "session_id",
        )
        _set_watermark(spark, t_sessions, bounds[1])
        counts["fct_sessions_recomputed"] = sessions.count()
    else:
        counts["fct_sessions_recomputed"] = 0

    affected.unpersist()
    ids.unpersist()
    new_events.unpersist()
    return counts
