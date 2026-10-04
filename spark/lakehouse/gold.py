"""Silver (and bronze history) -> gold star schema (ADR 015).

Dimensions are small and rebuilt on every run:
  dim_date, dim_product, dim_distribution_center (from silver), and
  dim_user, a type 2 slowly changing dimension built from the full
  bronze history of shop_users (one row per version of a user).
"""

import datetime

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

# The open end of the current version: a sentinel instead of NULL, so a
# point-in-time join is just `ts >= valid_from AND ts < valid_to`.
OPEN_END = datetime.datetime(9999, 12, 31)

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
    return versions.select(
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
