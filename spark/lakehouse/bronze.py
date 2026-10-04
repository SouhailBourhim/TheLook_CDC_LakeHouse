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


def write_bronze(
    spark: SparkSession, rows: DataFrame, table: str, query_id: str, batch_id: int
) -> None:
    """Append rows, creating the table on its first batch (ADR 006: the
    writer creates and evolves bronze tables)."""
    marks = {
        f"snapshot-property.{QUERY_ID}": query_id,
        f"snapshot-property.{BATCH_ID}": str(batch_id),
    }
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
    step: Callable[[], bool],
    attempts: int = 5,
    first_delay: float = 5.0,
    sleep: Callable[[float], None] = time.sleep,
) -> bool:
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
