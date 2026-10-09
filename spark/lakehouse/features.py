"""Clickstream change events -> the Redis writes of the online user features
(ADR 018, spec FR15).

Pure DataFrame-in, DataFrame-out functions, unit-tested without Kafka or
Redis. The features stream (jobs/features_stream.py) chains them in each
micro-batch:

    lakehouse.bronze.bronze_rows (decoding, as for bronze)
        -> feature_events -> redis_rows -> lakehouse.redis_writer (P5 step 4)

redis_rows returns one row per Redis write, in four key families (all keys
start with user:{id}:, so erasure finds them with one pattern, P8):

    viewed   sorted set  product id -> time of its last view      keep 10
    events   sorted set  event _id  -> event time                 last hour
    session  sorted set  session id -> time of its last event     keep 1
    cart     hash        item:<event _id> -> price, purchased -> 1

Every value is a maximum or a set union, so the writer can apply the rows
of any batch, replayed or out of order, and reach the same state. Times are
epoch milliseconds of the event (created_at), never of processing, and each
key expires at its newest event time plus the family's TTL: a rebuild from
Kafka gives the same keys and the same TTLs.
"""

from dataclasses import dataclass

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

from lakehouse.silver import EVENTS_JSON

HOUR_MS = 3_600_000


@dataclass(frozen=True)
class Family:
    kind: str  # "zset" or "hash"
    ttl_ms: int  # the key expires this long after its newest event
    keep: int | None = None  # zset: keep only the `keep` highest scores
    window_ms: int | None = None  # zset: drop members older than now - window


# 72 h = Kafka retention (log.retention.hours): whatever Redis holds can
# still be rebuilt from the topic. events: the feature is "the last hour".
FAMILIES = {
    "viewed": Family("zset", 72 * HOUR_MS, keep=10),
    "events": Family("zset", HOUR_MS, window_ms=HOUR_MS),
    "session": Family("zset", 72 * HOUR_MS, keep=1),
    "cart": Family("hash", 72 * HOUR_MS),
}

PRODUCT_URI = r"^/product/(\d+)$"


def feature_events(bronze: DataFrame) -> DataFrame:
    """Bronze-shaped rows of web.events (op, after as JSON) -> one typed row
    per event of a known user.

    Kept: inserts (`c`) and snapshot reads (`r`). Events are never updated,
    and their deletes come only from erasure, which deletes the user's keys
    itself (P8). Dropped: ghost sessions (no user_id), and documents that do
    not parse (silver sends those to the dead-letter area, P7).
    """
    doc = F.from_json("after", EVENTS_JSON)
    parsed = bronze.where(F.col("op").isin("c", "r")).select(doc.alias("d"))
    d = F.col("d")
    # A product view names its product only in the URI; a cart event
    # carries it (synthetic, spec 4.3).
    product_id = (
        F.when(
            d["event_type"] == "product",
            F.regexp_extract(d["uri"], PRODUCT_URI, 1).try_cast("long"),
        )
        .when(d["event_type"] == "cart", d["product_id"])
        .otherwise(F.lit(None).cast("long"))
    )
    return parsed.where(d["_id"].isNotNull() & d["user_id"].isNotNull()).select(
        d["user_id"].alias("user_id"),
        d["_id"].alias("event_id"),
        d["session_id"].alias("session_id"),
        d["event_type"].alias("event_type"),
        d["created_at"]["$date"].alias("event_ms"),
        # A URI that does not match gives "": try_cast makes it null. Spark 4
        # (ANSI mode) fails a plain cast, and one such event would fail every
        # replay of its micro-batch: the stream would never get past it.
        product_id.alias("product_id"),
        # Float artifacts from products.csv (34.9900016784668) -> "34.99".
        F.round(d["price"], 2).cast("decimal(10,2)").cast("string").alias("price"),
    )


def _key(family: str, *parts: Column) -> Column:
    """user:{id}:<family>[:<part>...]"""
    pieces = [F.lit("user:"), F.col("user_id"), F.lit(f":{family}")]
    for part in parts:
        pieces += [F.lit(":"), part]
    return F.concat(*pieces)


def _newest(rows: DataFrame, member: str, n: int) -> DataFrame:
    """The n rows with the highest ms per user, ties broken as Redis does.

    A sorted set orders equal scores by member, compared as bytes, and
    ZREMRANGEBYRANK removes from the lowest rank. Pre-trimming in the batch
    with the same order keeps exactly what Redis's trim would keep.
    """
    newest_first = Window.partitionBy("user_id").orderBy(
        F.col("ms").desc(), F.col(member).cast("string").cast("binary").desc()
    )
    rank = F.row_number().over(newest_first)
    return rows.withColumn("_rank", rank).where(F.col("_rank") <= n).drop("_rank")


def redis_rows(events: DataFrame, now_ms: int) -> DataFrame:
    """feature_events rows -> one row per Redis write:
    family, key, member, score (zset), value (hash), expire_at_ms.

    Aggregated per key first (the newest view per product, the newest
    session per user...), so a backlog batch sends a bounded number of
    writes. Every row of a key carries the key's expire_at_ms; keys that
    would already be expired at now_ms are dropped, and so are events-family
    members older than the window: a catch-up writes nothing that Redis
    would delete straight away. now_ms only filters; it never changes a
    value that is written.
    """
    views = events.where(
        (F.col("event_type") == "product") & F.col("product_id").isNotNull()
    )
    viewed = _newest(
        views.groupBy("user_id", "product_id").agg(F.max("event_ms").alias("ms")),
        "product_id",
        FAMILIES["viewed"].keep,
    ).select(
        F.lit("viewed").alias("family"),
        _key("viewed").alias("key"),
        F.col("product_id").cast("string").alias("member"),
        F.col("ms").cast("double").alias("score"),
        F.lit(None).cast("string").alias("value"),
    )

    recent = events.where(F.col("event_ms") >= now_ms - FAMILIES["events"].window_ms)
    in_window = recent.select(
        F.lit("events").alias("family"),
        _key("events").alias("key"),
        F.col("event_id").alias("member"),
        F.col("event_ms").cast("double").alias("score"),
        F.lit(None).cast("string").alias("value"),
    )

    sessions = events.groupBy("user_id", "session_id").agg(
        F.max("event_ms").alias("ms"),
        F.max(F.col("event_type") == "purchase").alias("purchased"),
    )
    session = _newest(sessions, "session_id", FAMILIES["session"].keep).select(
        F.lit("session").alias("family"),
        _key("session").alias("key"),
        F.col("session_id").alias("member"),
        F.col("ms").cast("double").alias("score"),
        F.lit(None).cast("string").alias("value"),
    )

    # Cart keys expire with their session's newest event (any type), so a
    # cart and the session that points to it live equally long.
    items = (
        events.where((F.col("event_type") == "cart") & F.col("price").isNotNull())
        .join(sessions.select("user_id", "session_id", "ms"), ["user_id", "session_id"])
        .select(
            "user_id",
            "session_id",
            "ms",
            F.concat(F.lit("item:"), F.col("event_id")).alias("member"),
            F.col("price").alias("value"),
        )
    )
    purchased = sessions.where("purchased").select(
        "user_id",
        "session_id",
        "ms",
        F.lit("purchased").alias("member"),
        F.lit("1").alias("value"),
    )
    cart = items.unionByName(purchased).select(
        F.lit("cart").alias("family"),
        _key("cart", F.col("session_id")).alias("key"),
        "member",
        F.lit(None).cast("double").alias("score"),
        "value",
        # Kept for the expiry below; dropped after.
        F.col("ms").alias("_ms"),
    )

    zsets = viewed.unionByName(in_window).unionByName(session)
    zsets = zsets.select(*zsets.columns, F.col("score").cast("long").alias("_ms"))
    rows = zsets.unionByName(cart)

    ttl = F.create_map(
        *[x for name, f in FAMILIES.items() for x in (F.lit(name), F.lit(f.ttl_ms))]
    )
    expire_at = F.max("_ms").over(Window.partitionBy("key")) + ttl[F.col("family")]
    return (
        rows.withColumn("expire_at_ms", expire_at)
        .where(F.col("expire_at_ms") > now_ms)
        .drop("_ms")
    )
