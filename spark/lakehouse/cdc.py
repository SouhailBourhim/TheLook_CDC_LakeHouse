"""Debezium change events read from Kafka -> bronze rows (ADR 012).

Pure DataFrame-in, DataFrame-out functions, so they are unit-tested without
Kafka, Schema Registry or AWS. The streaming job (P3) chains them in each
micro-batch:

    split_frames -> decode_by_schema (value, and key for MongoDB)
                 -> postgres_bronze / mongo_bronze

Kafka gives Spark the raw bytes of each record. Debezium's Avro converter
writes them in Confluent's wire format:

    byte 0      magic byte, always 0
    bytes 1-4   schema id in Schema Registry (big-endian int)
    bytes 5..   the Avro payload, written with that schema

Spark's from_avro reads a raw payload with one schema given as JSON, so the
framing is split off first and each schema id is decoded with its own
writer schema (one topic can hold several schema versions).
"""

from collections.abc import Callable

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.avro.functions import from_avro


def _schema_id(binary: str) -> Column:
    # Bytes 2-5 (1-based) as hex, then base 16 -> 10: the big-endian int.
    return F.conv(F.hex(F.substring(binary, 2, 4)), 16, 10).cast("int")


def _payload(binary: str) -> Column:
    # Everything after the 5-byte header (SQL substring with no length).
    return F.expr(f"substring({binary}, 6)")


def _frame_ok(binary: str) -> Column:
    return (F.length(binary) >= 5) & (F.substring(binary, 1, 1) == F.unhex(F.lit("00")))


def split_frames(records: DataFrame) -> DataFrame:
    """Kafka records -> rows with schema_id/payload (and key_schema_id/
    key_payload). Tombstones (null value) are dropped: the delete event
    before each one carries the information (ADR 012). Rows whose value is
    not in the wire format are dropped here; bad_frames() returns them."""
    return (
        records.where(F.col("value").isNotNull() & _frame_ok("value"))
        .withColumn("schema_id", _schema_id("value"))
        .withColumn("payload", _payload("value"))
        .withColumn("key_schema_id", _schema_id("key"))
        .withColumn("key_payload", _payload("key"))
    )


def bad_frames(records: DataFrame) -> DataFrame:
    """Non-null values that are not in Confluent's wire format (for the
    dead-letter area, FR7)."""
    return records.where(F.col("value").isNotNull() & ~_frame_ok("value"))


def decode_by_schema(
    framed: DataFrame,
    schema_for: Callable[[int], str],
    id_col: str = "schema_id",
    payload_col: str = "payload",
    out_col: str = "event",
) -> DataFrame:
    """Decode each row's payload with the writer schema of its own id.

    schema_for(id) returns the Avro schema as JSON (from Schema Registry,
    cached by the caller). The distinct ids of a micro-batch are few (one
    per schema version in use), so they are collected to the driver; each
    group is decoded, and the groups are unioned by name. A newer version
    with an added nullable field leaves that field null in older rows.
    """
    ids = sorted(r[0] for r in framed.select(id_col).distinct().collect())
    if not ids:
        return framed.withColumn(out_col, F.lit(None))
    parts = [
        framed.where(F.col(id_col) == i).withColumn(
            out_col, from_avro(F.col(payload_col), schema_for(i), {"mode": "FAILFAST"})
        )
        for i in ids
    ]
    decoded = parts[0]
    for part in parts[1:]:
        decoded = decoded.unionByName(part, allowMissingColumns=True)
    return decoded


def _common(ingested_at: Column) -> list[Column]:
    e = F.col("event")
    return [
        e["op"].alias("op"),
        e["source"]["ts_ms"].alias("source_ts_ms"),
        F.timestamp_millis(e["source"]["ts_ms"]).alias("source_ts"),
        e["ts_ms"].alias("ts_ms"),
        e["source"]["snapshot"].alias("snapshot"),
        F.col("topic").alias("kafka_topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
        F.col("schema_id"),
        ingested_at.alias("ingested_at"),
    ]


def postgres_bronze(decoded: DataFrame, ingested_at: Column) -> DataFrame:
    """Decoded PostgreSQL events -> bronze columns. before/after keep the
    table's columns as Debezium sent them (timestamps in microseconds)."""
    e = F.col("event")
    return decoded.select(
        *_common(ingested_at),
        e["source"]["lsn"].alias("lsn"),
        e["source"]["txId"].alias("tx_id"),
        e["before"].alias("before"),
        e["after"].alias("after"),
    )


def mongo_bronze(decoded: DataFrame, ingested_at: Column) -> DataFrame:
    """Decoded MongoDB events (value in "event", key in "key_event") ->
    bronze columns. after stays a JSON string (schemaless documents).

    doc_id comes from the record key, the only place a delete carries it.
    The key's id is the _id in Extended JSON: a string id arrives quoted
    ("\\"abc\\""); wrapping it as {"v": <id>} and reading $.v returns the
    plain string, and an ObjectId ({"$oid": ...}) would come back as its
    JSON text.
    """
    e = F.col("event")
    key_json = F.col("key_event")["id"]
    doc_id = F.get_json_object(F.concat(F.lit('{"v":'), key_json, F.lit("}")), "$.v")
    return decoded.select(
        *_common(ingested_at),
        doc_id.alias("doc_id"),
        e["source"]["ord"].alias("ord"),
        e["after"].alias("after"),
        e["updateDescription"].alias("update_description"),
    )
