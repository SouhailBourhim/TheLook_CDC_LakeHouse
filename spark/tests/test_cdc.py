"""Tests for lakehouse.cdc: Debezium records from Kafka -> bronze rows.

Real records captured from the local topics (fixtures/records.json, real
writer schemas in fixtures/*.avsc), plus two records encoded here with
fastavro from a real one: a PostgreSQL delete (only erasure scripts delete,
P8) and a record written with a newer schema version.
"""

import base64
import copy
import datetime
import io
import json
import struct

import fastavro
import pytest
from chispa import assert_df_equality
from pyspark.sql import functions as F

from lakehouse.cdc import (
    bad_frames,
    decode_by_schema,
    mongo_bronze,
    postgres_bronze,
    split_frames,
)

USERS = "thelook.shop.users"
REVIEWS = "thelook_mongo.web.reviews"
INGESTED_AT = datetime.datetime(2026, 10, 4, 12, 0)


def ingested():
    # A Column needs an active SparkSession, so it is built per call.
    return F.lit(INGESTED_AT).cast("timestamp")


# --- helpers -------------------------------------------------------------------


def unframe(record: dict) -> bytes:
    """The Avro payload of a captured record (after the 5-byte header)."""
    return base64.b64decode(record["value"])[5:]


def frame(schema_id: int, schema_json: str, datum: dict) -> str:
    """Encode datum in Confluent's wire format, base64 like the fixtures."""
    buf = io.BytesIO()
    fastavro.schemaless_writer(
        buf, fastavro.parse_schema(json.loads(schema_json)), datum
    )
    return base64.b64encode(
        b"\x00" + struct.pack(">I", schema_id) + buf.getvalue()
    ).decode()


def read(schema_json: str, payload: bytes) -> dict:
    return fastavro.schemaless_reader(
        io.BytesIO(payload), fastavro.parse_schema(json.loads(schema_json))
    )


def users_bronze(kafka_df, records, schema_for):
    return postgres_bronze(
        decode_by_schema(split_frames(kafka_df(records)), schema_for), ingested()
    )


def reviews_bronze(kafka_df, records, schema_for):
    framed = split_frames(kafka_df(records))
    decoded = decode_by_schema(framed, schema_for)
    decoded = decode_by_schema(
        decoded, schema_for, "key_schema_id", "key_payload", "key_event"
    )
    return mongo_bronze(decoded, ingested())


# --- framing -------------------------------------------------------------------


def test_split_frames_reads_schema_ids_and_drops_tombstones(kafka_df, captured):
    reviews = captured[REVIEWS]
    framed = split_frames(
        kafka_df([reviews[op] for op in ("c", "u", "d", "tombstone")])
    )
    rows = framed.select(
        "schema_id",
        "key_schema_id",
        F.length("payload").alias("payload_len"),
        F.length("value").alias("value_len"),
    ).collect()

    assert len(rows) == 3  # the tombstone (null value) is gone
    assert {r.schema_id for r in rows} == {21}
    assert {r.key_schema_id for r in rows} == {20}
    assert all(r.payload_len == r.value_len - 5 for r in rows)


def test_values_not_in_wire_format_go_to_bad_frames(kafka_df, captured):
    good = captured[USERS]["c"]
    broken = dict(good)
    value = bytearray(base64.b64decode(good["value"]))
    value[0] = 1  # not the magic byte
    broken["value"] = base64.b64encode(bytes(value)).decode()
    df = kafka_df([good, broken])

    assert split_frames(df).count() == 1
    assert bad_frames(df).count() == 1


# --- PostgreSQL ----------------------------------------------------------------


def test_postgres_insert_and_update(kafka_df, captured, schemas):
    users = captured[USERS]
    rows = {
        r.op: r
        for r in users_bronze(kafka_df, [users["c"], users["u"]], schemas.get).collect()
    }
    c, u = rows["c"], rows["u"]

    assert c.after.id and c.before is None
    # REPLICA IDENTITY DEFAULT: an update carries no before image, so silver
    # takes the key from after.
    assert u.before is None and u.after.id
    assert c.lsn < u.lsn
    assert c.source_ts == datetime.datetime.fromtimestamp(
        c.source_ts_ms / 1000, datetime.timezone.utc
    ).replace(tzinfo=None)
    # Kept as Debezium sent it: microseconds since epoch (ADR 012).
    assert isinstance(c.after.created_at, int) and c.after.created_at > 10**15
    assert (c.kafka_topic, c.kafka_offset, c.schema_id) == (
        USERS,
        users["c"]["offset"],
        3,
    )
    assert c.ingested_at == INGESTED_AT


def test_postgres_delete_carries_only_the_key_in_before(kafka_df, captured, schemas):
    real = captured[USERS]["u"]
    datum = read(schemas[3], unframe(real))
    user_id = datum["after"]["id"]
    datum["op"] = "d"
    datum["before"] = {name: None for name in datum["after"]} | {"id": user_id}
    datum["after"] = None
    delete = dict(real, value=frame(3, schemas[3], datum))

    (row,) = users_bronze(kafka_df, [delete], schemas.get).collect()

    assert row.op == "d"
    assert row.after is None
    assert row.before.id == user_id and row.before.email is None


def test_two_schema_versions_in_one_batch(kafka_df, captured, schemas):
    # Version 2 adds a nullable column, the change the pipeline must absorb.
    v2 = json.loads(schemas[3])
    value_record = next(f for f in v2["fields"] if f["name"] == "before")["type"][1]
    value_record["fields"].append(
        {"name": "loyalty_tier", "type": ["null", "string"], "default": None}
    )
    v2_json = json.dumps(v2)
    datum = read(schemas[3], unframe(captured[USERS]["c"]))
    datum = copy.deepcopy(datum)
    datum["after"]["loyalty_tier"] = "gold"
    newer = dict(captured[USERS]["c"], value=frame(900, v2_json, datum), offset=999_999)
    schema_for = {**schemas, 900: v2_json}.get

    rows = users_bronze(kafka_df, [captured[USERS]["u"], newer], schema_for).collect()
    by_id = {r.schema_id: r for r in rows}

    assert by_id[3].after.loyalty_tier is None  # older version: column absent
    assert by_id[900].after.loyalty_tier == "gold"


def test_empty_batch_decodes_to_no_rows(kafka_df, captured, schemas):
    tombstone_only = kafka_df([captured[REVIEWS]["tombstone"]])
    assert decode_by_schema(split_frames(tombstone_only), schemas.get).count() == 0


# --- MongoDB -------------------------------------------------------------------


def test_mongo_insert_update_delete(spark, kafka_df, captured, schemas):
    reviews = captured[REVIEWS]
    records = [reviews[op] for op in ("c", "u", "d", "tombstone")]
    bronze = reviews_bronze(kafka_df, records, schemas.get)

    after_ids = {
        r.op: json.loads(r.after)["_id"] if r.after else None
        for r in bronze.select("op", "after").collect()
    }
    expected = spark.createDataFrame(
        [(op, after_ids[op] or "", reviews[op]["offset"]) for op in ("c", "u")]
        + [("d", None, reviews["d"]["offset"])],
        "op string, after_id string, kafka_offset long",
    )
    # doc_id comes from the record key: the same as the document's _id for
    # an insert or update, and the only id a delete has.
    actual = bronze.select(
        "op",
        F.when(F.col("after").isNotNull(), F.col("doc_id")).alias("after_id"),
        "kafka_offset",
    )
    assert_df_equality(actual, expected, ignore_row_order=True, ignore_nullable=True)

    delete = bronze.where("op = 'd'").first()
    assert delete.after is None and delete.doc_id
    update = bronze.where("op = 'u'").first()
    assert json.loads(update.update_description.updatedFields)  # changed fields only
    assert update.ord is not None


@pytest.mark.parametrize("op", ["c", "u"])
def test_mongo_after_is_the_whole_document_as_json(kafka_df, captured, schemas, op):
    (row,) = reviews_bronze(kafka_df, [captured[REVIEWS][op]], schemas.get).collect()
    doc = json.loads(row.after)
    # Full document even for an update (capture.mode=change_streams_update_full).
    assert {"_id", "order_item_id", "rating", "created_at"} <= doc.keys()
    assert doc["created_at"].keys() == {"$date"}  # Extended JSON, legacy mode
