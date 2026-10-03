# /// script
# requires-python = ">=3.12"
# dependencies = [
#   "confluent-kafka[avro,schemaregistry]==2.15.1",
#   "psycopg[binary]==3.3.6",
#   "pymongo==4.18.2",
# ]
# ///
"""Prove that the Kafka topics hold exactly the current state of the sources.

For each captured PostgreSQL table and MongoDB collection, read the whole
topic (bounded to the end offsets at start, read_committed), keep the latest
version of each key, drop keys whose latest event is a delete, then compare
with the source.

- PostgreSQL: latest = highest source.lsn (the rule silver uses); compared
  column by column. Topics keep 3 days, so once a topic's first records are
  deleted, rows that have not changed since then have no event left in
  Kafka: they are reported as "outside retention", not as failures. Every
  key still in Kafka must match (no differ, no extra). Kafka is not the
  system of record; bronze is.
- MongoDB: latest = last record in the topic (one partition, keyed by _id,
  so Kafka order is change-stream order). Documents are compared through a
  hash of their canonical JSON, because holding 1.8 M events twice in memory
  would take gigabytes. The script also reports how often "highest
  (source.ts_ms, source.ord)" picks a different version than the Kafka
  order: evidence for open question E1 (silver's ordering key).

Run it when the sources are quiet (generator and review simulator stopped)
and Debezium has caught up; otherwise the sides are compared at different
moments.

Usage: uv run drills/verify_cdc.py [source ...]   (exit code 0 = identical)
       e.g. uv run drills/verify_cdc.py reviews orders   (only those)
"""

import datetime
import hashlib
import json
import sys
import time
from pathlib import Path
from urllib.parse import quote_plus

import psycopg
from bson import json_util
from confluent_kafka import OFFSET_BEGINNING, Consumer, TopicPartition
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext
from pymongo import MongoClient

# events moved to MongoDB in spec v2.0 (ADR 008).
TABLES = ["users", "orders", "order_items", "products", "dist_centers"]
COLLECTIONS = ["events", "reviews"]
EPOCH = datetime.datetime(1970, 1, 1)


def load_env(path: Path) -> dict:
    env = {}
    for line in path.read_text().splitlines():
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            env[k] = v
    return env


def low_watermark(topic: str) -> int:
    """First offset still on the broker (0 until retention deletes a segment)."""
    c = Consumer({"bootstrap.servers": "localhost:9092", "group.id": "verify-low"})
    low = c.get_watermark_offsets(TopicPartition(topic, 0), timeout=10)[0]
    c.close()
    return low


def read_topic(topic: str):
    """Yield every record of a one-partition topic still on the broker, up to
    its end offset at start (a live topic never goes quiet, so "read until
    idle" never ends)."""
    c = Consumer(
        {
            "bootstrap.servers": "localhost:9092",
            "group.id": f"verify-{time.time()}",
            "enable.auto.commit": False,
            "isolation.level": "read_committed",
        }
    )
    end = c.get_watermark_offsets(TopicPartition(topic, 0), timeout=10)[1]
    # OFFSET_BEGINNING = the first offset that still exists. Offset 0 would
    # be out of range once retention deleted it, and the consumer would then
    # jump to the end and read nothing.
    c.assign([TopicPartition(topic, 0, OFFSET_BEGINNING)])
    while c.position([TopicPartition(topic, 0)])[0].offset < end:
        for m in c.consume(10000, timeout=1.0):
            if not m.error() and m.offset() < end:
                yield m
    c.close()


# --- PostgreSQL ----------------------------------------------------------------


def latest_from_pg_topic(topic: str, deser: AvroDeserializer) -> dict:
    latest = {}  # pk -> (lsn, after or None for a delete)
    for m in read_topic(topic):
        if m.value() is None:
            continue  # tombstones carry no data; the preceding 'd' event does
        v = deser(m.value(), SerializationContext(topic, MessageField.VALUE))
        row = v["after"] if v["op"] != "d" else None
        pk = (v["after"] or v["before"])["id"]
        lsn = v["source"]["lsn"]
        if pk not in latest or lsn >= latest[pk][0]:
            latest[pk] = (lsn, row)
    return {pk: row for pk, (lsn, row) in latest.items() if row is not None}


def normalise(value, pg_type_is_timestamp: bool):
    # Debezium sends TIMESTAMP WITHOUT TIME ZONE as microseconds since epoch.
    if pg_type_is_timestamp and isinstance(value, int):
        return EPOCH + datetime.timedelta(microseconds=value)
    return value


def verify_postgres(env: dict, deser: AvroDeserializer) -> bool:
    ok = True
    with psycopg.connect(
        host="localhost",
        port=5432,
        dbname=env.get("POSTGRES_DB", "thelook"),
        user=env.get("POSTGRES_USER", "postgres"),
        password=env["POSTGRES_PASSWORD"],
    ) as conn:
        for table in TABLES:
            topic = f"thelook.shop.{table}"
            complete = low_watermark(topic) == 0
            kafka = latest_from_pg_topic(topic, deser)
            with conn.cursor() as cur:
                cur.execute(f"SELECT * FROM shop.{table}")
                cols = [d.name for d in cur.description]
                ts_cols = {d.name for d in cur.description if d.type_code == 1114}
                pg = {
                    r[cols.index("id")]: dict(zip(cols, r, strict=True))
                    for r in cur.fetchall()
                }
            # products/dist_centers keys are BIGINT; Kafka gives ints too.
            missing = pg.keys() - kafka.keys()
            extra = kafka.keys() - pg.keys()
            differ = [
                pk
                for pk in pg.keys() & kafka.keys()
                if any(normalise(kafka[pk][c], c in ts_cols) != pg[pk][c] for c in cols)
            ]
            # Missing rows only count as lost while the topic is complete.
            lost = missing if complete else set()
            status = "OK" if not (lost or extra or differ) else "MISMATCH"
            ok &= status == "OK"
            scope = "" if complete else f"  ({len(missing)} rows outside retention)"
            print(
                f"pg    {table:13} source={len(pg):>8} kafka_latest={len(kafka):>8} "
                f"missing={len(lost)} extra={len(extra)} differ={len(differ)}  "
                f"{status}{scope}"
            )
            for pk in list(differ)[:3]:
                print(
                    "   example diff",
                    pk,
                    {
                        c: (kafka[pk][c], pg[pk][c])
                        for c in cols
                        if normalise(kafka[pk][c], c in ts_cols) != pg[pk][c]
                    },
                )
    return ok


# --- MongoDB -------------------------------------------------------------------


def digest(doc: dict) -> bytes:
    """Hash of a document's canonical JSON: sorted keys, datetimes in ISO
    form. Equal documents give equal digests whatever their field order."""
    canonical = json.dumps(
        doc,
        sort_keys=True,
        separators=(",", ":"),
        default=lambda v: v.isoformat() if isinstance(v, datetime.datetime) else str(v),
    )
    return hashlib.blake2b(canonical.encode(), digest_size=16).digest()


def latest_from_mongo_topic(topic: str, key_deser, val_deser) -> tuple[dict, int]:
    """Latest digest per _id (None after a delete), by Kafka order, plus the
    number of documents whose latest version by (source.ts_ms, source.ord)
    is a different record."""
    by_offset = {}  # _id -> digest or None
    by_position = {}  # _id -> ((ts_ms, ord), digest or None)
    for m in read_topic(topic):
        if m.value() is None:
            continue  # tombstone after a delete
        # A delete has after = before = null (no pre-images): the document
        # id is only in the record key, as a JSON string ("\"<uuid>\"").
        key = key_deser(m.key(), SerializationContext(topic, MessageField.KEY))
        doc_id = json.loads(key["id"])
        v = val_deser(m.value(), SerializationContext(topic, MessageField.VALUE))
        state = None if v["op"] == "d" else digest(json_util.loads(v["after"]))
        by_offset[doc_id] = state
        pos = (v["source"]["ts_ms"], v["source"]["ord"])
        if doc_id not in by_position or pos >= by_position[doc_id][0]:
            by_position[doc_id] = (pos, state)
    disagree = sum(1 for k, (_, s) in by_position.items() if by_offset[k] != s)
    return {k: s for k, s in by_offset.items() if s is not None}, disagree


def verify_mongo(env: dict, key_deser, val_deser) -> bool:
    ok = True
    # Root user: the check must read both collections; directConnection
    # because "mongo" (the replica set member name) does not resolve here.
    client = MongoClient(
        f"mongodb://{env.get('MONGO_ROOT_USER', 'root')}:"
        f"{quote_plus(env['MONGO_ROOT_PASSWORD'])}@localhost:27017/"
        "?directConnection=true&authSource=admin",
        tz_aware=False,
    )
    for name in COLLECTIONS:
        topic = f"thelook_mongo.web.{name}"
        if low_watermark(topic) != 0:
            # Not expected before day 3 of the topic; treat as a failure so
            # it is noticed rather than silently half-checked.
            print(f"mongo {name:13} topic no longer complete (retention)  MISMATCH")
            ok = False
            continue
        kafka, disagree = latest_from_mongo_topic(topic, key_deser, val_deser)
        missing = differ = seen = 0
        examples = []
        for doc in client["web"][name].find():
            seen += 1
            kafka_digest = kafka.pop(doc["_id"], None)
            if kafka_digest is None:
                missing += 1
            elif kafka_digest != digest(doc):
                differ += 1
            if (kafka_digest is None or kafka_digest != digest(doc)) and len(
                examples
            ) < 3:
                examples.append(doc["_id"])
        extra = len(kafka)  # left in Kafka, absent from MongoDB
        status = "OK" if not (missing or extra or differ) else "MISMATCH"
        ok &= status == "OK"
        print(
            f"mongo {name:13} source={seen:>8} "
            f"missing={missing} extra={extra} differ={differ}  {status}   "
            f"[E1: ts_ms/ord order disagrees with Kafka order for {disagree} ids]"
        )
        for doc_id in examples:
            print("   example", doc_id)
    return ok


def main() -> int:
    # Optional names restrict the check (the full run reads 1.8 M events).
    only = set(sys.argv[1:])
    unknown = only - set(TABLES) - set(COLLECTIONS)
    if unknown:
        sys.exit(f"unknown source(s): {', '.join(sorted(unknown))}")
    if only:
        TABLES[:] = [t for t in TABLES if t in only]
        COLLECTIONS[:] = [c for c in COLLECTIONS if c in only]
    env = load_env(Path(__file__).resolve().parent.parent / "onprem" / ".env")
    registry = SchemaRegistryClient({"url": "http://localhost:8081"})
    deser = AvroDeserializer(registry)
    start = time.monotonic()
    ok = verify_postgres(env, deser)
    ok &= verify_mongo(env, AvroDeserializer(registry), deser)
    print(f"{'ALL OK' if ok else 'MISMATCH'} in {time.monotonic() - start:.0f} s")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
