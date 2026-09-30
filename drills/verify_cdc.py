# /// script
# requires-python = ">=3.12"
# dependencies = [
#   "confluent-kafka[avro,schemaregistry]==2.15.1",
#   "psycopg[binary]==3.3.6",
# ]
# ///
"""Prove that the Kafka topics hold exactly the current state of Postgres.

For each captured table, read the whole topic (bounded to the end offsets at
start, read_committed), keep the latest version of each primary key (highest
source.lsn, the rule silver will use), drop keys whose latest event is a
delete, then compare with the table column by column.

Run it when the source is quiet (generator stopped) and Debezium has caught
up; otherwise the two sides are compared at different moments.

Usage: uv run drills/verify_cdc.py        (exit code 0 = identical)
"""
import datetime
import sys
import time
from pathlib import Path

import psycopg
from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext

TABLES = ["users", "orders", "order_items", "events", "products", "dist_centers"]
EPOCH = datetime.datetime(1970, 1, 1)


def load_env(path: Path) -> dict:
    env = {}
    for line in path.read_text().splitlines():
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            env[k] = v
    return env


def latest_from_topic(topic: str, deser: AvroDeserializer) -> dict:
    c = Consumer({"bootstrap.servers": "localhost:9092", "group.id": f"verify-{time.time()}",
                  "enable.auto.commit": False, "isolation.level": "read_committed"})
    end = c.get_watermark_offsets(TopicPartition(topic, 0), timeout=10)[1]
    c.assign([TopicPartition(topic, 0, 0)])
    latest = {}  # pk -> (lsn, after or None for a delete)
    while c.position([TopicPartition(topic, 0)])[0].offset < end:
        for m in c.consume(10000, timeout=1.0):
            if m.error() or m.offset() >= end or m.value() is None:
                continue  # tombstones carry no data; the preceding 'd' event does
            v = deser(m.value(), SerializationContext(topic, MessageField.VALUE))
            row = v["after"] if v["op"] != "d" else None
            pk = (v["after"] or v["before"])["id"]
            lsn = v["source"]["lsn"]
            if pk not in latest or lsn >= latest[pk][0]:
                latest[pk] = (lsn, row)
    c.close()
    return {pk: row for pk, (lsn, row) in latest.items() if row is not None}


def normalise(value, pg_type_is_timestamp: bool):
    # Debezium sends TIMESTAMP WITHOUT TIME ZONE as microseconds since epoch.
    if pg_type_is_timestamp and isinstance(value, int):
        return EPOCH + datetime.timedelta(microseconds=value)
    return value


def main() -> int:
    env = load_env(Path(__file__).resolve().parent.parent / "onprem" / ".env")
    deser = AvroDeserializer(SchemaRegistryClient({"url": "http://localhost:8081"}))
    ok = True
    with psycopg.connect(host="localhost", port=5432, dbname=env.get("POSTGRES_DB", "thelook"),
                         user=env.get("POSTGRES_USER", "postgres"),
                         password=env["POSTGRES_PASSWORD"]) as conn:
        for table in TABLES:
            kafka = latest_from_topic(f"thelook.shop.{table}", deser)
            with conn.cursor() as cur:
                cur.execute(f"SELECT * FROM shop.{table}")
                cols = [d.name for d in cur.description]
                ts_cols = {d.name for d in cur.description if d.type_code == 1114}
                pg = {r[cols.index("id")]: dict(zip(cols, r)) for r in cur.fetchall()}
            # products/dist_centers keys are BIGINT; Kafka gives ints too.
            missing = pg.keys() - kafka.keys()
            extra = kafka.keys() - pg.keys()
            differ = [pk for pk in pg.keys() & kafka.keys()
                      if any(normalise(kafka[pk][c], c in ts_cols) != pg[pk][c] for c in cols)]
            status = "OK" if not (missing or extra or differ) else "MISMATCH"
            ok &= status == "OK"
            print(f"{table:13} postgres={len(pg):>7} kafka_latest={len(kafka):>7} "
                  f"missing={len(missing)} extra={len(extra)} differ={len(differ)}  {status}")
            for pk in list(differ)[:3]:
                print("   example diff", pk, {c: (kafka[pk][c], pg[pk][c]) for c in cols
                                              if normalise(kafka[pk][c], c in ts_cols) != pg[pk][c]})
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
