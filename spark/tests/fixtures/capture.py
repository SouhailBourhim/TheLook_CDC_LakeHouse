# /// script
# requires-python = ">=3.10"
# dependencies = ["confluent-kafka[avro,schemaregistry]==2.15.1"]
# ///
"""Capture real Debezium records (raw key/value bytes, base64) as test fixtures.

Dev helper, run once against the local stack: uv run spark/tests/fixtures/capture.py
Writes records.json next to this file: per topic, the first record found
for each wanted op (and one tombstone) among the last 5,000 records. The raw
bytes are kept as they are; the deserializer is only used to see each
record's op.
"""

import base64
import json
import time
from pathlib import Path

from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext

WANTED = {
    "thelook.shop.users": {"c", "u"},
    "thelook_mongo.web.reviews": {"c", "u", "d", "tombstone"},
}

deser = AvroDeserializer(SchemaRegistryClient({"url": "http://localhost:8081"}))
out = {}
for topic, wanted in WANTED.items():
    c = Consumer(
        {
            "bootstrap.servers": "localhost:9092",
            "group.id": f"capture-{time.time()}",
            "enable.auto.commit": False,
            "isolation.level": "read_committed",
        }
    )
    low, high = c.get_watermark_offsets(TopicPartition(topic, 0), timeout=10)
    found = {}
    start = max(low, high - 5000)
    c.assign([TopicPartition(topic, 0, start)])
    while len(found) < len(wanted):
        msgs = c.consume(500, timeout=2.0)
        if not msgs:
            break
        for m in msgs:
            if m.error():
                continue
            if m.value() is None:
                op = "tombstone"
            else:
                v = deser(m.value(), SerializationContext(topic, MessageField.VALUE))
                op = v["op"]
            if op in wanted and op not in found:
                found[op] = {
                    "op": op,
                    "topic": topic,
                    "partition": m.partition(),
                    "offset": m.offset(),
                    "timestamp_ms": m.timestamp()[1],
                    "key": base64.b64encode(m.key()).decode() if m.key() else None,
                    "value": base64.b64encode(m.value()).decode()
                    if m.value()
                    else None,
                }
    c.close()
    out[topic] = list(found.values())
    print(topic, sorted(found))
Path(__file__).with_name("records.json").write_text(json.dumps(out, indent=2) + "\n")
