"""Streaming job: every CDC topic -> bronze Iceberg tables (ADR 007).

Reads the 7 CDC topics (5 PostgreSQL tables, 2 MongoDB collections) as one
stream and, in each micro-batch, appends each topic's records to its bronze
table with lakehouse.bronze. Runs as the bronze writer IAM user
(thelook-spark-stream).

The same stream reads the GDPR erasure requests (plain JSON, ADR 021) and
applies them to its own tables with lakehouse.erasure, after the batch's
appends and before its ledger row.

Settings, overridable by environment variable:
  BRONZE_TRIGGER          micro-batch interval (default "60 seconds"):
                          freshness vs one Iceberg commit (and files) per
                          table per batch
  BRONZE_MAX_OFFSETS      records per micro-batch at most (default 200000),
                          so a backlog is read in bounded batches
  BRONZE_CHECKPOINT       checkpoint folder (default /opt/checkpoints/bronze)
"""

import datetime
import json
import logging
import os
import threading
import time
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from lakehouse.bronze import (
    LEDGER,
    TOPICS,
    SchemaRegistry,
    append_once,
    bronze_rows,
    heavy_upkeep,
    maintain_bronze,
    record_batch,
    table_for,
    with_retries,
)
from lakehouse.cdc import bad_frames
from lakehouse.erasure import (
    ERASURE_LOG,
    ERASURE_TOPIC,
    apply_requests,
    parse_request,
)
from lakehouse.session import lake_session
from lakehouse.stream_metrics import serve as serve_metrics
from lakehouse.upkeep import compact

logging.basicConfig(
    level=logging.INFO, format="[%(asctime)s] %(levelname)s: %(message)s"
)
log = logging.getLogger("bronze-stream")

TRIGGER = os.environ.get("BRONZE_TRIGGER", "60 seconds")
MAX_OFFSETS = os.environ.get("BRONZE_MAX_OFFSETS", "200000")
CHECKPOINT = os.environ.get("BRONZE_CHECKPOINT", "/opt/checkpoints/bronze")

# 2 of the worker's 4 cores: the stream runs forever, the rest is for the
# batch jobs (P4). FAIR scheduling: the maintenance thread's jobs (own pool)
# share the cores with the micro-batches instead of queueing ahead of them.
spark = lake_session(
    "bronze-stream", cores_max=2, conf={"spark.scheduler.mode": "FAIR"}
)
registry = SchemaRegistry("http://schema-registry:8081")
# Progress for Prometheus (monitoring profile): batch durations, time since
# the last batch, restarts (lakehouse.stream_metrics).
serve_metrics(spark, port=9108)


# Bronze upkeep (ADR 017): at the first batch after a start, then hourly,
# in a background thread so that micro-batches keep running meanwhile
# (compacting a table's day of small files takes minutes; O1 is 5 minutes).
# Concurrent commits on one table are safe: Iceberg retries the one that
# loses the race, and a compaction never changes which rows a table holds.
MAINTAIN_EVERY = 3600  # seconds
last_maintained = None
maintenance_running = threading.Lock()


def maybe_maintain() -> None:
    """Start the upkeep thread if an hour has passed and none is running."""
    global last_maintained
    due = (
        last_maintained is None or time.monotonic() - last_maintained >= MAINTAIN_EVERY
    )
    if due and not maintenance_running.locked():
        last_maintained = time.monotonic()
        threading.Thread(
            target=maintain, name="bronze-maintenance", daemon=True
        ).start()


def maintain() -> None:
    """Expire bronze snapshots, compact the ledger, and do at most one heavier
    task on one table (compaction or orphan removal). Best effort: a failure
    is logged and retried an hour later, never allowed to stop the stream."""
    start = time.monotonic()
    with maintenance_running:
        # Jobs started from this thread go to the "maintenance" pool.
        spark.sparkContext.setLocalProperty("spark.scheduler.pool", "maintenance")
        try:
            data_tables = [table_for(t) for t in TOPICS]
            now = datetime.datetime.now(datetime.timezone.utc)
            expired = maintain_bronze(spark, data_tables + [LEDGER, ERASURE_LOG], now)
            compacted = compact(spark, LEDGER)
            heavy = heavy_upkeep(spark, data_tables, now)
            log.info(
                "maintenance: expired snapshots %s, ledger files compacted %s, "
                "%s, in %.1f s",
                expired,
                compacted,
                heavy or "no heavy task due",
                time.monotonic() - start,
            )
        except Exception:
            log.exception("maintenance failed; next attempt in an hour")


def query_id() -> str:
    # Written by Spark into the checkpoint before the first batch; stays the
    # same across restarts that reuse the checkpoint.
    return json.loads((Path(CHECKPOINT) / "metadata").read_text())["id"]


def erase(batch: DataFrame, batch_id: int, qid: str) -> str:
    """Apply the batch's erasure requests to bronze; returns a log summary.

    Waits for the upkeep thread to finish first: a compaction committing
    while the DELETE rewrites the same files would make one of the two
    fail. Requests are rare, so a batch waiting minutes for one is fine.
    """
    records = batch.where(F.col("topic") == ERASURE_TOPIC).select("offset", "value")
    requests = []
    for r in records.collect():
        request = parse_request(r.value)
        if request is None:
            # The offset, not the content: it may hold a user id.
            log.warning(
                "batch %s: erasure request at offset %s refused", batch_id, r.offset
            )
        else:
            requests.append(request)
    if not requests:
        return "erasure=none valid"
    with maintenance_running:
        # Each step retried on its own (lakehouse.erasure).
        expired = apply_requests(spark, requests, qid, batch_id, retry=with_retries)
    if expired is None:
        return "erasure=skipped(replay)"
    return f"erasure={len(requests)} requests, snapshots expired {expired}"


def process_batch(batch: DataFrame, batch_id: int) -> None:
    start = time.monotonic()
    # Each topic's records are filtered out of the same batch: cache it so
    # Kafka is read once per batch, not once per table.
    batch.persist()
    try:
        per_topic = {
            r.topic: r["count"] for r in batch.groupBy("topic").count().collect()
        }
        # Erasure requests are plain JSON, not Confluent frames: not bad ones.
        bad = bad_frames(batch.where(F.col("topic") != ERASURE_TOPIC)).count()
        if bad:
            # Dead-letter area and alert come with the contracts (FR7, P7).
            log.warning(
                "batch %s: %s records not in Confluent wire format", batch_id, bad
            )
        qid = query_id()
        done = []
        for topic in sorted(per_topic.keys() - {ERASURE_TOPIC}):
            table = table_for(topic)
            rows = bronze_rows(batch, topic, registry, F.current_timestamp())
            if rows is None:  # e.g. only tombstones
                continue
            # Transient S3/Glue errors are retried inside the batch; the
            # replay check runs again on each attempt (lakehouse.bronze).
            written = with_retries(
                lambda rows=rows, table=table: append_once(
                    spark, rows, table, qid, batch_id
                )
            )
            name = table.rsplit(".", 1)[1]
            done.append(
                f"{name}={per_topic[topic]}" if written else f"{name}=skipped(replay)"
            )
        # After the appends, so that a change that reached bronze in this
        # batch is purged too; before the ledger row, so that the ledger
        # never points at a snapshot the purge expired (ADR 021).
        if ERASURE_TOPIC in per_topic:
            done.append(erase(batch, batch_id, qid))
        # Last, and only once every table above has committed: the ledger
        # row that tells silver this batch is complete (ADR 012, ADR 014).
        with_retries(
            lambda: record_batch(spark, qid, batch_id, [table_for(t) for t in TOPICS])
        )
        log.info(
            "batch %s: %s in %.1f s",
            batch_id,
            ", ".join(done) or "empty",
            time.monotonic() - start,
        )
        maybe_maintain()
    finally:
        batch.unpersist()


stream = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:29092")
    # Adding a topic to a checkpointed query: its partitions are new to the
    # checkpoint, so Spark reads them from the earliest offset (ADR 021).
    .option("subscribe", ",".join(TOPICS + [ERASURE_TOPIC]))
    # First start: everything still in Kafka. Later starts resume from the
    # checkpoint and ignore this.
    .option("startingOffsets", "earliest")
    # Debezium writes in transactions (exactly-once source): read only
    # committed records; the default would also return aborted ones.
    .option("kafka.isolation.level", "read_committed")
    .option("maxOffsetsPerTrigger", MAX_OFFSETS)
    # If retention deleted records the job had not read yet (it was down too
    # long), stop instead of silently skipping them.
    .option("failOnDataLoss", "true")
    .load()
)

query = (
    stream.writeStream.foreachBatch(process_batch)
    .option("checkpointLocation", CHECKPOINT)
    .trigger(processingTime=TRIGGER)
    .queryName("bronze")
    .start()
)
log.info("bronze stream started: query id %s, trigger %s", query.id, TRIGGER)
query.awaitTermination()
