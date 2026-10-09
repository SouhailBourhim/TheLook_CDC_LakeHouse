"""Streaming job: clickstream events -> online user features in Redis
(ADR 018, spec FR15).

A separate Spark application from the bronze stream, in local mode in its
own container (features-stream): it reads only the events topic, needs no
AWS identity, and cannot slow or stop bronze. In each micro-batch:

    bronze_rows (Avro decoding, as for bronze) -> feature_events
        -> redis_rows -> foreachPartition(redis_writer)

No replay guard: every Redis write is idempotent and order-independent, so
a replayed batch leaves the same state (at-least-once in, exactly-once
result).

Settings, overridable by environment variable:
  FEATURES_TRIGGER      micro-batch interval (default "10 seconds"): Redis
                        has no small-file cost, so short; leaves most of
                        the 1-minute target for capture and processing
  FEATURES_MAX_OFFSETS  records per micro-batch at most (default 50000),
                        so a catch-up is read in bounded batches
  FEATURES_CHECKPOINT   checkpoint folder (default /opt/checkpoints/features)
"""

import logging
import os
import time

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from lakehouse.bronze import SchemaRegistry, bronze_rows
from lakehouse.features import feature_events, redis_rows
from lakehouse.redis_writer import partition_writer

logging.basicConfig(
    level=logging.INFO, format="[%(asctime)s] %(levelname)s: %(message)s"
)
log = logging.getLogger("features-stream")

TOPIC = "thelook_mongo.web.events"
TRIGGER = os.environ.get("FEATURES_TRIGGER", "10 seconds")
MAX_OFFSETS = os.environ.get("FEATURES_MAX_OFFSETS", "50000")
CHECKPOINT = os.environ.get("FEATURES_CHECKPOINT", "/opt/checkpoints/features")

spark = (
    SparkSession.builder.appName("features-stream")
    # A batch is a few hundred events: the default 200 shuffle partitions
    # would mean 200 tiny tasks per shuffle, every 10 seconds.
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)
registry = SchemaRegistry("http://schema-registry:8081")


def process_batch(batch: DataFrame, batch_id: int) -> None:
    start = time.monotonic()
    # One "now" per batch, from the driver: it only filters out what Redis
    # would delete at once, it is never written (lakehouse.features).
    now_ms = int(time.time() * 1000)
    # Read Kafka once: the decoding collects the batch's schema ids first.
    batch.persist()
    try:
        records = batch.count()
        bronze = bronze_rows(batch, TOPIC, registry, F.current_timestamp())
        if bronze is None:
            # Nothing decodable, e.g. offsets lost to retention were skipped
            # (failOnDataLoss=false) and the batch came back empty.
            log.info("batch %s: %s records, nothing to decode", batch_id, records)
            return
        events = feature_events(bronze)
        rows = redis_rows(events, now_ms).persist()
        writes = rows.count()
        rows.foreachPartition(partition_writer(now_ms))
        rows.unpersist()
        log.info(
            "batch %s: %s records, %s writes in %.1f s",
            batch_id,
            records,
            writes,
            time.monotonic() - start,
        )
    finally:
        batch.unpersist()


stream = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:29092")
    .option("subscribe", TOPIC)
    # First start (or a rebuild, with a new checkpoint): everything still in
    # Kafka, i.e. the last 72 hours, as long as the longest TTL.
    .option("startingOffsets", "earliest")
    # Debezium writes in transactions: committed records only.
    .option("kafka.isolation.level", "read_committed")
    .option("maxOffsetsPerTrigger", MAX_OFFSETS)
    # The opposite of bronze: records deleted by retention before this job
    # read them are older than 72 hours, so every feature they would have
    # made has expired. Spark logs the offsets it skips (alert: P7).
    .option("failOnDataLoss", "false")
    .load()
)

query = (
    stream.writeStream.foreachBatch(process_batch)
    .option("checkpointLocation", CHECKPOINT)
    .trigger(processingTime=TRIGGER)
    .queryName("features")
    .start()
)
log.info("features stream started: query id %s, trigger %s", query.id, TRIGGER)
query.awaitTermination()
