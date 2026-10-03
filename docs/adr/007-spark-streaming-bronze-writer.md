# 007. Spark Structured Streaming writes bronze

- Status: Accepted
- Date: 2026-10-03 (decision D1, spec v2.0)
- Deciders: Souhail Bourhim (approves), Claude Code (drafts)

## Context

Spec v1.9 planned the Apache Iceberg Kafka Connect sink to write the bronze
tables. It was never built: at the start of the extension, only the S3
bucket, Glue databases and Athena workgroup exist. The extension adds
PySpark (a skill the project must show) and a Redis feature store fed from
the same CDC stream (ADR 010), which a Connect sink cannot do.

## Options considered

| Option | Assessment |
|---|---|
| A. Spark Structured Streaming replaces the sink | One engine for streaming and batch; envelope parsing is our code; the same job can feed Redis. More code; exactly-once becomes our responsibility |
| B. Iceberg sink for bronze, Spark streaming only for Redis | Keeps the configuration-only sink, but Spark streaming becomes a side feature, and the sink must still be built from source (spec risk) |
| C. Both write bronze | Two copies of every table, twice the S3 requests, two truths to reconcile |

## Decision

Option A. A PySpark Structured Streaming job reads every CDC topic and
appends one row per change event to an Iceberg bronze table per source
table or collection, in the Glue catalog:

- Kafka read with `kafka.isolation.level=read_committed` (Debezium writes
  in transactions; the default would also return aborted records).
- Debezium's Avro is Confluent-framed (magic byte, 4-byte schema ID,
  payload). Spark's `from_avro` reads raw Avro with one fixed schema, so
  each micro-batch is processed in `foreachBatch`: group by schema ID,
  fetch each writer schema from Schema Registry (cached), decode each
  group, `unionByName`.
- Exactly-once: the checkpoint records the Kafka offsets of each batch;
  the batch ID is written into the Iceberg snapshot summary, and a batch
  whose ID is already committed is skipped on replay.
- The Redis feature updates run as a **separate query** with its own
  checkpoint, so Redis being down never stops bronze.

**Table format stays Iceberg** (not Delta Lake): Athena engine v3 reads
and writes Iceberg (MERGE, OPTIMIZE, VACUUM, time travel), which the
maintenance and erasure workflows need; Athena only reads Delta. With
Iceberg's GlueCatalog, every Spark commit is visible in Athena at once;
open-source Delta needs a separate registration step. Delta's built-in
idempotent `foreachBatch` writes (`txnAppId`/`txnVersion`) are its main
advantage here; the batch-ID check above gives the same guarantee.

## Consequences

- ✅ One engine (Spark) to learn, tune and test for streaming and batch.
- ✅ Envelope parsing and Avro decoding are unit-tested code (pytest,
  chispa), not connector internals.
- ✅ No connector to build from source; Kafka Connect no longer needs AWS
  credentials (the Spark jobs get the IAM user instead, P3).
- ❌ More code to maintain than a sink configuration.
- ❌ Exactly-once depends on our batch-ID check being correct; the replay
  drill (FR14) must prove it.
- ❌ A long-running Spark driver and worker on the laptop (~4 GB RAM).
- ❌ Micro-batches create small files per table per trigger; compaction
  matters more.

## References

- Spark Structured Streaming + Kafka integration guide (`kafka.isolation.level`, `foreachBatch`)
- Apache Iceberg docs: Spark structured streaming writes, GlueCatalog
- Amazon Athena docs: querying Iceberg tables; Delta Lake read support
- Cahier des charges v2.0: 5.2, FR2, NFR delivery semantics
