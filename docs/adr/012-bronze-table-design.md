# 012. Bronze table design

- Status: Accepted
- Date: 2026-10-04 (P3, before the parsing code; spec FR2)
- Amended: 2026-10-05 (P4 step 8): a per-batch ledger, `stream_batches`.
- Deciders: Souhail Bourhim (approves), Claude Code (drafts)

## Context

The Spark streaming job (ADR 007) appends Debezium change events to
Iceberg tables in `thelook_bronze`. FR2 asks for one row per change event
with the operation, the before and after images, the log position, the
commit time and the Kafka coordinates; MongoDB images stay JSON text.
Bronze is the only complete history (Kafka keeps 3 days), so it must stay
faithful to what Debezium sent, and still be queryable and cheap to scan.

## Options considered

| Question | Options | Choice |
|---|---|---|
| What is stored | (a) raw Kafka bytes; (b) everything as JSON text; (c) decoded envelope: typed structs for PostgreSQL, JSON text for MongoDB | (c): typed and queryable, still one row per event, nothing dropped. (a) is unreadable in Athena; (b) throws away the types Avro already gives |
| Debezium encodings (timestamps as microseconds, `adaptive`) | convert in bronze; keep as sent | Keep as sent: bronze = what the log said; silver converts (`timestamp_micros`). Converting needs Debezium's `connect.name` hints, which Spark's `from_avro` drops |
| Table names | source table name only; `<database>_<table>` | `<database>_<table>`: `shop_users`, `shop_orders`, `shop_order_items`, `shop_products`, `shop_dist_centers`, `web_events`, `web_reviews`. Shows the origin and cannot collide across sources. The heartbeat topic is not ingested (B2) |
| Partitioning | none; by ingestion day; by commit day | `days(source_ts)`, the source commit time: queries and backfills filter by when the change happened. Iceberg hidden partitioning, so readers filter on the timestamp, not on a separate date column |

## Decision

Every bronze table has these columns:

| Column | Type | From |
|---|---|---|
| `op` | string | `c`, `u`, `d`, `r` (snapshot read) |
| `source_ts_ms` | bigint | `source.ts_ms`, commit time at the source |
| `source_ts` | timestamp | the same, as a timestamp (partition column) |
| `ts_ms` | bigint | when Debezium processed the change |
| `snapshot` | string | `source.snapshot` (`true`, `last`, `incremental`, `false`...) |
| `kafka_topic`, `kafka_partition`, `kafka_offset`, `kafka_timestamp` | string, int, bigint, timestamp | where the record was read |
| `schema_id` | int | Schema Registry id of the writer schema |
| `ingested_at` | timestamp | when the micro-batch ran |

PostgreSQL tables add `lsn` (bigint), `tx_id` (bigint), `before` and
`after` (structs with the table's columns, as decoded from Avro).
MongoDB tables add `doc_id` (string, from the record key, the only place a
delete carries it), `ord` (int, order within the cluster-time second),
`after` (JSON string) and `update_description` (struct, as sent).

Tombstones (null values) are not stored: the delete event before them
carries the information. Tables are Iceberg format v2, Parquet with zstd
(Iceberg's defaults). New nullable columns are accepted by schema merge on
write (ADR 006).

## Consequences

- ✅ Nothing in a change event is lost; Athena can query bronze directly.
- ✅ Silver dedups on (`lsn`) or (`source_ts_ms`, `ord`, `kafka_offset`) per
  key from columns, without re-parsing the envelope (E1 decides the Mongo
  order).
- ✅ The Kafka coordinates make a duplicate append detectable.
- ❌ Bronze timestamps are microsecond integers; readers must convert
  (silver does).
- ❌ PostgreSQL and MongoDB tables have different shapes; silver has one
  parser per source family.
- ❌ Partitioning by commit day puts a whole snapshot in the snapshot's
  day (snapshot rows carry the snapshot time).

## Amendment 2026-10-05: the stream's batch ledger

The stream commits a table only when the batch has rows for it, so the
tables' latest batch ids differ and nothing in them says whether the newest
batch has finished. Silver, reading each table at whatever snapshot was
current, merged order items whose orders were in the next batch (329 items
in one run, P4 step 8).

Each micro-batch now ends with one row in `lake.thelook_bronze.stream_batches`
(`query_id`, `batch_id`, `committed_at`, `snapshots`: bronze table -> its
snapshot id), written after every table append of the batch, with the same
replay guard (query id and batch id in the snapshot summary). A row for
batch N means all of batch N has committed. Rejected: "newest batch id - 1"
(never confirms the last batch once the writers stop, so silver could not
catch up for a reconciliation); committing every table every batch (seven
mostly empty commits a minute). Cost: one small commit per batch, compacted
by the maintenance job.

The snapshot ids are read from the catalog itself (Iceberg's
`table.refresh()`), not from Spark's cached copy of the table. Found on
2026-10-07: once the stream's maintenance thread committed through its own
table object, the cached copy (kept alive while in use) could lag behind,
and the ledger recorded the previous batch's snapshot for some tables
(3 rows in ~500, each right after a maintenance run). Silver then stopped
short on those tables: a delay, not a loss (its watermark is an
`ingested_at`), but an order item could again be merged without its order,
which is what the "items built without their order" seen on 2026-10-05
were.


## References

- Debezium PostgreSQL and MongoDB connector docs: event envelope, `source` fields
- Apache Iceberg docs: hidden partitioning (`days()`), Spark writes and schema merge
- Cahier des charges v2.0: FR2; ADR 006, ADR 007
