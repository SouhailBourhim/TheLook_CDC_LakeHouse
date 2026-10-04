# 012. Bronze table design

- Status: Proposed
- Date: 2026-10-04 (P3, before the parsing code; spec FR2)
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

## References

- Debezium PostgreSQL and MongoDB connector docs: event envelope, `source` fields
- Apache Iceberg docs: hidden partitioning (`days()`), Spark writes and schema merge
- Cahier des charges v2.0: FR2; ADR 006, ADR 007
