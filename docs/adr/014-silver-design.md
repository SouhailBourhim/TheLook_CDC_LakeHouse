# 014. Silver: incremental MERGE of the latest version per key

- Status: Accepted
- Date: 2026-10-04 (P4; spec FR3, ADR 009, ADR 012; settles open question E1)
- Deciders: Souhail Bourhim (approves), Claude Code (drafts)

## Context

Bronze (ADR 012) holds every change event. Silver must hold the current
state of each entity (FR3): latest version per key, deletes applied,
idempotent, incremental. Facts that shape the design, measured in P2-P3:

- A blocking snapshot gives every `r` row the pause LSN, and the first
  event streamed after the resume can have **the same LSN** (seen in P3).
- MongoDB has no LSN; ordering by (`source_ts_ms`, `ord`) matched Kafka
  order for every document checked (1.8 M events and the reviews) (E1).
- The generator re-writes all 29,120 products on every start (no-op
  updates), and picks orders to update at random across the whole table.
- Silver jobs run on the laptop and read S3 over the internet: data
  transfer out is free up to 100 GB/month, then $0.09/GB.

## Decision

**Tables.** `lake.thelook_silver.{users, orders, order_items, products,
dist_centers, events, reviews}`: the source columns (typed, below) plus
`_position` (bigint), `_source_ts` (commit time), `_row_hash` (SHA-256 of
the business columns) and `_merged_at`.

**Incremental read.** Each run reads only the bronze snapshots added since
the last run (Iceberg incremental read between two snapshot ids; bronze is
append-only, so every snapshot is an append). The last bronze snapshot id
processed is stored in the silver commit's summary
(`thelook.bronze-snapshot`): the state lives with the data, as for the
stream's batch id. No watermark yet means a full read (the first run).

**Latest event per key** in the increment (one row per key):

| Source | Key | Order (highest first) |
|---|---|---|
| PostgreSQL | `coalesce(after.id, before.id)` (a delete carries the key only in `before`) | `lsn`, then streamed before `r` (the P3 tie), then `kafka_offset` |
| MongoDB | `doc_id` (from the record key) | `source_ts_ms`, `ord`, then streamed before `r`, then `kafka_offset` (**E1 settled**) |

`_position` = `lsn` for PostgreSQL, `source_ts_ms x 1,000,000 + ord` for
MongoDB (one comparable number, same order).

**MERGE** (Spark SQL `MERGE INTO` on Iceberg), on the key:

- matched, event `d`, event newer than the row -> **delete**;
- matched, event newer than the row (`s._position > t._position`) ->
  **update** all columns, *including no-op updates*;
- not matched, event not `d` -> **insert**.

Updating on every newer event, even one that changes nothing, keeps
`_position` current. Skipping no-ops would leave an old position behind,
and an older event replayed later (after a lost checkpoint) would then pass
the "newer" test and overwrite the true state. FR3's "ignore no-op updates"
is met where it matters: a no-op changes no business column in silver, and
gold's SCD2 history skips versions whose `_row_hash` did not change.

The guard also makes MERGE idempotent: rerunning the same increment
changes nothing (no event is newer than itself).

**Merge-on-read** (`write.merge.mode` etc. = `merge-on-read`): matched rows
are recorded in delete files instead of rewriting their data files. Random
order updates touch files everywhere; copy-on-write would rewrite (and
download) most of the table on every run. Compaction comes with the
maintenance job.

**Types.** Debezium microsecond integers -> `timestamp`; money (`cost`,
`retail_price`, `sale_price`, review/cart `price`) -> `decimal(10,2)`,
which removes float artefacts such as 34.9900016784668; MongoDB JSON parsed
with an explicit schema (`{"$date": ms}` -> `timestamp`). A document whose
JSON does not parse (no `_id` after parsing) is counted and logged, not
written (dead-letter area: P7).

**events.** Same MERGE path (only inserts happen in practice; deletes will
come from erasure, P8), partitioned by `days(created_at)`; the MERGE
condition adds `t.created_at >= <oldest created_at in the batch>` so Iceberg
reads only the recent partitions of a 2.6 M-row table, not all of it.

## Consequences

- ✅ Correct under replays, duplicates and re-snapshots (position guard),
  and idempotent: a failed run is simply run again.
- ✅ Each run reads only new bronze data and the key columns of the target.
- ❌ A no-op update still writes (a delete-file entry and a new row): the
  29,120-product burst at each generator start costs one rewrite of a small
  table.
- ❌ Merge-on-read makes reads slower until compaction (maintenance DAG).
- ❌ The incremental read needs the last processed bronze snapshot to still
  exist: snapshot expiry must keep at least the snapshots silver has not
  processed yet (step 9). If it is gone, the job falls back to a full read,
  still correct thanks to the guard, but slower.
- ❌ A field whose type changes inside a MongoDB document becomes null
  rather than failing (JSON parsing is permissive); data contracts (P7)
  detect it.

## References

- Apache Iceberg docs: Spark incremental read (`start-snapshot-id`), `MERGE INTO`, write modes (copy-on-write vs merge-on-read)
- Debezium PostgreSQL connector: blocking snapshots; MongoDB connector: `source.ord`
- Cahier des charges v2.0: FR3, FR8; ADR 012
