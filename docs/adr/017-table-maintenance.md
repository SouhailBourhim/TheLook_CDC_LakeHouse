# 017. Table maintenance: each layer maintained by its writer

- Status: Accepted
- Date: 2026-10-05 (P4 step 9; spec FR12, NFR cost; ADR 012, ADR 014)
- Deciders: Souhail Bourhim (approves), Claude Code (drafts)

## Context

Every Iceberg commit adds a snapshot, and `metadata.json` lists all of them.
Bronze commits each busy table once a minute and had never been maintained:
after 444 batches, each busy table's `metadata.json` was 487 KB, rewritten at
every commit and loaded several times per batch (existence check, replay
guard, write). Measured on 2026-10-05:

- stream batches took ~110 s for a 60 s trigger (O1 at risk), up from well
  under a minute in P3 on young tables;
- the stream's driver downloaded ~9 MB/min (~0.55 GB per hour of the stream
  profile), almost all of it table metadata, growing with every batch.

Files pile up too: one data file per table per batch in bronze, delete files
from every merge-on-read MERGE in silver and gold, replaced versions of the
rebuilt gold dimensions, and orphans left by writes that failed.

Who maintains matters for security. Expiring snapshots, compacting files
and removing orphans all delete objects. The stream user already has delete
rights on bronze (and only bronze); the batch user already has them on
silver and gold. Airflow holds the batch key only (ADR 013), never a key
that can delete bronze, the one layer that cannot be rebuilt.

A second constraint came from silver: its watermark was the id of the last
bronze snapshot it processed, and an incremental read needs that snapshot to
exist. Expiring bronze snapshots would break it (ADR 014's "falls back to a
full read" was never implemented).

## Options

1. **Each layer maintained by its writer.** The stream maintains bronze in
   its own process (the key never leaves it); a daily Airflow DAG maintains
   silver and gold as the batch user.
2. **A separate maintenance user** with delete rights on all three layers,
   its key in Airflow, one daily DAG (FR12 as written).
3. **A bronze-only maintenance user** run outside Airflow by hand.

## Decision

Option 1 (Souhail, 2026-10-05). No new identity, no right widened.

**Silver no longer depends on old bronze snapshots** (ADR 014 amendment):
its watermark is the newest `ingested_at` it has processed. Each run reads
bronze as of the ledger's cut (`versionAsOf`, always a recent snapshot)
filtered on `ingested_at` greater than the watermark; Iceberg's per-file
min/max statistics skip the files already processed. Safe because the
stream commits one batch at a time, each table in one commit, so a row
committed later has a later `ingested_at`, as long as the driver's clock
never goes backwards (ADR 014 amendment).

**Bronze (in the stream, in a background thread of its driver):**

- hourly: expire snapshots older than 1 hour, keeping at least the last 5
  (bronze is an append-only log: its history is in the rows, so time travel
  adds little; the replay guard needs only the latest snapshot); compact the
  ledger (one tiny file per batch);
- at every commit: Iceberg deletes `metadata.json` files beyond the last 20
  (`write.metadata.delete-after-commit.enabled`);
- in each hourly run, at most one heavier task on one table: compact the
  partitions before today of the table compacted longest ago, if more than
  a day ago; otherwise remove the orphan files (older than 3 days) of the
  table cleaned longest ago, if more than 7 days ago. When each table last
  had each task is a table property (`thelook.compacted-at`,
  `thelook.orphans-removed-at`), so restarts do not reset it.
- The thread runs alongside the micro-batches: compaction costs about 0.8 s
  per small file over the internet (230 files: ~3.5 min), and a stream
  running all day makes 1,440 files per table, so blocking compaction could
  not keep O1 (5 minutes). The session uses Spark's FAIR scheduler with a
  "maintenance" pool, so its jobs share the stream's 2 cores instead of
  queueing ahead of the batches. Concurrent commits on one table are safe:
  Iceberg retries the one that loses the race, and compaction never changes
  which rows a table holds.

**Silver and gold (Airflow `maintenance` DAG, daily at 03:00 UTC, batch
user, `jobs/maintenance.py`):** for every table, compact data files
(applying the merge-on-read delete files, and removing the delete files
left dangling), compact position-delete files, expire snapshots older than
1 day keeping the last 5 (Java API, as for bronze); then remove orphan
files older than 3 days on at most 3 tables per run, those cleaned longest
ago (an orphan sweep costs minutes per table).

**One writer at a time.** A compaction rewriting silver files while a
transform MERGE writes delete files against them is a real Iceberg
conflict: one commit would fail. Every Spark task of both DAGs takes the
single slot of the Airflow pool `lake` (created by `airflow-init`), so two
lake writers never run together. The upkeep may run *between* two transform
tasks: compaction changes neither rows nor `_merged_at`, so gold's watermark
holds. The shared helpers live in `spark/lakehouse/upkeep.py`.

## Consequences

- FR12 changes: the maintenance DAG covers silver and gold; bronze is
  maintained by the stream.
- The stream pauses its batches while it maintains bronze. Measured
  (2026-10-05): the first run, catching up on ~437 snapshots per busy
  table, took 41 min; then Iceberg's `expire_snapshots` SQL procedure took
  15.6 min an hour for ~36 snapshots per table, a fixed ~2.5 min per table
  spent reading every manifest as Spark tasks to find unreferenced files.
  The same expiry through Iceberg's Java API (`table.expireSnapshots()`,
  whose incremental cleanup reads only what the expired snapshots
  referenced) takes **36 s** for all 8 tables. The stream uses the Java API.
- After the first expiry: `metadata.json` 487 KB -> 46 KB per busy table,
  batches ~110 s -> 34-45 s, stream driver download 9.2 -> 3.2 MB/min.
- Measured with the thread: an hourly run that also compacted 230 files of
  `shop_orders` took 101 s, during which batches kept completing (~44 s).
  A network failure during compaction ("Connection refused" to S3) was
  logged and left the table due for the next run; the stream was unaffected.
- Bronze time travel is limited to about an hour; "bronze as of T" is still
  a filter on `ingested_at`.
- Orphan removal waits 3 days so that files of a write still in progress are
  never taken for orphans.
