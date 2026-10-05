# 017. Table maintenance: each layer maintained by its writer

- Status: Proposed
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
min/max statistics skip the files already processed. Safe because the cut
holds only finished batches: a row committed later has a later
`ingested_at`.

**Bronze (in the stream):**

- hourly: expire snapshots older than 1 hour, keeping at least the last 5
  (bronze is an append-only log: its history is in the rows, so time travel
  adds little; the replay guard needs only the latest snapshot); compact the
  ledger (one tiny file per batch);
- at every commit: Iceberg deletes `metadata.json` files beyond the last 20
  (`write.metadata.delete-after-commit.enabled`);
- daily: compact the small data files of past days' partitions; remove
  orphan files older than 3 days.

**Silver and gold (Airflow `maintenance` DAG, daily, batch user):** expire
snapshots older than 1 day, compact data and position-delete files, remove
orphan files older than 3 days.

## Consequences

- FR12 changes: the maintenance DAG covers silver and gold; bronze is
  maintained by the stream.
- The stream pauses its batches while it maintains bronze (expected to be
  seconds hourly and a minute or two daily; measured when built).
- Bronze time travel is limited to about an hour; "bronze as of T" is still
  a filter on `ingested_at`.
- Orphan removal waits 3 days so that files of a write still in progress are
  never taken for orphans.
