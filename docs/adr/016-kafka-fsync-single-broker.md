# 016. Kafka fsyncs every write on the single broker

- Status: Proposed
- Date: 2026-10-05 (P4; spec NFR "Kafka configuration", risk register)
- Deciders: Souhail Bourhim (approves), Claude Code (drafts)

## Context

On 2026-10-04 at about 21:18:42 UTC the Docker VM died hard (cause
unknown: every container stopped at once; at the next start Kafka logged
"Recovering 144 logs ... no clean shutdown file was found"). About 16
seconds of PostgreSQL changes never reached bronze: 6 new users, 71 of the
75 order items created in that window, and the updates made then.

The trace (learning log, session 5):

- the lost rows are in PostgreSQL, not in Kafka, not even as aborted
  records (`read_uncommitted`);
- `_connect-offsets` kept Connect's position up to transaction 1197128
  (21:18:41), but `thelook.shop.users` kept records only up to 1196999
  (21:18:26);
- after the restart, Connect resumed right after its stored position
  (1197139): the records in between were never read again.

Kafka acknowledges a write once it is in the operating system's page cache
and leaves fsync to the OS. Durability is meant to come from **replicas on
other brokers**: a write acknowledged by `acks=all` sits in the memory of
several machines, and losing all of them at once is unlikely. With one
broker (RF=1, spec NFR), nothing covers a crash of that broker's machine.
Each partition is a separate file flushed independently, so a crash can
keep one partition's tail and lose another's. Connect's exactly-once
transactions write records and offsets atomically **in Kafka's view**;
they assume an acknowledged write is never lost. Here the offsets survived
and the records they covered did not: a silent gap.

## Options

1. **Fsync every write**: `log.flush.interval.messages=1` (broker
   default, so every topic, internal ones included). An append is flushed
   to disk before the producer gets its acknowledgement.
2. **Fsync on a timer**: `log.flush.interval.ms=1000`. Cheaper, but a
   crash still loses up to about a second of acknowledged writes, silently.
3. **Keep the defaults; detect and repair**: after every unclean shutdown,
   run the verify drill and re-snapshot what differs.
4. **Three brokers** (RF=3, `min.insync.replicas=2`): the production
   answer, but three brokers on one laptop share one VM, so the crash that
   happened would still take all of them down; and the RAM is not there.

## Decision

Option 1, plus option 3's runbook rule as a safety net: after any unclean
Kafka shutdown ("no clean shutdown file" in its log), run the
source-to-silver verify drill and re-snapshot the keys that differ.

## Consequences

Measured on this laptop, same broker, `acks=all`, 1 KB records, one topic
with fsync and one with the old default (topic-level `flush.messages`):

| Load | Without fsync | With fsync |
|---|---|---|
| As fast as possible | 43,253 records/s | 6,907 records/s |
| Steady 200 records/s | p50 1 ms, p99 2 ms | p50 2 ms, p99 5 ms |

- At the real rate (about 20 events/s) the cost is a few milliseconds per
  write. O8's target (200 events/s) keeps a 35x margin; any breaking-point
  measurement from now on includes the fsync.
- Snapshots and bulk replays are slower (about 7,000 records/s ceiling).
- Whether a write survives a crash also depends on the disk honouring the
  fsync: Docker Desktop's VM disk on Windows is assumed to; the runbook
  rule covers the case where it does not.
- A production design keeps the Kafka defaults and gets durability from
  three brokers on separate machines (option 4); this setting exists only
  because there is one broker.
- The repair of the 2026-10-04 loss is a separate, one-off procedure
  (runbook), not part of this decision.
