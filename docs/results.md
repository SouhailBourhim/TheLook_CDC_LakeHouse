# Results

Measured outcomes of drills and benchmarks. Each entry says how it was
measured, so it can be repeated. Raw logs stay local (`drills/logs/`, not in
Git).

## Replication slot drill — 2026-09-30 (P1 acceptance, part 2)

**Goal (spec 9, FR13):** stop Debezium for one hour while the generator
runs; retained WAL grows and the alert fires; after restart the slot catches
up with no data loss.

**Method:** `drills/slot-drill.sh` (STOP_MINUTES=60), generator at 5
iterations/s, core and monitoring profiles up. "No data loss" is checked by
`drills/verify_cdc.py`: for each table, rebuild the latest version of every
primary key from the Kafka topic (highest `source.lsn`, deletes removed,
`read_committed`) and compare with Postgres **column by column**, with the
generator stopped so both sides describe the same moment.

**Result: passed.**

| Step | Observation |
|---|---|
| Connect stopped 02:05:34 UTC | Slot `active=f`; `ReplicationSlotInactive` pending from minute 1 |
| Retained WAL | 32 MB at minute 1 → 275 MB at minute 30 → 547 MB at minute 60; `wal_status=reserved` throughout, 9.7 GB still safe |
| Alert | `ReplicationSlotInactive` **firing at minute 31** (30 min `for` + evaluation interval); no other alert |
| Verifier with Kafka behind | **MISMATCH** as expected: e.g. 112,046 events and 16,009 orders missing, 4,833 orders with an older status. Proves the check detects loss. |
| Connect restarted 03:06:28 | Slot confirmed the last source LSN (`0/67EAE098`) after **44 s**, worker start-up included |
| Verifier after catch-up | **All 6 tables identical**, every column (9,511 users, 69,719 orders, 100,900 order items, 484,623 events, 29,120 products, 10 distribution centres) |
| One minute later | Alert resolved |

**Findings**

- **Retained WAL starts above zero.** It is measured from `restart_lsn`,
  which advances in steps (at checkpoints and confirmations), so 32 MB was
  already retained one minute after the stop.
- **The WAL rate grows with table size.** 547 MB/hour here vs 106 MB/hour
  measured in commit 6 on nearly empty tables, at the same 5 iterations/s.
  Cause, measured with `pg_stat_wal`: full-page images. After each checkpoint
  (every 5 min) the first change to a page writes the whole 8 KiB page; with
  70,000 orders, random order updates mostly hit pages not yet touched since
  the checkpoint. In a 2-minute sample, 24.5 % of WAL records were full-page
  images and they were most of the WAL bytes. `wal_compression` is off; to
  measure in the baseline run.
- **Consequence for the slot cap:** at 5 iterations/s, 10 GiB now holds
  about 18 hours of consumer downtime, not ~4 days.

## Baseline throughput run — method (written before measuring, B5)

> **Context note (spec v2.0, 2026-10-03), measured in P2 below:** This run and the slot drill
> above were measured with `events` in PostgreSQL, where it made up about
> two thirds of all change events and most of the WAL. Since P2 the events
> live in MongoDB (ADR 008), so PostgreSQL's WAL rate and the Postgres
> connector's load are lower at the same generator rate. The numbers stay
> valid as P1 history; O8 is re-measured end to end, across both
> connectors, in P3.

**Question:** how fast can the on-prem capture path (generator → Postgres →
Debezium → Kafka) go, and what saturates first? The answer sets O8's
target, to be confirmed end to end in P2 when the sink consumer exists.

**Scope.** O1 (freshness) and O8 (consumer lag < 30 s) are end-to-end
objectives; in P1 there is no lake consumer yet, so this run measures the
**capture side only**: source commit → record written in Kafka.

**Load.** `GENERATOR_QPS` = 5, 10, 20, 40, 80 iterations/s, each held for
6 minutes. Each generator restart re-seeds 1,000 users and re-upserts
29,120 products (a burst, see `docs/source-schema.md`), so the **first
2 minutes of each level are warm-up and not measured**; the measurement
window is the last 4 minutes.

**Metrics per level (measurement window only)**

| Metric | Source | Why |
|---|---|---|
| Achieved iterations/s | `increase(pg_stat_user_tables_n_tup_ins{relname="orders"})` / window; each iteration inserts exactly one order | Is the generator keeping up with its target? |
| Change events/s captured | `increase(kafka_connect_source_task_metrics_source_record_write)` / window | Records Debezium actually wrote to Kafka (not end offsets, which include transaction markers) |
| Capture lag | `max_over_time` and `quantile_over_time(0.95, …)` of `debezium_streaming_millisecondsbehindsource` | Source commit → Debezium processing |
| WAL rate | `pg_current_wal_lsn()` delta over the window, MB/hour | Sizes the slot cap |
| Retained WAL trend | slot `restart_lsn` retained WAL at window start and end | A growing value means the consumer falls behind |
| CPU | `docker stats` at the end of the window, per container | Names the bottleneck (a single-threaded process caps near 100 %) |

**Counts while the generator runs (B5, O4).** Rates are computed from
monotonic counters over a fixed window, never from `count(*)` snapshots
taken at different moments. Exact equality checks (`verify_cdc.py`) are only
run with the generator stopped.

**A level is sustained when all hold:** achieved ≥ 90 % of target; capture
lag max < 30 s over the window; retained WAL not growing between window
start and end (± one WAL segment, 16 MB). The **breaking point** is the
first level that is not sustained; the component near a full CPU core (or
the metric that failed) is the bottleneck.

**Hypothesis (to confirm or reject):** the generator saturates first. Each
iteration runs `ORDER BY RANDOM() LIMIT 1` on `users` and on `orders`, a
full scan of a table that grows every second; that load lands on Postgres
CPU and on the generator's single event loop.

**Side experiment, same session:** WAL volume with `wal_compression` off
vs `lz4`, two consecutive 6-minute windows at 5 iterations/s, changed with
`ALTER SYSTEM` + reload (no restart). Full-page images dominated the WAL in
the slot drill; compression targets exactly them.

## Baseline throughput run — results, 2026-09-30

Method above, applied by `drills/throughput-baseline.sh`; ~100,000 orders
in the database at the end. Achieved = generator iterations per second.

| target/s | achieved/s | events/s captured | lag p95 (s) | lag max (s) | WAL MB/h | retained Δ MB | sustained (criteria as written) | CPU (generator / postgres / connect) |
|---|---|---|---|---|---|---|---|---|
| 5 | 4.5 | 47 | 0.4 | 0.5 | 500 | −2 | NO | 2 % / 8 % / 2 % |
| 10 | 7.9 | 82 | 0.3 | 0.4 | 750 | 1 | NO | 6 % / 15 % / 2 % |
| 20 | 13.3 | 140 | 0.2 | 0.2 | 1,069 | 0 | NO | 12 % / 16 % / 5 % |
| 40 | 20.4 | 212 | 0.0 | 0.1 | 1,458 | 0 | NO | 20 % / 45 % / 2 % |
| 80 | 25.6 | 266 | 0.0 | 0.0 | 872 | −6 | NO | 26 % / 39 % / 2 % |

| wal_compression (5/s, consecutive 6-min windows) | WAL MB/h |
|---|---|
| off | 623 |
| lz4 | 502 (−19 %) |

**Findings**

- **The capture side was never the limit.** At every level Debezium's lag
  stayed ≤ 0.5 s, retained WAL did not grow, and Connect used < 5 % CPU,
  up to 266 change events/s.
- **The load generator saturates first, by latency, not CPU.** Achieved
  rate flattens at ~25 iterations/s whatever the target; no process is near
  a full core. Each iteration runs its database calls one after another,
  including two `ORDER BY RANDOM() LIMIT 1` full scans (users, orders), and
  its pacing sleeps *before* the work, so the rate is
  `1 / (1/target + work time)`: ~39 ms of work caps it near 25/s, and the
  cap falls as the tables grow. The hypothesis is confirmed on "generator
  first", corrected on the mechanism.
- **The written criterion was flawed.** "Achieved ≥ 90 % of target" measures
  the generator's pacing, so no level passes, not even 5/s (the sanity check
  before the run showed 88 %; the criterion was kept as committed). For the
  capture path, the lag and retained-WAL criteria held at every level.
- **The capture side's breaking point was not reached.** Finding it needs a
  load generator that is not latency-bound (open question A9).
- **WAL per 4-minute window is noisy**: it depends on where the window
  falls in the 5-minute checkpoint cycle (full-page images follow each
  checkpoint), hence 80/s < 40/s. Worst case measured: ~1.5 GB/hour at the
  generator's maximum, so 10 GiB holds ~7 hours of consumer downtime there,
  ~16–20 hours at 5/s.
- **lz4 WAL compression saves ~19 %**, less than expected: page images are
  already stored without their free space, and UUIDs and random text
  compress poorly. Enabled anyway (cheap at these CPU levels).

**O8 (approved by Souhail, spec v1.8):** target **200 change events/s
(≈ 20 iterations/s)**, capture lag < 1 s in P1, below the observed maximum
(266/s) for margin; re-validated end to end in P2 with the sink's consumer
lag < 30 s. The breaking point is searched in P2 (A9, deferred).

## P2: MongoDB source — 2026-10-03

### Reconciliation, Kafka vs both sources

**Method:** `drills/verify_cdc.py` with the generator and review simulator
stopped and both connectors caught up (runbook, "Before a
reconciliation"). PostgreSQL: latest version per key by `source.lsn`,
column by column. MongoDB: latest version per `_id` by Kafka order (key
from the record key, so deletes count), compared by a hash of the
canonical JSON.

**Result: passed, all 7 sources identical, in 166 s.**

| Source | Rows / documents | Missing | Extra | Different |
|---|---|---|---|---|
| users | 29,184 | 0 | 0 | 0 |
| orders | 261,484 | 0 | 0 | 0 |
| order_items | 379,126 | 0 | 0 | 0 |
| products | 29,120 | 0 | 0 | 0 |
| dist_centers | 10 | 0 | 0 | 0 |
| events (MongoDB) | 1,817,027 | 0 | 0 | 0 |
| reviews (MongoDB) | 369 | 0 | 0 | 0 |

- **The check can fail:** with Connect stopped, one review's rating was
  changed in MongoDB: `differ=1` with that review's id. After restarting
  Connect, the connector resumed from its stored position and the check
  passed again.
- **Retention:** the `products` and `dist_centers` topics have lost their
  first records (3-day retention); every row is still covered because each
  generator start re-writes them. Rows unchanged for more than 3 days have
  no event left in Kafka: bronze, not Kafka, must hold the full history
  (P3 starts with an incremental snapshot; spec risk table).
- **E1 evidence:** ordering by (`source.ts_ms`, `source.ord`) picked the
  same latest version as Kafka order for every document. Weak evidence:
  events have one version each, so only the reviews test it.

### Initial snapshot and history copy

| Step | Volume | Time |
|---|---|---|
| Copy of the PostgreSQL events table to MongoDB (`scripts/seed_mongo_events.py`) | 1,756,486 events | 94 s (~19,000/s) |
| Debezium MongoDB initial snapshot | ~1.76 M documents | 79 s (~22,000/s) |

The copy used 1.16 GiB of the 2 GiB oplog: a bulk load shrinks the window
a lagging connector can rely on.

### Change-log growth after the move (clean 10-minute window)

Generator at 5 iterations/s, review simulator at 1 action/s, window
started well after the generator's startup burst (a first 2-minute sample
that included it read 586 MiB/h of WAL and was discarded).

| Log | Growth | Consumer outage it can absorb |
|---|---|---|
| MongoDB oplog (2 GiB, fixed) | 69 MiB/h | ~30 h, then re-snapshot |
| PostgreSQL WAL (`wal_compression=lz4`) | 242 MiB/h | ~42 h to the 10 GiB slot cap (P1: ~18 h) |

PostgreSQL WAL was ~550 MiB/h in the P1 slot drill (events included, no
compression); compression alone saved 19 % in the P1 baseline, so moving
`events` out accounts for most of the drop.

## P3: bronze on AWS with Spark Structured Streaming — 2026-10-04

### Acceptance 1, freshness (O1: source commit -> queryable in Athena, < 5 min)

**Method:** `drills/freshness.py` (written before measuring): take the
newest order (PostgreSQL, generator) or review (MongoDB, simulator), poll
Athena every 5 s until bronze returns it; latency = end of the first
successful query - the row's `source_ts` (Debezium commit time). Upper
bound by up to ~5 s + Athena query time. Generator at 5/s, simulator 1/s,
60 s trigger.

**Result: passed.** n = 6, min 26.0 s, median 75.3 s, **max 87.5 s**.

| Source | Samples (s) |
|---|---|
| PostgreSQL order | 26.0, 46.4, 46.4 |
| MongoDB review | 78.5, 75.3, 87.5 |

Reviews are consistently ~30-40 s later: a batch commits its tables one
after the other in topic-name order, and `thelook_mongo.web.reviews` is
last (7 commits x ~4 s of S3/Glue round trips from the laptop). Committing
tables in parallel would remove that gap (throughput work, O8).

### Acceptance 2, a killed and restarted job appends no duplicates

**Method:** wait until a micro-batch has committed two of its tables, then
`kill -9` the driver (SparkSubmit) inside the container: a crash, so
Docker's `restart: on-failure:3` applies. Then count, per bronze table,
rows vs distinct Kafka offsets (one partition per topic, so an offset
identifies a record).

**Result: passed.**

- Killed at 05:45:16 during batch 35, after `shop_order_items` and
  `shop_orders` had committed. Docker restarted the container (restart
  count 1); the job resumed with the same query id at 05:45:30 and
  replayed batch 35: `shop_order_items=skipped(replay)`,
  `shop_orders=skipped(replay)`, the 3 other tables written.
- An unplanned failure came first: at 05:20:21 AWS Glue was unreachable
  for a few seconds ("Connection refused" after the SDK's 3 attempts); the
  batch failed, the query stopped, and with no restart policy the stream
  stayed down for 21 minutes (found by the freshness drill timing out).
  Fixed: transient S3/Glue errors are retried inside the batch (5 attempts,
  5-40 s backoff, the replay check re-run before each attempt), plus the
  restart policy. On restart the job replayed batch 32 with the record
  ranges stored in the checkpoint, then caught up ~42,000 records.
- After both failures, all 7 tables: **0 duplicates** (4,178,308 rows =
  4,178,308 distinct offsets).

| Table | Rows | Distinct offsets |
|---|---|---|
| shop_order_items | 782,700 | 782,700 |
| shop_orders | 539,839 | 539,839 |
| shop_users | 60,266 | 60,266 |
| shop_products | 174,720 | 174,720 |
| shop_dist_centers | 60 | 60 |
| web_events | 2,594,904 | 2,594,904 |
| web_reviews | 25,819 | 25,819 |

### Initial load and snapshot

| Step | Volume | Time |
|---|---|---|
| Backlog still in Kafka (P1-P2 changes within retention, MongoDB from its initial snapshot) | ~3.3 M records, 17 batches of up to 200k | ~22 min (~2,500 records/s) |
| Blocking snapshot of the 5 PostgreSQL tables (Debezium) | 964,802 rows | 24 s |
| The same rows into bronze | 5 batches | ~5 min |
| Steady state | ~2,000-3,000 records/batch | ~30 s per batch |

Snapshot reconciliation, Athena vs Debezium's exported counts, rows `op='r'`
after the snapshot start: identical for all 5 tables (users 36,575, orders
366,935, order_items 532,162, products 29,120, dist_centers 10), with rows =
distinct ids = distinct offsets.

Debezium 3.7.0's read-only incremental snapshot crashed the PostgreSQL
task under live traffic (`ConcurrentModificationException`, runbook);
recovered from the exactly-once offset, then used a blocking snapshot.

### Storage after the run

759 objects, 279 MB in `s3://<lake>/bronze/` (before the restart); metadata
files outnumber data files ~3:1 (one metadata.json, manifest list and
manifest per commit), the small-files cost of a 60 s trigger: expiry and
compaction come with the maintenance DAG. Cost so far: under $0.05.

## P4: silver — 2026-10-04

### Silver reconciles with the sources (acceptance, part 1)

**Method:** `drills/verify_silver.py` (written before running it): read
each silver table with PyIceberg (delete files applied), digest every
row's canonical values, compare with every PostgreSQL row / MongoDB
document through the same canonical form (UTC timestamps; money rounded
half-up to 2 decimals as silver does). Writers stopped; bronze caught up
(no micro-batch after the last records: Structured Streaming skips
triggers without new data); silver run once more.

**Result: passed, all 7 tables identical, every column.**

| Table | Rows | Missing | Extra | Different |
|---|---|---|---|---|
| users | 39,111 | 0 | 0 | 0 |
| orders | 396,842 | 0 | 0 | 0 |
| order_items | 575,377 | 0 | 0 | 0 |
| products | 29,120 | 0 | 0 | 0 |
| dist_centers | 10 | 0 | 0 | 0 |
| events | 2,757,709 | 0 | 0 | 0 |
| reviews | 13,240 | 0 | 0 | 0 |

- **The check can fail:** one review's rating changed in MongoDB ->
  `differ=1` with that review's id. Then the change flowed through CDC ->
  bronze (batch 117, 1 record) -> silver (1 key merged) and the check
  passed again: the full path, end to end.

### Silver run times (2 cores, laptop -> S3 over the internet)

| Run | Time | Notes |
|---|---|---|
| First run (full read of bronze) | ~27 min | orders 327 s, order_items 447 s (incl. DNS retries), events the longest |
| Incremental (~1 h of changes) | 195 s for all 7 tables | `full_read: False`; products and dist_centers "up to date" |
| Incremental, sources quiet | ~2 min | only the last records |

During the backfill the bronze stream slowed down (batches of 200-280 s
instead of ~30 s: both jobs share the laptop's network and the 4 cores).
A ~3-minute DNS outage (18:39-18:42, "Temporary failure in name
resolution") exhausted the stream's in-batch retries; Docker restarted it
and the replay guard skipped the 3 tables already committed: recovered
without intervention. The silver job rode out its own DNS failures with
its retries (4 attempts on order_items).
