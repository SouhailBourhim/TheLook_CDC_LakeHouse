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

## P4: gold dimensions — 2026-10-04

`make spark-run JOB="jobs/gold.py dims"`: 78 s. dim_date 4,018 days,
dim_product 29,120, dim_distribution_center 10, **dim_user 48,146 versions
of 40,458 users**.

dim_user integrity (Athena): user_sk unique (48,146 distinct); exactly one
current version per user (40,458); **0 gaps or overlaps** (every closed
version ends where the next begins). Where the versions come from (bronze
`shop_users`):

| Bronze events | Count | In dim_user |
|---|---|---|
| `c` or `r`, the user's first event | 32,783 | version 1 |
| `u`, the user's first event (user created before bronze began) | 7,683 | version 1, starting at `created_at` |
| `u`, address changed | 7,718 | a new version (7,688 at build time; the rest arrived after) |
| `r`, same state as the previous event | 18,656 | nothing (snapshot repeats are collapsed) |

## P4: gold facts — 2026-10-05

First full build (`jobs/gold.py`, dims + facts): 919 s including two DNS
retries. A retry re-ran the facts stage after `fct_order_items` had already
been merged, and correctly found nothing left to do there (the logged
counts are "recomputed by this attempt", hence renamed).

| Table | Rows | Checks (Athena) |
|---|---|---|
| fct_order_items | 575,377 | = silver order_items; keys unique; 0 without a user version |
| fct_orders | 396,842 | keys unique; 0 without a user version; net = gross − returns in total (66,968,762.56 − 43,368.26 = 66,925,394.30) |
| fct_sessions | 666,781 | 79,369 ghost sessions flagged |

**Point-in-time join on real data:** 0 of 396,842 orders fall outside the
validity range of their dim_user version; 15,018 joined a later version of
their user and **101,683 orders keep an address the user no longer has**
(FR4: the address at purchase time, not the current one).

**Cycle times (silver then gold, as the DAG will run them):**

| Cycle | Silver | Gold | Notes |
|---|---|---|---|
| Catch-up after ~6 h without silver | 1,876 s | 253 s | events 544,334 keys (604 s) |
| Normal (~30 min of changes: ~10,000 orders) | 183 s | 310 s | ~8 min in total |

O2 estimate (measured end to end in step 10): 30-minute schedule + ~8 min
cycle + ~1.5 min bronze lag ≈ 40 min worst case, under the 1-hour target.

## P4: gold marts — 2026-10-05

Cycle silver (307 s) + gold (589 s: dims 136, facts 357, marts 90). First
marts run: 7 revenue days, 6 funnel days, 13,071 products rated.

**Marts reconcile with the facts:** Σ net revenue 82,536,511.85 in both
`mart_daily_revenue` and `fct_orders`; 489,346 orders in both.

What the numbers say, and why (synthetic data, spec 12):

- No 2026-10-02 row: the stack did not run that day.
- Session conversion ~98 % and every real session adds to cart: the
  generator creates sessions only around order items (purchase sessions
  end in cart + purchase; the ~2 % without a purchase are cancel/return
  sessions). The funnel logic is right; the data makes it uninformative.
- Return rate falls to 0 on recent days: returns come days after delivery
  (spec 8.4, "returns restate the past"); those days are recomputed as
  returns land.
- Gross margin ~0.52 and average order value ~171 are steady: products are
  drawn uniformly with fixed price/cost ratios.

## P4: data loss, repair and durability — 2026-10-05 to 2026-10-07

**Loss (2026-10-04, before ADR 016).** The Docker VM died hard; Kafka
(one broker, no fsync) lost the unflushed tail of some partitions while
Connect's offsets past them survived. Found by the verify drill:

| Table | Missing | Stale | Repair (key-filtered blocking snapshot) |
|---|---|---|---|
| users | 6 | 4 | 10 rows in 46 ms (with order_items) |
| order_items | 69 | 27 | 96 rows |
| events (MongoDB) | 314 | 0 | 314 documents in 41 ms |
| orders, products, dist_centers, reviews | 0 | 0 | — |

After the repair, all 7 tables identical to the sources (2026-10-05).

**Kafka fsync on every write (ADR 016):** 43,253 -> 6,907 records/s flat
out (1 KB, acks=all); p99 2 -> 5 ms at a steady 200 records/s.

**Second unclean shutdown (2026-10-07, after ADR 016).** The laptop slept
at ~03:35 UTC on 10-06 and the Docker VM died on wake ("Recovering 144 logs
... no clean shutdown file was found"). Runbook procedure: writers paused,
bronze caught up (the interrupted batch 894 was replayed and skipped on
every table), silver run, verify drill:

| Table | Rows | Missing | Extra | Different |
|---|---|---|---|---|
| users | 59,408 | 0 | 0 | 0 |
| orders | 643,247 | 0 | 0 | 0 |
| order_items | 932,558 | 0 | 0 | 0 |
| products | 29,120 | 0 | 0 | 0 |
| dist_centers | 10 | 0 | 0 | 0 |
| events | 4,472,038 | 0 | 0 | 0 |
| reviews | 22,225 | 0 | 0 | 0 |

**Result: nothing lost** (6.16 M rows, every column). The drill also
exposed a stale ledger row (fixed in a8606a7) and needed longer S3 timeouts
on this laptop's resolver.

## P4: maintenance and cost — 2026-10-05

| Measure | Before | After |
|---|---|---|
| bronze `metadata.json` per busy table | 487 KB | 46 KB |
| stream batch duration (60 s trigger) | ~110 s | 34-45 s |
| stream driver download | 9.2 MB/min | 3.2 MB/min |
| hourly bronze expiry | 935 s (SQL procedure) | 36 s (Java API) |
| silver `orders` files | 9 data + 33 delete | 1 data + 0 delete |
| transform run, normal increment | 8.8 min, ~670 MB | 6.3-6.7 min, ~290-340 MB |

Full stack: ~0.8 GB of S3 transfer per hour up (transform ~0.6, stream
~0.2): the 100 GB free allowance covers ~125 hours a month. Measured with
container network counters (idle baseline removed); Spark's "bytes read"
also counts reads from its own cache and is used only to rank queries.

## P4: gold freshness (O2: source commit -> gold, < 1 hour) — 2026-10-07

**Method:** `drills/gold_freshness.py` (written before measuring): take the
newest order committed in PostgreSQL, poll Athena every 30 s until
`gold.fct_orders` returns it; latency = first successful query - the
order's `created_at`. Gold freshness depends on when a change lands
relative to the 30-minute transform runs, so both ends were measured on
steady, on-time runs: worst case = an order committed just after a run's
silver task read its bronze cut (it must wait for the next run); best case
= an order committed 3 minutes before a slot.

**Result: passed.**

| Case | Order committed | In gold | Latency |
|---|---|---|---|
| Worst (just after the 19:30 run read its cut) | 19:31:07 | 20:07:21 | **36.2 min** |
| Best (3 min before the 20:00 slot) | 19:57:03 | 20:07:12 | **10.1 min** |

Both orders reached gold through the 20:00 run (silver ~2.5 min, gold
facts merged ~7 min after the slot). Bound: 30-minute schedule + one run.

**What can break it (observed the same day):**

- DNS outages on this laptop: one run's silver task succeeded only on its
  third try (`UnknownHostException` for S3, then "Read timed out"), 33 min
  late; the job's retries and Airflow's 2 task retries absorbed it.
- After downtime: the first runs absorb the backlog (a 4-hour sleep gave a
  20-minute catch-up run), and the first maintenance run holds the `lake`
  pool (39 min once, before delete files were compacted first). Gold is
  stale from the downtime anyway; steady runs resume within ~1 hour.

## P5: online features freshness (source commit -> API, < 1 minute) — 2026-10-09

**Method:** `drills/features_freshness.py` (P5 acceptance, spec section 9):
open a MongoDB change stream on `web.events`, take the next product view
by a known user; its change event's `wallTime` is the commit time. Poll
`GET /users/{id}/features` every 0.5 s until that product is in
`recently_viewed` with the view's own time; latency = first seen - commit
(an upper bound, +0.5 s). MongoDB's clock (Docker's VM) vs the host's:
+5 ms. Stack: core + serving only (no AWS); generator at its usual rate
(~200 events per 10-second batch).

**Result: passed.** 10 samples, every one under 11 s:

| min | median | max | target |
|---|---|---|---|
| 4.5 s | 9.6 s | **10.6 s** | < 60 s |

The samples are close to the worst case, not the average: each one takes
the next view right after the previous one was served, i.e. right after a
batch ran, so it waits nearly a full trigger. Only the first (random phase)
shows a typical value. Bound: trigger (10 s) + batch time (~2 s) + capture
(< 1 s), about 12 s.

**API latency** (1,000 GETs over one kept-alive connection, from the host):
p50 **2.0 ms**, p99 **4.2 ms**, max 9.8 ms. During a Redis outage the API
answers 503 in 1.0 s (step 7).

**Footprint:** features-stream 1.3 GB (768 MB heap, 2 CPUs), Redis 35 MB of
data for ~175k keys (container ~45 MB), API 42 MB.

**Drill bugs found before the first valid run** (no pipeline change):
pymongo returns naive UTC datetimes and `.timestamp()` reads a naive one as
local time (UTC+1 here: every commit looked an hour old); with
`tz_aware=True`, `Database.command()` still decodes with pymongo's defaults
(offset -3600 s, all latencies negative, and the < 60 s check passed them).
The drill now uses tz-aware datetimes everywhere and fails on a negative
latency.

## P5: Redis outage and rebuild drills — 2026-10-09/10

### Redis stopped for 5 minutes under load (`drills/redis_outage.py`)

Generator at its usual rate; core, stream and serving profiles up.

| What | Result |
|---|---|
| API during the outage | 503 throughout, in 1.0 s per request; container unhealthy |
| features stream | first batch failed after ~85 s (client retries, each slowed by an 8 s DNS lookup of the stopped container), then restarted every ~90 s (3 restarts, uncapped restart policy) |
| bronze | kept committing: 3 batches, no error, no restart (95, 100, 128 s; 84-95 s just before, durations already drifting with the network, see below) |
| after Redis restart | API healthy in 0.5 s; features stream writing again in 3.2 s |
| freshness after recovery | 7.6-9.6 s (3 samples) |

Bronze has no Redis client and runs in another process (ADR 018): its
isolation is structural, and the drill shows it kept committing. One batch
of four was slow (128 s), in a stretch where durations were already
drifting up before the outage (84 s at 23:37, 94 s at 23:39); the next was
back to 97 s. That batch cannot be told apart from network noise with this
data.

**Bronze is slower than in P4 tonight**: 84-95 s per batch against 34-45 s
measured after the maintenance fix (2026-10-05), at similar batch sizes.
Not P5 (the features stream and the API never reach AWS; CPU and memory
have headroom: 4.5 GB free, no swap). The link to AWS was degraded: TCP
connect to S3 us-east-1 median 249 ms (max 808 ms), a full HTTPS request
median 890 ms (max 2.6 s), ping 62-172 ms. Consistent with the link, not
proven against a baseline (P4 recorded no network latency). P9: re-measure
in daytime and record latency next to batch durations.

### Rebuild from Kafka equals the live features (`drills/features_rebuild.py`)

Generator paused; live snapshot of every `user:*` key; Redis flushed and
the checkpoint deleted; the stream re-read the whole topic (~72 h,
~600k events); second snapshot; compared on what the API could serve at
that moment (live keys, members inside their window).

| Measure | Result |
|---|---|
| servable keys compared | 224,587 live, 224,587 rebuilt |
| identical (members, scores, cart fields, expiry to the ms) | **224,587 (100 %)** |
| rebuild time | ~138 s |
| features-stream memory peak during the rebuild | 1,412 MiB (2 CPUs) -> `mem_limit: 2g` |

Expiry from event time is what makes the TTLs match: a rebuild 15 minutes
later writes the same `PEXPIREAT` values. The raw key counts differ (225,120
live, 224,605 rebuilt) only by keys expired at the comparison time and
members outside their window, which Redis still held but nothing serves.

## P6: co-purchase graph and recommendations — 2026-10-10

### The generator's affinity (step 2, ADR 019)

Measured in PostgreSQL on the first ~2,700 orders after the change, with
companions and the popularity ranking recomputed **in SQL** from their
sha256 rule (same top product as Python: 27809):

| Measure | Measured | Expected |
|---|---|---|
| 2-item orders whose second item is a companion of the first | 0.574 | 0.6 (± 0.021) |
| top product's share of 1-item orders | 2.70 % | 2.9 % (± 0.39) |
| order rate | ~12,100 orders/h | (18,000 assumed in the first draft of ADR 019, corrected) |

Same-category rate of 2-item orders by day: **0.05 from 09-29 to 10-01**
(P1, upstream generator, no affinity: the random baseline), **0.59-0.63
since 10-03** (P2 affinity; model 0.6 + 0.4 x 0.051 = 0.62). The ~50,000
uniform P1 orders keep diluting any same-category figure computed over all
orders.

### Neo4j resources (step 3)

| Setting | Memory | Threads |
|---|---|---|
| idle, CPUs uncapped (32 visible) | 1.02 GiB | 119 |
| idle, `cpus: 2` | 0.8 GiB | 60 |
| peak while loading 491,014 pairs | 926 MiB | |

`mem_limit` 1.5 GB kept. The JVM sizes its thread pools from the CPUs it
sees: the P5 lesson again.

### Graph build (steps 5-7)

| Run | Pairs | Max weight | Products | Time (pairs + load) |
|---|---|---|---|---|
| first, 04:37 (~2.7 h of companion orders in gold) | 491,014 | 43 | 29,120 | 55 s (21.8 + 33.0) |
| rerun, then the DAG's first run | same fingerprint, 0 stale | | | DAG 64 s |
| preview, 18:51 (~6 h) | 513,400 | 123 | 29,120 | 123.5 s (89.5 + 34.0) |

Writer tests on a throwaway Neo4j: a rerun gives the same fingerprint; a
crash mid-run keeps the old graph and the next run converges; weights go
down when orders disappear (disabling the stale delete fails 2 tests).
**Correctness against the source**: for 27809 and its companion 13561, the
graph's weight (100) equals the number of PostgreSQL orders holding both
before the gold cut (100). Pair computation took 89.5 s at 18:51 against
21.8 s at 04:37 for a similar input: not investigated (S3 link or memory,
3.6 GB free); P9.

### API (steps 8-9), through the container

| Case | Result |
|---|---|
| warm recommendations request (curl from the host) | 15-45 ms |
| first query after Neo4j restarts (cold caches) | 1.99 s (server-side timeout 2 s) |
| API restarted, Neo4j warm | 27 ms: connecting is cheap, the cold cost is Neo4j's |
| Neo4j stopped | 503 in 1.0 s (the first one in 4.5 ms: dead pooled connection); `/ready` 503 naming neo4j; `/health` 200; container healthy; features 200 in 4 ms |
| Neo4j started again | 200 after 8.5 s, no API restart, no 500 in the API log |

A query past the 2 s timeout used to be a **500**: Neo4j reports it as a
`ClientError` (like a syntax error), which the API did not catch; found by
forcing a 0.2 s timeout in the container. It is now a 503 (`GraphTimeout`),
tested on a real Neo4j.

### Acceptance (spec section 9): `drills/recommendations_affinity.py`

**Method** (decided before any official result): (1) for the 20
most-purchased products (distinct orders in PostgreSQL), at least 4 of the
API's top 5 are the product's companions, recomputed from the sha256 rule;
(2) over a fixed sample of 1,000 products, the share of same-category
recommendations is at least **5x** the random baseline
(n_category - 1) / (n - 1). The drill fails with a wrong companion rule
(0/20, exit 1). **Which run counts** was fixed before running: a preview at
~6 h of companion orders, the official run after a rebuild at ~22:00 UTC
(~10 h), whatever its outcome.

| Run | (1) companions in the top 5 | (2) same-category share |
|---|---|---|
| preview, 18:52, ~6 h (does not count) | 20/20, every product 5/5; weakest companion weight 4 | 30.1 % vs 5.1 % = 5.9x |
| **official, 22:19, ~9.5 h** | **20/20, every product 5/5; weakest companion weight 10; nothing else in any top 5** | **31.4 % vs 5.1 % = 6.2x** |

**Result: passed.** Official run on the graph rebuilt at 22:18 from the
22:00 transform run (silver cut 22:00:00): 533,430 pairs, max weight 184,
29,120 products, 0 skipped, 65.6 s (pairs 27.2 s: the 89.5 s of 18:51 was
transient). The top product's five companions weigh 159-184. The
same-category share rises slowly (5.7x, 5.9x, 6.2x over the day) because
the P1 uniform orders still weigh on every product with few orders.
API tests: 21 in CI (fakeredis and a fake graph store), 7 more on a real
Neo4j (`make test-graph`).

## P7: contracts, quality checks and alerting — 2026-10-11

### Contracts and their cost (steps 2-6)

| Measure | Result |
|---|---|
| contracts | 26 (silver 8 incl. dead letters, gold 10, bronze 8), ODCS v3, linted in CI |
| checks on the live tables | 836 at first run; all pass |
| full run, one contract at a time / 8 at a time | 1,275 s / 159-166 s |
| full run in Athena | 216 queries, 0.78 GB scanned; billed at least 10 MB a query: ~$0.011 |
| `transform`'s subset (gold facts + dead letters) | 44 queries, 0.16 GB, ~$0.002 a run, ~$0.55 a month at our hours |
| drift: what `datacontract test` misses | a column in the data but not in the contract passes (step 0); `drift.py` catches it |

Running every contract every 30 minutes would cost ~$15.5 a month, all of
O6: Athena's minimum per query, not the bytes, sets the price. So the full
set runs daily (`quality_report`), a subset in `transform`.

### Least privilege, tested with the real keys (step 4)

| Identity | Allowed | Denied |
|---|---|---|
| `thelook-quality` | Athena in workgroup `thelook`, Glue `GetTable` | the `primary` workgroup, S3 writes in the layers, Glue table creation |
| `thelook-alerts` | `sns:Publish` on `thelook-alerts` | other topics, S3, listing topics |
| Postgres role `quality` | `SELECT` on the five captured tables | inserts; `shop.events` |
| MongoDB user `quality` | read on `web` | inserts |

The contract checks run as `thelook-quality` only: with the process's own
AWS key set to garbage they still pass.

### The daily report, first live run (step 10, 04:24)

| Check | Result |
|---|---|
| contracts | 26 of 26 pass, 166 s |
| drift | 0 of 26 |
| freshness | bronze 68 s (PostgreSQL) and 72 s (MongoDB) behind the source; gold 25 min |
| reconciliation, as of a cut | exact on all 7 tables (reviews: 25,899 in silver - 64 deleted at the source since silver's cut = 25,835) |
| health | Prometheus reachable, no alert firing |

Earlier the same night, freshness failed for gold (3.6 h behind after the
stack's downtime): a true failure, as designed.

### Alert delivery (steps 5, 8, 11)

| Path | Result |
|---|---|
| Prometheus rule or posted alert -> Alertmanager -> SNS -> email | delivered; 0 failed notifications |
| failed Airflow task -> `AirflowTaskFailed` | routed to the receiver without "resolved" email; with Alertmanager unreachable the callback logs and returns |
| MongoDB oplog window (new) | 152 h (wall clock, stack downtime included; size-capped at 2 GiB); alerts below 24 h and 6 h |

### Acceptance (spec section 9): schema changes

| Drill | Result |
|---|---|
| (a) compatible column (`shop.dist_centers.manager`, nullable) | new Avro version registered after 5 s, in bronze after 20 s, in silver on the next scheduled run (590 s source to silver); the drift check caught it (`not in the contract ['manager']`); contract updated, drift and tests pass |
| (b) incompatible type (`latitude` double -> text) | task failed 5 s after the change (`Schema being registered is incompatible`), `ConnectorTaskFailed` 96 s after; registry unchanged, nothing in Kafka or the lake; recovery (one ALTER, offset moved past the rejected change, resume) 110 s; `verify_silver.py dist_centers` ALL OK |
| (c) malformed MongoDB review (schemaless: invisible to the registry) | first run: **not** a dead letter: Spark nulled the two mistyped fields and kept the document; the silver `reviews` contract caught both required nulls (`missing_count` 1 each). Fixed (a field present in the JSON that parses to null makes a dead letter); rerun: in `dead_letters` with its raw payload, 14 min after insertion, and absent from silver. Its own email was not observed: the laptop slept through the hour the `dead_letters` rule watches (the alert path is proven by (b) and by `quality_report.reconcile`) |

**Result: passed** ((a) and (b): a compatible column flows end to end;
an incompatible change is caught and alerts). The drills also found two
bugs, fixed the same night: the dead-letter rule above, and a false alarm
in the reconciliation (a transform run committed silver between two of its
queries; each silver table is now read at one pinned snapshot).

The first run of (b) failed its recovery: the value was restored with an
UPDATE while the column was still text, a second incompatible change after
the skipped position. Fixed (one ALTER ... USING); the corrected script
then ran clean.

### Provisional revenue days (step 12)

Order to return, 515 returns in silver: p50 4.2 days, p90 9.9, p99 10.9,
max 11.1, on 12 days of history (right-censored). Window: 14 days;
`mart_daily_revenue.provisional_until` = order day + 14.

## P8: GDPR erasure, backfill and failure drills — 2026-10-13 to 2026-10-15

| Acceptance (spec section 9) | Evidence | Result |
|---|---|---|
| Erasure verified in every layer and store, incl. clickstream, Redis, time travel | `drills/verify_erasure.py`, a fresh user | 54/54 places empty; request to done 1 h 45 min (O5: 24 h) |
| Backfill leaves no duplicates | `drills/backfill_drill.py` | bug caught, repaired, mart equal to the pre-bug snapshot, `date_key` unique |
| Connect killed, Kafka restarted (FR14) | `drills/connect_kafka_restarts.py` | 0 lost, 0 duplicated |
| Debezium stopped for an hour (FR14) | `drills/slot-drill.sh` | alert emailed, catch-up 44 s, 0 lost, 0 duplicated |
| Offsets reset (FR14) | `drills/offsets_replay.py` | bronze duplicates 16,625 of 16,625, silver unchanged |
| Stream killed mid-batch (FR14) | `drills/bronze_stream_kill.py` | replay skipped the committed table, no offset twice |
| Neo4j frozen (P8 plan) | `drills/neo4j_freeze.py` | 503 within 2 s throughout, recovered in 0.1 s |

The two FR14 schema drills (nullable column, incompatible type) ran in P7
(`drills/schema_compatible.py`, `drills/schema_breaks.py`, above).

Defects the drills found, all fixed: an erasure attempt that died between
orders and items left 115 item rows (children are now deleted first); the
source check could not tell a change lost in a drill from one expired by
retention; MongoDB updates read an hour late have no document, which
silver would have turned into a row of nulls; a frozen Neo4j held a pooled
request for up to 120 s.

### GDPR erasure (step 6, acceptance part 1), 2026-10-13

`drills/verify_erasure.py` captures the user's keys, `request_erasure.py`
triggers `gdpr_erasure`, the verification checks every place of ADR 021's
map by user id and by captured key, and that time travel to every captured
snapshot fails.

| Run | User | Result | Request to done |
|---|---|---|---|
| 1 | 31 orders, 48 items, 193 events, 6 reviews, 7 Redis keys | 53/54: **115 bronze item rows left** (an attempt died after deleting the orders, so the retry could not find the items through them); repaired with `jobs/repair_erasure.py`, then 54/54 | sources 5 s, bronze wait 2.5 min, bronze purge 58 min, lake 26 min once it had the slot |
| 2 (every fix deployed, no manual step) | 34 orders, 45 items, 189 events, 5 reviews, 4 Redis keys | **54/54** | **1 h 45 min**: bronze purge 71 min, lake 29 min |

The bronze purge costs about an hour under load: each copy-on-write DELETE
re-reads its whole table to find the files (web_events: 11 min) before
rewriting them. Bronze misses O1 for that hour (accepted, ADR 021). The
raw user id appears 0 times in the task logs. Kafka keeps the change
events until its 3-day retention (O5).


### `drills/backfill_drill.py` on 2026-10-05..07 (step 9, acceptance part 2)

Transform paused (silver frozen), generator running. The bug, in the
working tree only: `net_revenue` = sum of gross amounts instead of net.
Days chosen with returns, so the rule
`net_revenue = gross_revenue - returns` has something to catch.

| Step | Backfill | Duration | Result |
|---|---|---|---|
| 2. baseline, committed code | 5 | 51.0 min | 3 days rebuilt, quality passed on all 3; mart snapshot `1815688829346899471` |
| 3-4. the bug | 6 | 21.5 min | 3 days rebuilt; quality **failed** on all 3: net = gross on each day |
| 5. reverted | 7 | 18:30-20:12 UTC | 3 days rebuilt; quality passed on the last day (below) |
| 6. compare | | | 15 columns x 3 days **identical** to the baseline snapshot (Athena time travel); no extra day; no duplicate `date_key` in the mart |

| Day | Baseline gross | returns | net | With the bug: net |
|---|---|---|---|---|
| 2026-10-05 | 23,759,507.17 | 1,865.39 | 23,757,641.78 | 23,759,507.17 |
| 2026-10-06 | 7,733,030.48 | 339.83 | 7,732,690.65 | 7,733,030.48 |
| 2026-10-07 | 10,354,598.08 | 58.99 | 10,354,539.09 | 10,354,598.08 |

**The contracts check whole tables.** The revenue rule's count of bad rows,
per day's quality task:

| Run | 10-05 | 10-06 | 10-07 |
|---|---|---|---|
| bug (backfill 6) | 1 | 2 | 3 |
| fix (backfill 7) | 2 | 1 | 0 |

Days run oldest first, one at a time, so each check also sees the days not
yet rewritten. The first buggy day's count of 1 is its own row (the
baseline was clean): that is the catch. Only the last fixed day sees every
day rewritten: that is the repair. The drill first required every fixed day
to pass, which a table-wide check cannot give; it now judges "caught" on the
first day and "repaired" on the last (ADR 021 amendment, runbook "Backfill").

**Interruptions, and what they showed.**

- Docker Desktop stopped with WSL during step 5 (every container exit 255,
  about 18:35). The drill script was killed with it: no `finally`, so
  transform stayed paused and steps 6-7 did not run. After `make up`,
  Airflow resumed backfill 7 by itself (the 10-05 `rebuild_day` retried at
  18:37). Steps 6-7 were then run with the drill's own functions; the
  baseline snapshot id, never printed, was recovered from
  `mart_daily_revenue$snapshots` (last commit before backfill 6 began).
- 10-07's rebuild failed once after 25 min on DNS (`UnknownHostException`
  on the lake bucket; the SDK's 6 attempts and Spark's 4 task tries all
  fell inside the outage). The day is replaced in one commit, so the failed
  attempt left the old rows; Airflow retried 5 min later and the job's
  transient-error retries carried it through more drops (DNS, refused
  connections, cut downloads) to its commit.

### `drills/connect_kafka_restarts.py` (step 10), 21:59-22:17 local

Core, stream and airflow profiles up; generator and review simulator
running. Bronze checked over the drill window from the captured end offsets
(`drills/verify_bronze.py`); sources checked against Kafka once the load
stopped (`drills/verify_cdc.py`).

| What | Result |
|---|---|
| Connect `kill -9`, 60 s dead, then started | both connectors RUNNING 8 s after the start (tasks 6 s), no FAILED task, no operator action |
| Kafka restarted (clean stop) | healthy after 10 s; connectors RUNNING at once |
| bronze stream | kept running, 0 restarts |
| sources = Kafka | all 7 sources: 0 missing, 0 extra, 0 differ |
| Kafka = bronze, per topic over the window | users 154, orders 1,579, order_items 2,268, events 7,753, reviews 168 committed records: bronze rows and distinct offsets equal, **0 lost, 0 duplicated** (products, dist_centers: no change) |

No duplicate from the crash: Connect writes records and source offsets in
one Kafka transaction (exactly-once source). The transaction open at the
`kill -9` is aborted; the changes are sent again after the restart, and
bronze reads `read_committed`, so it never sees the aborted copy. Offsets
are counted, not checked for gaps: aborted records and transaction markers
take offsets too (bronze holds 154 records over 267 offsets for users).

**The source check had to be fixed first.** A first run (21:37) gave the
same restart times and bronze = Kafka exactly, but exited 1: the MongoDB
check stopped on "topic no longer complete (retention)". Topics keep 3 days
and both MongoDB topics had been trimmed. The Postgres check had coped by
calling every row missing from a trimmed topic "outside retention", which
would also hide a change lost in this drill. `verify_cdc.py` now takes each
topic's horizon (timestamp of its first record still on the broker): a
missing row that changed after it is lost, one that changed before is
outside retention (`updated_at`, `created_at` for events; a user's address
update does not move `updated_at`). Tested both ways: with Connect stopped
for 30 s under load, it reported 6 users, 134 orders, 196 order items, 659
events and 10 reviews missing, all changed in those 30 s, and kept 6.26 M
events outside retention; caught up, every source matched.

### `drills/slot-drill.sh`: Debezium stopped for an hour (step 11), 21:52-23:01 UTC

Core, stream, airflow and monitoring profiles up; generator and review
simulator running throughout the stop.

| What | Result |
|---|---|
| retained WAL while Connect was down | 702 kB -> ~200 MB at minute 60 (~3.5 MB/min), `wal_status` reserved, cap 10 GB |
| alerts | `KafkaConnectDown` firing from minute 2; `ReplicationSlotInactive` firing at minute 31 (rule: inactive 30 min) |
| delivery | 3 SNS notifications sent, 0 failed; the `[thelook] FIRING: ReplicationSlotInactive` email reached the owner's inbox |
| verifier with Connect down | MISMATCH, as required: 542 users, 13,735 orders, 19,841 order items, 73,676 events, 687 reviews missing, all changed during the hour |
| catch-up | slot confirmed the last LSN 44 s after `up` (worker start-up included) |
| sources = Kafka after catch-up | all 7 OK (reviews after the fix below) |
| Kafka = bronze over the drill | users 1,591, orders 14,982, order_items 21,638, events 73,681, reviews 1,678: **0 lost, 0 duplicated** |

The drill script counted emails on `integration="email"`; the label is the
integration type, `sns` here (`email` is SMTP, unused), so it would have
reported "no email sent". Fixed before the run; it also requires
`ReplicationSlotInactive` to have fired, since the SNS counter counts every
alert.

**A defect found: MongoDB updates read an hour late have no document.**
The verifier crashed on a review update with `after = null`. The connector
looks an update's document up when it reads the event
(`change_streams_update_full`), not when it happened: a review updated then
deleted during the hour no longer existed when Connect caught up. Bronze
held 9 such updates, all from the drill hour. Silver read them as updates
and would have set every field of the review to null, until the delete that
follows; tonight each delete came in the same transform run and won, so
silver held 0 null rows. Silver and the verifier now read an update without
a document as a delete (a test fails without the fix).

### `drills/bronze_stream_kill.py`: bronze stream killed mid-batch (step 12), 00:05-00:18 local

Load running. The drill follows the stream's log and kills the driver JVM
(`pkill -9 java` inside the container: a crash, so Docker's restart policy
must act; `docker kill` is a manual stop, never restarted) right after a
bronze table commits: that table holds the batch, the ledger row and
Spark's checkpoint commit do not.

| What | Result |
|---|---|
| kill | after `shop_dist_centers` committed in batch 2184 |
| restart | by Docker's `on-failure` policy, 2 s after the kill |
| replayed batch 2184 (first batch after the restart, 4.5 min later) | `shop_dist_centers=skipped(replay)`, the other six tables written |
| Kafka = bronze, window from the capture | all 7 topics exact (users 277, orders 2,487, order_items 3,617, events 12,123, reviews 243) |
| bronze rows against distinct offsets, everything ingested since 22:30 UTC | equal in all 7 tables (dist_centers 30, products 87,360, events 95,003, ...): **no offset appended twice** |

Spark replays batch 2184 from its offset log (no commit file for it); the
skipped table found the batch's mark (query id, batch id) in its snapshots
and appended nothing; the ledger row was written once, after all seven.

The capture window did not cover batch 2184 whole: a batch reads from where
the previous one ended, planned when that one started, so 2184 began before
the capture and its `dist_centers` rows were outside the window. The
offsets check since 22:30 covers it. The script now kills only in the third
batch after the capture, which starts after it.

### `drills/neo4j_freeze.py`: Neo4j frozen (step 13), 2026-10-15 ~01:40 UTC

`docker pause neo4j` for 120 s (Souhail's idea): a paused process keeps
its TCP connections open and never answers, unlike a stopped one. Neo4j,
Redis and the API up (not the features stream: 4 GB free with the rest).

| Run | First request (pooled connection) | Later requests | `/ready`, `/health` | After unpause |
|---|---|---|---|---|
| 1, defaults | **no answer within the client's 30 s** | 503 in 1.01 s | 503 naming neo4j, 200 | 200 after 0.1 s, no API restart |
| 2, keep-alive 1 s x 2 | **503 in 2.00 s** | 503 in 1.00-1.01 s | 503 naming neo4j, 200 | 200 after 0.1 s, no API restart |

**Why run 1 hung.** The 2 s query timeout (`neo4j.Query(timeout=2)`) is a
transaction timeout the server enforces: a frozen server enforces nothing.
The driver's only read timeout comes from the server's
`connection.recv_timeout_seconds` hint, which our Neo4j sent as 120 (its
keep-alive, 1 min, x 2 probes; read from the driver's debug log of the
handshake). A request on an open pooled connection waited up to 120 s,
holding an API thread. New connections failed in 1 s: the paused
container's kernel still completes the TCP handshake, but the Bolt
handshake gets no answer and the 1 s connection timeout ends it.

**Fix:** `server.bolt.connection_keep_alive` 1 s, probes 2, scheduling
interval 1 s: the hint becomes 2 s (checked), and the server sends a NOOP
every second while a query runs, so only silence trips the timeout, never
a slow query. A client-side deadline thread in the API was rejected: each
timeout would leave a blocked thread and a held connection behind.

### `drills/offsets_replay.py`: connector offsets reset (step 12), 2026-10-15 01:34-02:46 UTC

The MongoDB connector, rewound through Connect's REST API (stop, `PATCH
/connectors/mongo-source/offsets` with an offset read from
`_connect-offsets`, resume). Not Postgres: the slot sends nothing before
its `confirmed_flush_lsn`, so a rewound Postgres offset replays nothing;
its only replay is a re-snapshot, measured in P4. Load stopped throughout,
so only the replay could change silver.

| What | Result |
|---|---|
| baseline | bronze complete and covered by the ledger, transform, silver snapshots recorded |
| replay | 14 min of change stream (01:20:54-01:34:55) in ~75 s: 16,277 events, 348 reviews |
| Kafka = bronze over the window | exact for every topic |
| **bronze duplicates** | 16,277 of 16,277 event rows and 348 of 348 review rows repeat an earlier row (same `doc_id`, `source_ts_ms`, `ord`) |
| **silver after the next transform** | events 7,518,404 rows, reviews 27,695: count, sum of positions and newest `_merged_at` equal to the baseline; the 16,277 and 310 replayed keys' rows identical in every column. The MERGE commits added and deleted nothing |

Silver ignores the replay because its MERGE applies an event only if its
position is newer than the row's (ADR 014); a replayed event has the same
position.

- The offset found 20 min before the stop held only a resume token
  (`sec = -1`), which pointed to 01:20: the replay was 14 min, not 20.
- The first attempt's baseline transform failed three times on DNS
  (`UnknownHostException` on the bucket); the drill now triggers up to three
  transform runs. The second attempt's final comparison, a full-table
  `EXCEPT` on silver events, hit the workgroup's 1 GiB scan limit; the
  comparison above (fingerprint + replayed keys) was run on the recorded
  snapshots and is now what the drill does.

### Graph job with the 2 s Neo4j read timeout (after step 13)

`graph` DAG triggered by hand: `rebuild_graph` succeeded on its first try
(1 min 55 s); recommendations served in 0.1 s afterwards. The server's NOOP
every second keeps a long write alive under the 2 s limit.

## P9: load test — method (written before measuring, B5)

**Question:** does the pipeline hold O8 (source-to-bronze lag below
2 minutes, p95, at 200 change events/s), where does it break, and which
component reaches its limit first? The P1 baseline never found the
capture side's breaking point: the generator capped itself near
25 iterations/s (A9).

**Path measured:** generator → PostgreSQL and MongoDB → Debezium → Kafka →
bronze stream (Spark, 2 cores, on the laptop) → S3 and Glue (one Iceberg
commit per table per batch). Silver and gold are batch jobs on a 30-minute
schedule (O2): not part of this path. Profiles `core monitoring stream`;
`serving` and `airflow` stopped, so that nothing else competes for the
laptop's CPU or the link.

**Load.** The generator in load mode (P9): pacing on a schedule (open
loop), random rows through the primary key, `GENERATOR_WORKERS` simulators
sharing the rate, and the index on `order_items (order_id)`. Before the
run, a ramp against Kafka alone shows it follows its target at every level
used. Levels, in iterations/s (about 10 change events each, measured, not
assumed): **10, 20, 40, 80, 160**, i.e. about 100, 200, 400, 800 and 1,600
change events/s. Each level: **3 minutes of warm-up, then a 10-minute
window**. Then the generator goes back to 5/s and bronze drains (the rows
of the window all committed) before the lag is computed and the next level
starts. Bronze has no backlog when the first level starts. The run stays
within one UTC day: bronze compacts the days before today, which would
rewrite the files the lag is read from. The run stops at the first level
that is not sustained, or when the lag passes 10 minutes (to bound cost).

**Metrics per level (window only)**

| Metric | Source | Why |
|---|---|---|
| Achieved iterations/s | the generator's `pace:` lines, summed over workers | The level counts only if the generator kept up: ≥ 95 % of target, under 1 s of backlog dropped per minute |
| Change events/s captured | `rate(kafka_connect_source_task_metrics_source_record_write)`, both connectors | The real load, in the unit of O8 |
| Capture lag | `max_over_time(debezium_streaming_millisecondsbehindsource)` | Source commit → Debezium (P1: under 1 s) |
| **Source-to-bronze lag, per row** | each bronze row with `source_ts` in the window: commit time of the Iceberg snapshot that added its data file − `source_ts`. Row → `_file` → `entries.snapshot_id` → `snapshots.committed_at`, in Spark right after the level (bronze keeps 1 to 2 hours of snapshots) | O8 itself: p50, p95 and max over every row of all 7 tables |
| Bronze batch duration | the stream's `batch N: ... in X s` log lines | Batches longer than the 60 s trigger run back to back: the backlog grows |
| CPU per container | `docker stats` every 30 s, mean over the window | Names the component at its limit |
| Link to AWS | `scripts/link_probe.py` before and after each level | The commits go over this link; the same batch took 34 s or 95 s depending on it (P4, P5) |

`ingested_at` is not used for the lag: it is set when a table's rows are
built, before the write and the commit, and the 7 tables commit one after
the other, so it would hide up to ~30 s.

**A level is sustained when both hold:** (a) source-to-bronze lag p95 under
120 s; (b) the lag is not growing: the p95 of the rows from the window's
last 3 minutes is at most one trigger (60 s) above that of its first 3
minutes. (b) catches a backlog that grows but has not yet pushed a
10-minute window's p95 past 120 s. The **throughput limit** is the
highest sustained level, in change events/s; the **breaking point** is the
next one. O8 passes if the level near 200 events/s is sustained.

**Hypothesis (to confirm or reject):** the bronze stream saturates first,
and the link more than its CPU. Each batch makes 8 commits one after the
other (7 tables and the ledger), each a few S3 and Glue round trips, about
4 s each from here (P3), so ~30 s of every batch is fixed; decoding and
writing grow with the batch. Once a batch takes longer than the 60 s
trigger, batches run back to back and the lag grows. The capture side
(Postgres, Debezium, Kafka) is not expected to limit: the ramp below
already shows Debezium within milliseconds near 950 events/s.

### The generator in load mode, against Kafka alone (step 5), 2026-10-17 ~02:30-03:10 UTC

2.5 minutes per level, `core monitoring` only (no Spark, no AWS). Achieved
= the generator's `pace:` lines, summed over workers; events/s = Connect's
record write rate, both connectors.

**Before the index** on `order_items (order_id)` (deadline pacing and
key-range picks already in):

| target it/s | workers | events/s | Debezium lag max | Postgres CPU | generator CPU |
|---|---|---|---|---|---|
| 30 | 1 | 262 | 14 ms | 113 % | 19 % |
| 60 | 4 | 608 | 29 ms | 311 % | 43 % |
| 120 | 8 | 945 | 12 ms | **611 %** | 112 % |

Postgres was the limit: an order's status update (40 % of iterations)
reads its items by `order_id`, and `order_items` (1.6 M rows) had no index
on it, so each one scanned the whole table (57 billion rows read over the
stack's life, `pg_stat_user_tables.seq_tup_read`). The GDPR erasure's
`DELETE ... WHERE order_id IN (...)` did the same.

**After the index:**

| target it/s | workers | achieved it/s | backlog dropped | events/s | Debezium lag max | Postgres CPU | generator CPU |
|---|---|---|---|---|---|---|---|
| 30 | 1 | 30.1 | 0 s | 316 | 109 ms | 5 % | 25 % |
| 60 | 4 | 60.7 | 0 s | 655 | 9 ms | 10 % | 42 % |
| 120 | 8 | 118.7 | 5 s | 1,260 | 6 ms | 25 % | 97 % |
| 200 | 8 | 131.2 | 164 s | 1,354 | 11 ms | 24 % | 121 % |
| 200 | 16 | 130.6 | 335 s | 1,316 | 30 ms | 27 % | 122 % |

- **A9 closed:** the generator follows its target up to ~120 iterations/s
  (~1,260 change events/s), 6 times O8's load, then caps itself: one
  Python process near a full core (the GIL: 16 workers give the same
  ~131/s as 8), and the dropped backlog says so in its own log.
- **The capture side did not break:** Debezium stayed within milliseconds
  at 1,354 events/s, Connect at 16 % CPU, Kafka at 5 %. Its limit is above
  what one generator process can produce; the load test's question is
  therefore bronze.
- ~10.4 change events per iteration, as in P1.

### Load test results (step 6, acceptance part 1), 2026-10-17 03:51-05:51 UTC

`drills/throughput_run.py`, method above unchanged; profiles `core
monitoring stream` (serving and airflow stopped; Grafana up from 04:09, a
few Prometheus queries every 30 s). Night link to S3 us-east-1: TCP connect
median 129-145 ms (one level started at 298 ms), HTTPS request 628-721 ms.
Levels 6 and 7 were added after level 5 (more generator containers, so
that the generator could keep up); level 7 did not run, since the run stops
at the first level not sustained. Logs: `drills/logs/throughput_run-
20261017T035111Z.log`, `-20261017T053127Z.log`.

| level (it/s / workers / containers) | window (UTC) | achieved it/s | backlog dropped (s/min, all workers) | change events/s | Debezium lag max (s) | bronze batch mean / max (s) | source-to-bronze lag p50 / p95 / max (s) | growth (s) | rows | CPU % bronze driver / executors / postgres / connect / generator | sustained |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 10/1 | 03:54:25-04:04:25 | 9.9 | 0.0 | 106 | 0.32 | 37 / 39 | 48 / 79 / 100 | +7 | 63,301 | 7 / 3 / 3 / 4 / 8 | yes |
| 20/2 | 04:12:36-04:22:36 | 20.0 | 0.0 | 210 | 0.13 | 48 / 113 | 53 / 105 / 138 | -42 | 126,220 | 4 / 3 / 6 / 4 / 17 | yes |
| 40/4 | 04:30:23-04:40:23 | 39.9 | 0.0 | 423 | 0.07 | 41 / 43 | 50 / 82 / 97 | +3 | 253,551 | 4 / 5 / 10 / 6 / 32 | yes |
| 80/8 | 04:49:12-04:59:12 | 80.0 | 0.7 | 843 | 0.49 | 45 / 66 | 52 / 84 / 104 | +5 | 505,334 | 4 / 2 / 17 / 11 / 70 | yes |
| 160/8/2 | 05:08:35-05:18:35 | 158.8 | 10.7 | 1,673 | 0.02 | 54 / 63 | 59 / 93 / 112 | -3 | 1,000,017 | 7 / 5 / 36 / 20 / 86 | NO |
| 240/8/3 | 05:34:42-05:44:42 | 213.6 | 160.4 | 2,230 | 0.63 | 62 / 71 | 65 / 102 / 121 | -11 | 1,341,755 | 5 / 10 / 66 / 40 / 102 | NO |

**O8 passes:** 210 change events/s with a source-to-bronze lag p95 of
105 s (limit 120 s), not growing. That window includes bronze's hourly
maintenance (04:12-04:16, in the stream's own background thread on the
same 2 cores): two batches took 96 s and 113 s, every other one 37-41 s.
Without it the p95 sits at 79-84 s, as at the levels on either side.

**Throughput.** The pipeline carried every load the generator could make:
up to **2,230 change events/s** (10.6 x O8) with a lag p95 of 102 s and no
growth. Levels 5 and 6 do not count by the method's rule: the generator
dropped backlog (10.7 and 160 s/min); at level 6 it reached 214 of 240
iterations/s with Postgres at 66 % CPU and Connect at 40 %. **So the
pipeline's breaking point, as the method defines it, was not reached; the
load generator stopped first, at ~2,200 events/s.**

**What saturates first, though, is measured: bronze.** Its batch time
fits **~36 s fixed + ~0.18 ms per record** at every level (37 s at 6,400
records, 54 s at 100,000; the catch-up's 200,000-record batches took
68-85 s, the same line). The fixed part is the 8 commits made one after
the other (7 tables and the ledger), each a few S3 and Glue round trips at
~130 ms; bronze's CPU stayed under 10 % (driver 4-7 %, executors 2-10 %).
At 2,230 events/s the batches averaged 62 s, just over the 60 s trigger:
bronze runs back to back, with no slack left. The model predicted 60 s.

- **Projected limits** (from the same line, not measured): a steady batch
  time T = 36 s / (1 - 0.18 ms x rate) once batches run back to back. The
  p95 lag fits **0.7 x max(60 s, T) + T** at every level (a row waits for
  the next batch to start, then for its table's commit): 79, 84, 96, 105 s
  predicted for 79, 84, 93, 102 s measured at levels 1, 4, 5, 6. It
  crosses O8's 120 s at T = 71 s, i.e. **~2,700 events/s**; beyond ~5,500
  records/s no batch length keeps up (T unbounded).
- **The capture side never limited:** Debezium's lag stayed under 0.7 s at
  every level, Kafka under 35 % CPU.
- **Hypothesis confirmed:** bronze first, on the link rather than CPU.
  The lever, not taken in P9: commit the 7 tables in parallel (the fixed
  36 s would become the slowest table's commit), or a shorter trigger for
  freshness at low load. Both change ADR 012's per-batch protocol (the
  ledger row is written once every table has committed); recorded as a
  future option, not a P9 change.
- Each level's start forces a checkpoint (the generator re-upserts 29,120
  products at start and the rate jumps): full-page writes raise the WAL
  rate ~5x for a minute and the slot retains up to ~127 MB (level 4,
  04:48). Inside the 3-minute warm-up, never in a window.

## P9: scan cost — method and baseline (steps 7-8)

**Question:** where does reading the lake cost money, how much did the
design save, and what does compaction add? Two different costs:

- **Athena** bills bytes scanned ($5/TB, 10 MB minimum per query), for
  analysts and dashboards.
- **S3 egress** from the laptop: the Spark jobs download what they read
  (P4: ~0.8 GB per hour of the full stack; free up to 100 GB a month, then
  $0.09/GB). This is the cost that grows.

**Method.** Athena: a fixed set of 5 analyst queries (`scripts/
scan_benchmark.py`: last 7 days of revenue, revenue by country over a
month, one day of items, the mart's last 30 days, revenue by category all
time), dates as literals relative to the newest order day, bytes from
Athena's own statistics. S3: received bytes on the containers that read
the lake during one transform run, minus an idle baseline measured just
before (`scripts/transform_bytes.py`, P4's method; Spark's own "bytes
read" counts its cache too). The counterfactual "full reload" is computed
from table sizes in Iceberg's metadata, labelled computed, not run.
Order: baseline, then compaction (the maintenance DAG), then the same
measurements again. The Athena baseline (03:21) was taken with the fixed
query set before this text was written, and before any change.

**Athena baseline, 2026-10-17 03:21 UTC** (gold: 15 order days, no
maintenance since 10-15):

| Query | Bytes scanned | Engine ms |
|---|---|---|
| q1 revenue, last 7 days (fct_orders) | 3,004,661 | 1,988 |
| q2 revenue by country, last month (fct_order_items + dim_user) | 15,853,498 | 2,543 |
| q3 one day of items | 20,143 | 1,101 |
| q4 revenue mart, last 30 days | 1,812 | 1,466 |
| q5 revenue by category, all time (fct_order_items + dim_product) | 14,696,053 | 2,268 |

| Table | Data files | Size | Rows in data files | Live rows |
|---|---|---|---|---|
| fct_orders | 31 | 159 MB | 2,697,334 | 1,083,483 |
| fct_order_items | 31 | 291 MB | 3,909,187 | - |
| dim_user | 4 | 12 MB | 179,244 | - |

- **What already saves:** columnar reads (q1 read 3 MB of fct_orders'
  159 MB: 1.9 %; q5 15 MB of 291 MB: 5 %) and Iceberg's per-file min/max
  statistics (q3 read 20 KB for one day without any partitioning: each
  transform run writes its orders into new files, so files are already
  grouped by day). Every query is billed the 10 MB minimum or a little
  more: ~$0.00005 each.
- **Step 9 (partition gold by order day) dropped on this evidence:** file
  statistics already prune by day; partitioning 15 days of data would
  save KBs. Recorded rather than built.
- **Compaction is the visible gap:** 60 % of fct_orders' stored rows are
  old versions masked by merge-on-read delete files (no maintenance since
  10-15); every read of the table still opens them.

## P9b: Grafana dashboards — 2026-10-17

Grafana 13.2.3 with the Athena plugin 3.3.6 (`bi` profile, ADR 023),
started from the repository with no volume.

- **Provisioned on a fresh start:** both data sources pass Grafana's health
  check (Athena with the marts-only `thelook-grafana` key), both dashboards
  appear in the *theLook* folder without a login; every panel's query run
  through Grafana's API: 11 of 11 pipeline panels, 8 of 8 business panels
  return data.
- **Found by running the panels, not by reading them:** a conversion of
  1.13 (the mart's `sessions` already excludes ghost sessions; subtracted
  twice), a return rate of 0.00 (Athena keeps 2 decimals when dividing two
  `decimal(38,2)`: 0.06 % rounds to 0), an empty ratings table (at most 7
  reviews per product, a threshold of 20).
- **Grafana 13 downloads ~17 plugins at each start** (Prometheus, Loki,
  Tempo, MySQL...), which also briefly unregistered Prometheus (every
  pipeline panel answered "Plugin not registered" while it updated itself
  from 13.2.1 to 13.2.3). With `GF_PLUGINS_PREINSTALL_DISABLED`, nothing is
  downloaded; Prometheus is the 13.2.1 bundled in the pinned image.
- **Athena per refresh** (from Athena's own query statistics):

| Refresh | Bytes scanned | Billed (10 MB minimum per scanning query) |
|---|---|---|
| cold (new query text) | ~440 KB (ratings 403 KB) | 8 x 10 MB = 80 MB, ~$0.0004 |
| warm (within 30 min) | 303 bytes; 7 of 8 answered from result reuse | 1 x 10 MB, ~$0.00005 |

  An open dashboard (30-min refresh, 30-min reuse): at most ~$0.02/day.
  "Provisional days" scans each time (`current_date` prevents reuse).
- The container refuses to start with an empty key: `set
  GRAFANA_AWS_ACCESS_KEY_ID in onprem/.env`, exit 1.
