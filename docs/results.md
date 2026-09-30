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

**O8 (to be approved by Souhail):** proposed target **200 change events/s
(≈ 20 iterations/s) with capture lag < 1 s**, below the observed maximum
(266/s) for margin, to be re-validated end to end in P2 with the sink's
consumer lag < 30 s.
