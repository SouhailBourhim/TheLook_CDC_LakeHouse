# 015. Gold: star schema, SCD2 users, metric definitions

- Status: Proposed
- Date: 2026-10-04 (P4; spec FR4, FR5, FR6)
- Deciders: Souhail Bourhim (approves), Claude Code (drafts)

## Context

Silver holds the current state of each entity (ADR 014). Gold must answer
business questions (FR5): facts and dimensions in a star schema, a user
dimension with history (FR4), and one agreed definition per metric (FR6).
Constraints: jobs run on the laptop and read S3 over the internet (data
transfer out free up to 100 GB/month), every 30 minutes (O2: gold < 1 h
old); history in bronze starts with what Kafka still held when bronze
began (3-day retention) plus the P3 snapshot.

## Decision

### Dimensions: rebuilt every run (small, always consistent)

| Table | Grain | From | Notes |
|---|---|---|---|
| `dim_date` | one day, 2020-01-01 to 2030-12-31 | generated | `date_key` = yyyymmdd |
| `dim_product` | one product | silver `products` | money in `decimal(10,2)` |
| `dim_distribution_center` | one centre | silver `dist_centers` | |
| `dim_user` | **one version of a user** (SCD2) | **bronze** `shop_users` (full history) | below |

**`dim_user` (SCD2, FR4)**, rebuilt from every `shop_users` event in bronze:

1. order each user's events by log position (`lsn`, streamed before `r`,
   `kafka_offset`: the silver order);
2. hash the business columns; **an event whose hash equals the previous
   one starts no version** (no-op updates, and the snapshot rows that
   repeat the current state; FR3's "ignore no-ops" lives here);
3. `valid_from` = the version's commit time; `valid_to` = the next
   version's `valid_from`, or `9999-12-31` for the current version (a
   sentinel instead of NULL, so a join is just
   `ts >= valid_from AND ts < valid_to`); a delete closes the last version
   (`valid_to` = delete time, no current row);
4. **the first known version starts at the user's `created_at`**:
   history before bronze began was lost to Kafka retention, and without
   this an old order would match no version (documented limitation:
   address changes before bronze began are not recoverable);
5. `user_sk` = `xxhash64(user_id, lsn of the version's first event)`:
   deterministic, so a full rebuild gives every version the same key and
   facts keep matching; and unique, which `(user_id, valid_from)` would not
   be if two changes committed in the same millisecond.

Measured on 2026-10-04: bronze `shop_users` holds 66,700 events for 40,426
users (15,301 updates, no deletes), the oldest from 2026-10-03 19:00, while
users were created from 2026-09-29: changes made before bronze began are
not recoverable, hence point 4.

Rebuilding from the log every run is simple and always correct, even when
events arrive late or out of order; ~70,000 bronze rows make it cheap.

### Facts: incremental

| Table | Grain | Measures and keys |
|---|---|---|
| `fct_order_items` | one order item | `user_sk` (version valid at the order's `created_at`), `product_id`, `distribution_center_id`, `order_date_key`; `quantity`, `sale_price`, `gross_amount` = `sale_price x quantity`, `cost_amount` = `cost x quantity`; status flags; hours to ship / deliver |
| `fct_orders` | one order | items, amounts, status, timestamps, flags |
| `fct_sessions` | one session (events) | user (null for ghosts), `is_ghost`, start, end, events, viewed product / added to cart / purchased, cart value |

Each run recomputes only what changed: order items and orders whose silver
row was merged since the last gold run (silver's `_merged_at` is the
watermark, stored as a gold table property like silver's), and sessions
with new events (reading only recent `events` partitions). Results are
MERGEd on the grain key.

### Marts: recomputed for the affected days or products

`mart_daily_revenue` (per order day), `mart_session_funnel` (per session
day), `mart_product_ratings` (per product; reviews are small, rebuilt).

### Metric definitions (FR6), one place, tested

| Metric | Definition |
|---|---|
| Gross revenue | Σ `sale_price x quantity` over items **not cancelled** |
| Returns | Σ `sale_price x quantity` over items with status `Returned` |
| Net revenue | gross revenue − returns |
| Gross margin | (net revenue − Σ `cost x quantity` of non-cancelled, non-returned items) / net revenue |
| Average order value | net revenue / number of orders not cancelled |
| Cancellation rate | cancelled orders / all orders (per order day) |
| Return rate | returned items / delivered or returned items |
| Time to ship / deliver | hours from `created_at` to `shipped_at` / `delivered_at` (median per day) |
| Session conversion | sessions with a `purchase` event / sessions, **ghost sessions excluded** (they contain fake purchases, `docs/source-schema.md`) |
| Average rating | mean `rating` of reviews (deleted reviews excluded) |

Metrics for past days can change: a return restates the day of the
original order (spec 8.4); marts recompute the affected days.

## Consequences

- ✅ A star schema Athena can query directly; one definition per metric.
- ✅ Orders join the address valid when they were placed (FR4).
- ✅ Incremental facts and marts keep each run's S3 reads small.
- ❌ `dim_user` history is only as complete as bronze (from P3 onwards).
- ❌ Incremental facts trust silver's `_merged_at`; a full rebuild (backfill
  DAG, P8) is the fallback after a logic fix.
- ❌ Erasure (P8) must also remove a user's versions and facts here.
- ❌ `dim_user` holds PII (names, emails, addresses): analysts get masked
  access through Lake Formation (FR11, P8).

## References

- Kimball, *The Data Warehouse Toolkit*: SCD type 2, surrogate keys, fact grain
- Apache Iceberg docs: `REPLACE TABLE`, `MERGE INTO`
- Cahier des charges v2.0: FR4, FR5, FR6, spec 8.4 (returns restate the past)
