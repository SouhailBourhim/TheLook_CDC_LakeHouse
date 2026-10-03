# Source schema: what the generator actually writes

Spec section 4.1 asks for the source schema to be checked against the
generator before it is frozen in the data contracts. This document records
what the vendored generator (`onprem/generator/`, upstream commit `4f7537b`)
really does, read from the code, not from its README. Where this document
and the table in spec 4.1 disagree, this document is correct.

Sources: `src/models.py` (DDL and data rules), `generator.py` (simulation
loop and CLI defaults), `src/db_writer.py` (how rows are written).

## Summary

| Table | Primary key | Rows come from | Operations | Captured to the lake |
|---|---|---|---|---|
| `users` | `id` TEXT (UUID) | Seed at start + sign-ups | INSERT, UPDATE (address) | Yes |
| `orders` | `id` TEXT (UUID) | Every iteration | INSERT, UPDATE (status) | Yes |
| `order_items` | `id` TEXT (UUID) | Every iteration | INSERT, UPDATE (status) | Yes |
| `events` | `id` TEXT (UUID) | Until spec v2.0 only | INSERT only (frozen since P2) | No: moved to MongoDB `web.events` (ADR 008) |
| `products` | `id` BIGINT | `products.csv` at start | INSERT (re-written on every start) | Yes |
| `dist_centers` | `id` BIGINT | `distribution_centers.csv` at start | INSERT (re-written on every start) | Yes |
| `heartbeat` | `id` INT | Never written by the generator | Debezium's heartbeat query only | No (publication only, see B2) |

- **The generator never runs DELETE.** All deletes come from the synthetic
  erasure scripts (spec 4.3, v1.7).
- **No foreign keys and no NOT NULL constraints** (except `heartbeat.ts`).
  Referential integrity is only a convention of the generator, so the data
  contracts and dbt tests must check it.
- Default database `fh_dev`, schema `demo` (CLI flags `--db-name`,
  `--db-schema`). We will set our own names in commit 6.

## Tables

All timestamps are `TIMESTAMP WITHOUT TIME ZONE`, filled with Python's
`datetime.now()`, meaning the generator container's local time. The
container must run in UTC, otherwise the values are silently shifted.

### users

| Column | Type | Notes |
|---|---|---|
| id | TEXT PK | UUID v4 |
| first_name, last_name | TEXT | Faker; PII |
| email | TEXT | `first.last@<example domain>`; PII. Not unique. |
| age | INT | 12–70. Includes minors. |
| gender | TEXT | `M` / `F` |
| street_address | TEXT | Faker; PII |
| postal_code, city, state, country | TEXT | Weighted by population from `world_pop.csv` |
| latitude, longitude | DOUBLE PRECISION | Of the postal code, not the street; PII when combined with the rest |
| traffic_source | TEXT | Organic, Facebook, Search, Email, Display |
| created_at, updated_at | TIMESTAMP | Set at sign-up |

**Updates:** with probability `--user-update-prob` (default 0.1 per
iteration), the buyer's address changes: `street_address`, `postal_code`,
`city`, `state`, `country`, `latitude`, `longitude`.
**`updated_at` is not changed by an address update.** The SCD2 dimension
must therefore take validity dates from the change event (commit timestamp /
LSN), never from `updated_at`. Spec 4.3's synthetic address-change script
is not needed.

### orders

| Column | Type | Notes |
|---|---|---|
| id | TEXT PK | UUID v4 |
| user_id | TEXT | Buyer; no FK |
| status | TEXT | See state machine below |
| num_of_items | INT | 1 (70%), 2 (20%), 3 (5%), 4 (5%) |
| created_at, updated_at | TIMESTAMP | `updated_at` changes only when the status changes |
| shipped_at, delivered_at, cancelled_at, returned_at | TIMESTAMP | NULL until the matching transition |

Status values are **Processing, Shipped, Delivered, Cancelled, Returned**
(spec 4.1 says "Complete"; the code says "Delivered"). Transitions, applied
to one random order with probability `--order-update-prob` (default 0.4 per
iteration):

```text
Processing --95%--> Shipped --100%--> Delivered --2%--> Returned
     \--5%--> Cancelled
```

Cancelled and Returned are terminal. A Delivered order that is not returned
(98% of draws) also stays unchanged.

### order_items

| Column | Type | Notes |
|---|---|---|
| id | TEXT PK | UUID v4 |
| order_id | TEXT | No FK |
| product_id | BIGINT | Random product; no FK |
| status | TEXT | Always copied from the order |
| quantity | INT | 1–3 |
| sale_price | DOUBLE PRECISION | The product's `retail_price`, for one unit (not multiplied by quantity) |
| created_at, updated_at, shipped_at, delivered_at, cancelled_at, returned_at | TIMESTAMP | Copied from the order |

Updated together with its order: every item of the order is rewritten with
the order's status and timestamps.

### events (PostgreSQL until spec v2.0)

Frozen since P2: the patched generator writes events to MongoDB (see
"MongoDB" below); the rows here were copied there once. The description
below still holds for the documents' fields.


| Column | Type | Notes |
|---|---|---|
| id | TEXT PK | UUID v4 |
| user_id | TEXT | **NULL for anonymous ("ghost") sessions** |
| sequence_number | INT | 1..n within the session |
| session_id | TEXT | UUID v4. A new session **per order item**, so a 2-item order gives two sessions |
| ip_address | TEXT | Faker IPv4; PII |
| city, state, postal_code | TEXT | The user's, or random for ghosts |
| browser | TEXT | IE, Edge, Chrome, Safari, Firefox, Other |
| traffic_source | TEXT | Email, Adwords, Organic, YouTube, Facebook (a different list from `users.traffic_source`) |
| uri | TEXT | `/product/<id>`, `/department/<dept>/category/<cat>`, `/cancel/item/<order_item_id>`, ... The product and item ids exist only inside this string |
| event_type | TEXT | home, department, category, product, cart, purchase, cancel, return |
| created_at | TIMESTAMP | Back-dated: earlier events in a session are 20 s to a few minutes before the last one |

Append-only: every row has a new UUID, so the upsert is always a plain
insert. Session shapes:

- **purchase**: 1–3 of home/department/product, then cart, purchase.
- **cancel / return**: product, cart, cancel or return; written when an
  order moves to Cancelled or Returned.
- **ghost** (probability `--ghost-create-prob`, default 0.2): 3–6 random
  types, `user_id` NULL. They can include "purchase", "cancel" or "return"
  with no matching order, so funnels must not treat ghost purchases as sales.

### products

`id` BIGINT PK, `cost`, `retail_price` DOUBLE PRECISION, `category`, `name`,
`brand`, `department`, `sku` TEXT, `distribution_center_id` BIGINT.
29,120 rows from `products.csv`.

### dist_centers

`id` BIGINT PK, `name` TEXT, `latitude`, `longitude` DOUBLE PRECISION.
10 rows. The table is called `dist_centers`, not `distribution_centers`
as in spec 4.1.

### heartbeat

`id` INT PK, `ts` TIMESTAMP NOT NULL DEFAULT now(). Created by the
generator but never written by it. It exists for Debezium's heartbeat
action query (commit 11), which needs a captured table to write to (B2).

## MongoDB (since spec v2.0)

Database `web` on the replica set `rs0` (ADR 008). One document per record;
`_id` is the primary key. Fields that are null are left out, so documents
of one collection do not all have the same fields (schemaless). Dates are
BSON dates (UTC, millisecond precision).

### web.events

Written by the patched generator (`src/mongo_writer.py`), upserted by
`_id`, never updated: append-only. 1,756,486 historical rows were copied
from PostgreSQL by `scripts/seed_mongo_events.py`.

| Field | Type | Notes |
|---|---|---|
| _id | string | The event's UUID (was `events.id`) |
| user_id | string | **Absent for ghost sessions** |
| sequence_number | int | 1..n within the session |
| session_id, ip_address, city, state, postal_code, browser, traffic_source, uri, event_type | string | As in the PostgreSQL table above; `ip_address` is PII |
| created_at | date | Back-dated within a session, as before |
| product_id | int | **Cart events only** (synthetic, spec 4.3): the session's product. Absent on events before the patch |
| price | double | **Cart events only** (synthetic): that product's `retail_price` (float artifacts from `products.csv`, e.g. 34.9900016784668) |

### web.reviews

Written by the review simulator (synthetic, spec 4.3) for delivered order
items. Unique index `uniq_order_item` on `order_item_id`: one review per
item.

| Field | Type | Notes |
|---|---|---|
| _id | string | UUID |
| order_item_id | string | Unique; references `order_items.id` (no FK across databases) |
| product_id | int | The item's product |
| user_id | string | The buyer (`orders.user_id`) |
| rating | int | 1–5, skewed positive (5: 40 %, 4: 30 %, 3: 15 %, 2: 8 %, 1: 7 %) |
| title, text | string | Faker sentences |
| tags | array of string | 0–3 of fit, quality, value, comfort, style, shipping, size |
| helpful_votes | int | Starts at 0, incremented |
| created_at, updated_at | date | `updated_at` changes on every vote and edit |
| edited_at | date | **Only after an edit** |

Operations per second (1 action/s on average): create 60 %, helpful vote
25 %, edit 10 % (rating ±1, new text), delete 5 %. The deletes are the only
MongoDB deletes besides erasure requests.

### How documents reach Kafka

Debezium MongoDB connector, topics `thelook_mongo.web.events` and
`thelook_mongo.web.reviews`. `after` is the **whole document as a JSON
string** (MongoDB Extended JSON, `legacy` mode: dates as
`{"$date": <epoch ms>}`), for inserts and updates (`capture.mode =
change_streams_update_full`); `before` is always null (no pre-images); a
delete carries the `_id` only in the record key, followed by a tombstone.
The log position is `source.ts_ms` plus `source.ord`; there is no LSN.

## How rows are written, and what it means for CDC

1. **Every write is an upsert that rewrites every column.**
   `INSERT ... ON CONFLICT (id) DO UPDATE SET <all columns> = EXCLUDED.<col>`.
   Postgres writes a new row version even when no value changed, so
   Debezium emits an update event (`op = u`) whose values are identical.
   This happens:
   - when an order update draws a terminal or unchanged status (Cancelled,
     Returned, or Delivered and not returned). Over time most orders reach a
     terminal state, so **most order updates become no-op events** on
     `orders` and `order_items`;
   - **at every generator start**: all 29,120 products and 10 distribution
     centers are upserted again, so a restart produces a burst of about
     29,000 no-op product updates.
   Silver must not create a new version from an update that changes nothing
   (important for SCD2).
2. **Every start also inserts `--init-num-users` new users** (default
   1,000), not only the first start.
3. **Transaction boundaries mean nothing.** An order, its items and its
   events are written by three concurrent threads that share one database
   connection, and each upsert calls `commit()` itself. The three writes
   can land in separate transactions in any order, or one commit can carry
   another thread's rows. A failure can leave an order without items. The lake must not assume a whole order arrives at once.
4. **Money is `DOUBLE PRECISION`** (`cost`, `retail_price`, `sale_price`).
   Floating-point sums drift in the last digits; gold should cast to
   `DECIMAL`.
5. **No REPLICA IDENTITY is set**, so Postgres uses the default (primary
   key): update and delete events carry only the key in `before`. The
   choice is made in commit 10.
6. **Random picks use `ORDER BY RANDOM() LIMIT 1`** on `users` and `orders`,
   a full table scan every iteration. It gets slower as `orders` grows, so
   the generator itself may become the throughput bottleneck (B5, commit 15).
7. **No random seed**: runs are not reproducible.

## Rate

`--avg-qps` (default 20) sets iterations per second, spaced by an
exponential random delay. Expected rows per iteration from the code
(to be measured in commit 6):

| Table | Per iteration (approx.) |
|---|---|
| orders | 1 insert + 0.4 update |
| order_items | 1.45 inserts + ~0.6 updates |
| events | ~7 inserts (~6 purchase, ~0.9 ghost, a few cancel/return) |
| users | 0.05 inserts + 0.1 updates |

`events` is the high-volume table, about two thirds of all change events.

## Upstream problems found (fixed in later commits)

- The Dockerfile runs `data_generator.py`; the file is `generator.py`.
- The README lists wrong defaults (0 for the update probabilities) and
  wrong paths (`datagen/look-ecomm`).
- Leftover Kafka flags (`--bootstrap-servers`, `--topic-prefix`) are unused
  and have no client library in `requirements.txt`.
