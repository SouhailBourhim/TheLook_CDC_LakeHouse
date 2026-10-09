# 018. Online user features: a separate stream into Redis

- Status: Proposed
- Date: 2026-10-09 (P5; spec FR15, FR17; ADR 007, ADR 010)
- Deciders: Souhail Bourhim (approves), Claude Code (drafts)

## Context

FR15 asks for three per-user features in Redis, updated within a minute of
the event: the last 10 products viewed, the value of the current session's
cart, and the number of events in the last hour. FR17 serves them at
`GET /users/{id}/features`. ADR 010 fixed the principles (idempotent writes,
a TTL on every key, ghost sessions skipped, Redis rebuildable from the
stream) and left the key design and TTLs to P5. ADR 007 gives the Redis
writer its own checkpoint so that Redis being down never stops bronze.

Facts that shape the design:

- The Spark worker has 4 cores and 3 GB. The bronze stream holds 2 cores
  permanently and each Airflow batch job takes 2 more while it runs (P4).
- `web.events` is append-only (~7 inserts per generator iteration). Only cart
  events carry `product_id` and `price` (spec 4.3); product views name the
  product in `uri` (`/product/<id>`) only. Ghost sessions have no `user_id`.
- The generator writes a whole session at once with back-dated `created_at`
  values (`docs/source-schema.md`), so a purchase session's cart is usually
  checked out by the time it reaches Redis.
- Kafka keeps 3 days of events (`log.retention.hours=72`).

## Options considered

Where the features query runs:

| Option | Assessment |
|---|---|
| A. A second query inside `bronze_stream.py` (FR15 as written) | One driver. But it shares the stream's 2 cores with the bronze batches and the maintenance thread (O1 at risk), a features change restarts bronze, and a failed query needs a hand-written restart loop |
| B. A separate application on the shared cluster | Isolated, but it needs a free core: when transform runs (2 + 2 = 4 cores taken) it waits minutes and misses the 1-minute target, unless the worker or the batch jobs are resized |
| C. A separate application in Spark local mode, in its own container | Isolated, no core contention, no AWS identity at all; one more driver JVM (~0.7 GB) |

## Decision

**Option C** (Souhail, 2026-10-09). Spec v2.1 updates FR15's wording.

**The `features-stream` container** runs `spark/jobs/features_stream.py`
with `--master local[2]`, in a new `serving` Compose profile with Redis and
the API. It reads only `thelook_mongo.web.events` from Kafka and never
touches AWS, so it holds no IAM key (the stream key stays in bronze-stream
only), and the profile runs without `stream` or S3 costs. The same code runs
on the cluster unchanged if it ever needs to. Decoding reuses `lakehouse.cdc`
and `SchemaRegistry`; the document is parsed with silver's `EVENTS_JSON`.

**Keys.** Every key starts with `user:{id}:`, so erasure (FR9, P8) finds all
of a user's data with one pattern. Every value is a maximum or a set union,
so replaying any batch, in any order, leaves the same state:

| Key | Type | Content | Kept |
|---|---|---|---|
| `user:{id}:viewed` | sorted set | product id → time of its last view (ms) | the 10 newest |
| `user:{id}:events` | sorted set | event `_id` → event time | members within the last hour |
| `user:{id}:session` | sorted set | session id → time of its last event | the newest one |
| `user:{id}:cart:{session}` | hash | cart event `_id` → price; `purchased` when the session has a purchase | |

- `ZADD … GT` only raises a score, so an older replayed view cannot make a
  product look less recent. Trimming happens after each write
  (`ZREMRANGEBYRANK`) and always keeps the highest scores, so the result is
  the same whatever the arrival order.
- "Events in the last hour" is counted **at read time** (`ZCOUNT` from now
  minus one hour). An `INCR` counter would count a replayed batch twice and
  would never go down when the user stops.
- The cart is keyed by session, and the API reads the cart of the newest
  session. It returns the value and whether the session ended in a purchase,
  since most carts arrive already checked out.

**TTLs come from event time.** Each write sets `EXPIREAT` to the key's
newest event time plus the TTL: 72 hours for `viewed`, `session` and `cart`,
1 hour for `events`. 72 hours equals Kafka retention, so everything Redis
holds can still be rebuilt from Kafka. Because the expiry depends on the
data and not on when it was written, a replay or a full rebuild gives the
same TTLs. Events older than their TTL are dropped before writing, so a
catch-up writes nothing that would expire at once. `EXPIREAT … GT` treats a
key without a TTL as infinite and does nothing on a new key, so each write
sends `NX` (new key) and then `GT` (extend).

**Write path.** Each micro-batch is aggregated per user in Spark, then
written with `foreachPartition`, one non-transactional redis-py pipeline per
partition: nothing is collected to the driver. There is no batch-ID guard as
in bronze. The writes are idempotent, so at-least-once delivery gives an
exactly-once result.

**Stream settings.** Trigger every 10 seconds (Redis has no small-file cost;
it leaves most of the minute for capture and processing). `read_committed`,
`startingOffsets=earliest`, a bounded `maxOffsetsPerTrigger`, its own
checkpoint on the `spark-checkpoints` volume. `failOnDataLoss=false`, the
opposite of bronze: events that left Kafka before being read are older than
72 hours, so their features would already have expired; a gap in bronze would
be permanent. Redis errors are retried inside the batch; the container
restarts on failure.

**Redis 8** (exact version pinned when introduced):

- AOF with `appendfsync everysec` on a named volume. The cache and the
  stream's checkpoint must survive a restart together: an empty Redis behind
  a checkpoint that has moved on would stay silently empty until new events
  arrive. A hard crash can lose up to a second of writes; the runbook's
  rebuild (delete the checkpoint, flush `user:*`) repairs it.
- `maxmemory 256mb`, policy `volatile-ttl`: under pressure, evict the keys
  closest to expiring.
- ACL users, least privilege as for PostgreSQL and MongoDB: `features` reads
  and writes `user:*` keys only; `api` only reads them; `default` is
  disabled. Passwords in `onprem/.env`.

**API** (`api/`, FastAPI, Python 3.12): `GET /users/{id}/features` returns
the recently viewed products, the current cart (`null` when there is none)
and `events_last_hour`, as typed models (OpenAPI at `/docs`). 404 when the
user has no keys, 503 when Redis is unreachable, `/health` for the
container. Tested with TestClient and fakeredis.

## Consequences

- ✅ Bronze cannot be slowed or stopped by the features path; no new AWS
  identity, and `core` + `serving` demo P5 with no S3 cost.
- ✅ Correctness rests on data structures (max, union, read-time window),
  not on a replay guard; a rebuild from Kafka must give the same keys and
  TTLs, which the rebuild drill checks.
- ✅ One key prefix per user makes erasure a pattern delete.
- ❌ One more driver JVM (~0.7 GB) and one more checkpoint to manage.
- ❌ Local mode is not distributed; enough at demo volume.
- ❌ Features older than 72 hours disappear, and "last 10 products" holds
  distinct products (a repeated view moves a product up, it does not add a
  duplicate).
- ❌ "Purchased" carts reflect how the generator writes sessions; a real
  website would see the cart fill up before the purchase.

## References

- Redis docs: `ZADD` (GT), `EXPIREAT` (NX, GT, non-volatile keys), ACL,
  persistence (AOF), key eviction
- Spark Structured Streaming guide: `foreachBatch`, `failOnDataLoss`
- Cahier des charges v2.1: FR15, FR17; ADR 007, ADR 010
