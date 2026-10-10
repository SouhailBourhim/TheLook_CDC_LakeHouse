# 019. Co-purchase graph: companion products, full rebuild into Neo4j

- Status: Accepted
- Date: 2026-10-10 (P6; spec FR16, FR17, 4.3; ADR 010)
- Deciders: Souhail Bourhim (approves), Claude Code (drafts)

## Context

FR16 asks for `(:Product)-[:BOUGHT_WITH {weight}]->(:Product)` built from
gold order items, weight = number of orders containing both products, rebuilt
idempotently. FR17 serves the top-N neighbours at
`GET /products/{id}/recommendations`. ADR 010 chose Neo4j Community and fixed
that the graph holds products only (no personal data). P6 is accepted when
the recommendations follow the synthetic basket affinity.

Facts that shape the design:

- **The affinity as generated cannot produce weights (E2, open since P2).**
  29,120 products in 26 categories (37 to 2,363 products each). Orders have
  1 to 4 items (70 % have one), and each later item comes from the first
  item's category with probability 0.6, but the partner is drawn uniformly
  from ~1,100 products and the first item uniformly from all 29,120. A given
  pair repeats about once per 4,000 generator hours: every weight is 1 and
  "top 5 by weight" is a tie broken by id.
- `thelook_gold.fct_order_items` has ~1 M rows (one per item, with
  `order_id` and `product_id`); `dim_product` has every product. Erasure (P8)
  deletes a user's orders from gold.
- Neo4j Community runs a single user database (no second database to swap
  in) and has no roles: "all users have implied administrator privileges"
  (Operations Manual, managing users).
- Batch jobs run on the standalone cluster from Airflow, client mode, driver
  in the scheduler container, pool `lake` (ADR 013, ADR 017). The worker has
  4 cores; the bronze stream holds 2.

## Options considered

Making weights meaningful (spec 4.3, decided by Souhail on 2026-10-10):

| Option | Assessment |
|---|---|
| A. Fixed companion products per product, plus a popularity skew on the first item | Pairs repeat; popular products build clear weights within hours; the expected top 5 is known, so acceptance is exact |
| B. Companions only | Weights repeat, but each product anchors a multi-item order only every ~5 h at 5 iterations/s: weights stay at 1 to 3 for days |
| C. No change; accept on the share of same-category recommendations | No generator work, but the ranking by weight is never shown |

Rebuilding the graph:

| Option | Assessment |
|---|---|
| A. Delete everything, then load | Simple, but the API serves an empty or partial graph during every load |
| B. Incremental: add the new orders' pairs to the weights | Cheap per run, but `+=` is not idempotent under a retry and can never lower a weight when orders are erased |
| C. Recompute all pairs, upsert them tagged with the run id, then delete relationships of older runs | Idempotent, erasure-proof, no empty window; recomputes ~1 M rows every run |

Writing to Neo4j:

| Option | Assessment |
|---|---|
| A. Neo4j Connector for Apache Spark, from the executors | Parallel; another jar set to align with Spark 4, and parallel `MERGE`s on shared product nodes contend for the same locks (deadlocks, retries) |
| B. Python `neo4j` driver from the Spark driver, one writer, batches with `UNWIND` | No lock contention, no new jars; one writer is enough for 10^5 to 10^6 pairs |

## Decision

**Generator: option A** (spec v2.2, 4.3). Both additions are labelled
synthetic and can be switched off (`0` gives the previous behaviour):

- `--companions 5`: each product's companions are the 5 other products of
  its category with the smallest `sha256("<product id>:<candidate id>")`. A
  hash, not a seeded `random.Random`: Python only guarantees that `random()`
  repeats across versions, not `sample()` or `choice()`. The set is the same
  after any restart, in any language, so the drill can recompute it. With
  `--basket-affinity-prob 0.6`, each later item is a companion of the first
  item not already in the order; otherwise uniform, as upstream.
- `--popularity-skew 0.8`: the first item is drawn with Zipf weights
  `1 / rank^0.8`, the rank coming from `sha256("popularity:<id>")`. Shares:
  top product 2.9 % of orders, top 20 13.6 %, top 100 23.5 %. An exponent of
  1 would give the top product 9.2 %, too much for the marts; 0.6 gives the
  20th product too few orders per hour to separate in a session.
- Expected separation at the measured ~12,100 orders/h (P6 step 2; not one
  order per iteration at 5 iterations/s, as first assumed) and 0.45 later
  items per order: the 20th product anchors ~32 orders/h, so each of its
  companion pairs gains ~0.9 per hour while any other pair stays near 1.

**Pairs (Spark, `lakehouse/graph.py`).** Read `fct_order_items`, keep
distinct `(order_id, product_id)`, self-join on `order_id` with
`a.product_id < b.product_id`, count orders per pair. All statuses count:
the basket is chosen when the order is placed; a cancellation or a return
happens later, for reasons unrelated to which products go together, and
FR16 says "orders containing both". One relationship per unordered pair,
from the lower id to the higher, queried without direction: storing both
directions doubles the writes and leaves two weights that could disagree.

**Rebuild: option C.** Every run:

1. `CREATE CONSTRAINT … IF NOT EXISTS` on `Product.id` (also the index that
   makes `MERGE` and the API's lookup fast).
2. `MERGE` every product of `dim_product` as a node and `SET` its name,
   category, brand and department (29,120 nodes; a product with no
   co-purchase still exists, so the API can tell `[]` from unknown).
3. `UNWIND` the pairs in batches of 10,000: `MATCH` both nodes, `MERGE` the
   relationship, `SET r.weight, r.run = $run_id`.
4. Delete the `BOUGHT_WITH` relationships whose `run` differs, with
   `CALL { … } IN TRANSACTIONS` (bounded transactions).

A crash at any point leaves the previous graph with some weights already
updated, which the API can serve; the next run converges to the same graph.
Rerunning a finished run changes nothing.

**Writer: option B**, the `neo4j` Python driver from the job's driver
process (`toLocalIterator()`, so the pairs are never all in memory at once).

**Job and schedule.** `spark/jobs/graph.py`, run by a `graph` DAG once a day
(FR12), `catchup=False` (a stack started after days off runs once), in the
`lake` pool: the job only reads the lake, but the pool stops it from taking
the worker's last 2 cores while transform runs. It reads one Iceberg
snapshot, so commits made meanwhile do not affect it. It uses the batch IAM
user (read access to gold is enough) and needs Neo4j reachable from the
Airflow scheduler.

**Neo4j 2026.08.1 Community** (the calendar-versioned line the spec asks
for; 2026.09.0 was 4 days old; 5.26 is the LTS but frozen at Cypher 5),
pinned by digest, in the `serving` profile with a named volume. Heap 512 MB,
page cache 256 MB (the graph is tens of MB), `mem_limit` set from a
measurement; usage reporting off; ports bound to localhost. **One database
user**, password in `onprem/.env`: a second user for the API would be an
administrator too, so it would add a password, not a boundary. The API opens
read transactions (`execute_read`): a guard against mistakes, not a security
control. Python driver `neo4j` 6.3.1 (Python 3.10+, so the Spark image's 3.10
and the API's 3.12).

**API.** `GET /products/{id}/recommendations?limit=5` (`limit` 1 to 20, 422
outside): neighbours ordered by weight, then by id so ties are stable, with
name, category and weight. 404 when the product is not in the graph; `[]`
when it has no co-purchase yet; 503 when Neo4j is unreachable, within about
a second (driver `connection_timeout=1`, `max_transaction_retry_time=0`; the
defaults retry for 30 s, the P5 Redis lesson). `/health` checks both stores
and names the one that is down.

**Tests.** Pair logic with chispa (CI). The writer against a real Neo4j
(pytest marker `neo4j`, skipped without `NEO4J_URI`; CI does not pull the
380 MB image while Docker Hub's anonymous rate limit already failed a run).
The API with a fake graph dependency (CI), as with fakeredis.

## Consequences

- ✅ Weights carry the generator's affinity, and the expected top 5 of a
  popular product is computable, so P6's acceptance is exact rather than a
  share.
- ✅ The graph follows erasure (P8) without extra work: deleted orders are
  missing from the next recompute. The graph holds no personal data (ADR
  010), so it is not in the erasure workflow.
- ✅ Never an empty graph during a rebuild; reruns are no-ops.
- ❌ The skew changes every product-level figure in gold (popular products
  dominate the marts); labelled synthetic, as the affinity already is.
- ❌ Every run recomputes all pairs and rewrites every relationship's
  properties: fine at ~10^6 rows, would need an incremental design at
  production scale.
- ❌ Recommendations are up to a day old (daily DAG); new products have none
  until they are bought with something.
- ❌ No access control inside Neo4j Community; the database is reachable
  only on the Compose network and localhost.
- ❌ Orders placed before the change carry the old, diluted affinity; their
  pairs sit at weight 1 below the companions.

## References

- Neo4j Operations Manual: managing users (Community has no roles), memory
  configuration, Docker
- Cypher manual: `MERGE`, constraints, `CALL { … } IN TRANSACTIONS`
- Neo4j Python driver manual: transactions, configuration (timeouts, retries)
- Python `random` docs: notes on reproducibility
- Cahier des charges v2.2: 4.3, FR16, FR17; ADR 010, ADR 013, ADR 017
