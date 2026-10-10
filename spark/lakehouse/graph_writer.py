"""Idempotent load of the co-purchase graph into Neo4j (FR16, ADR 019).

Neo4j Community has a single database, so there is no second graph to build
and swap in. Instead every run:

  1. creates the uniqueness constraint on Product.id (also the index that
     the MERGEs and the API's lookups use);
  2. upserts every product node and every pair, stamping each with the run
     id, in batches of UNWIND (one transaction per batch);
  3. deletes the relationships, then the nodes, that this run did not
     stamp: pairs whose orders are gone (erasure), products that left
     dim_product.

The API is never served an empty graph: until step 3, it sees the old graph
with some weights already updated. A crash anywhere leaves that state, and
the next run converges to the same graph as a clean one.

One writer only (the job's driver process): parallel writers would MERGE the
same product nodes and contend for their locks.
"""

from collections.abc import Iterable, Iterator
from itertools import islice

import neo4j

BATCH = 10_000

CONSTRAINT = (
    "CREATE CONSTRAINT product_id IF NOT EXISTS FOR (p:Product) REQUIRE p.id IS UNIQUE"
)

UPSERT_NODES = """
UNWIND $rows AS row
MERGE (p:Product {id: row.product_id})
SET p.name = row.name, p.category = row.category, p.brand = row.brand,
    p.department = row.department, p.run = $run
"""

# MATCH, not MERGE, on the nodes: a pair whose product is not a node is
# skipped and counted, rather than creating a node with no properties.
UPSERT_PAIRS = """
UNWIND $rows AS row
MATCH (a:Product {id: row.product_a})
MATCH (b:Product {id: row.product_b})
MERGE (a)-[r:BOUGHT_WITH]->(b)
SET r.weight = row.weight, r.run = $run
RETURN count(r) AS written
"""

# CALL { } IN TRANSACTIONS commits every 10,000 deletions, so a large
# cleanup never builds one huge transaction. It needs an auto-commit
# transaction (session.run), not a managed one.
DELETE_STALE_PAIRS = """
MATCH (:Product)-[r:BOUGHT_WITH]->(:Product)
WHERE r.run IS NULL OR r.run <> $run
CALL (r) { DELETE r } IN TRANSACTIONS OF 10000 ROWS
"""
DELETE_STALE_NODES = """
MATCH (p:Product)
WHERE p.run IS NULL OR p.run <> $run
CALL (p) { DETACH DELETE p } IN TRANSACTIONS OF 10000 ROWS
"""


def _batches(rows: Iterable[dict], size: int) -> Iterator[list[dict]]:
    it = iter(rows)
    while batch := list(islice(it, size)):
        yield batch


def load_graph(
    driver: neo4j.Driver,
    nodes: Iterable[dict],
    pairs: Iterable[dict],
    run_id: str,
    batch: int = BATCH,
    database: str = "neo4j",
) -> dict[str, int]:
    """Make the graph equal to (nodes, pairs). Rows are dicts: nodes with
    product_id, name, category, brand, department; pairs with product_a,
    product_b, weight. Both are consumed lazily, a batch at a time.

    Returns counts: nodes and pairs written, pairs skipped (a product that
    is not a node), stale relationships and nodes deleted.
    """
    counts = dict.fromkeys(
        ["nodes", "pairs", "pairs_skipped", "stale_pairs", "stale_nodes"], 0
    )
    driver.execute_query(CONSTRAINT, database_=database)

    # Managed transactions (execute_query): a transient error, such as a
    # lock timeout, is retried by the driver; the batch is idempotent.
    for rows in _batches(nodes, batch):
        driver.execute_query(UPSERT_NODES, rows=rows, run=run_id, database_=database)
        counts["nodes"] += len(rows)
    for rows in _batches(pairs, batch):
        records, _, _ = driver.execute_query(
            UPSERT_PAIRS, rows=rows, run=run_id, database_=database
        )
        written = records[0]["written"]
        counts["pairs"] += written
        counts["pairs_skipped"] += len(rows) - written

    with driver.session(database=database) as session:
        summary = session.run(DELETE_STALE_PAIRS, run=run_id).consume()
        counts["stale_pairs"] = summary.counters.relationships_deleted
        summary = session.run(DELETE_STALE_NODES, run=run_id).consume()
        counts["stale_nodes"] = summary.counters.nodes_deleted
    return counts
