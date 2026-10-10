"""Batch job: gold -> Neo4j co-purchase graph (FR16, ADR 019).

Recomputes every pair from fct_order_items (one Iceberg snapshot, so gold
commits made meanwhile do not mix in) and makes Neo4j equal to it with
lakehouse.graph_writer.load_graph: never empty, idempotent, a crash is fixed
by the next run.

Runs as the batch IAM user (read gold). The driver writes to Neo4j, so it
needs the neo4j client, installed in the Airflow image only: it runs from the
graph DAG, or by hand in the scheduler container:

  docker compose -f onprem/compose.yaml exec airflow-scheduler \
    spark-submit --master spark://spark-master:7077 /opt/lakehouse/jobs/graph.py
"""

import datetime
import logging
import os
import time
import uuid

import neo4j

from lakehouse.graph import co_purchase_pairs, product_nodes
from lakehouse.graph_writer import load_graph
from lakehouse.session import lake_session

logging.basicConfig(
    level=logging.INFO, format="[%(asctime)s] %(levelname)s: %(message)s"
)
log = logging.getLogger("graph")

GOLD = "lake.thelook_gold"

spark = lake_session(
    "graph",
    cores_max=2,
    conf={"spark.executor.memory": "1500m", "spark.sql.shuffle.partitions": "8"},
)
start = time.monotonic()

# Cached: counted once for the log, then streamed to Neo4j partition by
# partition (toLocalIterator), so the driver never holds every pair at once.
pairs = co_purchase_pairs(spark.table(f"{GOLD}.fct_order_items")).cache()
nodes = product_nodes(spark.table(f"{GOLD}.dim_product"))
stats = pairs.selectExpr("count(*) AS n", "max(weight) AS max_weight").first()
log.info(
    "pairs computed: %s (max weight %s) in %.1f s",
    stats["n"],
    stats["max_weight"],
    time.monotonic() - start,
)

# A new id per run: what the run did not stamp is deleted at the end.
run_id = f"{datetime.datetime.now(datetime.timezone.utc):%Y%m%dT%H%M%SZ}-{uuid.uuid4().hex[:8]}"
load_start = time.monotonic()
with neo4j.GraphDatabase.driver(
    os.environ.get("NEO4J_URI", "bolt://neo4j:7687"),
    auth=("neo4j", os.environ["NEO4J_PASSWORD"]),
) as driver:
    counts = load_graph(
        driver,
        (row.asDict() for row in nodes.toLocalIterator()),
        (row.asDict() for row in pairs.toLocalIterator()),
        run_id=run_id,
    )
log.info(
    "run %s loaded: %s in %.1f s (total %.1f s)",
    run_id,
    counts,
    time.monotonic() - load_start,
    time.monotonic() - start,
)
spark.stop()
