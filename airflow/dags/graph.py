"""graph: daily rebuild of the Neo4j co-purchase graph from gold (FR16, ADR 019).

One spark-submit of jobs/graph.py, as the batch user (it only reads gold):
every pair is recomputed from fct_order_items and Neo4j is made equal to it
without ever being emptied. A rerun gives the same graph, so a retry is safe.

- 04:00 UTC, after maintenance (03:00); catchup=False: after days off, the
  next start runs the latest missed slot once.
- Pool "lake": the job writes nothing to the lake, but the pool keeps it
  from taking the worker's last cores while transform runs (bronze holds
  the other two).
- The driver runs in the scheduler, which holds NEO4J_URI and
  NEO4J_PASSWORD; Neo4j itself is in the serving profile, which must be up.
"""

from datetime import datetime, timedelta

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sdk import DAG

JOBS = "/opt/lakehouse/jobs"

# spark-submit is not on the image's PATH; the connection spark_default
# points to spark://spark-master:7077 (AIRFLOW_CONN_SPARK_DEFAULT).
SPARK = {
    "conn_id": "spark_default",
    "spark_binary": "/opt/spark/bin/spark-submit",
    "pool": "lake",
}

with DAG(
    dag_id="graph",
    description="gold -> Neo4j co-purchase graph (full idempotent rebuild)",
    schedule="0 4 * * *",
    start_date=datetime(2026, 10, 10),
    catchup=False,
    max_active_runs=1,
    default_args={
        # The rebuild is idempotent; a retry also covers Neo4j restarting.
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "execution_timeout": timedelta(minutes=60),
    },
    tags=["serving", "p6"],
) as dag:
    SparkSubmitOperator(
        task_id="rebuild_graph", application=f"{JOBS}/graph.py", **SPARK
    )
