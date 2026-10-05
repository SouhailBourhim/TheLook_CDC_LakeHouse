"""maintenance: daily upkeep of the silver and gold tables (ADR 017).

One spark-submit of jobs/maintenance.py, as the batch user: compact data and
delete files, expire snapshots older than a day, remove orphan files on a
few tables. Bronze is not here: the stream maintains it with its own key,
which never enters Airflow.

- 03:00 UTC; catchup=False: if the stack was down, the next start runs the
  latest missed slot once, not every missed day.
- Pool "lake" (one slot, shared with transform): the upkeep and transform's
  MERGEs never commit on the same table at the same time.
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
    dag_id="maintenance",
    description="silver and gold upkeep: compaction, expiry, orphans",
    schedule="0 3 * * *",
    start_date=datetime(2026, 10, 5),
    catchup=False,
    max_active_runs=1,
    default_args={
        # Compaction and expiry are idempotent: a retry is safe.
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "execution_timeout": timedelta(minutes=60),
    },
    tags=["lakehouse", "p4"],
) as dag:
    SparkSubmitOperator(
        task_id="silver_gold_upkeep", application=f"{JOBS}/maintenance.py", **SPARK
    )
