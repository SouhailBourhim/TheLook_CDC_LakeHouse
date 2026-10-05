"""transform: bronze -> silver -> gold every 30 minutes (spec FR12, ADR 013).

Four tasks, one spark-submit each, in client mode against the standalone
cluster (the driver runs in the scheduler container, on the same Python 3.10
as the executors):

    silver -> gold_dims -> gold_facts -> gold_marts

- Every job is incremental and idempotent (ADR 014, ADR 015): a retry, or a
  run after downtime, simply processes what is new.
- catchup=False: missed slots are not replayed one by one; the next run's
  incremental read covers them.
- max_active_runs=1: runs never overlap, so silver and gold never interleave
  (the facts' watermark relies on it) and drivers do not stack up.
- Each job brings its own AWS identity (the batch user, via lakehouse.session)
  from the scheduler's environment: no AWS key in Airflow connections.
"""

from datetime import datetime, timedelta

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sdk import DAG

JOBS = "/opt/lakehouse/jobs"

# spark-submit is not on the image's PATH; the connection spark_default
# points to spark://spark-master:7077 (AIRFLOW_CONN_SPARK_DEFAULT).
SPARK = {"conn_id": "spark_default", "spark_binary": "/opt/spark/bin/spark-submit"}

with DAG(
    dag_id="transform",
    description="bronze -> silver -> gold (incremental)",
    schedule="*/30 * * * *",
    start_date=datetime(2026, 10, 5),
    catchup=False,
    max_active_runs=1,
    default_args={
        # Jobs are idempotent, so retrying is always safe; covers network
        # blips beyond the jobs' own S3/Glue retries.
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
        # A catch-up after hours of downtime took ~31 min for silver.
        "execution_timeout": timedelta(minutes=60),
    },
    tags=["lakehouse", "p4"],
) as dag:
    silver = SparkSubmitOperator(
        task_id="silver", application=f"{JOBS}/silver.py", **SPARK
    )
    gold_dims = SparkSubmitOperator(
        task_id="gold_dims",
        application=f"{JOBS}/gold.py",
        application_args=["dims"],
        **SPARK,
    )
    gold_facts = SparkSubmitOperator(
        task_id="gold_facts",
        application=f"{JOBS}/gold.py",
        application_args=["facts"],
        **SPARK,
    )
    gold_marts = SparkSubmitOperator(
        task_id="gold_marts",
        application=f"{JOBS}/gold.py",
        application_args=["marts"],
        **SPARK,
    )

    silver >> gold_dims >> gold_facts >> gold_marts
