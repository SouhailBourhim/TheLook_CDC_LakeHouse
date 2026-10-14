"""backfill: recompute gold for past days from silver (spec FR10, FR12;
ADR 021). Runs only through

    airflow backfill create --dag-id backfill \\
        --from-date 2026-10-04 --to-date 2026-10-06 \\
        [--reprocess-behavior completed]   (to rerun days already done)

Airflow then creates one run per day; each run's logical date ({{ ds }}) is
the day jobs/backfill.py recomputes: the facts of the orders created and
the sessions started that day, then the daily marts, replacing what gold
holds for it. Rerunning a day gives the same rows, so duplicates cannot
appear.

The DAG stays unpaused, because Airflow starts no run of a paused DAG, not
even a backfill's (airflow/models/dagrun.py, get_queued_dag_runs_to_set_
running, joins on is_paused = false; found in P8 step 8). The daily runs
the scheduler creates on its own end at the first task, only_backfills, in
a second: the Spark job runs for backfill runs only.

- Pool "lake" (one slot, with transform and maintenance): a backfilled day
  never commits beside another MERGE.
- max_active_runs=1: days run one after the other.
"""

from datetime import datetime, timedelta

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.standard.operators.python import ShortCircuitOperator
from airflow.sdk import DAG

from alerting import alert_on_failure

JOBS = "/opt/lakehouse/jobs"


def is_backfill(dag_run=None, **_) -> bool:
    """True for a run created by `airflow backfill create`.

    run_type is the Task SDK's DagRunType, a str enum: compare it as a
    string. str() of it is "DagRunType.BACKFILL", which first made every
    backfill run skip (P8 step 8)."""
    return dag_run.run_type == "backfill"


with DAG(
    dag_id="backfill",
    description="recompute gold facts and daily marts for past days",
    # One run per day: backfill create enumerates the days from it. The
    # scheduler's own daily run is skipped by only_backfills.
    schedule="@daily",
    start_date=datetime(2026, 10, 1),
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs=1,
    default_args={
        "on_failure_callback": alert_on_failure,
        # A day is replaced as a whole: retrying is always safe.
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
        "execution_timeout": timedelta(minutes=60),
    },
    tags=["lakehouse", "p8"],
) as dag:
    only_backfills = ShortCircuitOperator(
        task_id="only_backfills", python_callable=is_backfill
    )
    rebuild = SparkSubmitOperator(
        task_id="rebuild_day",
        application=f"{JOBS}/backfill.py",
        application_args=["{{ ds }}"],
        conn_id="spark_default",
        spark_binary="/opt/spark/bin/spark-submit",
        pool="lake",
    )
    only_backfills >> rebuild
