"""gdpr_erasure: remove one user from every source, layer and store (spec
FR9, FR12; ADR 021). Triggered per request, never scheduled:

    airflow dags trigger gdpr_erasure --conf '{"user_id": "<uuid>"}'
    (or drills/request_erasure.py <uuid>); the run id is the request id.

    delete_sources -> wait_bronze -> request_bronze -> wait_ack
      -> erase_redis -> audit_counts -> erase_lake

- delete_sources: PostgreSQL and MongoDB first, so that no re-snapshot or
  replay can bring the user back.
- wait_bronze: until bronze holds every delete event of that deletion; a
  request sent earlier could be overtaken by a change still in Kafka.
- request_bronze, wait_ack: the bronze stream purges bronze with its own
  key (ADR 017: no key that can delete bronze in Airflow) and logs it in
  thelook_bronze.erasure_log.
- erase_redis: the feature keys user:{id}:*.
- erase_lake: silver and gold (jobs/erase.py, batch user, pool "lake"),
  which also writes every step's counts to thelook_silver.erasure_audit.

The user id reaches the steps as an environment variable, never on a logged
command line, and every task masks it in its log (it still shows in the
spark-submit arguments, masked, and stays in the run's conf in Airflow's
database: docs/runbook.md). Steps print counts only.
"""

import json
from datetime import datetime, timedelta

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, Param, task
from airflow.sdk.log import mask_secret

from alerting import alert_on_failure

STEPS = "cd /opt/erasure && /opt/erasure-venv/bin/python steps.py"
JOBS = "/opt/lakehouse/jobs"
UUID_PATTERN = "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
REQUEST_ENV = {
    "ERASURE_USER_ID": "{{ params.user_id }}",
    "ERASURE_REQUEST_ID": "{{ run_id }}",
}


def mask_user(context) -> None:
    """Before each task: its log shows *** wherever the user id appears."""
    mask_secret(context["params"]["user_id"])


def step(task_id: str, command: str, timeout: timedelta, env=None) -> BashOperator:
    """One command of erasure/steps.py; its last output line (counts as
    JSON) becomes the task's XCom."""
    return BashOperator(
        task_id=task_id,
        bash_command=f"{STEPS} {command}",
        # Templated: the request's values are filled in at run time.
        env={**REQUEST_ENV, **(env or {})},
        # Keep the scheduler's environment (the eraser passwords, the quality
        # user's key) next to the request's values.
        append_env=True,
        execution_timeout=timeout,
        pre_execute=mask_user,
    )


@task(pre_execute=mask_user)
def audit_counts(ti=None) -> str:
    """Every earlier step's counts in one JSON object, for the audit."""
    counts = {}
    for task_id in ("delete_sources", "wait_ack", "erase_redis"):
        counts.update(json.loads(ti.xcom_pull(task_ids=task_id)))
    return json.dumps(counts)


with DAG(
    dag_id="gdpr_erasure",
    description="erase one user from every source, layer and store",
    schedule=None,
    start_date=datetime(2026, 10, 13),
    catchup=False,
    # One request at a time: each run waits on bronze and holds the lake pool.
    max_active_runs=1,
    params={
        "user_id": Param(
            type="string",
            pattern=UUID_PATTERN,
            description="the user's id, a lowercase UUID as in shop.users",
        )
    },
    default_args={
        "on_failure_callback": alert_on_failure,
        # Every step is idempotent (deleting what is gone deletes nothing; a
        # second request finds nothing in bronze).
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
    },
    tags=["lakehouse", "p8", "gdpr"],
) as dag:
    deleted = step("delete_sources", "delete-sources", timedelta(minutes=5))
    in_bronze = step(
        "wait_bronze",
        "wait-bronze",
        timedelta(minutes=35),
        env={"ERASURE_DELETED": "{{ ti.xcom_pull(task_ids='delete_sources') }}"},
    )
    requested = step("request_bronze", "request-bronze", timedelta(minutes=5))
    # The stream may first finish its upkeep, then scans bronze (P8: ~15 min).
    acked = step("wait_ack", "wait-ack", timedelta(minutes=65))
    redis = step("erase_redis", "erase-redis", timedelta(minutes=5))
    counts = audit_counts()
    lake = SparkSubmitOperator(
        task_id="erase_lake",
        application=f"{JOBS}/erase.py",
        application_args=[
            "{{ params.user_id }}",
            "{{ run_id }}",
            "{{ ti.xcom_pull(task_ids='audit_counts') }}",
        ],
        conn_id="spark_default",
        spark_binary="/opt/spark/bin/spark-submit",
        # One slot shared with transform and maintenance (ADR 017).
        pool="lake",
        # Measured: 26 min for the 10 tables (P8 step 4).
        execution_timeout=timedelta(minutes=90),
        pre_execute=mask_user,
    )
    deleted >> in_bronze >> requested >> acked >> redis >> counts >> lake
