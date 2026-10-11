"""quality_report: the daily quality checks (spec FR8, FR12; ADR 020).

Five independent tasks, each failing (and so alerting through Alertmanager)
on its own:

    contracts   every contract (25 tables and dead letters) in Athena
    drift       each contract's fields vs its table's columns in Glue
    freshness   source -> bronze (10 min) and source -> gold (1 hour) lag
    reconcile   source vs silver row counts, as of a cut
    health      Prometheus snapshot (fails only if Prometheus is unreachable)

All read-only: the lake as the quality user (Athena, Glue), the sources as
the `quality` role and user. The full contract run is ~216 Athena queries,
~$0.011 at Athena's 10 MB minimum per query: daily, not every 30 minutes
(P7 step 6; transform runs a cheap subset).
"""

from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

from alerting import alert_on_failure
from quality import CONTRACTS, PYTHON, contract_check

with DAG(
    dag_id="quality_report",
    description="daily: contracts, drift, freshness, reconciliation, health",
    # After maintenance (03:00) and graph (04:00).
    schedule="0 5 * * *",
    start_date=datetime(2026, 10, 11),
    catchup=False,
    max_active_runs=1,
    default_args={
        "on_failure_callback": alert_on_failure,
        "retries": 0,
        "execution_timeout": timedelta(minutes=20),
    },
    tags=["lakehouse", "p7"],
) as dag:
    contract_check("contracts", ["*/*.odcs.yaml"], workers=8)
    for check, script in [
        ("drift", "drift.py */*.odcs.yaml"),
        ("freshness", "report.py freshness"),
        ("reconcile", "report.py reconcile"),
        ("health", "report.py health"),
    ]:
        BashOperator(task_id=check, bash_command=f"cd {CONTRACTS} && {PYTHON} {script}")
