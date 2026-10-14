"""DAG checks, run in CI with Airflow installed exactly as in the image
(ADR 013): every DAG file parses, and transform has the agreed shape."""

import sys
from datetime import timedelta
from pathlib import Path

import pytest
from airflow.dag_processing.dagbag import DagBag

DAGS = Path(__file__).resolve().parent.parent / "dags"
# Airflow puts the DAG folder on sys.path when it parses DAGs; so does this
# test, for the DAGs' shared helper (alerting.py).
sys.path.insert(0, str(DAGS))

from alerting import build_alert  # noqa: E402


@pytest.fixture(scope="module")
def bag():
    # bag.dags holds the DAGs parsed from the files; bag.get_dag() would also
    # query Airflow's metadata database, which a unit test does not have.
    return DagBag(dag_folder=str(DAGS))


def test_every_dag_file_parses(bag):
    assert bag.import_errors == {}


def test_transform_runs_silver_then_the_gold_stages_then_quality(bag):
    dag = bag.dags["transform"]
    chain = ["silver", "gold_dims", "gold_facts", "gold_marts", "quality"]
    for upstream, downstream in zip(chain, chain[1:], strict=False):
        assert dag.get_task(upstream).downstream_task_ids == {downstream}
    assert set(dag.task_ids) == set(chain)


def test_transform_never_overlaps_or_replays_missed_slots(bag):
    dag = bag.dags["transform"]
    assert dag.max_active_runs == 1  # silver and gold never interleave
    assert not dag.catchup  # the incremental read covers missed slots
    assert "*/30 * * * *" in repr(dag.timetable)


def test_tasks_retry_and_allow_catch_up_runs(bag):
    for task in bag.dags["transform"].tasks:
        if task.task_id == "quality":
            continue
        assert task.retries == 2
        assert task.execution_timeout == timedelta(minutes=60)
        # spark-submit is not on the image's PATH.
        assert task._spark_binary == "/opt/spark/bin/spark-submit"


def test_maintenance_runs_daily_without_replaying_missed_days(bag):
    dag = bag.dags["maintenance"]
    assert dag.task_ids == ["silver_gold_upkeep"]
    assert dag.max_active_runs == 1 and not dag.catchup
    assert "0 3 * * *" in repr(dag.timetable)


def test_graph_rebuilds_daily_after_maintenance(bag):
    dag = bag.dags["graph"]
    assert dag.task_ids == ["rebuild_graph"]
    assert dag.max_active_runs == 1 and not dag.catchup
    assert "0 4 * * *" in repr(dag.timetable)  # maintenance runs at 03:00
    assert dag.get_task("rebuild_graph").retries == 2


def test_every_spark_task_shares_the_one_slot_lake_pool(bag):
    # A compaction must never commit at the same time as a MERGE (ADR 017);
    # the graph job only reads, but must not take the worker's last cores.
    # Contract checks run in Athena, not on the cluster: they take no slot.
    for dag_id in ("transform", "maintenance", "graph", "gdpr_erasure", "backfill"):
        for task in bag.dags[dag_id].tasks:
            if task.task_type == "SparkSubmitOperator":
                assert task.pool == "lake", f"{dag_id}.{task.task_id}"


def test_every_task_alerts_on_its_final_failure(bag):
    for dag in bag.dags.values():
        for task in dag.tasks:
            callbacks = task.on_failure_callback
            callbacks = callbacks if isinstance(callbacks, list) else [callbacks]
            assert any(
                getattr(c, "__name__", "") == "alert_on_failure" for c in callbacks
            ), (
                dag.dag_id,
                task.task_id,
            )


def test_an_alert_names_its_dag_and_task():
    alert = build_alert("transform", "silver", 3, "boom" * 400, "http://log")
    assert alert["labels"] == {
        "alertname": "AirflowTaskFailed",
        "severity": "critical",
        "dag_id": "transform",
        "task_id": "silver",
    }
    assert (
        alert["annotations"]["summary"] == "transform.silver failed after 3 attempt(s)"
    )
    assert len(alert["annotations"]["description"]) == 1000


def test_transform_quality_checks_the_gold_facts_and_dead_letters(bag):
    task = bag.dags["transform"].get_task("quality")
    assert task.retries == 0  # check.py retries checks that could not run
    for contract in ["gold/fct_order_items", "gold/fct_orders", "silver/dead_letters"]:
        assert contract in task.bash_command


def test_quality_report_runs_its_five_checks_daily_after_the_graph(bag):
    dag = bag.dags["quality_report"]
    assert set(dag.task_ids) == {
        "contracts",
        "drift",
        "freshness",
        "reconcile",
        "health",
    }
    assert "0 5 * * *" in repr(dag.timetable)  # graph runs at 04:00
    assert dag.max_active_runs == 1 and not dag.catchup
    for task in dag.tasks:
        assert task.retries == 0 and not task.upstream_task_ids  # independent checks


ERASURE_CHAIN = [
    "delete_sources",
    "wait_bronze",
    "request_bronze",
    "wait_ack",
    "erase_redis",
    "audit_counts",
    "erase_lake",
]


def test_gdpr_erasure_runs_its_steps_in_order(bag):
    # Sources first; the request only once bronze holds their deletes.
    dag = bag.dags["gdpr_erasure"]
    for upstream, downstream in zip(ERASURE_CHAIN, ERASURE_CHAIN[1:], strict=False):
        assert dag.get_task(upstream).downstream_task_ids == {downstream}
    assert set(dag.task_ids) == set(ERASURE_CHAIN)


def test_gdpr_erasure_is_triggered_one_request_at_a_time(bag):
    dag = bag.dags["gdpr_erasure"]
    assert type(dag.timetable).__name__ == "NullTimetable"  # schedule=None
    assert not dag.catchup
    assert dag.max_active_runs == 1


def test_gdpr_erasure_accepts_only_a_lowercase_uuid(bag):
    from airflow.sdk.definitions.param import ParamValidationError

    param = bag.dags["gdpr_erasure"].params.get_param("user_id")
    assert param.resolve("6f1c2a3b-0d4e-4f5a-8b6c-7d8e9f0a1b2c")
    for bad in ["42", "6F1C2A3B-0D4E-4F5A-8B6C-7D8E9F0A1B2C", "x' OR '1'='1"]:
        with pytest.raises(ParamValidationError):
            param.resolve(bad)


def test_gdpr_erasure_never_puts_the_user_id_on_a_bash_command_line(bag):
    for task in bag.dags["gdpr_erasure"].tasks:
        if task.task_type == "BashOperator":
            assert "params" not in task.bash_command, task.task_id
            assert task.env["ERASURE_USER_ID"] == "{{ params.user_id }}"
            assert task.append_env  # keeps the scheduler's passwords
        # Every task masks the user id in its log.
        hook = getattr(task, "_pre_execute_hook", None)
        assert getattr(hook, "__name__", "") == "mask_user", task.task_id


def test_the_lake_erasure_goes_first_for_the_lake_slot(bag):
    # Default weights ("downstream": own weight + every downstream task's)
    # let transform and graph tasks take the slot first (P8 step 6).
    task = bag.dags["gdpr_erasure"].get_task("erase_lake")
    assert str(task.weight_rule).lower().endswith("absolute")
    for dag_id in ("transform", "maintenance", "graph", "backfill"):
        for other in bag.dags[dag_id].tasks:
            if other.pool == "lake":
                downstream = other.get_flat_relative_ids(upstream=False)
                assert task.priority_weight > other.priority_weight + len(downstream)


def test_backfill_recomputes_the_runs_day_for_backfill_runs_only(bag):
    dag = bag.dags["backfill"]
    assert dag.get_task("only_backfills").downstream_task_ids == {"rebuild_day"}
    # Airflow starts no run of a paused DAG, not even a backfill's.
    assert not dag.is_paused_upon_creation and not dag.catchup
    assert dag.max_active_runs == 1  # one day at a time
    task = dag.get_task("rebuild_day")
    # The run's logical date is the day recomputed.
    assert task.application.endswith("/backfill.py")
    assert task.application_args == ["{{ ds }}"]
    assert task.retries == 2  # a day is replaced as a whole


def test_only_backfill_runs_reach_the_spark_job():
    from types import SimpleNamespace

    from airflow.sdk.api.datamodels._generated import DagRunType
    from backfill import is_backfill

    # The type tasks really get (a plain string here first hid a bug).
    assert is_backfill(dag_run=SimpleNamespace(run_type=DagRunType.BACKFILL))
    for other in (DagRunType.SCHEDULED, DagRunType.MANUAL):
        assert not is_backfill(dag_run=SimpleNamespace(run_type=other))
