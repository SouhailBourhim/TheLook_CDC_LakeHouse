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


def test_transform_runs_silver_then_the_gold_stages(bag):
    dag = bag.dags["transform"]
    chain = ["silver", "gold_dims", "gold_facts", "gold_marts"]
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
    for dag_id in ("transform", "maintenance", "graph"):
        for task in bag.dags[dag_id].tasks:
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
