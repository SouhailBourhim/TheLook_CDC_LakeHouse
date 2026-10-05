"""DAG checks, run in CI with Airflow installed exactly as in the image
(ADR 013): every DAG file parses, and transform has the agreed shape."""

from datetime import timedelta
from pathlib import Path

import pytest
from airflow.dag_processing.dagbag import DagBag

DAGS = Path(__file__).resolve().parent.parent / "dags"


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
