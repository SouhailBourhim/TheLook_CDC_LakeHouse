"""Contract checks as Airflow tasks (ADR 020), shared by transform and
quality_report.

The checks run in Athena as the read-only quality user (contracts/check.py
passes its key to each datacontract process), from datacontract-cli's own
virtualenv in the image; the contracts are mounted at /opt/contracts.
"""

from datetime import timedelta

from airflow.providers.standard.operators.bash import BashOperator

CONTRACTS = "/opt/contracts"
PYTHON = "/opt/datacontract/venv/bin/python"


def contract_check(
    task_id: str, contracts: list[str], workers: int = 4
) -> BashOperator:
    """A task that fails (and so alerts) if any of these contracts fails.
    No Airflow retries: check.py already retries checks that could not run,
    and retrying a real data failure would only repeat the same alert."""
    return BashOperator(
        task_id=task_id,
        bash_command=(
            f"cd {CONTRACTS} && {PYTHON} check.py --workers {workers} "
            + " ".join(contracts)
        ),
        retries=0,
        execution_timeout=timedelta(minutes=15),
    )
