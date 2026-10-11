"""Alert on a task's final failure (ADR 020): post it to Alertmanager.

Alertmanager is the single alert hub; it emails through SNS. Every DAG sets
`on_failure_callback` in its default_args. Airflow calls it only when a task
has failed for good, after its retries (`on_retry_callback` would fire on
each retried attempt): a network blip the retries absorb alerts nobody.

The callback never raises: if Alertmanager is down (the monitoring profile is
not running), it logs a warning instead of hiding the task's own error.
"""

import json
import logging
import os
import urllib.request
from datetime import datetime, timezone

ALERTMANAGER = os.environ.get("ALERTMANAGER_URL", "http://alertmanager:9093")
log = logging.getLogger(__name__)


def build_alert(
    dag_id: str, task_id: str, try_number: int, error: str, log_url: str
) -> dict:
    """One Alertmanager alert (API v2). No endsAt: Alertmanager resolves it
    after its resolve_timeout, and Airflow alerts go to a receiver that sends
    no "resolved" email (a failed task is not fixed by time passing)."""
    return {
        "labels": {
            "alertname": "AirflowTaskFailed",
            "severity": "critical",
            "dag_id": dag_id,
            "task_id": task_id,
        },
        "annotations": {
            "summary": f"{dag_id}.{task_id} failed after {try_number} attempt(s)",
            "description": error[:1000],
            "log_url": log_url,
        },
        "startsAt": datetime.now(timezone.utc).isoformat(),
    }


def post(alerts: list[dict]) -> None:
    request = urllib.request.Request(
        f"{ALERTMANAGER}/api/v2/alerts",
        data=json.dumps(alerts).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=5):
        pass


def alert_on_failure(context: dict) -> None:
    ti = context["ti"]
    alert = build_alert(
        ti.dag_id,
        ti.task_id,
        ti.try_number,
        str(context.get("exception") or "no exception recorded"),
        getattr(ti, "log_url", ""),
    )
    try:
        post([alert])
    except Exception as error:  # noqa: BLE001 - an alerting failure must not mask the task's
        log.warning("could not post the alert to %s: %s", ALERTMANAGER, error)
