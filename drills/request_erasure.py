# /// script
# requires-python = ">=3.12"
# dependencies = []
# ///
"""The synthetic deletion request (spec 4.3): trigger gdpr_erasure for one
user through Airflow's REST API, and optionally wait for the run to end.

The user id goes in the request body (the run's conf), never in a URL. The
run id that Airflow returns is the request id: it is the key of the bronze
stream's erasure_log row and of the erasure_audit rows.

Needs the airflow profile up and AIRFLOW_ADMIN_PASSWORD (onprem/.env).
Usage: uv run drills/request_erasure.py <user_id> [--wait]
       exit code 0 = triggered (with --wait: the run succeeded)
"""

import json
import os
import sys
import time
import urllib.request
import uuid
from pathlib import Path

API = os.environ.get("AIRFLOW_URL", "http://localhost:8088")
DAG = "gdpr_erasure"


def env_file(name: str) -> str:
    """A value from the environment, else from onprem/.env."""
    if name in os.environ:
        return os.environ[name]
    for line in (
        (Path(__file__).parent.parent / "onprem" / ".env").read_text().splitlines()
    ):
        if line.startswith(f"{name}="):
            return line.split("=", 1)[1]
    sys.exit(f"{name} is not set")


def call(method: str, path: str, body: dict | None = None, token: str | None = None):
    request = urllib.request.Request(
        f"{API}{path}",
        method=method,
        data=None if body is None else json.dumps(body).encode(),
        headers={
            "Content-Type": "application/json",
            **({"Authorization": f"Bearer {token}"} if token else {}),
        },
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.load(response)


def main() -> int:
    if len(sys.argv) < 2:
        sys.exit(__doc__)
    user_id = sys.argv[1]
    if str(uuid.UUID(user_id)) != user_id:
        sys.exit("the user id must be a lowercase UUID, as in shop.users")
    token = call(
        "POST",
        "/auth/token",
        {"username": "admin", "password": env_file("AIRFLOW_ADMIN_PASSWORD")},
    )["access_token"]
    # A new DAG starts paused: unpausing is idempotent.
    call("PATCH", f"/api/v2/dags/{DAG}", {"is_paused": False}, token)
    run = call(
        "POST",
        f"/api/v2/dags/{DAG}/dagRuns",
        {"logical_date": None, "conf": {"user_id": user_id}},
        token,
    )
    run_id = run["dag_run_id"]
    print(f"request {run_id} triggered at {time.strftime('%H:%M:%S')}")
    if "--wait" not in sys.argv:
        return 0
    start = time.monotonic()
    while True:
        time.sleep(30)
        state = call("GET", f"/api/v2/dags/{DAG}/dagRuns/{run_id}", token=token)[
            "state"
        ]
        if state in ("success", "failed"):
            break
    print(f"request {run_id}: {state} in {(time.monotonic() - start) / 60:.1f} min")
    return 0 if state == "success" else 1


if __name__ == "__main__":
    sys.exit(main())
