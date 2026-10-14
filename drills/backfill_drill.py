# /// script
# requires-python = ">=3.12"
# dependencies = ["boto3==1.40.40"]
# ///
"""Backfill drill (P8 step 9; spec FR10, ADR 021): a logic bug reaches the
daily revenue mart through a backfill, the backfill's contract check catches
it, the fix is backfilled, and the mart is back to its exact pre-bug values.

  1. pause transform and wait until none of its tasks runs: silver, the
     backfill's only input, cannot change during the drill (bronze keeps
     ingesting). A run caught mid-way stays frozen until the unpause, then
     goes on; transform catches up incrementally
  2. baseline: backfill the days with the committed code. The incremental
     mart is not a fair reference (erasures before session 15 could leave
     it stale); a recompute is. Record the mart's snapshot id.
  3. the bug, in the working tree only (never committed): net revenue
     counts returns as revenue (sum of gross instead of net amounts)
  4. backfill: every day's rebuild_day succeeds, the first day's quality
     task fails (net_revenue <> gross_revenue - returns on days with
     returns)
  5. revert (always, also on a crash or Ctrl-C), backfill: the last day's
     quality task passes
  6. the days' rows equal the baseline snapshot (Athena time travel),
     column by column; date_key unique
  7. unpause transform

The quality task checks whole tables, so a day's result depends on the
days not yet rewritten (ADR 021 amendment): with the bug, only the first
day is bad when its check runs, and its failure is its own; repaired, only
the last day's check sees every day rewritten. The days between are not
judged.

Pick days with returns > 0, or the contract has nothing to catch. Needs the
core, stream and airflow profiles up, spark/lakehouse/gold.py without
local changes, and onprem/.env. The days' summary goes to drills/logs/.
Usage: uv run drills/backfill_drill.py <first day> <last day>  (YYYY-MM-DD)
Exit code 0 = caught, repaired, equal.
"""

import datetime
import json
import signal
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

import boto3
import botocore.exceptions

ROOT = Path(__file__).resolve().parent.parent
GOLD_PY = ROOT / "spark" / "lakehouse" / "gold.py"
LOGS = ROOT / "drills" / "logs"
API = "http://localhost:8088"
MART = "thelook_gold.mart_daily_revenue"
COLUMNS = [
    "date_key",
    "date",
    "orders",
    "cancelled_orders",
    "gross_revenue",
    "returns",
    "net_revenue",
    "gross_margin",
    "average_order_value",
    "cancellation_rate",
    "return_rate",
    "median_hours_to_ship",
    "median_hours_to_deliver",
    "provisional_until",
]
CORRECT = 'F.sum("net_amount").cast(AMOUNT).alias("net_revenue"),'
BUG = 'F.sum("gross_amount").cast(AMOUNT).alias("net_revenue"),'


def load_env() -> dict:
    env = {}
    for line in (ROOT / "onprem" / ".env").read_text().splitlines():
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            env[k] = v
    return env


ENV = load_env()
# A network drop here lasts up to 6 minutes: retry for 10 (40 x 15 s).
NETWORK_ATTEMPTS = 40
NETWORK_PAUSE = 15


def athena(sql: str) -> list[list[str | None]]:
    """Rows as strings, as the read-only quality user. A query only reads:
    on a dropped connection it is run again, for up to 10 minutes (this
    host's network drops last up to 6, P8 step 9)."""
    for attempt in range(NETWORK_ATTEMPTS):
        try:
            return athena_once(sql)
        except (
            botocore.exceptions.ConnectionError,
            botocore.exceptions.HTTPClientError,
        ):
            if attempt == NETWORK_ATTEMPTS - 1:
                raise
            time.sleep(NETWORK_PAUSE)


def athena_once(sql: str) -> list[list[str | None]]:
    client = boto3.client(
        "athena",
        aws_access_key_id=ENV["QUALITY_AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=ENV["QUALITY_AWS_SECRET_ACCESS_KEY"],
        region_name="us-east-1",
    )
    qid = client.start_query_execution(QueryString=sql, WorkGroup="thelook")[
        "QueryExecutionId"
    ]
    while True:
        status = client.get_query_execution(QueryExecutionId=qid)["QueryExecution"][
            "Status"
        ]
        if status["State"] in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(1)
    if status["State"] != "SUCCEEDED":
        raise RuntimeError(status.get("StateChangeReason", status["State"]))
    rows = client.get_query_results(QueryExecutionId=qid)["ResultSet"]["Rows"]
    return [[c.get("VarCharValue") for c in r["Data"]] for r in rows[1:]]


class Airflow:
    def __init__(self) -> None:
        self.token = self.call(
            "POST",
            "/auth/token",
            {"username": "admin", "password": ENV["AIRFLOW_ADMIN_PASSWORD"]},
        )["access_token"]

    def call(self, method: str, path: str, body: dict | None = None):
        token = getattr(self, "token", None)
        request = urllib.request.Request(
            f"{API}{path}",
            method=method,
            data=None if body is None else json.dumps(body).encode(),
            headers={
                "Content-Type": "application/json",
                **({"Authorization": f"Bearer {token}"} if token else {}),
            },
        )
        # A dropped connection (the host's network, P8) is retried for 10
        # minutes, except creating a backfill: a lost answer would create a
        # second one. 5 tries 10 s apart ended a run in step 2 (P8 step 9).
        attempts = 1 if path == "/api/v2/backfills" else NETWORK_ATTEMPTS
        for attempt in range(attempts):
            try:
                with urllib.request.urlopen(request, timeout=30) as response:
                    return json.load(response)
            except (urllib.error.URLError, TimeoutError):
                if attempt == attempts - 1:
                    raise
                time.sleep(NETWORK_PAUSE)

    def pause(self, dag: str, paused: bool) -> None:
        self.call("PATCH", f"/api/v2/dags/{dag}", {"is_paused": paused})

    def active_tasks(self, dag: str) -> int:
        """Tasks of the DAG running or queued. A paused DAG's running run
        starts no new task (the scheduler skips paused DAGs' runs,
        airflow/models/dagrun.py, get_running_dag_runs_to_examine): it
        never ends while paused, but its tasks do."""
        tis = self.call(
            "GET",
            f"/api/v2/dags/{dag}/dagRuns/~/taskInstances?state=running&state=queued",
        )
        return tis["total_entries"]

    def backfill(self, first: datetime.date, last: datetime.date) -> dict:
        """Run the backfill DAG over the days, rerunning days already done;
        return {day: {task: state}} once every run has ended. Days whose
        rebuild failed (network) get one more backfill: a comparison is
        only fair if every day was rebuilt."""
        start = time.monotonic()
        self.run_backfill(first, last, "completed")
        result = self.states(first, last)
        failed = [d for d, s in result.items() if s.get("rebuild_day") == "failed"]
        if failed:
            print(f"  rebuild failed on {failed}: one more backfill of those days")
            self.run_backfill(
                datetime.date.fromisoformat(min(failed)),
                datetime.date.fromisoformat(max(failed)),
                "failed",
            )
            result = self.states(first, last)
        print(f"  {(time.monotonic() - start) / 60:.1f} min")
        for day, states in sorted(result.items()):
            print(f"    {day}: {states}")
        return result

    def run_backfill(self, first, last, reprocess: str) -> None:
        backfill = self.call(
            "POST",
            "/api/v2/backfills",
            {
                "dag_id": "backfill",
                "from_date": f"{first}T00:00:00Z",
                "to_date": f"{last}T00:00:00Z",
                "reprocess_behavior": reprocess,
                "max_active_runs": 1,
            },
        )
        print(f"  backfill {backfill['id']} ({reprocess}) {first}..{last}")
        while not self.call("GET", f"/api/v2/backfills/{backfill['id']}")[
            "completed_at"
        ]:
            time.sleep(30)

    def states(self, first, last) -> dict:
        runs = self.call(
            "GET",
            "/api/v2/dags/backfill/dagRuns"
            f"?logical_date_gte={first}T00:00:00Z&logical_date_lte={last}T00:00:00Z",
        )["dag_runs"]
        result = {}
        for run in runs:
            tis = self.call(
                "GET",
                f"/api/v2/dags/backfill/dagRuns/{run['dag_run_id']}/taskInstances",
            )["task_instances"]
            result[run["logical_date"][:10]] = {t["task_id"]: t["state"] for t in tis}
        return result

    def busy(self) -> int:
        """Backfill tasks running or queued (an earlier backfill's)."""
        return self.active_tasks("backfill")


def date_keys(first: datetime.date, last: datetime.date) -> str:
    days = [first + datetime.timedelta(days=n) for n in range((last - first).days + 1)]
    return ", ".join(d.strftime("%Y%m%d") for d in days)


def mart_rows(keys: str, snapshot: str | None = None) -> dict:
    version = f" FOR VERSION AS OF {snapshot}" if snapshot else ""
    rows = athena(
        f"SELECT {', '.join(COLUMNS)} FROM {MART}{version} "
        f"WHERE date_key IN ({keys}) ORDER BY date_key"
    )
    return {r[0]: dict(zip(COLUMNS, r, strict=True)) for r in rows}


def current_snapshot() -> str:
    return athena(
        'SELECT snapshot_id FROM thelook_gold."mart_daily_revenue$snapshots" '
        "ORDER BY committed_at DESC LIMIT 1"
    )[0][0]


def tree_is_clean() -> bool:
    out = subprocess.run(
        ["git", "status", "--porcelain", "--", str(GOLD_PY)],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    return out == ""


def set_bug(on: bool) -> None:
    text = GOLD_PY.read_text()
    old, new = (CORRECT, BUG) if on else (BUG, CORRECT)
    if text.count(old) != 1:
        sys.exit(f"gold.py: expected one '{old}'")
    GOLD_PY.write_text(text.replace(old, new))


def main() -> int:
    if len(sys.argv) != 3:
        sys.exit(__doc__)
    # Progress shows as it happens, also when the output goes to a file.
    sys.stdout.reconfigure(line_buffering=True)
    first, last = (datetime.date.fromisoformat(a) for a in sys.argv[1:])
    if not tree_is_clean():
        sys.exit("spark/lakehouse/gold.py has local changes: commit or stash them")
    keys = date_keys(first, last)
    airflow = Airflow()
    summary = {"days": [str(first), str(last)]}

    # A closed terminal (SIGHUP) or a stop (SIGTERM) ends Python without
    # running finally blocks; as an exception they do, so transform is
    # unpaused. A killed run once left it paused for 12 hours (P8 step 9).
    def stop(signum, _frame):
        raise SystemExit(f"stopped by {signal.Signals(signum).name}")

    signal.signal(signal.SIGHUP, stop)
    signal.signal(signal.SIGTERM, stop)

    print("1. pause transform")
    airflow.pause("transform", True)
    try:
        while airflow.active_tasks("transform") or airflow.busy():
            time.sleep(30)
        print("2. baseline backfill (committed code)")
        baseline_states = airflow.backfill(first, last)
        baseline = current_snapshot()
        before = mart_rows(keys)
        summary["baseline"] = {"snapshot": baseline, "states": baseline_states}
        no_returns = [k for k, r in before.items() if r["returns"] in ("0.00", None)]
        if no_returns:
            print(f"  warning: no returns on {no_returns}: nothing to catch there")

        print("3-4. the bug, then backfill")
        set_bug(True)
        try:
            buggy_states = airflow.backfill(first, last)
            buggy = mart_rows(keys)
        finally:
            set_bug(False)
        summary["bug"] = {"states": buggy_states}
        for k, r in buggy.items():
            print(f"    {k}: gross {r['gross_revenue']} net {r['net_revenue']}")

        print("5. reverted, backfill")
        fixed_states = airflow.backfill(first, last)
        after = mart_rows(keys)
        summary["fix"] = {"states": fixed_states}

        print("6. compare with the baseline snapshot")
        then = mart_rows(keys, baseline)
        diffs = {
            k: {c: (then[k][c], after.get(k, {}).get(c)) for c in COLUMNS} for k in then
        }
        diffs = {
            k: {c: v for c, v in cols.items() if v[0] != v[1]}
            for k, cols in diffs.items()
        }
        diffs = {k: cols for k, cols in diffs.items() if cols}
        extra = sorted(set(after) - set(then))
        dupes = athena(
            f"SELECT date_key FROM {MART} GROUP BY date_key HAVING count(*) > 1"
        )
        summary["compare"] = {"diffs": diffs, "extra": extra, "duplicates": dupes}
        print(f"    differences: {diffs or 'none'}; extra days: {extra or 'none'}")
        print(f"    duplicate date_keys: {dupes or 'none'}")
    finally:
        print("7. unpause transform")
        airflow.pause("transform", False)
        LOGS.mkdir(exist_ok=True)
        out = LOGS / f"backfill-drill-{time.strftime('%Y%m%d-%H%M%S')}.json"
        out.write_text(json.dumps(summary, indent=2))
        print(f"summary: {out.relative_to(ROOT)}")

    def all_tasks(states: dict, task: str, state: str) -> bool:
        return bool(states) and all(s.get(task) == state for s in states.values())

    # Contracts that already fail on correct code would make "caught" void:
    # a clean baseline is what makes the first buggy day's failure its own.
    clean = all_tasks(baseline_states, "quality", "success")
    caught = (
        all_tasks(buggy_states, "rebuild_day", "success")
        and buggy_states[str(first)].get("quality") == "failed"
    )
    repaired = all_tasks(fixed_states, "rebuild_day", "success") and (
        fixed_states[str(last)].get("quality") == "success"
    )
    equal = not diffs and not extra and not dupes
    print(
        f"baseline clean: {clean}, caught: {caught}, repaired: {repaired}, "
        f"equal to baseline: {equal}"
    )
    return 0 if clean and caught and repaired and equal else 1


if __name__ == "__main__":
    sys.exit(main())
