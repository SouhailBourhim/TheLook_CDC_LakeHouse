# /// script
# requires-python = ">=3.12"
# dependencies = [
#   "boto3==1.40.40",
#   "psycopg[binary]==3.3.6",
# ]
# ///
"""Measure O2: source commit -> order visible in gold (fct_orders, < 1 hour).

Method (docs/results.md, P4): take the newest order the generator has
committed in PostgreSQL, then query Athena every POLL seconds until
gold.fct_orders returns it. Latency = time the first successful query
finished - the order's created_at (the generator stamps it at insert, a
moment before the commit; the script prints the gap to "now" when it
samples). Polling makes it an upper bound (+POLL s + Athena query time).

Gold freshness depends on when a change lands relative to the transform
runs (every 30 minutes): run this right after a run has read its bronze
cut for the worst case, right before a run for the best case.

Usage: uv run drills/gold_freshness.py [label]
"""

import datetime
import sys
import time
from pathlib import Path

import boto3
import psycopg
from botocore.exceptions import BotoCoreError, ClientError

POLL = 30
TIMEOUT = 2 * 3600
UTC = datetime.UTC


def load_env(path: Path) -> dict:
    env = {}
    for line in path.read_text().splitlines():
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            env[k] = v
    return env


env = load_env(Path(__file__).resolve().parent.parent / "onprem" / ".env")
athena = boto3.Session(profile_name="thelook", region_name="us-east-1").client("athena")


def athena_rows(sql: str) -> list[list[str]]:
    qid = athena.start_query_execution(QueryString=sql, WorkGroup="thelook")[
        "QueryExecutionId"
    ]
    while True:
        state = athena.get_query_execution(QueryExecutionId=qid)["QueryExecution"][
            "Status"
        ]["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(1)
    if state != "SUCCEEDED":
        raise RuntimeError(f"Athena query {qid} {state}")
    rows = athena.get_query_results(QueryExecutionId=qid)["ResultSet"]["Rows"][1:]
    return [[c.get("VarCharValue") for c in r["Data"]] for r in rows]


def main() -> int:
    label = sys.argv[1] if len(sys.argv) > 1 else "sample"
    with psycopg.connect(
        host="localhost",
        dbname=env.get("POSTGRES_DB", "thelook"),
        user=env.get("POSTGRES_USER", "postgres"),
        password=env["POSTGRES_PASSWORD"],
    ) as pg:
        order_id, created_at = pg.execute(
            "SELECT id::text, created_at FROM shop.orders ORDER BY created_at DESC LIMIT 1"
        ).fetchone()
    created_at = created_at.replace(tzinfo=UTC)  # the generator writes UTC
    now = datetime.datetime.now(UTC)
    print(
        f"[{label}] order {order_id} created {created_at:%H:%M:%S} "
        f"({(now - created_at).total_seconds():.0f} s before sampling)",
        flush=True,
    )
    while (datetime.datetime.now(UTC) - now).total_seconds() < TIMEOUT:
        try:
            found = athena_rows(
                f"SELECT count(*) FROM thelook_gold.fct_orders WHERE order_id = '{order_id}'"
            )
        except (BotoCoreError, ClientError, RuntimeError) as error:
            # A network blip (this laptop's DNS) is a missed poll, not the
            # end of the measurement: it once ended a two-hour drill.
            print(f"[{label}] poll failed, retrying: {str(error)[:120]}", flush=True)
            time.sleep(POLL)
            continue
        if found and found[0][0] != "0":
            seen = datetime.datetime.now(UTC)
            minutes = (seen - created_at).total_seconds() / 60
            print(f"[{label}] in gold at {seen:%H:%M:%S}: {minutes:.1f} min", flush=True)
            return 0 if minutes < 60 else 1
        time.sleep(POLL)
    print(f"[{label}] not in gold after {TIMEOUT // 60} min")
    return 1


if __name__ == "__main__":
    sys.exit(main())
