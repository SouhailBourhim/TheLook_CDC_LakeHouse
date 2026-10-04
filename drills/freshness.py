# /// script
# requires-python = ">=3.12"
# dependencies = [
#   "boto3==1.40.40",
#   "psycopg[binary]==3.3.6",
#   "pymongo==4.18.2",
# ]
# ///
"""Measure O1: source commit -> row queryable in Athena (bronze).

Method (docs/results.md, P3): take a change just committed at the source
(the newest order written by the generator in PostgreSQL, or the newest
review written by the simulator in MongoDB), then query Athena every
POLL seconds until bronze returns it. Latency = time the first successful
query finished - the row's source_ts (Debezium source.ts_ms, the commit
time). Polling makes it an upper bound (+POLL s + Athena query time).

The query filters on today's source_ts partition and the row's id, so
each poll scans a slice of one table (cents for the whole run).

Usage: uv run drills/freshness.py [samples per source, default 3]
"""

import datetime
import sys
import time
from pathlib import Path
from urllib.parse import quote_plus

import boto3
import psycopg
from pymongo import MongoClient

POLL = 5
TIMEOUT = 600
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
        ]
        if state["State"] in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(0.5)
    if state["State"] != "SUCCEEDED":
        raise RuntimeError(state.get("StateChangeReason"))
    rows = athena.get_query_results(QueryExecutionId=qid)["ResultSet"]["Rows"][1:]
    return [[c.get("VarCharValue") for c in r["Data"]] for r in rows]


def newest_order() -> str:
    # Seq scan of shop.orders (no index on created_at): acceptable for a
    # drill run a few times, not something the pipeline does (O3).
    with psycopg.connect(
        host="localhost",
        dbname=env.get("POSTGRES_DB", "thelook"),
        user=env.get("POSTGRES_USER", "postgres"),
        password=env["POSTGRES_PASSWORD"],
    ) as conn:
        return conn.execute(
            "SELECT id FROM shop.orders ORDER BY created_at DESC LIMIT 1"
        ).fetchone()[0]


def newest_review() -> str:
    client = MongoClient(
        f"mongodb://{env.get('MONGO_ROOT_USER', 'root')}:{quote_plus(env['MONGO_ROOT_PASSWORD'])}"
        "@localhost:27017/?directConnection=true&authSource=admin"
    )
    doc = client["web"]["reviews"].find_one(sort=[("created_at", -1)])
    return doc["_id"]


def measure(table: str, id_expr: str, row_id: str) -> float:
    day = datetime.datetime.now(UTC).strftime("%Y-%m-%d")
    sql = (
        f"SELECT to_unixtime(min(source_ts)) FROM thelook_bronze.{table} "
        f"WHERE source_ts >= TIMESTAMP '{day} 00:00:00' AND {id_expr} = '{row_id}'"
    )
    start = time.time()
    while time.time() - start < TIMEOUT:
        rows = athena_rows(sql)
        if rows and rows[0][0] is not None:
            return time.time() - float(rows[0][0])
        time.sleep(POLL)
    raise TimeoutError(f"{table} {row_id} not visible after {TIMEOUT} s")


def main() -> int:
    samples = int(sys.argv[1]) if len(sys.argv) > 1 else 3
    results = []
    for i in range(samples):
        for name, table, id_expr, pick in (
            ("postgres order", "shop_orders", "after.id", newest_order),
            ("mongodb review", "web_reviews", "doc_id", newest_review),
        ):
            latency = measure(table, id_expr, pick())
            results.append((name, latency))
            print(f"sample {i + 1}: {name:15} visible after {latency:6.1f} s")
    lat = sorted(r[1] for r in results)
    print(
        f"n={len(lat)}  min={lat[0]:.1f} s  median={lat[len(lat) // 2]:.1f} s  max={lat[-1]:.1f} s"
    )
    return 0 if lat[-1] < 300 else 1  # O1 target: < 5 minutes


if __name__ == "__main__":
    sys.exit(main())
