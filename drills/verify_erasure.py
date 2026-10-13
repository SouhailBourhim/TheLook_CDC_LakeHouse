# /// script
# requires-python = ">=3.12"
# dependencies = [
#   "boto3==1.40.40",
#   "psycopg[binary]==3.3.6",
#   "pymongo==4.18.2",
#   "redis==8.1.0",
# ]
# ///
"""Prove that a GDPR erasure reached every place of the map (ADR 021).

Once the erasure is done, nothing links an order item or an event back to
the user any more (that is the point), so the user's keys are captured
before the request:

  1. uv run drills/verify_erasure.py --capture <user_id>
     the user's order, item, event and review ids (sources), Redis key
     count, and every affected table's current snapshot id (Athena);
     written to drills/logs/ (git-ignored: keys identify a person)
  2. uv run drills/request_erasure.py <user_id> --wait
  3. uv run drills/verify_erasure.py <user_id> [<request_id>]
     every place must hold 0 rows of the user, by user id and by captured
     key; time travel to every captured snapshot must fail. With the
     request id: the bronze erasure_log and the erasure_audit hold it, and
     no affected table keeps a snapshot from before its purge (but the one
     kept).

Kafka is not checked: it keeps the change events up to its 3-day retention
(O5). Exit code 0 = erased everywhere.
"""

import hashlib
import json
import os
import re
import sys
import time
import uuid
from pathlib import Path

import boto3
import psycopg
import redis
from pymongo import MongoClient

ROOT = Path(__file__).resolve().parent.parent
LOGS = ROOT / "drills" / "logs"
UUID_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
BRONZE = ["shop_users", "shop_orders", "shop_order_items", "web_events", "web_reviews"]
SILVER = ["users", "orders", "order_items", "events", "reviews", "dead_letters"]
GOLD = ["dim_user", "fct_order_items", "fct_orders", "fct_sessions"]


def load_env() -> dict:
    env = {}
    for line in (ROOT / "onprem" / ".env").read_text().splitlines():
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            env[k] = v
    return env


ENV = load_env()


def athena(sql: str) -> list[list[str | None]]:
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


def pg():
    return psycopg.connect(
        host="localhost",
        dbname=ENV.get("POSTGRES_DB", "thelook"),
        user=ENV["POSTGRES_USER"],
        password=ENV["POSTGRES_PASSWORD"],
    )


def mongo():
    return MongoClient(
        f"mongodb://{ENV['MONGO_ROOT_USER']}:{ENV['MONGO_ROOT_PASSWORD']}"
        "@localhost:27017/?directConnection=true&authSource=admin"
    )["web"]


def redis_keys(uid: str) -> int:
    r = redis.Redis(
        host="localhost", username="admin", password=ENV["REDIS_ADMIN_PASSWORD"]
    )
    return sum(1 for _ in r.scan_iter(match=f"user:{uid}:*", count=1000))


def sql_list(keys: list[str]) -> str:
    """Keys as an SQL list. Only UUIDs get in: nothing else can reach the
    statement (the ids are UUIDs in every source)."""
    bad = [k for k in keys if not UUID_RE.match(k)]
    if bad:
        raise ValueError(f"{len(bad)} keys are not UUIDs")
    return ", ".join(f"'{k}'" for k in keys) or "NULL"


def capture_file(uid: str) -> Path:
    return (
        LOGS / f"erasure-capture-{hashlib.sha256(uid.encode()).hexdigest()[:12]}.json"
    )


def snapshot_of(database: str, table: str) -> str:
    return athena(
        f'SELECT snapshot_id FROM "{database}"."{table}$snapshots" '
        "ORDER BY committed_at DESC LIMIT 1"
    )[0][0]


def capture(uid: str) -> None:
    with pg() as conn:
        orders = [
            r[0]
            for r in conn.execute(
                "SELECT id FROM shop.orders WHERE user_id = %s", (uid,)
            )
        ]
        items = [
            r[0]
            for r in conn.execute(
                "SELECT id FROM shop.order_items WHERE order_id = ANY(%s)", (orders,)
            )
        ]
    web = mongo()
    events = [d["_id"] for d in web.events.find({"user_id": uid}, {"_id": 1})]
    reviews = [d["_id"] for d in web.reviews.find({"user_id": uid}, {"_id": 1})]
    snapshots = {
        f"thelook_bronze.{t}": snapshot_of("thelook_bronze", t) for t in BRONZE
    }
    snapshots |= {
        f"thelook_silver.{t}": snapshot_of("thelook_silver", t) for t in SILVER
    }
    snapshots |= {f"thelook_gold.{t}": snapshot_of("thelook_gold", t) for t in GOLD}
    data = {
        "orders": orders,
        "order_items": items,
        "events": [str(e) for e in events],
        "reviews": [str(r) for r in reviews],
        "redis_keys": redis_keys(uid),
        "snapshots": snapshots,
    }
    LOGS.mkdir(exist_ok=True)
    capture_file(uid).write_text(json.dumps(data))
    print(
        f"captured: {len(orders)} orders, {len(items)} items, {len(events)} events, "
        f"{len(reviews)} reviews, {data['redis_keys']} Redis keys, "
        f"{len(snapshots)} snapshots -> {capture_file(uid).relative_to(ROOT)}"
    )


def verify(uid: str, request_id: str | None) -> int:
    cap = json.loads(capture_file(uid).read_text())
    u = f"'{uid}'"  # a validated UUID
    orders, items = sql_list(cap["orders"]), sql_list(cap["order_items"])
    events, reviews = sql_list(cap["events"]), sql_list(cap["reviews"])
    pg_key = "coalesce(after.id, before.id)"
    json_user = f"json_extract_scalar(after, '$.user_id') = {u}"
    checks: dict[str, int] = {}

    with pg() as conn:
        for name, sql, arg in [
            ("postgres users", "SELECT count(*) FROM shop.users WHERE id = %s", uid),
            (
                "postgres orders",
                "SELECT count(*) FROM shop.orders WHERE user_id = %s",
                uid,
            ),
            (
                "postgres order_items",
                "SELECT count(*) FROM shop.order_items WHERE order_id = ANY(%s)",
                cap["orders"],
            ),
            (
                "postgres events (legacy)",
                "SELECT count(*) FROM shop.events WHERE user_id = %s",
                uid,
            ),
        ]:
            checks[name] = conn.execute(sql, (arg,)).fetchone()[0]
    web = mongo()
    checks["mongo events"] = web.events.count_documents({"user_id": uid})
    checks["mongo reviews"] = web.reviews.count_documents({"user_id": uid})
    checks["redis keys"] = redis_keys(uid)

    lake = {
        "bronze shop_users": f"thelook_bronze.shop_users WHERE {pg_key} = {u}",
        "bronze shop_orders": f"thelook_bronze.shop_orders WHERE {pg_key} IN ({orders}) "
        f"OR after.user_id = {u}",
        "bronze shop_order_items": f"thelook_bronze.shop_order_items "
        f"WHERE {pg_key} IN ({items}) OR after.order_id IN ({orders})",
        "bronze web_events": f"thelook_bronze.web_events WHERE doc_id IN ({events}) "
        f"OR {json_user}",
        "bronze web_reviews": f"thelook_bronze.web_reviews WHERE doc_id IN ({reviews}) "
        f"OR {json_user}",
        "silver users": f"thelook_silver.users WHERE id = {u}",
        "silver orders": f"thelook_silver.orders WHERE user_id = {u} OR id IN ({orders})",
        "silver order_items": f"thelook_silver.order_items WHERE id IN ({items}) "
        f"OR order_id IN ({orders})",
        "silver events": f"thelook_silver.events WHERE user_id = {u} OR id IN ({events})",
        "silver reviews": f"thelook_silver.reviews WHERE user_id = {u} "
        f"OR id IN ({reviews})",
        "silver dead_letters": f"thelook_silver.dead_letters WHERE strpos(payload, {u}) > 0",
        "gold dim_user": f"thelook_gold.dim_user WHERE user_id = {u}",
        "gold fct_order_items": f"thelook_gold.fct_order_items WHERE user_id = {u} "
        f"OR order_item_id IN ({items})",
        "gold fct_orders": f"thelook_gold.fct_orders WHERE user_id = {u} "
        f"OR order_id IN ({orders})",
        "gold fct_sessions": f"thelook_gold.fct_sessions WHERE user_id = {u}",
    }
    for name, where in lake.items():
        checks[name] = int(athena(f"SELECT count(*) FROM {where}")[0][0])

    # Time travel to before the erasure must fail on every affected table.
    for table, snapshot in cap["snapshots"].items():
        try:
            athena(f"SELECT count(*) FROM {table} FOR VERSION AS OF {snapshot}")
            checks[f"time travel {table}"] = 1
        except RuntimeError as error:
            checks[f"time travel {table}"] = (
                0 if "snapshot" in str(error).lower() else 1
            )

    if request_id:
        rid = request_id.replace("'", "''")
        checks["bronze erasure_log row missing"] = 1 - min(
            1,
            int(
                athena(
                    f"SELECT count(*) FROM thelook_bronze.erasure_log WHERE request_id = '{rid}'"
                )[0][0]
            ),
        )
        audit = athena(
            "SELECT step, rows_deleted FROM thelook_silver.erasure_audit "
            f"WHERE request_id = '{rid}' ORDER BY step"
        )
        checks["erasure_audit rows missing"] = 0 if len(audit) >= 10 else 1
        print("audit:", {step: int(n) if n else n for step, n in audit})
        # No snapshot from before the purge may remain, except the one kept
        # (the empty commit that ends each purge): a snapshot taken between
        # the request and the purge would still hold the rows, and the
        # captured snapshots above would not show it.
        purged = f"(SELECT max(purged_at) FROM thelook_bronze.erasure_log WHERE request_id = '{rid}')"
        for t in BRONZE:
            checks[f"old snapshots thelook_bronze.{t}"] = max(
                0,
                int(
                    athena(
                        f'SELECT count(*) FROM "thelook_bronze"."{t}$snapshots" '
                        f"WHERE committed_at < {purged}"
                    )[0][0]
                )
                - 1,
            )
        for layer, tables in (("silver", SILVER), ("gold", GOLD)):
            for t in tables:
                done = (
                    "(SELECT max(done_at) FROM thelook_silver.erasure_audit "
                    f"WHERE request_id = '{rid}' AND step = '{layer}.{t}')"
                )
                checks[f"old snapshots thelook_{layer}.{t}"] = max(
                    0,
                    int(
                        athena(
                            f'SELECT count(*) FROM "thelook_{layer}"."{t}$snapshots" '
                            f"WHERE committed_at < {done}"
                        )[0][0]
                    )
                    - 1,
                )

    width = max(map(len, checks))
    for name, count in checks.items():
        print(f"{name:<{width}}  {'OK' if count == 0 else f'FAIL ({count})'}")
    failed = [n for n, c in checks.items() if c]
    print(f"{len(checks) - len(failed)}/{len(checks)} checks passed")
    return 1 if failed else 0


def main() -> int:
    args = [a for a in sys.argv[1:] if a != "--capture"]
    if not args or str(uuid.UUID(args[0])) != args[0]:
        sys.exit(__doc__)
    if "--capture" in sys.argv:
        capture(args[0])
        return 0
    return verify(args[0], args[1] if len(args) > 1 else None)


if __name__ == "__main__":
    os.chdir(ROOT)
    sys.exit(main())
