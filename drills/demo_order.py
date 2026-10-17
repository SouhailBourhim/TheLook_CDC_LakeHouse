# /// script
# requires-python = ">=3.12"
# dependencies = ["boto3==1.40.40"]
# ///
"""The demo's story (spec section 10, docs/demo.md), timed: an order the generator
just created is returned, and the change reaches bronze, then gold and the
revenue mart that a dashboard reads.

  1. the newest order the generator committed in PostgreSQL
  2. bronze: poll Athena until thelook_bronze.shop_orders holds it
  3. return it: status 'Returned' and returned_at on the order and its
     items, the change the generator's own status updates make
  4. bronze: poll until the update arrives (op 'u')
  5. trigger transform (airflow profile); gold: fct_orders shows the
     return, and mart_daily_revenue's returns for that day include it

Each step prints the seconds since the previous one started (the order is
picked a moment after the generator committed it), for the voice-over.
Needs the core, stream and airflow profiles up. The returned order is a
real change in the source: a demo order, nothing else depends on it.

Usage: uv run drills/demo_order.py
"""

import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import backfill_drill as bd  # noqa: E402  Airflow and Athena helpers

ROOT = Path(__file__).resolve().parent.parent
COMPOSE = ["docker", "compose", "-f", str(ROOT / "onprem" / "compose.yaml")]
POLL = 5


def psql(sql: str) -> str:
    out = subprocess.run(
        [
            *COMPOSE,
            "exec",
            "-T",
            "postgres",
            "psql",
            "-U",
            "postgres",
            "-d",
            "thelook",
            "-X",
            "-A",
            "-t",
            "-c",
            sql,
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    return out.stdout.strip()


def wait_for(label: str, sql: str, since: float) -> None:
    while not bd.athena(sql):
        time.sleep(POLL)
    print(f"  {label}: {time.time() - since:.0f} s later")


def main() -> int:
    order_id, user_id = psql(
        "SELECT id || '|' || user_id FROM shop.orders ORDER BY created_at DESC LIMIT 1"
    ).split("|")
    created = time.time()
    print(f"1. order {order_id} (user {user_id[:8]}...), just created")
    wait_for(
        "2. in bronze",
        "SELECT 1 FROM thelook_bronze.shop_orders "
        f"WHERE after.id = '{order_id}' LIMIT 1",
        created,
    )
    psql(
        "BEGIN;"
        f" UPDATE shop.orders SET status = 'Returned', returned_at = now()"
        f"  WHERE id = '{order_id}';"
        f" UPDATE shop.order_items SET status = 'Returned', returned_at = now()"
        f"  WHERE order_id = '{order_id}';"
        " COMMIT;"
    )
    returned = time.time()
    print("3. returned in PostgreSQL (one transaction: the order and its items)")
    wait_for(
        "4. the return in bronze",
        "SELECT 1 FROM thelook_bronze.shop_orders "
        f"WHERE after.id = '{order_id}' AND after.status = 'Returned' LIMIT 1",
        returned,
    )
    airflow = bd.Airflow()
    run = airflow.call("POST", "/api/v2/dags/transform/dagRuns", {"logical_date": None})
    print(f"5. transform triggered ({run['dag_run_id']})")
    while (
        state := airflow.call(
            "GET", f"/api/v2/dags/transform/dagRuns/{run['dag_run_id']}"
        )["state"]
    ) not in ("success", "failed"):
        time.sleep(30)
    if state != "success":
        print(f"   transform {state}: see the Airflow UI")
        return 1
    rows = bd.athena(
        "SELECT o.is_returned, o.order_date_key, m.returns "
        "FROM thelook_gold.fct_orders o "
        "JOIN thelook_gold.mart_daily_revenue m ON m.date_key = o.order_date_key "
        f"WHERE o.order_id = '{order_id}'"
    )
    print(
        f"   gold: is_returned={rows[0][0]}, the day's returns in the mart "
        f"{rows[0][2]} ({time.time() - returned:.0f} s after the return)"
    )
    return 0 if rows and rows[0][0] == "true" else 1


if __name__ == "__main__":
    sys.exit(main())
