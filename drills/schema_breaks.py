# /// script
# requires-python = ">=3.12"
# dependencies = []
# ///
"""Schema drills (b) and (c), P7 acceptance part 1 (spec FR14, ADR 020): a
schema break is caught and an alert is raised.

  incompatible  shop.dist_centers.latitude double -> text, one UPDATE, with
                the writers stopped. Schema Registry rejects the new Avro
                schema (BACKWARD), the connector's task fails, the
                ConnectorTaskFailed alert fires; the registry and the lake
                are unchanged. Recovery: restore the value and the type,
                stop the connector, move its offset past the rejected
                transaction (Connect offsets API), resume, restart the
                writers. The restoring UPDATE then flows normally.
  malformed     a review whose fields do not fit the silver schema
                (created_at not a date, rating not a number): Schema
                Registry cannot see it (MongoDB documents are schemaless);
                silver keeps it as a dead letter or a contract rule fails,
                and the quality task alerts.

Needs core, stream, airflow and monitoring up. Run from the repo root:
  uv run drills/schema_breaks.py incompatible
  uv run drills/schema_breaks.py malformed
"""

import datetime
import json
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
COMPOSE = ["docker", "compose", "-f", str(ROOT / "onprem" / "compose.yaml")]
CONNECT = "http://localhost:8083/connectors/postgres-source"
SUBJECT = "thelook.shop.dist_centers-value"


def sh(*args: str, check: bool = True) -> str:
    return subprocess.run(
        args, capture_output=True, text=True, check=check
    ).stdout.strip()


def psql(sql: str) -> str:
    return sh(
        *COMPOSE,
        "exec",
        "-T",
        "postgres",
        "sh",
        "-c",
        f'psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" -Atc "{sql}"',
    )


def http(method: str, url: str, body: dict | None = None):
    data = json.dumps(body).encode() if body is not None else None
    request = urllib.request.Request(
        url, data=data, method=method, headers={"Content-Type": "application/json"}
    )
    with urllib.request.urlopen(request, timeout=30) as r:
        text = r.read().decode()
    return json.loads(text) if text else None


def task_state() -> str:
    status = http("GET", f"{CONNECT}/status")
    tasks = status["tasks"]
    return tasks[0]["state"] if tasks else status["connector"]["state"]


def firing(alertname: str) -> list[dict]:
    alerts = http("GET", "http://localhost:9093/api/v2/alerts")
    return [a for a in alerts if a["labels"]["alertname"] == alertname]


def wait(what: str, check, timeout_s: int, every: int = 5):
    start = time.monotonic()
    while time.monotonic() - start < timeout_s:
        result = check()
        if result:
            print(f"  {what}: after {time.monotonic() - start:.0f} s")
            return result
        time.sleep(every)
    sys.exit(f"  {what}: not within {timeout_s} s")


def lsn_to_int(lsn: str) -> int:
    """A PostgreSQL LSN ("X/Y", hex) as the number Debezium stores."""
    high, low = lsn.split("/")
    return (int(high, 16) << 32) + int(low, 16)


def incompatible() -> int:
    registry = http("GET", f"http://localhost:8081/subjects/{SUBJECT}/versions")
    original = psql("SELECT latitude FROM shop.dist_centers WHERE id = 2")
    print(f"registry versions: {registry}; dist_centers 2 latitude = {original}")
    sh(*COMPOSE, "stop", "generator", "review-simulator")
    print("writers stopped")

    t0 = time.monotonic()
    psql(
        "ALTER TABLE shop.dist_centers ALTER COLUMN latitude TYPE text USING latitude::text"
    )
    psql("UPDATE shop.dist_centers SET latitude = 'not a number' WHERE id = 2")
    # Any position after the rejected transaction's commit and before the next
    # write we care about: read right after the UPDATE committed.
    after_bad = psql("SELECT pg_current_wal_lsn()")
    print(f"1. latitude is text, row 2 updated; WAL position after it: {after_bad}")

    wait("2. connector task FAILED", lambda: task_state() == "FAILED", 300)
    trace = http("GET", f"{CONNECT}/status")["tasks"][0].get("trace", "")
    reason = next(
        (line for line in trace.splitlines() if "ncompatible" in line or "409" in line),
        "",
    )
    print(f"     {reason.strip()[:200]}")
    wait(
        "3. ConnectorTaskFailed firing in Alertmanager",
        lambda: firing("ConnectorTaskFailed"),
        600,
        15,
    )
    print(f"   failure -> alert: {time.monotonic() - t0:.0f} s after the UPDATE")
    unchanged = (
        http("GET", f"http://localhost:8081/subjects/{SUBJECT}/versions") == registry
    )
    print(f"4. registry unchanged: {unchanged}")

    # Recovery. One ALTER restores the type and row 2's value together: DDL
    # produces no change event. A first version restored the value with an
    # UPDATE while the column was still text: a second incompatible change,
    # after the skipped position, and the task failed again.
    psql(
        "ALTER TABLE shop.dist_centers ALTER COLUMN latitude TYPE double precision "
        f"USING CASE WHEN id = 2 THEN {original} ELSE latitude::double precision END"
    )
    after_restore = psql("SELECT pg_current_wal_lsn()")
    print(f"5. type and value restored by one ALTER; WAL position {after_restore}")
    http("PUT", f"{CONNECT}/stop")
    wait(
        "   connector STOPPED",
        lambda: http("GET", f"{CONNECT}/status")["connector"]["state"] == "STOPPED",
        120,
    )
    offsets = http("GET", f"{CONNECT}/offsets")["offsets"]
    old = offsets[0]["offset"]
    # Past the rejected change; nothing captured since (writers stopped, the
    # ALTER is DDL) but Debezium's own heartbeats.
    skip = lsn_to_int(after_restore)
    # The three LSN fields at the position read right after the rejected
    # transaction; the fields that point inside the old transaction go.
    new = {**old, "lsn": skip, "lsn_proc": skip, "lsn_commit": skip}
    for inside in ("txId", "lsn_events_processed", "messageType"):
        new.pop(inside, None)
    print(f"   offset lsn {old.get('lsn')} -> {skip} (past the rejected transaction)")
    http(
        "PATCH",
        f"{CONNECT}/offsets",
        {"offsets": [{"partition": offsets[0]["partition"], "offset": new}]},
    )
    http("PUT", f"{CONNECT}/resume")
    wait("6. connector and task RUNNING", lambda: task_state() == "RUNNING", 300)
    sh(*COMPOSE, "start", "generator", "review-simulator")
    print(
        f"   writers restarted; recovery took {time.monotonic() - t0:.0f} s from the UPDATE"
    )
    print(
        "Then: the next transform run, and uv run drills/verify_silver.py dist_centers"
    )
    return 0


def malformed() -> int:
    stamp = datetime.datetime.now(datetime.UTC).strftime("%Y%m%d%H%M%S")
    doc_id = f"schema-drill-{stamp}"
    doc = (
        f'{{_id: "{doc_id}", order_item_id: "{doc_id}", product_id: 1, user_id: "drill", '
        f'rating: "five", title: "drill", text: "malformed on purpose", tags: [], '
        f'helpful_votes: 0, created_at: "yesterday", updated_at: new Date()}}'
    )
    password = sh(
        "sh", "-c", f'. {ROOT}/onprem/.env && printf %s "$MONGO_ROOT_PASSWORD"'
    )
    sh(
        *COMPOSE,
        "exec",
        "-T",
        "-e",
        f"P={password}",
        "mongo",
        "bash",
        "-c",
        f'mongosh --quiet "mongodb://root:$P@localhost:27017/admin?directConnection=true" '
        f"--eval 'db.getSiblingDB(\"web\").reviews.insertOne({doc})'",
    )
    print(f"inserted review {doc_id} (created_at a string, rating a word)")
    print(
        "Watch: thelook_silver.dead_letters after the next transform run, the quality task, the alert."
    )
    return 0


if __name__ == "__main__":
    sys.exit({"incompatible": incompatible, "malformed": malformed}[sys.argv[1]]())
