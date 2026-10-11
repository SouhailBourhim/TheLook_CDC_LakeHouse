"""Daily quality report (ADR 020; spec FR8, FR12): freshness, row counts
reconciled with the sources, and a health snapshot.

    python report.py freshness   source -> bronze and source -> gold lag
    python report.py reconcile   source vs silver row counts, as of a cut
    python report.py health      Prometheus snapshot (fails only if unreachable)

Exit 1 when a check fails. Reads the lake through Athena as the read-only
quality user (QUALITY_AWS_*), PostgreSQL as role `quality`, MongoDB as user
`quality`: none of them can write.
"""

import datetime
import json
import os
import sys
import time
import urllib.request

import boto3

# O1 is 5 minutes (measured max 88 s); the margin avoids alerting on one slow batch.
BRONZE_LIMIT = datetime.timedelta(minutes=10)
GOLD_LIMIT = datetime.timedelta(hours=1)  # O2
CUT_MARGIN = datetime.timedelta(minutes=10)
# Reviews are the only rows deleted outside erasure (the review simulator).
# Silver lags the source, so it still holds reviews deleted since its cut;
# those deletions are counted from bronze, up to the moment the source was
# counted, once bronze has passed that moment: the comparison stays exact.
DELETED_AT_SOURCE = {"reviews"}
# Debezium's capture lag is below a second (P1): an event committed this long
# after a moment proves bronze read everything captured before it.
CAPTURE_MARGIN = datetime.timedelta(seconds=5)


# --- pure logic (unit-tested) -----------------------------------------------------


def parse_ts(value: str | None) -> datetime.datetime | None:
    """An Athena timestamp ("2026-10-11 02:58:00.123000 UTC") as naive UTC."""
    if not value:
        return None
    return datetime.datetime.fromisoformat(value.removesuffix(" UTC").strip())


def lag(source_latest, lake_latest) -> datetime.timedelta | None:
    """How far the lake is behind its source; None if the lake has nothing
    recent at all. Never negative (clocks and commit order can disagree by
    milliseconds)."""
    if lake_latest is None:
        return None
    return max(source_latest - lake_latest, datetime.timedelta(0))


def fresh(delay: datetime.timedelta | None, limit: datetime.timedelta) -> bool:
    return delay is not None and delay <= limit


def cut(silver_latest: list[datetime.datetime]) -> datetime.datetime:
    """Rows created before this time are in silver on every table: the
    oldest of the tables' newest source times, minus a margin. Whole seconds,
    and the same value on both sides: a first version sent Athena the
    seconds and MongoDB the microseconds, and counted 3 events twice."""
    return (min(silver_latest) - CUT_MARGIN).replace(microsecond=0)


def counts_match(source: int, silver: int, pending_deletes: int = 0) -> bool:
    """Silver, minus the rows deleted at the source that silver has not
    applied yet, must equal the source exactly."""
    return silver - pending_deletes == source


# --- I/O --------------------------------------------------------------------------


def athena(sql: str) -> list[list[str | None]]:
    client = boto3.client(
        "athena",
        aws_access_key_id=os.environ["QUALITY_AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["QUALITY_AWS_SECRET_ACCESS_KEY"],
        region_name=os.environ.get("AWS_REGION", "us-east-1"),
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
        raise RuntimeError(
            f"Athena {status['State']}: {status.get('StateChangeReason')} ({sql})"
        )
    # Results come in pages of at most 1,000 rows; the first row is the header.
    rows, token = [], None
    while True:
        page = client.get_query_results(
            QueryExecutionId=qid, **({"NextToken": token} if token else {})
        )
        rows += page["ResultSet"]["Rows"]
        token = page.get("NextToken")
        if not token:
            break
    return [[c.get("VarCharValue") for c in r["Data"]] for r in rows[1:]]


def pg(sql: str):
    import psycopg

    with psycopg.connect(
        host=os.environ.get("POSTGRES_HOST", "postgres"),
        dbname=os.environ.get("POSTGRES_DB", "thelook"),
        user="quality",
        password=os.environ["QUALITY_DB_PASSWORD"],
    ) as conn:
        return conn.execute(sql).fetchone()[0]


def mongo():
    from pymongo import MongoClient

    uri = (
        f"mongodb://quality:{os.environ['MONGO_QUALITY_PASSWORD']}@"
        f"{os.environ.get('MONGO_HOST', 'mongo')}:27017/web?authSource=web&directConnection=true"
    )
    return MongoClient(uri, serverSelectionTimeoutMS=5000)["web"]


def newest_event() -> datetime.datetime:
    doc = mongo().events.find_one(
        sort=[("created_at", -1)], projection={"created_at": 1}
    )
    return doc["created_at"]


# --- checks ------------------------------------------------------------------------


def freshness() -> bool:
    day = "source_ts > current_timestamp - INTERVAL '1' DAY"  # prunes to recent partitions
    checks = [
        (
            "postgres -> bronze",
            pg("SELECT max(created_at) FROM shop.orders"),
            f"SELECT max(source_ts) FROM thelook_bronze.shop_orders WHERE {day}",
            BRONZE_LIMIT,
        ),
        (
            "mongodb -> bronze",
            newest_event(),
            f"SELECT max(source_ts) FROM thelook_bronze.web_events WHERE {day}",
            BRONZE_LIMIT,
        ),
        (
            "postgres -> gold",
            pg("SELECT max(created_at) FROM shop.orders"),
            "SELECT max(created_at) FROM thelook_gold.fct_orders",
            GOLD_LIMIT,
        ),
    ]
    ok = True
    for name, source_latest, sql, limit in checks:
        delay = lag(source_latest, parse_ts(athena(sql)[0][0]))
        good = fresh(delay, limit)
        ok &= good
        shown = (
            "nothing in the last day"
            if delay is None
            else f"{delay.total_seconds():.0f} s"
        )
        print(f"{'ok  ' if good else 'FAIL'} {name}: {shown} behind (limit {limit})")
    return ok


SILVER_DATED = ["users", "orders", "order_items", "events", "reviews"]
SILVER_STATIC = ["products", "dist_centers"]


def wait_for_bronze(moment: datetime.datetime, timeout_s: int = 600) -> dict:
    """The snapshot ids of the newest finished bronze batch that read past
    `moment` (plus the capture margin). A batch commits its tables one by
    one (web_reviews last); its ledger row is written after all of them, so
    the ledger's snapshots form a consistent cut (ADR 012, as silver reads
    bronze). Checking web_events alone, as a first version did, could see a
    batch whose web_reviews commit had not landed yet."""
    stamp = (moment + CAPTURE_MARGIN).strftime("%Y-%m-%d %H:%M:%S")
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        events, reviews = athena(
            "SELECT snapshots['web_events'], snapshots['web_reviews'] "
            "FROM thelook_bronze.stream_batches ORDER BY committed_at DESC LIMIT 1"
        )[0]
        rows = athena(
            f"SELECT count(*) FROM thelook_bronze.web_events FOR VERSION AS OF {events} "
            f"WHERE source_ts > TIMESTAMP '{stamp}'"
        )
        if int(rows[0][0]):
            return {"web_events": events, "web_reviews": reviews}
        time.sleep(20)
    raise RuntimeError(f"no finished bronze batch past {stamp} within {timeout_s} s")


def silver_at(table: str, snapshot: str) -> str:
    """A silver table as of one snapshot, for Athena (Iceberg time travel)."""
    return f"thelook_silver.{table} FOR VERSION AS OF {snapshot}"


def silver_snapshots(tables: list[str]) -> dict[str, str]:
    """Each silver table's current snapshot, read once: every silver query of
    the run reads these, so a transform run committing meanwhile cannot move
    silver between two queries (a first version counted silver, then the
    pending deletions after silver had applied some: a false alarm)."""
    return {
        t: athena(
            f'SELECT snapshot_id FROM thelook_silver."{t}$snapshots" '
            "ORDER BY committed_at DESC LIMIT 1"
        )[0][0]
        for t in tables
    }


def pending_deletes(
    table: str, silver: str, stamp: str, upto: str, snapshot: str
) -> int:
    """Rows of `silver` (one snapshot) created before the cut whose deletion
    bronze holds (as of a finished batch) after silver's newest change and up
    to `upto` (when the source was counted): deleted at the source, not yet in
    silver."""
    return int(
        athena(
            f"SELECT count(*) FROM {silver} s "
            f"WHERE s.created_at < TIMESTAMP '{stamp}' AND s.id IN ("
            f"SELECT doc_id FROM thelook_bronze.web_{table} FOR VERSION AS OF {snapshot} "
            f"WHERE op = 'd' AND source_ts <= TIMESTAMP '{upto}' "
            f"AND source_ts > (SELECT max(_source_ts) FROM {silver}))"
        )[0][0]
    )


def reconcile() -> bool:
    pinned = silver_snapshots(SILVER_DATED + SILVER_STATIC)
    union = " UNION ALL ".join(
        f"SELECT max(_source_ts) FROM {silver_at(t, pinned[t])}" for t in SILVER_DATED
    )
    latest = [parse_ts(r[0]) for r in athena(union)]
    before = cut(latest)
    stamp = before.strftime("%Y-%m-%d %H:%M:%S")
    print(f"cut: rows created before {stamp} UTC")
    web = mongo()
    ok = True
    for table in SILVER_DATED + SILVER_STATIC:
        where = (
            f" WHERE created_at < TIMESTAMP '{stamp}'" if table in SILVER_DATED else ""
        )
        silver_table = silver_at(table, pinned[table])
        silver = int(athena(f"SELECT count(*) FROM {silver_table}{where}")[0][0])
        counted_at = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
        if table in ("events", "reviews"):
            source = web[table].count_documents({"created_at": {"$lt": before}})
        else:
            source = pg(f"SELECT count(*) FROM shop.{table}{where}")
        pending = 0
        if table in DELETED_AT_SOURCE:
            snapshots = wait_for_bronze(counted_at)
            upto = counted_at.strftime("%Y-%m-%d %H:%M:%S.%f")
            pending = pending_deletes(
                table, silver_table, stamp, upto, snapshots[f"web_{table}"]
            )
        good = counts_match(source, silver, pending)
        ok &= good
        note = (
            f", deleted at the source since silver's cut {pending}" if pending else ""
        )
        print(
            f"{'ok  ' if good else 'FAIL'} {table}: source {source}, silver {silver}{note}"
        )
    return ok


def health() -> bool:
    base = os.environ.get("PROMETHEUS_URL", "http://prometheus:9090")

    def get(path: str) -> dict:
        with urllib.request.urlopen(f"{base}{path}", timeout=10) as r:
            return json.load(r)["data"]

    try:
        firing = [
            a["labels"]["alertname"]
            for a in get("/api/v1/alerts")["alerts"]
            if a["state"] == "firing"
        ]
        targets = {
            t["labels"]["job"]: t["health"]
            for t in get("/api/v1/targets")["activeTargets"]
        }
    except OSError as error:
        print(f"FAIL Prometheus unreachable at {base}: {error}")
        return False
    print(json.dumps({"firing_alerts": firing, "scrape_targets": targets}))
    # Firing alerts already email through Alertmanager: not failed again here.
    return True


if __name__ == "__main__":
    check = {"freshness": freshness, "reconcile": reconcile, "health": health}[
        sys.argv[1]
    ]
    sys.exit(0 if check() else 1)
