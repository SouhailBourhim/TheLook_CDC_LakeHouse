# /// script
# requires-python = ">=3.12"
# dependencies = ["boto3==1.40.40", "confluent-kafka==2.15.1"]
# ///
"""Connector offsets reset to force a replay (P8 step 12; spec FR14):
bronze receives duplicates, silver is unchanged.

The MongoDB connector is rewound, not the PostgreSQL one: Postgres streams
nothing older than the slot's confirmed_flush_lsn, so a rewound Postgres
offset replays nothing, and its only replay is a full re-snapshot (the
repair measured in P4). A MongoDB resume token can be moved back.

  1. stop the load; wait until bronze holds every record of both MongoDB
     topics, run transform: silver is complete; record its snapshots
  2. capture the topics' end offsets; find in Connect's offsets topic the
     newest offset mongo-source committed REWIND minutes before the stop
  3. stop the connector, set that offset (Connect REST API), resume it:
     Debezium replays REWIND minutes of change stream
  4. once the topics are quiet: Kafka = bronze over the window
     (drills/verify_bronze.py), and every bronze row of the window repeats
     an earlier one (same doc_id, source_ts_ms, ord): the duplicates
  5. run transform; silver events and reviews at the snapshot it wrote
     equal step 1's: same fingerprint (rows, positions, newest merge
     time) and every replayed key's row identical in every column (the
     MERGE ignores an event that is not newer than the row, ADR 014)
  6. start the load again (always), the connector resumed (always)

Needs the core, stream and airflow profiles up, both connectors RUNNING, no
erasure in progress. Usage: uv run drills/offsets_replay.py
Exit code 0 = duplicates in bronze, silver unchanged.
"""

import json
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

from confluent_kafka import Consumer, TopicPartition

sys.path.insert(0, str(Path(__file__).resolve().parent))
import backfill_drill as bd  # noqa: E402  Airflow and Athena helpers
import verify_bronze as vb  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent
COMPOSE = ["docker", "compose", "-f", str(ROOT / "onprem" / "compose.yaml")]
CONNECT = "http://localhost:8083"
CONNECTOR = "mongo-source"
TOPICS = ["thelook_mongo.web.events", "thelook_mongo.web.reviews"]
SILVER = ["thelook_silver.events", "thelook_silver.reviews"]
LOAD = ["generator", "review-simulator"]
REWIND = 20  # minutes of change stream replayed


def now() -> str:
    return time.strftime("%H:%M:%S")


def connect(method: str, path: str, body: dict | None = None):
    request = urllib.request.Request(
        f"{CONNECT}{path}",
        method=method,
        data=None if body is None else json.dumps(body).encode(),
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(request, timeout=30) as r:
        text = r.read()
    return json.loads(text) if text else None


def wait_state(state: str, limit: int = 300) -> None:
    deadline = time.monotonic() + limit
    while time.monotonic() < deadline:
        status = connect("GET", f"/connectors/{CONNECTOR}/status")
        tasks = [t["state"] for t in status["tasks"]]
        if status["connector"]["state"] == state and all(s == state for s in tasks):
            return
        time.sleep(2)
    raise RuntimeError(f"{CONNECTOR} not {state} after {limit} s: {status}")


def offset_before(cutoff_ms: int) -> dict:
    """The newest offset the connector committed at or before cutoff_ms,
    read from Connect's offsets topic (compacted: only the latest per key
    survives in old segments, so a too-old cutoff finds nothing)."""
    c = Consumer(
        {
            "bootstrap.servers": "localhost:9092",
            "group.id": f"offsets-replay-{time.time()}",
            "enable.auto.commit": False,
            "isolation.level": "read_committed",
        }
    )
    end = c.get_watermark_offsets(TopicPartition("_connect-offsets", 0), timeout=10)[1]
    c.assign([TopicPartition("_connect-offsets", 0, 0)])
    found = None
    idle = 0
    while (
        c.position([TopicPartition("_connect-offsets", 0)])[0].offset < end
        and idle < 30
    ):
        batch = c.consume(10000, timeout=1.0)
        idle = 0 if batch else idle + 1
        for m in batch:
            if m.error() or m.value() is None:
                continue
            key = json.loads(m.key())
            if key[0] == CONNECTOR and m.timestamp()[1] <= cutoff_ms:
                found = {"partition": key[1], "offset": json.loads(m.value())}
    c.close()
    if found is None:
        raise RuntimeError(f"no {CONNECTOR} offset committed before the cutoff")
    return found


def bronze_complete() -> None:
    """Wait until bronze holds the last record of each MongoDB topic, and
    the stream's ledger covers it: silver reads bronze only up to the
    newest ledger row (ADR 014), written just after a batch's appends."""
    ends = vb.end_offsets()
    for topic in TOPICS:
        _count, last = vb.kafka_window(topic, max(0, ends[topic] - 1000), ends[topic])
        while last is not None:
            _rows, _distinct, top = vb.bronze_window(topic, last, last + 1)
            if top is not None:
                break
            time.sleep(30)
    appends = " , ".join(
        f'(SELECT max(committed_at) FROM {db}."{name}$snapshots" '
        "WHERE operation = 'append')"
        for db, name in (vb.table_for(t).split(".") for t in TOPICS)
    )
    while (
        bd.athena(
            "SELECT (SELECT max(committed_at) FROM thelook_bronze.stream_batches) "
            f">= greatest({appends})"
        )[0][0]
        != "true"
    ):
        time.sleep(15)


def transform(airflow: bd.Airflow, runs: int = 3) -> str:
    """Trigger transform runs until one succeeds (at most `runs`); returns
    the last run's state. A run that failed on the network is run again:
    silver's MERGE only applies newer events, so a rerun is safe. All three
    tries of one silver task failed on DNS in a single night (P8 step 12)."""
    for attempt in range(1, runs + 1):
        run = airflow.call(
            "POST", "/api/v2/dags/transform/dagRuns", {"logical_date": None}
        )
        while True:
            state = airflow.call(
                "GET", f"/api/v2/dags/transform/dagRuns/{run['dag_run_id']}"
            )["state"]
            if state in ("success", "failed"):
                break
            time.sleep(30)
        print(f"  {now()} transform run {attempt}: {state}")
        if state == "success":
            break
    return state


def silver_snapshot(table: str) -> str:
    db, name = table.split(".")
    return bd.athena(
        f'SELECT snapshot_id FROM {db}."{name}$snapshots" ORDER BY committed_at DESC LIMIT 1'
    )[0][0]


def fingerprint(table: str, snapshot: str) -> list:
    """Row count, sum of positions and newest merge time at a snapshot. A
    MERGE insert or update stamps _merged_at and moves _position; a delete
    changes the count: equal fingerprints mean the MERGE wrote nothing. A
    full-table EXCEPT read every column of 7.5 M events and hit the
    workgroup's 1 GiB scan limit; these columns cost a few MB. Decimal:
    7.5 M positions of ~1.8e18 overflow a bigint."""
    return vb.athena(
        "SELECT count(*), sum(CAST(_position AS decimal(38, 0))), max(_merged_at) "
        f"FROM {table} FOR VERSION AS OF {snapshot}"
    )[0]


def main() -> int:
    sys.stdout.reconfigure(line_buffering=True)
    airflow = bd.Airflow()
    if any(
        connect("GET", f"/connectors/{c}/status")["tasks"][0]["state"] != "RUNNING"
        for c in ("postgres-source", CONNECTOR)
    ):
        sys.exit("connectors not all RUNNING")
    stopped = False
    try:
        print(f"{now()} 1. stop the load; silver complete")
        subprocess.run([*COMPOSE, "stop", *LOAD], capture_output=True, check=True)
        stop_ms = int(time.time() * 1000)
        time.sleep(60)  # Connect is under a second behind (P1)
        bronze_complete()
        print(f"  {now()} bronze holds both topics; transform")
        state = transform(airflow)
        if state != "success":
            sys.exit(f"baseline transform {state}")
        baseline = {t: silver_snapshot(t) for t in SILVER}
        print(f"  {now()} silver snapshots: {baseline}")

        print(f"{now()} 2. capture; the offset {REWIND} min before the stop")
        capture = subprocess.run(
            ["uv", "run", "-q", str(ROOT / "drills" / "verify_bronze.py"), "--capture"],
            capture_output=True,
            text=True,
            check=True,
        ).stdout.strip()
        old = offset_before(stop_ms - REWIND * 60_000)
        print(f"  {capture}; rewind to sec={old['offset'].get('sec')}")

        print(f"{now()} 3. stop {CONNECTOR}, set the offset, resume")
        connect("PUT", f"/connectors/{CONNECTOR}/stop")
        stopped = True
        wait_state("STOPPED")
        connect("PATCH", f"/connectors/{CONNECTOR}/offsets", {"offsets": [old]})
        connect("PUT", f"/connectors/{CONNECTOR}/resume")
        wait_state("RUNNING")
        stopped = False
        quiet, ends = 0, vb.end_offsets()
        while quiet < 60:  # seconds without a new record
            time.sleep(10)
            latest = vb.end_offsets()
            quiet = quiet + 10 if latest == ends else 0
            ends = latest
        print(f"  {now()} replay over")

        print(f"{now()} 4. Kafka = bronze; duplicates")
        exact = vb.verify(ROOT / capture) == 0
        starts = json.loads((ROOT / capture).read_text())
        duplicated = True
        for topic in TOPICS:
            table = vb.table_for(topic)
            start, end = starts[topic], ends[topic]
            rows, repeats = vb.athena(
                f"SELECT count(*), count(o.doc_id) FROM {table} w "
                f"LEFT JOIN (SELECT DISTINCT doc_id, source_ts_ms, ord FROM {table} "
                f"WHERE kafka_offset < {start}) o "
                f"ON o.doc_id = w.doc_id AND o.source_ts_ms = w.source_ts_ms "
                f"AND o.ord = w.ord "
                f"WHERE w.kafka_offset >= {start} AND w.kafka_offset < {end}"
            )[0]
            print(f"  {table}: {rows} rows replayed, {repeats} repeat an earlier row")
            duplicated &= int(rows) > 0 and rows == repeats

        print(f"{now()} 5. transform; silver against step 1")
        state = transform(airflow)
        print(f"  transform {state}")
        unchanged = state == "success"
        for table, topic in zip(SILVER, TOPICS, strict=True):
            # Compared with the snapshot this transform wrote, not the live
            # table: the load starts again right after.
            before, after = baseline[table], silver_snapshot(table)
            prints = [fingerprint(table, s) for s in (before, after)]
            keys = (
                f"SELECT DISTINCT doc_id FROM {vb.table_for(topic)} "
                f"WHERE kafka_offset >= {starts[topic]} AND kafka_offset < {ends[topic]}"
            )
            changed, gone = (
                vb.athena(
                    f"SELECT count(*) FROM (SELECT * FROM {table} FOR VERSION AS OF {a} "
                    f"WHERE id IN ({keys}) EXCEPT SELECT * FROM {table} "
                    f"FOR VERSION AS OF {b} WHERE id IN ({keys}))"
                )[0][0]
                for a, b in ((after, before), (before, after))
            )
            print(
                f"  {table}: rows, sum(_position), max(_merged_at) {prints[0]} -> "
                f"{prints[1]}; replayed keys changed {changed}, gone {gone}"
            )
            unchanged &= prints[0] == prints[1] and changed == gone == "0"
    finally:
        if stopped:
            connect("PUT", f"/connectors/{CONNECTOR}/resume")
        print(f"{now()} 6. start the load again")
        subprocess.run([*COMPOSE, "start", *LOAD], capture_output=True, check=False)
    print(
        f"bronze = Kafka: {exact}, bronze duplicates: {duplicated}, "
        f"silver unchanged: {unchanged}"
    )
    return 0 if exact and duplicated and unchanged else 1


if __name__ == "__main__":
    sys.exit(main())
