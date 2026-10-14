# /// script
# requires-python = ">=3.12"
# dependencies = ["boto3==1.40.40", "confluent-kafka==2.15.1"]
# ///
"""Prove that bronze holds exactly what Kafka committed during a drill: 0
lost, 0 duplicated (P8 steps 10-12; spec FR14).

  1. uv run drills/verify_bronze.py --capture
     every CDC topic's end offset now, written to drills/logs/
  2. run the drill
  3. uv run drills/verify_bronze.py <capture file>
     per topic, over [captured offset, end offset now): the records Kafka
     committed (read_committed, tombstones left out: bronze keeps neither
     aborted records nor tombstones) against bronze's rows in that range
     and their distinct offsets. Waits for bronze to reach the end first.

One partition per topic, so an offset identifies a record. The offsets are
counted, not checked for gaps: with exactly-once Connect, transaction
markers take offsets too. Run it with no erasure in progress (a purge
removes bronze rows on purpose). Exit code 0 = every topic exact.
"""

import json
import sys
import time
from pathlib import Path

import boto3
import botocore.exceptions
from confluent_kafka import Consumer, TopicPartition

ROOT = Path(__file__).resolve().parent.parent
LOGS = ROOT / "drills" / "logs"
KAFKA = "localhost:9092"
TOPICS = [
    *(
        f"thelook.shop.{t}"
        for t in ("users", "orders", "order_items", "products", "dist_centers")
    ),
    *(f"thelook_mongo.web.{c}" for c in ("events", "reviews")),
]
WAIT = 20 * 60  # seconds for bronze to catch up
# A network drop here lasts up to 6 minutes: retry for 10 (40 x 15 s).
NETWORK_ATTEMPTS = 40
NETWORK_PAUSE = 15


def load_env() -> dict:
    env = {}
    for line in (ROOT / "onprem" / ".env").read_text().splitlines():
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            env[k] = v
    return env


ENV = load_env()


def athena(sql: str) -> list[list[str | None]]:
    """Rows as strings, as the read-only quality user. A query only reads:
    on a dropped connection it is run again, for up to 10 minutes."""
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


def table_for(topic: str) -> str:
    """thelook.shop.users -> thelook_bronze.shop_users (lakehouse.bronze)."""
    _prefix, database, name = topic.split(".")
    return f"thelook_bronze.{database}_{name}"


def end_offsets() -> dict[str, int]:
    c = Consumer({"bootstrap.servers": KAFKA, "group.id": "verify-bronze-ends"})
    ends = {
        t: c.get_watermark_offsets(TopicPartition(t, 0), timeout=10)[1] for t in TOPICS
    }
    c.close()
    return ends


def kafka_window(topic: str, start: int, end: int) -> tuple[int, int | None]:
    """Committed records with a value in [start, end), and the last one's
    offset (None if there is none)."""
    c = Consumer(
        {
            "bootstrap.servers": KAFKA,
            "group.id": f"verify-bronze-{time.time()}",
            "enable.auto.commit": False,
            "isolation.level": "read_committed",
        }
    )
    count, last = 0, None
    if start >= end:
        return count, last
    c.assign([TopicPartition(topic, 0, start)])
    # The last offsets of a window can be transaction markers, which the
    # consumer never returns: stop after 30 s without a record too.
    idle = 0
    while c.position([TopicPartition(topic, 0)])[0].offset < end and idle < 30:
        batch = c.consume(10000, timeout=1.0)
        idle = 0 if batch else idle + 1
        for m in batch:
            if m.error() or m.offset() >= end or m.value() is None:
                continue
            count, last = count + 1, m.offset()
    c.close()
    return count, last


def bronze_window(topic: str, start: int, end: int) -> tuple[int, int, int | None]:
    """Rows, distinct offsets, and the highest offset bronze holds in range."""
    rows, distinct, top = athena(
        f"SELECT count(*), count(DISTINCT kafka_offset), max(kafka_offset) "
        f"FROM {table_for(topic)} "
        f"WHERE kafka_offset >= {int(start)} AND kafka_offset < {int(end)}"
    )[0]
    return int(rows), int(distinct), None if top is None else int(top)


def capture() -> int:
    LOGS.mkdir(exist_ok=True)
    out = LOGS / f"bronze-capture-{time.strftime('%Y%m%d-%H%M%S')}.json"
    out.write_text(json.dumps(end_offsets(), indent=2))
    print(out.relative_to(ROOT))
    return 0


def verify(path: Path) -> int:
    starts = json.loads(path.read_text())
    ends = end_offsets()
    ok = True
    for topic in TOPICS:
        start, end = starts[topic], ends[topic]
        committed, last = kafka_window(topic, start, end)
        deadline = time.monotonic() + WAIT
        while True:
            rows, distinct, top = bronze_window(topic, start, end)
            if last is None or (top is not None and top >= last):
                break
            if time.monotonic() > deadline:
                print(f"{topic}: bronze did not reach offset {last} in time")
                break
            time.sleep(30)
        exact = rows == distinct == committed
        ok &= exact
        print(
            f"{topic}: offsets {start}..{end}, Kafka committed {committed}, "
            f"bronze rows {rows}, distinct offsets {distinct}"
            f" -> {'exact' if exact else 'MISMATCH'}"
        )
    print("bronze = Kafka" if ok else "bronze differs from Kafka")
    return 0 if ok else 1


def main() -> int:
    # Progress shows as it happens, also when the output goes to a file.
    sys.stdout.reconfigure(line_buffering=True)
    if sys.argv[1:] == ["--capture"]:
        return capture()
    if len(sys.argv) == 2:
        return verify(Path(sys.argv[1]))
    sys.exit(__doc__)


if __name__ == "__main__":
    sys.exit(main())
