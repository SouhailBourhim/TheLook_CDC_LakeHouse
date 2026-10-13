"""The gdpr_erasure DAG's steps outside Spark (ADR 021), one per command.

Each command reads the request from the environment (ERASURE_USER_ID,
ERASURE_REQUEST_ID; never from the command line, which Airflow logs), does
one step, and prints its counts as one JSON line, the last line of output
(Airflow keeps it as the task's XCom). Counts only: no identifier is ever
printed.

  delete-sources  PostgreSQL (role eraser) and MongoDB (user eraser)
  wait-bronze     until none of the user's keys is alive in bronze any more
                  (ERASURE_DELETED: the counts printed by delete-sources,
                  to skip the tables it did not touch)
  request-bronze  the request, to the bronze stream's Kafka topic
  wait-ack        until the stream's erasure_log has the request; prints
                  the rows it deleted per bronze table
  erase-redis     the user's feature keys (ACL user eraser)

Runs in its own virtualenv in the Airflow image (/opt/erasure-venv).
Usage: ERASURE_USER_ID=... ERASURE_REQUEST_ID=... python steps.py <command>
"""

import json
import os
import sys
import time
import uuid

TOPIC = "thelook.erasure-requests"
# Athena reads bronze as the read-only quality user (ADR 020).
WORKGROUP = "thelook"
POLL_SECONDS = 30


def user_id() -> str:
    """The request's user id: a UUID in the canonical lowercase form the
    source writes (anything else could not match, or would be a mistake)."""
    value = os.environ["ERASURE_USER_ID"]
    try:
        canonical = str(uuid.UUID(value))
    except ValueError:
        canonical = None
    if value != canonical:
        sys.exit("ERASURE_USER_ID is not a canonical lowercase UUID")
    return value


def request_id() -> str:
    return os.environ["ERASURE_REQUEST_ID"]


# --- delete-sources ------------------------------------------------------------

# Children first: order items through the user's orders, then the orders,
# the legacy events (P1, no longer captured), the user. One transaction:
# all or nothing. The user id is always a bound parameter (%s).
POSTGRES_DELETES = {
    "postgres.order_items": "DELETE FROM shop.order_items WHERE order_id IN "
    "(SELECT id FROM shop.orders WHERE user_id = %s)",
    "postgres.orders": "DELETE FROM shop.orders WHERE user_id = %s",
    "postgres.events": "DELETE FROM shop.events WHERE user_id = %s",
    "postgres.users": "DELETE FROM shop.users WHERE id = %s",
}
MONGO_COLLECTIONS = {"mongo.events": "events", "mongo.reviews": "reviews"}


def delete_sources(uid: str) -> dict[str, int]:
    import psycopg
    from pymongo import MongoClient

    counts = {}
    with psycopg.connect(
        host=os.environ.get("POSTGRES_HOST", "postgres"),
        dbname=os.environ.get("POSTGRES_DB", "thelook"),
        user="eraser",
        password=os.environ["ERASER_DB_PASSWORD"],
    ) as conn:
        for step, sql in POSTGRES_DELETES.items():
            counts[step] = conn.execute(sql, (uid,)).rowcount
    uri = (
        f"mongodb://eraser:{os.environ['MONGO_ERASER_PASSWORD']}@"
        f"{os.environ.get('MONGO_HOST', 'mongo')}:27017/web"
        "?authSource=web&directConnection=true"
    )
    with MongoClient(uri, serverSelectionTimeoutMS=5000) as client:
        web = client["web"]
        for step, collection in MONGO_COLLECTIONS.items():
            counts[step] = web[collection].delete_many({"user_id": uid}).deleted_count
    return counts


# --- wait-bronze ----------------------------------------------------------------

# Bronze is read as of the stream's newest ledger row (a consistent cut,
# ADR 012). For each table: how many of the user's keys are still alive
# there, i.e. whose latest event (highest Kafka offset: one partition per
# topic, read in order) is not a delete. Not "delete events >= rows
# deleted": the review simulator deletes reviews too, so older delete
# events would satisfy that before ours arrive (seen in P8 step 5).
# ? is the user id (Athena execution parameters, never text in the SQL);
# {<table>} that table's snapshot id in the cut: snapshot ids are per table.
_ORDERS = (
    "SELECT after.id FROM thelook_bronze.shop_orders "
    "FOR VERSION AS OF {shop_orders} WHERE after.user_id = ?"
)
_ITEMS = (
    "SELECT after.id FROM thelook_bronze.shop_order_items "
    f"FOR VERSION AS OF {{shop_order_items}} WHERE after.order_id IN ({_ORDERS})"
)
_DOCS = (
    "SELECT doc_id FROM thelook_bronze.{t} FOR VERSION AS OF {{{t}}} "
    "WHERE strpos(after, ?) > 0 AND json_extract_scalar(after, '$.user_id') = ?"
)


def _alive(table: str, key: str, keys: str) -> str:
    return (
        "SELECT count(*) FROM (SELECT max_by(op, kafka_offset) AS last_op "
        f"FROM thelook_bronze.{table} FOR VERSION AS OF {{{table}}} "
        f"WHERE {key} IN ({keys}) GROUP BY {key}) WHERE last_op <> 'd'"
    )


_PG_KEY = "coalesce(after.id, before.id)"
BRONZE_ALIVE = {
    # source step: (SQL, how many times the user id is bound)
    "postgres.users": (_alive("shop_users", _PG_KEY, "?"), 1),
    "postgres.orders": (_alive("shop_orders", _PG_KEY, _ORDERS), 1),
    "postgres.order_items": (_alive("shop_order_items", _PG_KEY, _ITEMS), 1),
    "mongo.events": (_alive("web_events", "doc_id", _DOCS.format(t="web_events")), 2),
    "mongo.reviews": (
        _alive("web_reviews", "doc_id", _DOCS.format(t="web_reviews")),
        2,
    ),
}


def literal(value: str) -> str:
    """An Athena execution parameter: a string goes in as an SQL literal,
    quoted by the caller, any quote inside doubled. Athena then binds it as
    one value; it never becomes part of the statement."""
    return "'" + value.replace("'", "''") + "'"


def athena(sql: str, params: list[str] | None = None) -> list[list[str | None]]:
    """Run one query as the quality user; rows without the header. `params`
    fill the ? placeholders in order (plain strings, quoted here)."""
    import boto3
    from botocore.config import Config

    client = boto3.client(
        "athena",
        # Standard mode retries throttling and connection errors (an SSL
        # connection cut mid-request, seen in P8), up to 10 attempts.
        config=Config(retries={"mode": "standard", "max_attempts": 10}),
        aws_access_key_id=os.environ["QUALITY_AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["QUALITY_AWS_SECRET_ACCESS_KEY"],
        region_name=os.environ.get("AWS_REGION", "us-east-1"),
    )
    extra = {"ExecutionParameters": [literal(p) for p in params]} if params else {}
    qid = client.start_query_execution(QueryString=sql, WorkGroup=WORKGROUP, **extra)[
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
        # The reason, not the SQL's parameters (they hold the user id).
        raise RuntimeError(
            f"Athena {status['State']}: {status.get('StateChangeReason')}"
        )
    rows = client.get_query_results(QueryExecutionId=qid)["ResultSet"]["Rows"]
    return [[c.get("VarCharValue") for c in r["Data"]] for r in rows[1:]]


def network_error(error: Exception) -> bool:
    """A failure of the connection to AWS, worth polling again: the waits
    run for up to an hour, and the network here drops now and then (P8)."""
    from botocore.exceptions import ConnectionError, HTTPClientError

    return isinstance(error, ConnectionError | HTTPClientError)


def bronze_cut() -> dict[str, int]:
    """Every bronze table's snapshot id in the stream's newest ledger row."""
    rows = athena(
        "SELECT t.k, t.v FROM thelook_bronze.stream_batches "
        "CROSS JOIN UNNEST(snapshots) AS t(k, v) WHERE committed_at = "
        "(SELECT max(committed_at) FROM thelook_bronze.stream_batches)"
    )
    return {k: int(v) for k, v in rows}


def wait_bronze(uid: str, deleted: dict[str, int], timeout_s: int) -> dict[str, int]:
    """Wait until, in the newest ledger cut, none of the user's keys is
    alive any more in the tables the source deletion touched; returns how
    many polls it took.

    Sending the request earlier would let a change still in Kafka land in
    bronze after the purge. Each topic has one partition, read in order:
    once a key's delete event is in bronze, so is everything before it."""
    deadline = time.monotonic() + timeout_s
    polls = 0
    while True:
        polls += 1
        try:
            cut = bronze_cut()
            alive = {
                step: int(athena(sql.format(**cut), [uid] * binds)[0][0])
                for step, (sql, binds) in BRONZE_ALIVE.items()
                if deleted.get(step, 0)
            }
        except Exception as error:
            if not network_error(error):
                raise
            print(f"network error, polling again: {error}", file=sys.stderr)
            alive = {"unknown": 1}
        alive = {step: n for step, n in alive.items() if n}
        if not alive:
            return {"bronze.polls": polls}
        if time.monotonic() > deadline:
            raise TimeoutError(f"keys still alive in bronze: {alive}")
        print(f"waiting for bronze, keys still alive: {alive}", file=sys.stderr)
        time.sleep(POLL_SECONDS)


# --- request-bronze and wait-ack -------------------------------------------------


def request_bronze(uid: str, rid: str) -> dict[str, int]:
    """Publish the request; the bronze stream applies it (lakehouse.erasure)."""
    from confluent_kafka import Producer

    errors = []
    producer = Producer(
        {
            "bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP", "kafka:29092"),
            # Written once, acknowledged by the broker, or an error.
            "enable.idempotence": True,
            "acks": "all",
        }
    )
    producer.produce(
        TOPIC,
        key=rid.encode(),
        value=json.dumps({"request_id": rid, "user_id": uid}).encode(),
        on_delivery=lambda err, _msg: err and errors.append(str(err)),
    )
    if producer.flush(30) or errors:
        raise RuntimeError(f"request not delivered: {errors or 'timeout'}")
    return {"kafka.requests": 1}


def wait_ack(rid: str, timeout_s: int) -> dict[str, int]:
    """Wait for the stream's erasure_log row; returns its rows deleted per
    bronze table (as audit steps "bronze.<table>")."""
    deadline = time.monotonic() + timeout_s
    while True:
        try:
            rows = athena(
                "SELECT t.k, t.v FROM thelook_bronze.erasure_log "
                "CROSS JOIN UNNEST(rows_deleted) AS t(k, v) WHERE request_id = ?",
                [rid],
            )
        except Exception as error:
            if not network_error(error):
                raise
            print(f"network error, polling again: {error}", file=sys.stderr)
            rows = []
        if rows:
            return {f"bronze.{k}": int(v) for k, v in rows}
        if time.monotonic() > deadline:
            raise TimeoutError("the bronze stream did not acknowledge the request")
        time.sleep(POLL_SECONDS)


# --- erase-redis -----------------------------------------------------------------


def redis_pattern(uid: str) -> str:
    """Every feature key of the user starts with user:{id}: (features.py)."""
    return f"user:{uid}:*"


def erase_redis(uid: str) -> dict[str, int]:
    import redis

    client = redis.Redis(
        host=os.environ.get("REDIS_HOST", "redis"),
        username="eraser",
        password=os.environ["REDIS_ERASER_PASSWORD"],
    )
    deleted = 0
    # SCAN walks the keyspace in small steps (no blocking KEYS); DEL in
    # batches as it goes.
    batch = []
    for key in client.scan_iter(match=redis_pattern(uid), count=1000):
        batch.append(key)
        if len(batch) == 500:
            deleted += client.delete(*batch)
            batch = []
    if batch:
        deleted += client.delete(*batch)
    return {"redis.keys": deleted}


def main(command: str) -> dict[str, int]:
    uid = user_id()
    if command == "delete-sources":
        return delete_sources(uid)
    if command == "wait-bronze":
        deleted = json.loads(os.environ["ERASURE_DELETED"])
        return wait_bronze(uid, deleted, timeout_s=1800)
    if command == "request-bronze":
        return request_bronze(uid, request_id())
    if command == "wait-ack":
        # Measured in P8: a bronze purge takes about an hour under load
        # (every copy-on-write DELETE re-reads its table), more with
        # network retries.
        return wait_ack(request_id(), timeout_s=7200)
    if command == "erase-redis":
        return erase_redis(uid)
    sys.exit(f"unknown command: {command}")


if __name__ == "__main__":
    print(json.dumps(main(sys.argv[1])))
