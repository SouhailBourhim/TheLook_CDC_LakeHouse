"""lakehouse.features rows -> Redis commands (ADR 018).

Every command is idempotent, and their effects do not depend on order:

    zset   ZADD key GT score member      a score only goes up
           ZREMRANGEBYRANK key 0 -(n+1)  keep the n highest (viewed, session)
           ZREMRANGEBYSCORE key -inf (c  drop members older than the window
    hash   HSET key field value          same field, same value
    both   PEXPIREAT key t NX            a new key gets its expiry
           PEXPIREAT key t GT            an existing one only gets a later one

GT alone would do nothing on a new key: Redis treats a key without an
expiry as expiring never, and nothing is greater than never. Hence NX first.

Replaying a batch, or applying two batches in either order, therefore
leaves the same state, and so does retrying a pipeline after a dropped
connection: no batch-id guard is needed, unlike bronze.
"""

import os
from collections.abc import Iterable

import redis
from redis.backoff import ExponentialBackoff
from redis.retry import Retry

from lakehouse.features import FAMILIES

# Commands per round trip: bounds the client's buffer on a backlog batch.
CHUNK = 1000


def connect() -> redis.Redis:
    """A client logged in as the features ACL user (onprem/.env).

    Dropped connections and timeouts are retried with backoff (1, 2, 4...
    capped at 10 s, 6 times: about 40 s), which is safe only because every
    command is idempotent. After that the error fails the micro-batch.
    """
    return redis.Redis(
        host=os.environ.get("REDIS_HOST", "redis"),
        port=int(os.environ.get("REDIS_PORT", "6379")),
        username="features",
        password=os.environ["REDIS_FEATURES_PASSWORD"],
        socket_timeout=10,
        socket_connect_timeout=10,
        retry=Retry(ExponentialBackoff(cap=10, base=1), 6),
        retry_on_error=[redis.ConnectionError, redis.TimeoutError],
    )


def queue(pipe, row, now_ms: int) -> None:
    """Queue the commands of one features row on a pipeline."""
    family = FAMILIES[row.family]
    if family.kind == "zset":
        pipe.zadd(row.key, {row.member: row.score}, gt=True)
        if family.keep:
            pipe.zremrangebyrank(row.key, 0, -(family.keep + 1))
        if family.window_ms:
            # "(" = exclusive: a member exactly at the cut stays, as in the
            # API's count (ZCOUNT from now - window, inclusive).
            pipe.zremrangebyscore(row.key, "-inf", f"({now_ms - family.window_ms}")
    else:
        pipe.hset(row.key, row.member, row.value)
    pipe.pexpireat(row.key, row.expire_at_ms, nx=True)
    pipe.pexpireat(row.key, row.expire_at_ms, gt=True)


def write(client: redis.Redis, rows: Iterable, now_ms: int) -> int:
    """Send the rows' commands in pipelines of CHUNK rows; returns the
    number of rows. Non-transactional (no MULTI): nothing here needs
    atomicity, since a partial pipeline is completed by the replay."""
    pipe = client.pipeline(transaction=False)
    count = 0
    for row in rows:
        queue(pipe, row, now_ms)
        count += 1
        if count % CHUNK == 0:
            pipe.execute()
    pipe.execute()
    return count


def partition_writer(now_ms: int):
    """The function for DataFrame.foreachPartition: runs in the executor,
    so it opens its own connection (a client cannot be pickled)."""

    def write_partition(rows) -> None:
        client = connect()
        try:
            write(client, rows, now_ms)
        finally:
            client.close()

    return write_partition
