"""Tests for erasure/steps.py: input checks, what waits for what, and the
Redis and Kafka steps against fakes (no stack needed)."""

import json
import sys
import types

import fakeredis
import pytest

import steps

ALICE = "6f1c2a3b-0d4e-4f5a-8b6c-7d8e9f0a1b2c"
BOB = "0a1b2c3d-4e5f-4a6b-9c7d-8e9f0a1b2c3d"


@pytest.mark.parametrize("value", [ALICE])
def test_a_canonical_uuid_is_accepted(monkeypatch, value):
    monkeypatch.setenv("ERASURE_USER_ID", value)
    assert steps.user_id() == value


@pytest.mark.parametrize("value", ["42", ALICE.upper(), f"{ALICE}' OR '1'='1", ""])
def test_anything_else_stops_the_step(monkeypatch, value):
    monkeypatch.setenv("ERASURE_USER_ID", value)
    with pytest.raises(SystemExit):
        steps.user_id()


def test_literal_quotes_and_doubles_quotes():
    assert steps.literal(ALICE) == f"'{ALICE}'"
    assert steps.literal("manual__it's") == "'manual__it''s'"


def test_every_captured_source_step_is_awaited_in_bronze():
    # The legacy shop.events is no longer captured (P2): nothing to await.
    captured = set(steps.POSTGRES_DELETES) - {"postgres.events"}
    assert set(steps.BRONZE_ALIVE) == captured | set(steps.MONGO_COLLECTIONS)


def test_bind_counts_match_the_placeholders():
    for step, (sql, binds) in steps.BRONZE_ALIVE.items():
        assert sql.count("?") == binds, step


def test_each_table_is_read_at_its_own_snapshot():
    # Snapshot ids belong to one table: a subquery on shop_orders at the
    # order items' snapshot fails in Athena (seen in P8 step 5).
    import re

    tables = [
        "shop_users",
        "shop_orders",
        "shop_order_items",
        "web_events",
        "web_reviews",
    ]
    cut = {t: i for i, t in enumerate(tables)}
    for step, (sql, _binds) in steps.BRONZE_ALIVE.items():
        reads = re.findall(
            r"thelook_bronze\.(\w+) FOR VERSION AS OF (\d+)", sql.format(**cut)
        )
        assert reads, step
        for table, snapshot in reads:
            assert int(snapshot) == cut[table], (step, table)


def test_a_key_counts_as_alive_unless_its_last_event_is_a_delete():
    sql, _ = steps.BRONZE_ALIVE["postgres.orders"]
    assert "max_by(op, kafka_offset)" in sql and "last_op <> 'd'" in sql


def test_wait_bronze_polls_until_no_key_is_alive(monkeypatch):
    # Users are checked before orders: first pass 0 and 1 (one order's
    # delete still in Kafka), second pass 0 and 0.
    answers = iter(["0", "1", "0", "0"])
    monkeypatch.setattr(
        steps, "bronze_cut", lambda: {"shop_orders": 7, "shop_users": 8}
    )
    monkeypatch.setattr(steps, "athena", lambda sql, params: [[next(answers)]])
    monkeypatch.setattr(steps, "POLL_SECONDS", 0)

    found = steps.wait_bronze(
        ALICE, {"postgres.orders": 3, "postgres.users": 1}, timeout_s=60
    )

    assert found == {"bronze.polls": 2}


def test_wait_bronze_skips_tables_the_deletion_did_not_touch(monkeypatch):
    seen = []
    monkeypatch.setattr(steps, "bronze_cut", lambda: {"shop_users": 8})
    monkeypatch.setattr(
        steps, "athena", lambda sql, params: seen.append((sql, params)) or [["0"]]
    )

    steps.wait_bronze(ALICE, {"postgres.users": 1, "mongo.reviews": 0}, timeout_s=60)

    ((sql, params),) = seen
    assert "FOR VERSION AS OF 8" in sql and ALICE not in sql
    assert params == [ALICE]


def test_wait_bronze_gives_up_after_its_timeout(monkeypatch):
    monkeypatch.setattr(steps, "bronze_cut", lambda: {"shop_users": 8})
    monkeypatch.setattr(steps, "athena", lambda sql, params: [["1"]])

    with pytest.raises(TimeoutError):
        steps.wait_bronze(ALICE, {"postgres.users": 1}, timeout_s=-1)


def test_wait_ack_returns_the_rows_deleted_per_bronze_table(monkeypatch):
    monkeypatch.setattr(
        steps, "athena", lambda sql, params: [["shop_users", "3"], ["web_events", "9"]]
    )
    assert steps.wait_ack("run-1", timeout_s=60) == {
        "bronze.shop_users": 3,
        "bronze.web_events": 9,
    }


def test_erase_redis_deletes_only_the_users_keys(monkeypatch):
    server = fakeredis.FakeServer()
    client = fakeredis.FakeRedis(server=server)
    for i in range(1200):  # more than one SCAN step and one DEL batch
        client.set(f"user:{ALICE}:cart:{i}", 1)
    client.set(f"user:{ALICE}:viewed", 1)
    client.set(f"user:{BOB}:viewed", 1)
    client.set("product:1:views", 1)
    eraser = fakeredis.FakeRedis(server=server)  # before redis is replaced
    fake = types.SimpleNamespace(Redis=lambda **_: eraser)
    monkeypatch.setitem(sys.modules, "redis", fake)
    monkeypatch.setenv("REDIS_ERASER_PASSWORD", "x")

    assert steps.erase_redis(ALICE) == {"redis.keys": 1201}
    assert sorted(client.keys()) == [b"product:1:views", f"user:{BOB}:viewed".encode()]


def test_the_request_message_is_what_the_stream_parses(monkeypatch):
    sent = []

    class Producer:
        def __init__(self, config):
            assert config["acks"] == "all" and config["enable.idempotence"]

        def produce(self, topic, key, value, on_delivery):
            sent.append((topic, key, json.loads(value)))
            on_delivery(None, None)

        def flush(self, timeout):
            return 0

    monkeypatch.setitem(
        sys.modules, "confluent_kafka", types.SimpleNamespace(Producer=Producer)
    )

    assert steps.request_bronze(ALICE, "run-1") == {"kafka.requests": 1}
    # The same fields lakehouse.erasure.parse_request reads.
    assert sent == [
        (
            "thelook.erasure-requests",
            b"run-1",
            {"request_id": "run-1", "user_id": ALICE},
        )
    ]


def test_an_undelivered_request_fails_the_step(monkeypatch):
    class Producer:
        def __init__(self, config):
            pass

        def produce(self, topic, key, value, on_delivery):
            on_delivery("broker down", None)

        def flush(self, timeout):
            return 0

    monkeypatch.setitem(
        sys.modules, "confluent_kafka", types.SimpleNamespace(Producer=Producer)
    )
    with pytest.raises(RuntimeError, match="not delivered"):
        steps.request_bronze(ALICE, "run-1")


def test_a_network_error_does_not_end_a_wait(monkeypatch):
    from botocore.exceptions import SSLError

    answers = iter(
        [SSLError(endpoint_url="https://athena", error="EOF"), [["shop_users", "1"]]]
    )

    def athena(sql, params):
        answer = next(answers)
        if isinstance(answer, Exception):
            raise answer
        return answer

    monkeypatch.setattr(steps, "athena", athena)
    monkeypatch.setattr(steps, "POLL_SECONDS", 0)
    assert steps.wait_ack("run-1", timeout_s=60) == {"bronze.shop_users": 1}


def test_any_other_error_still_ends_a_wait(monkeypatch):
    def athena(sql, params):
        raise RuntimeError("Athena FAILED: TABLE_NOT_FOUND")

    monkeypatch.setattr(steps, "athena", athena)
    with pytest.raises(RuntimeError):
        steps.wait_ack("run-1", timeout_s=60)
