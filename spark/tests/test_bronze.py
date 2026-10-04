"""Tests for lakehouse.bronze: table names, the replay guard, the schema cache,
and routing each topic to the right parser."""

import io
import json

import pytest
from pyspark.sql import functions as F

from lakehouse import bronze
from lakehouse.bronze import (
    BATCH_ID,
    QUERY_ID,
    TOPICS,
    SchemaRegistry,
    already_committed,
    bronze_rows,
    table_for,
)


@pytest.mark.parametrize(
    ("topic", "table"),
    [
        ("thelook.shop.users", "lake.thelook_bronze.shop_users"),
        ("thelook.shop.order_items", "lake.thelook_bronze.shop_order_items"),
        ("thelook_mongo.web.events", "lake.thelook_bronze.web_events"),
    ],
)
def test_table_for(topic, table):
    assert table_for(topic) == table


def test_seven_topics_seven_distinct_tables():
    assert len({table_for(t) for t in TOPICS}) == len(TOPICS) == 7


def test_heartbeat_topic_is_not_ingested():
    assert "thelook.shop.heartbeat" not in TOPICS


def snap(query_id, batch_id):
    return {QUERY_ID: query_id, BATCH_ID: str(batch_id), "added-records": "10"}


def test_replayed_batch_is_skipped():
    summaries = [snap("q1", 4), snap("q1", 5)]
    assert already_committed(summaries, "q1", 5)  # replay of the last batch
    assert already_committed(summaries, "q1", 3)  # older batch, also done


def test_next_batch_is_written():
    assert not already_committed([snap("q1", 5)], "q1", 6)


def test_batches_of_another_query_do_not_count():
    # A new checkpoint is a new query whose batch ids restart at 0.
    assert not already_committed([snap("old-query", 900)], "new-query", 0)


def test_snapshots_without_marks_are_ignored():
    # e.g. a compaction or maintenance commit (no query/batch id).
    assert not already_committed([{"operation": "replace"}], "q1", 0)


def test_schema_registry_fetches_each_id_once(monkeypatch):
    calls = []

    def fake_urlopen(url, timeout):
        calls.append(url)
        return io.BytesIO(json.dumps({"schema": '"long"'}).encode())

    monkeypatch.setattr(bronze.urllib.request, "urlopen", fake_urlopen)
    registry = SchemaRegistry("http://schema-registry:8081/")

    assert registry(7) == registry(7) == '"long"'
    assert calls == ["http://schema-registry:8081/schemas/ids/7"]


def test_bronze_rows_routes_each_topic_to_its_parser(kafka_df, captured, schemas):
    users = captured["thelook.shop.users"]
    reviews = captured["thelook_mongo.web.reviews"]
    batch = kafka_df([users["c"], reviews["c"], reviews["d"], reviews["tombstone"]])
    now = F.current_timestamp()

    pg = bronze_rows(batch, "thelook.shop.users", schemas.get, now)
    mongo = bronze_rows(batch, "thelook_mongo.web.reviews", schemas.get, now)

    assert "lsn" in pg.columns and "doc_id" not in pg.columns
    assert "doc_id" in mongo.columns and "lsn" not in mongo.columns
    assert pg.count() == 1
    assert sorted(r.op for r in mongo.collect()) == ["c", "d"]  # tombstone dropped
