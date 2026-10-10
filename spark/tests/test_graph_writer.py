"""Tests for the idempotent graph load (FR16, ADR 019) against a real Neo4j.

They delete every node: run them on a throwaway instance, never on the
serving one (Community has a single database). `make test-graph` starts one
from the pinned image, sets NEO4J_TEST_URI and removes it afterwards.
Skipped when NEO4J_TEST_URI is unset (CI).
"""

import hashlib
import os

import pytest

pytestmark = pytest.mark.neo4j

neo4j = pytest.importorskip("neo4j")

from lakehouse.graph_writer import load_graph  # noqa: E402

URI = os.environ.get("NEO4J_TEST_URI")


@pytest.fixture
def driver():
    if not URI:
        pytest.skip("NEO4J_TEST_URI not set (make test-graph)")
    auth = ("neo4j", os.environ["NEO4J_TEST_PASSWORD"])
    with neo4j.GraphDatabase.driver(URI, auth=auth) as d:
        d.execute_query("MATCH (n) DETACH DELETE n")
        yield d


def node(pid, category="Jeans"):
    return {
        "product_id": pid,
        "name": f"product {pid}",
        "category": category,
        "brand": "Acme",
        "department": "Men",
    }


NODES = [node(i) for i in range(1, 7)]


def pair(a, b, weight):
    return {"product_a": a, "product_b": b, "weight": weight}


def triples(driver):
    records, _, _ = driver.execute_query(
        "MATCH (a:Product)-[r:BOUGHT_WITH]->(b:Product) "
        "RETURN a.id AS a, b.id AS b, r.weight AS w ORDER BY a, b"
    )
    return [(r["a"], r["b"], r["w"]) for r in records]


def fingerprint(driver):
    """A hash of the whole graph: nodes with their properties, and pairs."""
    records, _, _ = driver.execute_query(
        "MATCH (p:Product) RETURN p.id AS id, p.name AS name, p.category AS c "
        "ORDER BY id"
    )
    nodes = [(r["id"], r["name"], r["c"]) for r in records]
    return hashlib.sha256(repr((nodes, triples(driver))).encode()).hexdigest()


def test_a_rerun_gives_the_same_graph(driver):
    pairs = [pair(1, 2, 5), pair(1, 3, 2), pair(4, 5, 1)]
    first = load_graph(driver, NODES, pairs, run_id="r1", batch=2)
    once = fingerprint(driver)

    again = load_graph(driver, NODES, pairs, run_id="r1", batch=2)
    assert fingerprint(driver) == once
    again = load_graph(driver, NODES, pairs, run_id="r2", batch=2)
    assert fingerprint(driver) == once

    assert first == {
        "nodes": 6,
        "pairs": 3,
        "pairs_skipped": 0,
        "stale_pairs": 0,
        "stale_nodes": 0,
    }
    assert again["stale_pairs"] == 0  # r2 restamped every r1 pair


def test_weights_go_down_and_vanished_pairs_are_deleted(driver):
    load_graph(driver, NODES, [pair(1, 2, 5), pair(1, 3, 2)], run_id="r1")
    # An erased order held 1-2 and 1-3: one fewer for 1-2, 1-3 is gone.
    counts = load_graph(driver, NODES, [pair(1, 2, 4)], run_id="r2")

    assert triples(driver) == [(1, 2, 4)]
    assert counts["stale_pairs"] == 1


def test_a_crash_keeps_the_old_graph_and_the_rerun_converges(driver):
    load_graph(driver, NODES, [pair(1, 2, 5), pair(3, 4, 1)], run_id="r1")
    new_pairs = [pair(1, 2, 6), pair(1, 3, 1), pair(5, 6, 2)]

    def crashing():
        yield from new_pairs[:2]  # the first batch (size 2) is committed
        raise RuntimeError("driver killed")

    with pytest.raises(RuntimeError):
        load_graph(driver, NODES, crashing(), run_id="r2", batch=2)
    # Never empty: the old pairs are still there, next to the new weights.
    assert triples(driver) == [(1, 2, 6), (1, 3, 1), (3, 4, 1)]

    load_graph(driver, NODES, new_pairs, run_id="r3", batch=2)
    crashed_then_rerun = fingerprint(driver)
    driver.execute_query("MATCH (n) DETACH DELETE n")
    load_graph(driver, NODES, new_pairs, run_id="r4", batch=2)
    assert fingerprint(driver) == crashed_then_rerun
    assert triples(driver) == [(1, 2, 6), (1, 3, 1), (5, 6, 2)]


def test_one_relationship_per_pair_read_in_both_directions(driver):
    load_graph(driver, NODES, [pair(1, 2, 5)], run_id="r1")
    records, _, _ = driver.execute_query(
        "MATCH (:Product {id: 2})-[r:BOUGHT_WITH]-(o:Product) RETURN o.id AS id"
    )
    assert [r["id"] for r in records] == [1]
    records, _, _ = driver.execute_query(
        "MATCH ()-[r:BOUGHT_WITH]->() RETURN count(r) AS n"
    )
    assert records[0]["n"] == 1


def test_products_follow_dim_product(driver):
    load_graph(driver, NODES, [pair(5, 6, 1)], run_id="r1")
    renamed = [node(1, category="Tops & Tees")] + NODES[1:5]  # 6 has left
    counts = load_graph(driver, renamed, [pair(5, 6, 1)], run_id="r2")

    records, _, _ = driver.execute_query(
        "MATCH (p:Product) RETURN p.id AS id, p.category AS c ORDER BY id"
    )
    assert [(r["id"], r["c"]) for r in records][:2] == [
        (1, "Tops & Tees"),
        (2, "Jeans"),
    ]
    assert [r["id"] for r in records] == [1, 2, 3, 4, 5]
    # Product 6 is still a node while the pairs are written (stale nodes go
    # last), so 5-6 is written, then deleted with node 6.
    assert counts["stale_nodes"] == 1 and counts["pairs_skipped"] == 0
    assert triples(driver) == []


def test_a_pair_with_an_unknown_product_is_skipped_not_given_an_empty_node(driver):
    counts = load_graph(driver, NODES, [pair(1, 2, 3), pair(1, 99, 4)], run_id="r1")

    assert counts["pairs"] == 1 and counts["pairs_skipped"] == 1
    records, _, _ = driver.execute_query("MATCH (p:Product {id: 99}) RETURN p")
    assert records == []


def test_unique_constraint_on_product_id(driver):
    load_graph(driver, NODES, [], run_id="r1")
    with pytest.raises(neo4j.exceptions.ConstraintError):
        driver.execute_query("CREATE (:Product {id: 1})")
