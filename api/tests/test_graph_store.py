"""The real recommendations query (GraphStore) against a throwaway Neo4j.

The tests delete every node: never the serving Neo4j (Community has a
single database). `make test-graph` starts one and sets NEO4J_TEST_URI;
skipped without it (CI).
"""

import os

import neo4j
import pytest

from app import GraphStore

pytestmark = pytest.mark.neo4j

URI = os.environ.get("NEO4J_TEST_URI")


@pytest.fixture
def store():
    if not URI:
        pytest.skip("NEO4J_TEST_URI not set (make test-graph)")
    auth = ("neo4j", os.environ["NEO4J_TEST_PASSWORD"])
    with neo4j.GraphDatabase.driver(URI, auth=auth) as driver:
        driver.execute_query("MATCH (n) DETACH DELETE n")
        # As the graph writer stores them: one relationship per pair, from
        # the lower id to the higher.
        driver.execute_query(
            """
            UNWIND range(1, 5) AS id
            CREATE (:Product {id: id, name: 'p' + id, category: 'Swim'})
            WITH count(*) AS created
            MATCH (a:Product), (b:Product)
            WHERE [a.id, b.id] IN [[1, 2], [1, 3], [1, 4], [2, 4]]
            CREATE (a)-[:BOUGHT_WITH {weight: CASE [a.id, b.id]
              WHEN [1, 2] THEN 5 WHEN [1, 3] THEN 9 WHEN [1, 4] THEN 5 ELSE 7 END}]->(b)
            """
        )
        yield GraphStore(driver)


def ids(found):
    return [(r["product_id"], r["weight"]) for r in found["recommendations"]]


def test_neighbours_heaviest_first_ties_by_id(store):
    found = store.recommendations(1, 5)
    assert (found["name"], found["category"]) == ("p1", "Swim")
    assert ids(found) == [(3, 9), (2, 5), (4, 5)]


def test_relationships_are_read_in_both_directions(store):
    # 4 only has incoming relationships (from 1 and 2).
    assert ids(store.recommendations(4, 5)) == [(2, 7), (1, 5)]


def test_limit(store):
    assert ids(store.recommendations(1, 1)) == [(3, 9)]


def test_no_neighbour_is_an_empty_list_and_unknown_is_none(store):
    assert store.recommendations(5, 5)["recommendations"] == []
    assert store.recommendations(99, 5) is None


def test_ping(store):
    store.ping()
