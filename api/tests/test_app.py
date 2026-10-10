# Run from api/:  python -m pytest
"""GET /users/{id}/features on fakeredis with a fixed clock, GET
/products/{id}/recommendations on a fake graph store, /health (liveness)
and /ready (both stores).

Keys are seeded as the features stream writes them
(spark/lakehouse/redis_writer.py). The real Cypher query runs against a
throwaway Neo4j in test_graph_store.py (make test-graph).
"""

import fakeredis
import pytest
from fastapi.testclient import TestClient
from neo4j.exceptions import ServiceUnavailable

import app as api
from app import GraphTimeout, app, get_clock, get_graph, get_redis

USER = "6b69f59b-eb74-45da-9aae-cc31dfef1d77"
NOW = 1_791_400_000.0  # seconds, as time.time()
NOW_MS = int(NOW * 1000)
MIN = 60_000


@pytest.fixture
def server():
    return fakeredis.FakeServer()


@pytest.fixture
def r(server):
    return fakeredis.FakeRedis(server=server, decode_responses=True)


@pytest.fixture
def clock():
    state = {"now": NOW}
    yield state
    app.dependency_overrides.clear()


class FakeGraph:
    """GraphStore's interface over a dict: {product: (name, category,
    [(neighbour, name, category, weight), ...])}. Sorts and limits as the
    Cypher query does."""

    def __init__(self, products):
        self.products = products
        self.down = False
        self.slow = False

    def recommendations(self, product_id, limit):
        if self.down:
            raise ServiceUnavailable("connection refused")
        if self.slow:
            raise GraphTimeout("query ran past 2 s")
        if product_id not in self.products:
            return None
        name, category, neighbours = self.products[product_id]
        ranked = sorted(neighbours, key=lambda n: (-n[3], n[0]))[:limit]
        return {
            "name": name,
            "category": category,
            "recommendations": [
                {"product_id": p, "name": n, "category": c, "weight": w}
                for p, n, c, w in ranked
            ],
        }

    def ping(self):
        if self.down:
            raise ServiceUnavailable("connection refused")


SWIM = "Swim"


@pytest.fixture
def graph():
    return FakeGraph(
        {
            27809: (
                "Bikini",
                SWIM,
                [
                    (13561, "Swimsuit A", SWIM, 43),
                    (13363, "Swimsuit B", SWIM, 42),
                    (8675, "Coat", "Outerwear & Coats", 2),
                    (13731, "Belt", "Accessories", 2),
                    (84, "Tee", "Tops & Tees", 1),
                    (28192, "Swimsuit C", SWIM, 40),
                ],
            ),
            5: ("Lonely sock", "Socks", []),
        }
    )


@pytest.fixture
def client(r, graph, clock):
    app.dependency_overrides[get_redis] = lambda: r
    app.dependency_overrides[get_graph] = lambda: graph
    app.dependency_overrides[get_clock] = lambda: lambda: clock["now"]
    return TestClient(app)


def seed(r, user=USER):
    p = f"user:{user}"
    r.zadd(f"{p}:viewed", {"26756": NOW_MS - 5 * MIN, "42": NOW_MS - 2 * MIN})
    r.zadd(f"{p}:events", {"e1": NOW_MS - 59 * MIN, "e2": NOW_MS - MIN, "e3": NOW_MS})
    r.zadd(f"{p}:session", {"s2": NOW_MS - MIN})
    r.hset(f"{p}:cart:s1", mapping={"item:old": "5.00"})  # an older session
    r.hset(
        f"{p}:cart:s2",
        mapping={"item:c1": "19.99", "item:c2": "34.99", "purchased": "1"},
    )


def test_features_of_a_user(client, r):
    seed(r)
    response = client.get(f"/users/{USER}/features")
    assert response.status_code == 200
    assert response.json() == {
        "user_id": USER,
        "recently_viewed": [  # newest first
            {"product_id": 42, "viewed_at": "2026-10-07T19:04:40Z"},
            {"product_id": 26756, "viewed_at": "2026-10-07T19:01:40Z"},
        ],
        # The newest session's cart only; prices summed exactly (Decimal).
        "cart": {"session_id": "s2", "value": "54.98", "items": 2, "purchased": True},
        "events_last_hour": 3,
    }


def test_events_last_hour_falls_as_time_passes(client, r, clock):
    seed(r)
    clock["now"] = NOW + 2 * 60  # 2 minutes later, no new event: e1 is out
    assert client.get(f"/users/{USER}/features").json()["events_last_hour"] == 2
    clock["now"] = NOW + 61 * 60
    assert client.get(f"/users/{USER}/features").json()["events_last_hour"] == 0


def test_no_cart_when_the_newest_session_has_none(client, r):
    seed(r)
    r.zadd(f"user:{USER}:session", {"s3": NOW_MS})  # newer, no cart key
    r.zremrangebyrank(f"user:{USER}:session", 0, -2)
    assert client.get(f"/users/{USER}/features").json()["cart"] is None


def test_unknown_user_is_404(client, r):
    seed(r)
    other = "00000000-0000-4000-8000-000000000000"
    assert client.get(f"/users/{other}/features").status_code == 404


def test_uppercase_id_finds_the_same_user(client, r):
    # Source ids are lowercase UUIDs; the path is parsed as a UUID, so any
    # spelling of it reaches the same keys.
    seed(r)
    assert client.get(f"/users/{USER.upper()}/features").status_code == 200


def test_malformed_id_is_422(client):
    assert client.get("/users/not-a-uuid/features").status_code == 422


def test_redis_down_is_503(client, server):
    server.connected = False
    response = client.get(f"/users/{USER}/features")
    assert response.status_code == 503
    ready = client.get("/ready")
    assert ready.status_code == 503
    assert ready.json() == {"redis": "down", "neo4j": "ok"}


def test_ready_when_both_stores_answer(client):
    ready = client.get("/ready")
    assert ready.status_code == 200
    assert ready.json() == {"redis": "ok", "neo4j": "ok"}


def test_ready_reports_both_stores_when_both_are_down(client, server, graph):
    # Checking the second store even when the first failed: one outage must
    # not hide another.
    server.connected = False
    graph.down = True
    assert client.get("/ready").json() == {"redis": "down", "neo4j": "down"}


def test_health_is_liveness_only(client, server, graph):
    # The process answers: 200 even with both stores down, so a store outage
    # never gets a working API restarted.
    server.connected = False
    graph.down = True
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


# --- recommendations (Neo4j) -----------------------------------------------------


def test_recommendations_heaviest_first_ties_by_id(client):
    response = client.get("/products/27809/recommendations")
    assert response.status_code == 200
    body = response.json()
    assert (body["product_id"], body["name"], body["category"]) == (
        27809,
        "Bikini",
        SWIM,
    )
    # Default limit 5; the two weight-2 products ordered by id (8675 < 13731).
    assert [(x["product_id"], x["weight"]) for x in body["recommendations"]] == [
        (13561, 43),
        (13363, 42),
        (28192, 40),
        (8675, 2),
        (13731, 2),
    ]


def test_limit_is_between_1_and_20(client):
    url = "/products/27809/recommendations"
    assert len(client.get(f"{url}?limit=2").json()["recommendations"]) == 2
    assert len(client.get(f"{url}?limit=20").json()["recommendations"]) == 6
    assert client.get(f"{url}?limit=0").status_code == 422
    assert client.get(f"{url}?limit=21").status_code == 422


def test_a_product_never_bought_with_another_has_no_recommendations(client):
    response = client.get("/products/5/recommendations")
    assert response.status_code == 200
    assert response.json()["recommendations"] == []


def test_unknown_product_is_404(client):
    assert client.get("/products/99999/recommendations").status_code == 404


def test_malformed_or_out_of_range_product_id_is_422(client):
    for bad in ("abc", "0", "-1", str(2**63)):  # Neo4j integers are 64-bit
        assert client.get(f"/products/{bad}/recommendations").status_code == 422


def test_neo4j_down_is_503_and_ready_names_it(client, graph):
    graph.down = True
    response = client.get("/products/27809/recommendations")
    assert response.status_code == 503
    assert response.json() == {"detail": "graph store unavailable"}
    ready = client.get("/ready")
    assert ready.status_code == 503
    assert ready.json() == {"redis": "ok", "neo4j": "down"}
    # The features keep working: each endpoint degrades on its own store.
    assert client.get(f"/users/{USER}/features").status_code == 404


def test_a_query_timeout_is_503(client, graph):
    # GraphStore turns Neo4j's timeout (a ClientError) into GraphTimeout:
    # tested on a real Neo4j in test_graph_store.py.
    graph.slow = True
    response = client.get("/products/27809/recommendations")
    assert response.status_code == 503
    assert response.json() == {"detail": "graph store unavailable"}


def test_the_neo4j_driver_fails_fast(monkeypatch):
    # Driver defaults: 30 s connect, 60 s waiting for a connection, 30 s of
    # transaction retries. The API's: 1 s, 1 s, none.
    monkeypatch.setenv("NEO4J_PASSWORD", "x")
    monkeypatch.setattr(api, "_graph", None)
    # Private attributes of driver 6.3 (no public getter): pinned version.
    driver = get_graph()._driver
    try:
        assert driver._pool.pool_config.connection_timeout == 1
        workspace = driver._default_workspace_config
        assert workspace.connection_acquisition_timeout == 1
        assert workspace.max_transaction_retry_time == 0
    finally:
        driver.close()
        monkeypatch.setattr(api, "_graph", None)


def test_openapi_documents_the_typed_response(client):
    spec = client.get("/openapi.json").json()
    for path, model in [
        ("/users/{user_id}/features", "UserFeatures"),
        ("/products/{product_id}/recommendations", "ProductRecommendations"),
    ]:
        get = spec["paths"][path]["get"]
        assert get["responses"]["200"]["content"]["application/json"]["schema"] == {
            "$ref": f"#/components/schemas/{model}"
        }
        assert "404" in get["responses"] and "422" in get["responses"]


def test_the_redis_client_fails_fast(monkeypatch):
    # No retries and 1 s timeouts: a 503 in about a second while Redis is
    # down, not after redis-py's default 10 retries (measured: 60 s).
    monkeypatch.setenv("REDIS_API_PASSWORD", "x")
    monkeypatch.setattr(api, "_client", None)
    client = get_redis()
    assert client.get_retry().get_retries() == 0
    kwargs = client.connection_pool.connection_kwargs
    assert kwargs["socket_timeout"] == 1 and kwargs["socket_connect_timeout"] == 1


def test_views_older_than_72_hours_are_not_served(client, r):
    # Present only because the user went quiet (no write trimmed it yet).
    seed(r)
    r.zadd(f"user:{USER}:viewed", {"9": NOW_MS - 73 * 3_600_000})
    products = [
        v["product_id"]
        for v in client.get(f"/users/{USER}/features").json()["recently_viewed"]
    ]
    assert products == [42, 26756]
