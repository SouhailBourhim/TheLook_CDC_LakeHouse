# Run from api/:  python -m pytest
"""GET /users/{id}/features and /health on fakeredis, with a fixed clock.

Keys are seeded as the features stream writes them
(spark/lakehouse/redis_writer.py).
"""

import fakeredis
import pytest
from fastapi.testclient import TestClient

from app import app, get_clock, get_redis

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


@pytest.fixture
def client(r, clock):
    app.dependency_overrides[get_redis] = lambda: r
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
    assert client.get("/health").status_code == 503


def test_health(client):
    assert client.get("/health").json() == {"status": "ok"}


def test_openapi_documents_the_typed_response(client):
    spec = client.get("/openapi.json").json()
    get = spec["paths"]["/users/{user_id}/features"]["get"]
    assert get["responses"]["200"]["content"]["application/json"]["schema"] == {
        "$ref": "#/components/schemas/UserFeatures"
    }
    assert "404" in get["responses"] and "422" in get["responses"]
