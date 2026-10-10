"""Serving API (spec FR17, ADR 010, ADR 018, ADR 019).

GET /users/{user_id}/features reads the online features that the features
stream keeps in Redis (spark/lakehouse/features.py).
GET /products/{product_id}/recommendations reads the co-purchase graph that
the graph job rebuilds daily in Neo4j (spark/lakehouse/graph_writer.py):

    (:Product {id, name, category, brand, department})
    -[:BOUGHT_WITH {weight}]->  one relationship per pair, read both ways;
                                weight = orders containing both products

Redis keys, as written by the stream (keep in step with features.py):

    user:{id}:viewed           sorted set  product id -> last view (epoch ms),
                                           views of the last 72 h
    user:{id}:events           sorted set  event id -> event time (epoch ms)
    user:{id}:session          sorted set  session id -> its last event (ms)
    user:{id}:cart:{session}   hash        item:<event id> -> price,
                                           purchased -> 1

GET /health is liveness (the process answers, no store called); GET /ready
reports each store (503 if one is down).

Redis: logs in as the read-only api ACL user. Neo4j Community has no roles,
so the API only opens read transactions (a guard against mistakes, not a
security boundary). Run:  uvicorn app:app
"""

import os
import time
from collections.abc import Callable
from datetime import UTC, datetime
from decimal import Decimal
from typing import Annotated, Literal
from uuid import UUID

import neo4j
import redis
from fastapi import Depends, FastAPI, HTTPException, Path, Query, Response
from neo4j.exceptions import ServiceUnavailable, SessionExpired, TransientError
from pydantic import BaseModel, Field
from redis.backoff import NoBackoff
from redis.retry import Retry

RECENT = 10  # products in recently_viewed (the stream keeps 10)
HOUR_MS = 3_600_000
VIEWED_WINDOW_MS = 72 * HOUR_MS  # as the stream's viewed window
MAX_RECOMMENDATIONS = 20
INT64_MAX = 2**63 - 1  # Neo4j integers are 64-bit

app = FastAPI(
    title="theLook serving API",
    description=(
        "Online user features (Redis) and co-purchase recommendations (Neo4j). "
        "Synthetic data (theLook); the basket affinity is synthetic too."
    ),
    version="0.6.0",
)


# --- response models (also the OpenAPI schema at /docs) ----------------------


class ViewedProduct(BaseModel):
    product_id: int
    viewed_at: datetime = Field(description="Time of the user's last view (UTC)")


class Cart(BaseModel):
    session_id: str
    value: Decimal = Field(description="Sum of the prices of the cart's items")
    items: int
    purchased: bool = Field(description="True when the session ended in a purchase")


class UserFeatures(BaseModel):
    user_id: UUID
    recently_viewed: list[ViewedProduct] = Field(description="Newest first, at most 10")
    cart: Cart | None = Field(description="The newest session's cart, if it has one")
    events_last_hour: int


class Recommendation(BaseModel):
    product_id: int
    name: str | None
    category: str | None
    weight: int = Field(description="Number of orders containing both products")


class ProductRecommendations(BaseModel):
    product_id: int
    name: str | None
    category: str | None
    recommendations: list[Recommendation] = Field(
        description="Heaviest first, ties by product id; empty if never bought "
        "with another product"
    )


class Health(BaseModel):
    status: str


StoreStatus = Literal["ok", "down"]


class Readiness(BaseModel):
    redis: StoreStatus = Field(description="Feature store (user features)")
    neo4j: StoreStatus = Field(description="Graph store (recommendations)")


# --- dependencies (replaced in tests) ------------------------------------------

_client: redis.Redis | None = None


def get_redis() -> redis.Redis:
    """One client per process (it holds a connection pool), created on first
    use so that importing the module needs no Redis."""
    global _client
    if _client is None:
        _client = redis.Redis(
            host=os.environ.get("REDIS_HOST", "redis"),
            port=int(os.environ.get("REDIS_PORT", "6379")),
            username="api",
            password=os.environ["REDIS_API_PASSWORD"],
            # Fail fast: a request should get a 503 in about a second, not
            # hang while Redis is down. No retries either: redis-py 8 retries
            # 10 times with backoff by default (measured: 60 s to a 503); the
            # 503 already tells the caller to retry. The container's DNS
            # options bound the name lookup of a stopped Redis (8 s -> 1 s).
            socket_timeout=1,
            socket_connect_timeout=1,
            retry=Retry(NoBackoff(), 0),
            decode_responses=True,
        )
    return _client


# Top neighbours of one product. The OPTIONAL MATCH keeps the product's row
# when it has no neighbour, so "no co-purchase yet" ([]) differs from "no
# such product" (no row, 404). collect() skips the nulls of that case.
RECOMMENDATIONS = """
MATCH (p:Product {id: $id})
OPTIONAL MATCH (p)-[r:BOUGHT_WITH]-(o:Product)
WITH p, r, o ORDER BY r.weight DESC, o.id ASC
RETURN p.name AS name, p.category AS category,
       collect(CASE WHEN o IS NULL THEN null ELSE
         {product_id: o.id, name: o.name, category: o.category, weight: r.weight}
       END)[..$limit] AS recommendations
"""


class GraphStore:
    """Read access to the co-purchase graph (replaced by a fake in tests)."""

    def __init__(self, driver: neo4j.Driver):
        self._driver = driver

    def recommendations(self, product_id: int, limit: int) -> dict | None:
        """The product's name, category and top neighbours; None if unknown."""
        records, _, _ = self._driver.execute_query(
            # Server-side limit: a stuck query fails instead of holding a
            # thread of the API's pool.
            neo4j.Query(RECOMMENDATIONS, timeout=2),
            id=product_id,
            limit=limit,
            routing_=neo4j.RoutingControl.READ,  # a read transaction
            database_="neo4j",
        )
        return records[0].data() if records else None

    def ping(self) -> None:
        self._driver.verify_connectivity()


_graph: GraphStore | None = None


def get_graph() -> GraphStore:
    """One driver per process (it holds a connection pool), created on first
    use. Fails fast like the Redis client. The driver's defaults (6.3) are a
    30 s connect timeout, up to 60 s waiting for a pooled connection and 30 s
    of transaction retries; a 503 already tells the caller to retry."""
    global _graph
    if _graph is None:
        _graph = GraphStore(
            neo4j.GraphDatabase.driver(
                os.environ.get("NEO4J_URI", "bolt://neo4j:7687"),
                auth=("neo4j", os.environ["NEO4J_PASSWORD"]),
                connection_timeout=1,
                connection_acquisition_timeout=1,
                max_transaction_retry_time=0,
            )
        )
    return _graph


def get_clock() -> Callable[[], float]:
    return time.time


RedisDep = Annotated[redis.Redis, Depends(get_redis)]
GraphDep = Annotated[GraphStore, Depends(get_graph)]
ClockDep = Annotated[Callable[[], float], Depends(get_clock)]

UNAVAILABLE = HTTPException(status_code=503, detail="feature store unavailable")
GRAPH_UNAVAILABLE = HTTPException(status_code=503, detail="graph store unavailable")
# The driver's errors when Neo4j cannot be reached or gives up on a query.
GRAPH_ERRORS = (ServiceUnavailable, SessionExpired, TransientError)


def _utc(ms: float) -> datetime:
    return datetime.fromtimestamp(ms / 1000, UTC)


# --- endpoints ---------------------------------------------------------------------


@app.get(
    "/users/{user_id}/features",
    response_model=UserFeatures,
    responses={
        404: {"description": "No features for this user (unknown or inactive for 72 h)"}
    },
)
def user_features(user_id: UUID, r: RedisDep, clock: ClockDep) -> UserFeatures:
    """Recently viewed products, the current session's cart, and the number
    of events in the last hour. A cache: up to ~1 minute behind the site."""
    prefix = f"user:{user_id}"  # str(UUID): lowercase, as in the source
    now_ms = clock() * 1000
    try:
        # One round trip for the three independent reads.
        pipe = r.pipeline(transaction=False)
        # Views of the last 72 h only: the stream trims older ones when it
        # writes, but a user who went quiet gets no more writes.
        pipe.zrevrangebyscore(
            f"{prefix}:viewed",
            "+inf",
            now_ms - VIEWED_WINDOW_MS,
            start=0,
            num=RECENT,
            withscores=True,
        )
        # Counted at read time, so the count falls as the hour passes even
        # when no new event arrives (ADR 018).
        pipe.zcount(f"{prefix}:events", now_ms - HOUR_MS, "+inf")
        pipe.zrevrange(f"{prefix}:session", 0, 0)
        viewed, events, session = pipe.execute()
        cart_fields = r.hgetall(f"{prefix}:cart:{session[0]}") if session else {}
    except (redis.ConnectionError, redis.TimeoutError) as error:
        raise UNAVAILABLE from error

    # Every event belongs to a session, so a user with any event in the
    # last 72 h has a session key.
    if not viewed and not session and not events:
        raise HTTPException(status_code=404, detail="no features for this user")

    prices = [Decimal(v) for k, v in cart_fields.items() if k.startswith("item:")]
    cart = (
        Cart(
            session_id=session[0],
            value=sum(prices, Decimal("0.00")),
            items=len(prices),
            purchased="purchased" in cart_fields,
        )
        if cart_fields
        else None
    )
    return UserFeatures(
        user_id=user_id,
        recently_viewed=[
            ViewedProduct(product_id=int(p), viewed_at=_utc(ms)) for p, ms in viewed
        ],
        cart=cart,
        events_last_hour=events,
    )


@app.get(
    "/products/{product_id}/recommendations",
    response_model=ProductRecommendations,
    responses={404: {"description": "No such product in the graph"}},
)
def product_recommendations(
    product_id: Annotated[int, Path(ge=1, le=INT64_MAX)],
    graph: GraphDep,
    limit: Annotated[int, Query(ge=1, le=MAX_RECOMMENDATIONS)] = 5,
) -> ProductRecommendations:
    """Products most often bought in the same orders, from the co-purchase
    graph. Rebuilt daily from gold: up to a day behind the site."""
    try:
        found = graph.recommendations(product_id, limit)
    except GRAPH_ERRORS as error:
        raise GRAPH_UNAVAILABLE from error
    if found is None:
        raise HTTPException(status_code=404, detail="no such product")
    return ProductRecommendations(product_id=product_id, **found)


@app.get("/health", response_model=Health)
def health() -> Health:
    """Liveness, for the container's healthcheck: the process answers. Calls
    no store, so a store outage never marks a working API unhealthy (an
    orchestrator would restart it for nothing). Store status: /ready."""
    return Health(status="ok")


@app.get(
    "/ready",
    response_model=Readiness,
    responses={503: {"model": Readiness, "description": "A store is down"}},
)
def ready(r: RedisDep, graph: GraphDep, response: Response) -> Readiness:
    """Readiness: can the API reach each store? Checks both (about a second
    each at worst) and reports both, so one outage does not hide another.
    503 if either is down; each data endpoint still works on its own store."""
    try:
        r.ping()
        redis_status = "ok"
    except (redis.ConnectionError, redis.TimeoutError):
        redis_status = "down"
    try:
        graph.ping()
        neo4j_status = "ok"
    except GRAPH_ERRORS:
        neo4j_status = "down"
    if "down" in (redis_status, neo4j_status):
        response.status_code = 503
    return Readiness(redis=redis_status, neo4j=neo4j_status)
