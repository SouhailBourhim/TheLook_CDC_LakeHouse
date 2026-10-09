"""Serving API (spec FR17, ADR 010, ADR 018).

GET /users/{user_id}/features reads the online features that the features
stream keeps in Redis (spark/lakehouse/features.py). P6 adds
GET /products/{product_id}/recommendations (Neo4j) to the same app.

Redis keys, as written by the stream (keep in step with features.py):

    user:{id}:viewed           sorted set  product id -> last view (epoch ms),
                                           views of the last 72 h
    user:{id}:events           sorted set  event id -> event time (epoch ms)
    user:{id}:session          sorted set  session id -> its last event (ms)
    user:{id}:cart:{session}   hash        item:<event id> -> price,
                                           purchased -> 1

Logs in as the read-only api ACL user. Run:  uvicorn app:app
"""

import os
import time
from collections.abc import Callable
from datetime import UTC, datetime
from decimal import Decimal
from typing import Annotated
from uuid import UUID

import redis
from fastapi import Depends, FastAPI, HTTPException
from pydantic import BaseModel, Field
from redis.backoff import NoBackoff
from redis.retry import Retry

RECENT = 10  # products in recently_viewed (the stream keeps 10)
HOUR_MS = 3_600_000
VIEWED_WINDOW_MS = 72 * HOUR_MS  # as the stream's viewed window

app = FastAPI(
    title="theLook serving API",
    description="Online user features (Redis). Synthetic data (theLook).",
    version="0.5.0",
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


class Health(BaseModel):
    status: str


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


def get_clock() -> Callable[[], float]:
    return time.time


RedisDep = Annotated[redis.Redis, Depends(get_redis)]
ClockDep = Annotated[Callable[[], float], Depends(get_clock)]

UNAVAILABLE = HTTPException(status_code=503, detail="feature store unavailable")


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
    "/health", response_model=Health, responses={503: {"description": "Redis down"}}
)
def health(r: RedisDep) -> Health:
    """For the container's healthcheck: the API is up and reaches Redis."""
    try:
        r.ping()
    except (redis.ConnectionError, redis.TimeoutError) as error:
        raise UNAVAILABLE from error
    return Health(status="ok")
