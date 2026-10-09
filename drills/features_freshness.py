# /// script
# requires-python = ">=3.12"
# dependencies = [
#   "pymongo==4.18.2",
# ]
# ///
"""Measure P5's acceptance: a product view appears in the user's features
(GET /users/{id}/features) within 1 minute (spec section 9, FR15).

Method (docs/results.md, P5): open a MongoDB change stream on web.events
and take the next product view by a known user (ghost sessions are not
features). Its change event carries wallTime, the server's commit time
(ms). Then poll the API every POLL seconds until that product is in the
user's recently_viewed with the view's own time (created_at), or later if
the user viewed it again. Latency = time first seen - commit time; polling
makes it an upper bound (+POLL s). Each sample opens a fresh change stream,
so samples are independent views.

MongoDB's clock (in Docker's VM) and this machine's may differ: the offset
is measured first and printed, since it shifts every latency.

Then the API's own latency: REQUESTS GETs over one kept-alive connection,
cycling over the sampled users; p50 and p99.

Needs the core and serving profiles up. Usage:
  uv run drills/features_freshness.py [samples, default 5]
"""

import datetime
import http.client
import json
import statistics
import sys
import time
from pathlib import Path
from urllib.parse import quote_plus

from pymongo import MongoClient

POLL = 0.5
TIMEOUT = 120
TARGET = 60  # seconds (spec section 9)
REQUESTS = 1000
API = ("localhost", 8000)


def load_env(path: Path) -> dict:
    env = {}
    for line in path.read_text().splitlines():
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            env[k] = v
    return env


env = load_env(Path(__file__).resolve().parent.parent / "onprem" / ".env")
mongo = MongoClient(
    f"mongodb://{env.get('MONGO_ROOT_USER', 'root')}:{quote_plus(env['MONGO_ROOT_PASSWORD'])}"
    "@localhost:27017/?directConnection=true&authSource=admin",
    # Datetimes come back with their UTC zone. Naive ones (the default) are
    # UTC, but .timestamp() reads a naive datetime as local time: on this
    # UTC+1 laptop every commit looked an hour old (first run: a timeout
    # before the first poll).
    tz_aware=True,
)
events = mongo["web"]["events"]

PRODUCT_VIEWS = [
    {
        "$match": {
            "operationType": "insert",
            "fullDocument.event_type": "product",
            "fullDocument.user_id": {"$exists": True},
        }
    }
]


def clock_offset() -> float:
    """MongoDB's clock minus this machine's, in seconds (midpoint of the
    round trip)."""
    before = time.time()
    # command() does not inherit the client's tz_aware: it decodes with
    # pymongo's defaults unless told otherwise (second run: offset -3600 s).
    server = mongo.admin.command("hello", codec_options=mongo.codec_options)[
        "localTime"
    ]
    after = time.time()
    return server.timestamp() - (before + after) / 2


def next_view() -> tuple[str, int, datetime.datetime, float]:
    """(user_id, product_id, created_at, commit time in s) of the next
    product view committed from now on."""
    with events.watch(PRODUCT_VIEWS) as stream:
        change = stream.next()
    doc = change["fullDocument"]
    product_id = int(doc["uri"].rsplit("/", 1)[1])
    created_at = doc["created_at"]
    return doc["user_id"], product_id, created_at, change["wallTime"].timestamp()


def features(conn: http.client.HTTPConnection, user_id: str) -> dict | None:
    conn.request("GET", f"/users/{user_id}/features")
    response = conn.getresponse()
    body = response.read()
    return json.loads(body) if response.status == 200 else None


def seen(body: dict | None, product_id: int, created_at: datetime.datetime) -> bool:
    for view in (body or {}).get("recently_viewed", []):
        viewed_at = datetime.datetime.fromisoformat(view["viewed_at"])
        # The API's viewed_at is in ms; a later one means a newer view of
        # the same product, which also proves ours went through.
        if view[
            "product_id"
        ] == product_id and viewed_at >= created_at - datetime.timedelta(
            milliseconds=1
        ):
            return True
    return False


def measure(conn, offset: float) -> tuple[str, float]:
    user_id, product_id, created_at, committed = next_view()
    committed -= offset  # on this machine's clock
    while time.time() - committed < TIMEOUT:
        if seen(features(conn, user_id), product_id, created_at):
            latency = time.time() - committed
            if latency < 0:
                # Impossible: a clock or time-zone error, not a fast pipeline.
                raise RuntimeError(f"negative latency {latency:.1f} s: check clocks")
            return user_id, latency
        time.sleep(POLL)
    raise TimeoutError(
        f"view of {product_id} by {user_id} not served after {TIMEOUT} s"
    )


def api_latency(conn, users: list[str]) -> list[float]:
    ms = []
    for i in range(REQUESTS):
        start = time.perf_counter()
        features(conn, users[i % len(users)])
        ms.append((time.perf_counter() - start) * 1000)
    return ms


def main() -> int:
    samples = int(sys.argv[1]) if len(sys.argv) > 1 else 5
    offset = clock_offset()
    print(f"clock offset (MongoDB - this machine): {offset * 1000:+.0f} ms")
    conn = http.client.HTTPConnection(*API, timeout=5)
    users, latencies = [], []
    for i in range(samples):
        user_id, latency = measure(conn, offset)
        users.append(user_id)
        latencies.append(latency)
        print(
            f"sample {i + 1}: in the API {latency:5.1f} s after the commit", flush=True
        )
    lat = sorted(latencies)
    print(
        f"freshness n={len(lat)}  min={lat[0]:.1f} s  "
        f"median={statistics.median(lat):.1f} s  max={lat[-1]:.1f} s  "
        f"(target < {TARGET} s, upper bounds +{POLL} s)"
    )
    ms = sorted(api_latency(conn, users))
    print(
        f"API latency n={len(ms)}  p50={ms[len(ms) // 2]:.1f} ms  "
        f"p99={ms[int(len(ms) * 0.99)]:.1f} ms  max={ms[-1]:.1f} ms"
    )
    return 0 if lat[-1] < TARGET else 1


if __name__ == "__main__":
    sys.exit(main())
