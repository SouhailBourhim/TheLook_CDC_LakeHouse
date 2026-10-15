# /// script
# requires-python = ">=3.12"
# dependencies = []
# ///
"""Neo4j frozen drill (P8 step 13; spec FR14): `docker pause neo4j` for
PAUSE seconds. Unlike a stop, a paused process keeps its TCP connections
open and never answers: the client sees silence, not a refused connection,
so only its own timeouts can end a request.

Expected, at every sample during the freeze:
- a recommendation request answers 503 within LIMIT seconds (the driver's
  1 s connection timeouts and the 2 s query timeout, api/app.py);
- /ready answers 503 and names neo4j down, Redis ok; /health stays 200
  (liveness only, so Docker does not restart a healthy API);
and once unpaused, recommendations answer 200 again within RECOVER seconds,
without restarting the API.

Needs the serving profile up and the graph built (P6).
Usage: uv run drills/neo4j_freeze.py [pause seconds, default 120]
Exit code 0 = every expectation held.
"""

import json
import subprocess
import sys
import time
import urllib.error
import urllib.request

API = "http://localhost:8000"
SAMPLE = 10  # seconds between samples
LIMIT = 3.0  # seconds a 503 may take
RECOVER = 60  # seconds to answer 200 again after the unpause


def get(path: str) -> tuple[int, float, dict]:
    """Status, seconds taken, JSON body ({} if none)."""
    start = time.monotonic()
    try:
        with urllib.request.urlopen(f"{API}{path}", timeout=30) as r:
            code, body = r.status, r.read()
    except urllib.error.HTTPError as e:
        code, body = e.code, e.read()
    except OSError:
        code, body = 0, b""
    try:
        parsed = json.loads(body or b"{}")
    except ValueError:
        parsed = {}
    return code, time.monotonic() - start, parsed


def a_product() -> int:
    """A product the graph has recommendations for."""
    for pid in range(1, 2000):
        if get(f"/products/{pid}/recommendations")[0] == 200:
            return pid
    sys.exit("no product with recommendations in ids 1-1999: is the graph built?")


def docker(*args: str) -> None:
    subprocess.run(["docker", *args], check=True, capture_output=True)


def api_restarts() -> int:
    out = subprocess.run(
        ["docker", "inspect", "thelook-api-1", "--format", "{{.RestartCount}}"],
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    return int(out.strip() or 0)


def main() -> int:
    sys.stdout.reconfigure(line_buffering=True)
    pause = int(sys.argv[1]) if len(sys.argv) > 1 else 120
    if get("/ready")[0] != 200:
        sys.exit(f"API not ready before the drill: {get('/ready')}")
    pid = a_product()
    recs = f"/products/{pid}/recommendations"
    restarts = api_restarts()
    print(f"product {pid}; before: recommendations {get(recs)[:2]}")

    ok = True
    docker("pause", "thelook-neo4j-1")
    start = time.monotonic()
    print(f"neo4j paused for {pause} s, sampling every {SAMPLE} s")
    try:
        while time.monotonic() - start < pause:
            code, took, _ = get(recs)
            ready, _, body = get("/ready")
            health = get("/health")[0]
            fine = (
                code == 503
                and took <= LIMIT
                and ready == 503
                and body.get("neo4j") == "down"
                and body.get("redis") == "ok"
                and health == 200
            )
            ok &= fine
            print(
                f"  t+{time.monotonic() - start:4.0f} s  recommendations {code} in "
                f"{took:.2f} s, /ready {ready} {body}, /health {health}"
                f"{'' if fine else '  <- unexpected'}"
            )
            time.sleep(SAMPLE)
    finally:
        docker("unpause", "thelook-neo4j-1")
    back = time.monotonic()
    while get(recs)[0] != 200 and time.monotonic() - back < RECOVER:
        time.sleep(0.5)
    recovered = time.monotonic() - back
    code = get(recs)[0]
    ok &= code == 200 and recovered < RECOVER
    print(
        f"unpaused: recommendations {code} after {recovered:.1f} s, "
        f"/ready {get('/ready')[0]}, API restarts during the drill "
        f"{api_restarts() - restarts}"
    )
    ok &= api_restarts() == restarts
    print("every expectation held" if ok else "an expectation failed")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
