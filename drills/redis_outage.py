# /// script
# requires-python = ">=3.12"
# dependencies = []
# ///
"""Redis outage drill (P5, ADR 007 and ADR 018): Redis stops for OUTAGE
seconds while the generator writes as usual.

Expected: bronze never notices (a separate application, ADR 018); the API
answers 503 and /ready reports Redis down, while /health (liveness) stays
200 and the container healthy; the features stream fails its batch once its
client's retries run out, restarts from its checkpoint (restart policy) and,
once Redis is back, catches up; new views are served again (checked with
drills/features_freshness.py, 3 samples).

Needs the core, stream and serving profiles up (serving includes Neo4j,
which /ready checks too), bronze caught up. Usage:
  uv run drills/redis_outage.py [outage seconds, default 300]
"""

import re
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
COMPOSE = ["docker", "compose", "-f", str(ROOT / "onprem" / "compose.yaml")]
SAMPLE = 10  # seconds between samples during the outage


def sh(*args: str) -> str:
    return subprocess.run(args, capture_output=True, text=True, check=False).stdout


def compose(*args: str) -> str:
    return sh(*COMPOSE, *args)


def status(path: str) -> int:
    try:
        with urllib.request.urlopen(f"http://localhost:8000{path}", timeout=3) as r:
            return r.status
    except urllib.error.HTTPError as e:
        return e.code
    except OSError:
        return 0  # no answer at all


def restarts(service: str) -> int:
    return int(
        sh(
            "docker", "inspect", f"thelook-{service}-1", "--format", "{{.RestartCount}}"
        ).strip()
        or 0
    )


def state(service: str) -> str:
    return compose("ps", service, "--format", "{{.Status}}").strip() or "not running"


BATCH = re.compile(r"INFO: batch (\d+): (.*) in ([\d.]+) s")


def batches(service: str, since: float) -> list[tuple[int, str, float]]:
    """(batch id, summary, seconds) of the batches a stream logged since."""
    logs = compose("logs", "--since", f"{int(time.time() - since) + 1}s", service)
    return [(int(m[1]), m[2], float(m[3])) for m in BATCH.finditer(logs)]


def main() -> int:
    outage = int(sys.argv[1]) if len(sys.argv) > 1 else 300
    restarts_before = restarts("features-stream")
    print(
        f"before: API /ready {status('/ready')}, "
        f"features-stream {state('features-stream')}"
    )

    start = time.time()
    compose("stop", "-t", "10", "redis")
    print(f"redis stopped; {outage} s outage, sampling every {SAMPLE} s")
    while time.time() - start < outage:
        time.sleep(SAMPLE)
        print(
            f"  t+{time.time() - start:4.0f} s  API /ready {status('/ready')} "
            f"/health {status('/health')} ({state('api')})  "
            f"features-stream: {state('features-stream')}, "
            f"restarts +{restarts('features-stream') - restarts_before}",
            flush=True,
        )
    bronze = batches("bronze-stream", start)
    stopped_for = time.time() - start

    compose("start", "redis")
    back = time.time()
    while status("/ready") != 200:
        time.sleep(0.5)
    api_back = time.time() - back
    while not any(
        "writes" in summary for _, summary, _ in batches("features-stream", back)
    ):
        time.sleep(1)
    features_back = time.time() - back

    print(f"\nRedis down for {stopped_for:.0f} s")
    print(f"bronze during the outage: {len(bronze)} batches committed")
    for batch_id, summary, seconds in bronze:
        print(f"  batch {batch_id}: {summary[:90]} in {seconds:.1f} s")
    print(f"features-stream restarts: {restarts('features-stream') - restarts_before}")
    print(
        f"after Redis restart: API ready in {api_back:.1f} s, "
        f"features stream writing again in {features_back:.1f} s"
    )
    for batch_id, summary, seconds in batches("features-stream", back)[:3]:
        print(f"  features batch {batch_id}: {summary} in {seconds:.1f} s")

    print("\nfreshness after recovery:", flush=True)
    return subprocess.run(
        ["uv", "run", "-q", str(ROOT / "drills" / "features_freshness.py"), "3"],
        check=False,
    ).returncode


if __name__ == "__main__":
    sys.exit(main())
