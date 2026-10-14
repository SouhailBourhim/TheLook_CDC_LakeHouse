# /// script
# requires-python = ">=3.12"
# dependencies = []
# ///
"""Connect and Kafka restart drill (P8 step 10; spec FR14): the Connect
worker is killed mid-stream, then Kafka is restarted, both while the
generator writes; nothing may be lost or duplicated.

  1. capture the topics' end offsets (drills/verify_bronze.py --capture)
  2. kill -9 the Connect worker (a crash: no clean shutdown, a transaction
     may be open), wait KILLED seconds, start it again (Connect has no
     restart policy: this is the operator's start), time until both
     connectors' tasks run
  3. restart Kafka (clean stop, 60 s grace): time until the broker is
     healthy, then watch the connectors' tasks and the bronze stream
  4. let the load run SETTLE seconds, stop the generator and the review
     simulator, wait for the topics to go quiet
  5. sources = Kafka (drills/verify_cdc.py), Kafka = bronze over the drill
     window (drills/verify_bronze.py)
  6. start the generator and the review simulator again

A connector task found FAILED is restarted through Connect's REST API and
reported: the drill says what an operator had to do.

Needs the core, stream and airflow profiles up, both connectors RUNNING, no
erasure in progress. Usage: uv run drills/connect_kafka_restarts.py
Exit code 0 = no loss, no duplicate.
"""

import json
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
COMPOSE = ["docker", "compose", "-f", str(ROOT / "onprem" / "compose.yaml")]
CONNECT = "http://localhost:8083"
CONNECTORS = ["postgres-source", "mongo-source"]
KILLED = 60  # seconds the worker stays dead
SETTLE = 300  # seconds of load after the Kafka restart
LOAD = ["generator", "review-simulator"]


def now() -> str:
    return time.strftime("%H:%M:%S")


def sh(*args: str) -> subprocess.CompletedProcess:
    return subprocess.run(args, capture_output=True, text=True, check=False)


def compose(*args: str) -> subprocess.CompletedProcess:
    return sh(*COMPOSE, *args)


def tasks() -> dict[str, list[str]]:
    """Each connector's task states; {} while the worker does not answer."""
    out = {}
    for name in CONNECTORS:
        try:
            with urllib.request.urlopen(
                f"{CONNECT}/connectors/{name}/status", timeout=5
            ) as r:
                status = json.load(r)
        except OSError:
            return {}
        out[name] = [t["state"] for t in status["tasks"]]
    return out


def restart_failed(states: dict[str, list[str]]) -> list[str]:
    restarted = []
    for name, task_states in states.items():
        for i, state in enumerate(task_states):
            if state == "FAILED":
                request = urllib.request.Request(
                    f"{CONNECT}/connectors/{name}/tasks/{i}/restart", method="POST"
                )
                urllib.request.urlopen(request, timeout=10).close()
                restarted.append(f"{name}/{i}")
    return restarted


def wait_running(limit: int = 600) -> float:
    """Seconds until every task of both connectors is RUNNING; restarts a
    FAILED task once and says so."""
    start = time.monotonic()
    restarted = False
    while time.monotonic() - start < limit:
        states = tasks()
        if states and all(s == ["RUNNING"] for s in states.values()):
            return time.monotonic() - start
        if states and not restarted and (done := restart_failed(states)):
            print(f"  {now()} FAILED task restarted by the drill: {done}")
            restarted = True
        time.sleep(2)
    raise RuntimeError(f"connectors not RUNNING after {limit} s: {tasks()}")


def container(service: str, field: str) -> str:
    return sh(
        "docker", "inspect", f"thelook-{service}-1", "--format", f"{{{{{field}}}}}"
    ).stdout.strip()


def healthy(service: str) -> bool:
    return container(service, ".State.Health.Status") == "healthy"


def uv(script: str, *args: str) -> subprocess.CompletedProcess:
    """A drill script, its output shown as it comes."""
    return subprocess.run(["uv", "run", "-q", str(ROOT / "drills" / script), *args])


def main() -> int:
    sys.stdout.reconfigure(line_buffering=True)
    if tasks() == {} or not all(s == ["RUNNING"] for s in tasks().values()):
        sys.exit(f"connectors not all RUNNING: {tasks()}")
    print(f"{now()} 1. capture")
    capture = subprocess.run(
        ["uv", "run", "-q", str(ROOT / "drills" / "verify_bronze.py"), "--capture"],
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()
    print(f"  {capture}")
    stream_restarts = int(container("bronze-stream", ".RestartCount") or 0)

    print(f"{now()} 2. kill -9 connect")
    sh("docker", "kill", "--signal", "KILL", "thelook-connect-1")
    time.sleep(KILLED)
    started = time.monotonic()
    compose("start", "connect")
    seconds = wait_running()
    print(
        f"  {now()} connectors RUNNING {time.monotonic() - started:.0f} s after "
        f"the start ({KILLED} s dead before it; tasks {seconds:.0f} s)"
    )

    print(f"{now()} 3. restart kafka")
    started = time.monotonic()
    compose("restart", "kafka")
    while not healthy("kafka"):
        time.sleep(2)
    print(f"  {now()} kafka healthy after {time.monotonic() - started:.0f} s")
    seconds = wait_running()
    print(f"  {now()} connectors RUNNING ({seconds:.0f} s after kafka was healthy)")

    print(f"{now()} 4. load for {SETTLE} s, then stop it")
    time.sleep(SETTLE)
    compose("stop", *LOAD)
    time.sleep(120)  # capture lag is below a second (P1); bronze needs a batch
    restarts = int(container("bronze-stream", ".RestartCount") or 0)
    print(
        f"  bronze stream: {container('bronze-stream', '.State.Status')}, "
        f"restarts during the drill {restarts - stream_restarts}"
    )

    try:
        print(f"{now()} 5. sources = Kafka")
        sources = uv("verify_cdc.py").returncode
        print(f"{now()}    Kafka = bronze")
        bronze = uv("verify_bronze.py", str(ROOT / capture)).returncode
    finally:
        print(f"{now()} 6. start the load again")
        compose("start", *LOAD)
    print(f"sources = Kafka: {sources == 0}, Kafka = bronze: {bronze == 0}")
    return 0 if sources == 0 and bronze == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
