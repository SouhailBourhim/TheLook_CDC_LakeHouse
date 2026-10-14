# /// script
# requires-python = ">=3.12"
# dependencies = []
# ///
"""Bronze stream killed mid-batch (P8 step 12; spec FR14): the replayed
batch must not be appended twice.

  1. capture the topics' end offsets (drills/verify_bronze.py --capture)
  2. follow the stream's log; at the first bronze table commit of a batch,
     kill -9 the driver JVM inside the container. That is the hardest
     moment: some tables hold the batch, the ledger row and Spark's
     checkpoint commit do not, so the restart replays the whole batch.
     A crash, not `docker kill` (a manual stop, which Docker never
     restarts): the restart policy must start the stream again
  3. time until the stream is back and its first batch line, which must
     show a table skipped(replay): the batch mark found in its snapshots
  4. let the load run SETTLE seconds, stop the generator and the review
     simulator, then Kafka = bronze over the window (drills/verify_bronze.py)
  5. start the load again

Needs the core and stream profiles up, the load running, no erasure in
progress. Usage: uv run drills/bronze_stream_kill.py
Exit code 0 = killed mid-batch, restarted alone, replay skipped, exact.
"""

import re
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
COMPOSE = ["docker", "compose", "-f", str(ROOT / "onprem" / "compose.yaml")]
CONTAINER = "thelook-bronze-stream-1"
SETTLE = 300  # seconds of load after the restart
LOAD = ["generator", "review-simulator"]
# A data table's commit; the ledger (stream_batches) and erasure_log are
# written after the appends.
TABLE_COMMIT = re.compile(
    r"Successfully committed to table lake\.thelook_bronze\.((shop|web)_\w+)"
)
BATCH_LINE = re.compile(r"INFO: batch (\d+): (.*) in [\d.]+ s")


def now() -> str:
    return time.strftime("%H:%M:%S")


def sh(*args: str) -> subprocess.CompletedProcess:
    return subprocess.run(args, capture_output=True, text=True, check=False)


def inspect(field: str) -> str:
    return sh(
        "docker", "inspect", CONTAINER, "--format", f"{{{{{field}}}}}"
    ).stdout.strip()


def follow(since: str):
    """Yield the stream's log lines from `since` (docker logs --since)."""
    p = subprocess.Popen(
        ["docker", "logs", "-f", "--since", since, CONTAINER],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    try:
        yield from p.stdout
    finally:
        p.kill()


def main() -> int:
    sys.stdout.reconfigure(line_buffering=True)
    if inspect(".State.Status") != "running":
        sys.exit("bronze stream not running")
    print(f"{now()} 1. capture")
    capture = subprocess.run(
        ["uv", "run", "-q", str(ROOT / "drills" / "verify_bronze.py"), "--capture"],
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()
    print(f"  {capture}")
    restarts = int(inspect(".RestartCount") or 0)

    print(f"{now()} 2. wait for a table commit, then kill -9 the driver")
    last_batch, complete = None, 0
    for line in follow("1s"):
        if m := BATCH_LINE.search(line):
            last_batch, complete = int(m.group(1)), complete + 1
        elif complete >= 2 and (m := TABLE_COMMIT.search(line)):
            # A batch reads from where the previous one ended, planned when
            # that one started: only the third batch after the capture
            # starts after it, so the window covers the killed batch whole
            # (the first run killed a batch that began before it, P8 step 12).
            sh("docker", "exec", CONTAINER, "pkill", "-9", "-o", "java")
            print(
                f"  {now()} killed after {m.group(1)} committed in batch "
                f"{last_batch + 1} (batch {last_batch} was the last complete one)"
            )
            break

    print(f"{now()} 3. restart")
    killed = time.monotonic()
    while int(inspect(".RestartCount") or 0) == restarts:
        if time.monotonic() - killed > 300:
            sys.exit("the restart policy did not start the stream in 5 min")
        time.sleep(1)
    print(
        f"  {now()} restarted by Docker {time.monotonic() - killed:.0f} s after the kill"
    )
    replayed = False
    for line in follow(inspect(".State.StartedAt")):
        if m := BATCH_LINE.search(line):
            print(f"  {now()} first batch: {m.group(1)}: {m.group(2)}")
            replayed = int(m.group(1)) == last_batch + 1 and "skipped(replay)" in line
            break
    print(f"  replayed batch skipped what it had committed: {replayed}")

    print(f"{now()} 4. load for {SETTLE} s, then stop it")
    time.sleep(SETTLE)
    subprocess.run([*COMPOSE, "stop", *LOAD], capture_output=True, check=False)
    try:
        print(f"{now()}    Kafka = bronze")
        exact = (
            subprocess.run(
                [
                    "uv",
                    "run",
                    "-q",
                    str(ROOT / "drills" / "verify_bronze.py"),
                    str(ROOT / capture),
                ]
            ).returncode
            == 0
        )
    finally:
        print(f"{now()} 5. start the load again")
        subprocess.run([*COMPOSE, "start", *LOAD], capture_output=True, check=False)
    print(f"replay skipped: {replayed}, Kafka = bronze: {exact}")
    return 0 if replayed and exact else 1


if __name__ == "__main__":
    sys.exit(main())
