# /// script
# requires-python = ">=3.12"
# dependencies = ["boto3==1.40.40"]
# ///
"""AWS unreachable for a few minutes in the middle of a transform run
(P9 step 2): the run must finish without an Airflow retry, its jobs waiting
inside with_retries (lakehouse.bronze) instead of failing.

The outage is the host's (the network drops seen since P4): traffic leaving
the Spark driver's container (airflow-scheduler, LocalExecutor) and the
worker's (the executors, bronze's too) is dropped, except to Docker's
private networks. Docker's DNS forwards external names from inside the
container's network namespace, so S3 and Glue names stop resolving, as in
the real outages, while container names and Kafka keep working. The rules
are set from a helper container sharing each target's network namespace
(NET_ADMIN on the helper only; the targets stay unprivileged), and always
removed at the end.

  1. trigger transform; wait until its silver task has run 60 s
  2. drop external traffic for OUTAGE seconds, then restore it
  3. wait for the run; every task must succeed on its first try, and the
     logs show the jobs' own retries

Needs the core, stream and airflow profiles up, transform not running.
Usage: uv run drills/aws_outage.py [outage seconds, default 180]
Exit code 0 = the run succeeded with no Airflow retry.
"""

import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import backfill_drill as bd  # noqa: E402  Airflow helpers

TARGETS = ["thelook-airflow-scheduler-1", "thelook-spark-worker-1"]
HELPER = "alpine:3.22"
PRIVATE = ["127.0.0.0/8", "10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16"]


def now() -> str:
    return time.strftime("%H:%M:%S", time.gmtime())


def helper(target: str) -> str:
    """A helper container in target's network namespace, iptables installed
    while the network still works; reused to cut and to restore (a new
    helper could not install anything once traffic is dropped)."""
    name = f"outage-{target}"
    subprocess.run(["docker", "rm", "-f", name], capture_output=True, check=False)
    subprocess.run(
        [
            "docker",
            "run",
            "-d",
            "--name",
            name,
            "--network",
            f"container:{target}",
            "--cap-add",
            "NET_ADMIN",
            HELPER,
            "sh",
            "-c",
            "apk add -q iptables && touch /ready && sleep 3600",
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    deadline = time.monotonic() + 60
    while subprocess.run(
        ["docker", "exec", name, "test", "-f", "/ready"],
        capture_output=True,
        check=False,
    ).returncode:
        if time.monotonic() > deadline:
            raise RuntimeError(f"{name}: iptables not installed")
        time.sleep(1)
    return name


def iptables(name: str, script: str) -> None:
    out = subprocess.run(
        ["docker", "exec", name, "sh", "-c", script],
        capture_output=True,
        text=True,
        check=False,
    )
    if out.returncode:
        raise RuntimeError(f"{name}: {out.stderr.strip()}")


def cut(name: str) -> None:
    # A chain that returns for private ranges and drops everything else.
    accept = " ".join(f"iptables -A OUTAGE -d {net} -j RETURN;" for net in PRIVATE)
    iptables(
        name,
        f"iptables -N OUTAGE && {accept} iptables -A OUTAGE -j DROP"
        " && iptables -I OUTPUT -j OUTAGE",
    )


def restore(name: str) -> None:
    iptables(
        name,
        "iptables -D OUTPUT -j OUTAGE; iptables -F OUTAGE; iptables -X OUTAGE; true",
    )


def main() -> int:
    outage = int(sys.argv[1]) if len(sys.argv) > 1 else 180
    airflow = bd.Airflow()
    run = airflow.call("POST", "/api/v2/dags/transform/dagRuns", {"logical_date": None})
    run_id = run["dag_run_id"]
    tasks = f"/api/v2/dags/transform/dagRuns/{run_id}/taskInstances"
    print(f"{now()} transform triggered: {run_id}")
    while True:
        silver = next(
            t
            for t in airflow.call("GET", tasks)["task_instances"]
            if t["task_id"] == "silver"
        )
        if silver["state"] == "running" and silver.get("start_date"):
            break
        time.sleep(5)
    time.sleep(60)
    helpers = [helper(t) for t in TARGETS]
    try:
        for name in helpers:
            cut(name)
        print(f"{now()} AWS unreachable from {', '.join(TARGETS)} for {outage} s")
        time.sleep(outage)
    finally:
        for name in helpers:
            restore(name)
            subprocess.run(
                ["docker", "rm", "-f", name], capture_output=True, check=False
            )
        print(f"{now()} network restored")
    while (
        state := airflow.call("GET", f"/api/v2/dags/transform/dagRuns/{run_id}")[
            "state"
        ]
    ) not in ("success", "failed"):
        time.sleep(30)
    instances = airflow.call("GET", tasks)["task_instances"]
    print(f"{now()} run {state}")
    retried = []
    for t in sorted(instances, key=lambda t: t.get("start_date") or ""):
        print(f"  {t['task_id']}: {t['state']}, try {t['try_number']}")
        if t["try_number"] > 1:
            retried.append(t["task_id"])
    ok = state == "success" and not retried
    print("PASS: no Airflow retry" if ok else f"FAIL: retried {retried or '-'}")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
