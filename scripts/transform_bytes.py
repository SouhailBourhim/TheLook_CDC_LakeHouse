"""Bytes one transform run downloads (P9 scan benchmark, the S3 half; the
method is P4's): received bytes on the containers that read the lake, the
Spark drivers (airflow-scheduler, LocalExecutor) and executors
(spark-worker), during the run, minus an idle baseline measured just
before. Bronze's executors share the worker, so the baseline is mostly
bronze's own traffic at the same load.

Network counters are the ground truth (P4): Spark's "bytes read" also
counts reads from its own cache.

Usage: uv run --with boto3==1.40.40 python scripts/transform_bytes.py [idle s]
  (default 600). Triggers one transform run after the baseline; transform
  must not be running. Prints MB received per container, net of baseline.
"""

import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "drills"))
import backfill_drill as bd  # noqa: E402  Airflow helpers

CONTAINERS = ["thelook-airflow-scheduler-1", "thelook-spark-worker-1"]


def rx() -> dict[str, int]:
    """Bytes received so far on each container's eth0."""
    out = {}
    for c in CONTAINERS:
        r = subprocess.run(
            ["docker", "exec", c, "cat", "/sys/class/net/eth0/statistics/rx_bytes"],
            capture_output=True,
            text=True,
            check=True,
        )
        out[c] = int(r.stdout)
    return out


def main() -> int:
    idle = int(sys.argv[1]) if len(sys.argv) > 1 else 600
    a0, t0 = rx(), time.time()
    time.sleep(idle)
    a1, t1 = rx(), time.time()
    rate = {c: (a1[c] - a0[c]) / (t1 - t0) for c in CONTAINERS}
    airflow = bd.Airflow()
    run = airflow.call("POST", "/api/v2/dags/transform/dagRuns", {"logical_date": None})
    b0, s0 = rx(), time.time()
    while (
        state := airflow.call(
            "GET", f"/api/v2/dags/transform/dagRuns/{run['dag_run_id']}"
        )["state"]
    ) not in ("success", "failed"):
        time.sleep(15)
    b1, s1 = rx(), time.time()
    took = s1 - s0
    print(f"transform {run['dag_run_id']}: {state} in {took / 60:.1f} min")
    total = 0.0
    for c in CONTAINERS:
        gross = (b1[c] - b0[c]) / 1e6
        net = gross - rate[c] * took / 1e6
        total += net
        print(
            f"  {c}: {gross:.0f} MB received, idle baseline "
            f"{rate[c] * 60 / 1e6:.1f} MB/min, net {net:.0f} MB"
        )
    print(f"  net total: {total:.0f} MB")
    return 0 if state == "success" else 1


if __name__ == "__main__":
    sys.exit(main())
