# /// script
# requires-python = ">=3.12"
# dependencies = []
# ///
"""Load test (P9 step 6; O8): applies the method in docs/results.md
("P9: load test — method"), written before this run.

For each level (iterations/s, workers):
  1. link probe; the generator at the level's rate (load mode)
  2. WARMUP s, then a WINDOW s measurement window: docker stats every 30 s
  3. link probe; the generator back to 5/s; wait until bronze has finished
     a batch that started after the window (every window row committed)
  4. Prometheus over the window (events/s, Debezium lag, bronze batch
     durations), the generator's pace lines, and the per-row
     source-to-bronze lag (spark/jobs/bronze_lag.py on the cluster)
  5. verdict: sustained = generator kept up, lag p95 < 120 s, and the lag
     of the window's last 3 minutes at most one trigger above its first 3

Stops at the first level not sustained, or when the lag passes 10 minutes.
Needs the core, monitoring and stream profiles up (serving and airflow
stopped), bronze caught up, within one UTC day.

Usage: uv run drills/throughput_run.py [level ...]
  level = rate/workers[/containers]: total iterations/s, workers per
  generator container, generator containers sharing the rate (one Python
  process caps near 130 iterations/s, the GIL; default 1).
  Default: 10/1 20/2 40/4 80/8 160/8/2. Results: drills/logs/throughput_run-*.log
"""

import datetime
import json
import os
import re
import statistics
import subprocess
import sys
import threading
import time
import urllib.parse
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
COMPOSE = ["docker", "compose", "-f", str(ROOT / "onprem" / "compose.yaml")]
PROMETHEUS = "http://localhost:9090"
LEVELS = ["10/1", "20/2", "40/4", "80/8", "160/8/2"]
# The method's timings; overridable only for a short smoke run of the drill.
WARMUP = int(os.environ.get("THROUGHPUT_WARMUP", 180))
WINDOW = int(os.environ.get("THROUGHPUT_WINDOW", 600))
TRIGGER = 60
BASE_RATE = "5"
CONTAINERS = [
    "generator",
    "postgres",
    "connect",
    "kafka",
    "mongodb",
    "bronze-stream",
    "spark-worker",
]
LOG = (
    ROOT
    / "drills"
    / "logs"
    / f"throughput_run-{time.strftime('%Y%m%dT%H%M%SZ', time.gmtime())}.log"
)


def log(line: str) -> None:
    stamped = f"[{time.strftime('%H:%M:%S', time.gmtime())}] {line}"
    print(stamped, flush=True)
    with LOG.open("a") as f:
        f.write(stamped + "\n")


def sh(*args: str, env: dict | None = None) -> subprocess.CompletedProcess:
    return subprocess.run(
        args,
        capture_output=True,
        text=True,
        check=False,
        env={**os.environ, **(env or {})},
    )


def prom(query: str, at: float | None = None) -> float | None:
    params = {"query": query}
    if at:
        params["time"] = str(at)
    url = f"{PROMETHEUS}/api/v1/query?{urllib.parse.urlencode(params)}"
    result = json.load(urllib.request.urlopen(url, timeout=30))["data"]["result"]
    return float(result[0]["value"][1]) if result else None


def set_generator(rate: str, workers: str, containers: int = 1) -> None:
    """The total rate, shared by `containers` generator containers."""
    out = sh(
        *COMPOSE,
        "--profile",
        "core",
        "up",
        "-d",
        "--scale",
        f"generator={containers}",
        "generator",
        env={
            "GENERATOR_QPS": str(float(rate) / containers),
            "GENERATOR_WORKERS": workers,
        },
    )
    if out.returncode:
        raise RuntimeError(out.stderr)


def link() -> str:
    out = sh(sys.executable, str(ROOT / "scripts" / "link_probe.py"), "5")
    return " | ".join(out.stdout.strip().splitlines()[1:]) or out.stderr.strip()


def cpu_sampler(stop: threading.Event, samples: dict[str, list[float]]) -> None:
    while not stop.is_set():
        out = sh("docker", "stats", "--no-stream", "--format", "{{.Name}} {{.CPUPerc}}")
        for line in out.stdout.splitlines():
            name, pct = line.split()
            short = name.removeprefix("thelook-").removesuffix("-1")
            if short in CONTAINERS:
                samples.setdefault(short, []).append(float(pct.rstrip("%")))
        stop.wait(30)


def pace(
    start: datetime.datetime, end: datetime.datetime, workers: int
) -> tuple[float, float]:
    """Achieved iterations/s and backlog dropped per minute, from the
    generators' once-a-minute pace lines (one per worker, every container;
    `workers` = workers x containers)."""
    out = sh(
        *COMPOSE,
        "logs",
        "--no-log-prefix",
        "--since",
        start.isoformat(),
        "--until",
        end.isoformat(),
        "generator",
    )
    rates, dropped = [], []
    for m in re.finditer(
        r"pace: ([\d.]+) iterations/s .*?, ([\d.]+) s behind dropped",
        out.stdout + out.stderr,
    ):
        rates.append(float(m[1]))
        dropped.append(float(m[2]))
    minutes = max(1, len(rates) / workers)
    return sum(rates) / minutes, sum(dropped) / minutes


def bronze_lag(start: datetime.datetime, end: datetime.datetime) -> dict:
    fmt = "%Y-%m-%dT%H:%M:%S"
    out = sh(
        "make",
        "-C",
        str(ROOT),
        "--no-print-directory",
        "spark-run",
        f"JOB=jobs/bronze_lag.py {start.strftime(fmt)} {end.strftime(fmt)}",
    )
    for line in out.stdout.splitlines():
        if line.startswith("LAG "):
            return json.loads(line[4:])
    raise RuntimeError("bronze_lag failed:\n" + (out.stdout + out.stderr)[-3000:])


def wait_for_drain(after: float) -> None:
    """Until bronze finishes a batch that started after `after`: the batch
    in progress at the window's end may have been planned before it."""
    first = prom('thelook_stream_batch_id{query="bronze"}')
    deadline = time.time() + 1800
    while time.time() < deadline:
        current = prom('thelook_stream_batch_id{query="bronze"}')
        if current is not None and first is not None and current >= first + 2:
            return
        time.sleep(15)
    raise RuntimeError("bronze did not drain within 30 minutes")


def run_level(level: str) -> dict:
    rate, workers, *rest = level.split("/")
    containers = int(rest[0]) if rest else 1
    log(f"level {rate} it/s, {workers} workers; link before: {link()}")
    set_generator(rate, workers, containers)
    time.sleep(WARMUP)
    start = datetime.datetime.now(datetime.UTC).replace(microsecond=0)
    stop, samples = threading.Event(), {}
    sampler = threading.Thread(target=cpu_sampler, args=(stop, samples), daemon=True)
    sampler.start()
    time.sleep(WINDOW)
    end = datetime.datetime.now(datetime.UTC).replace(microsecond=0)
    stop.set()
    sampler.join()
    link_after = link()
    set_generator(BASE_RATE, "1")
    w, t_end = f"{WINDOW}s", end.timestamp()
    result = {
        "level": level,
        "window": f"{start:%H:%M:%S}-{end:%H:%M:%S}",
        "events_s": prom(
            f"sum(increase(kafka_connect_source_task_metrics_source_record_write[{w}])) / {WINDOW}",
            t_end,
        ),
        "debezium_lag_max_s": (
            prom(
                f"max(max_over_time(debezium_streaming_millisecondsbehindsource[{w}]))",
                t_end,
            )
            or 0
        )
        / 1000,
        "batch_s_mean": prom(
            f'avg_over_time(thelook_stream_batch_duration_seconds{{query="bronze"}}[{w}])',
            t_end,
        ),
        "batch_s_max": prom(
            f'max_over_time(thelook_stream_batch_duration_seconds{{query="bronze"}}[{w}])',
            t_end,
        ),
        "cpu": {k: round(statistics.mean(v)) for k, v in samples.items()},
        "link_after": link_after,
    }
    result["achieved"], result["dropped_per_min"] = pace(
        start, end, int(workers) * containers
    )
    log(f"window {result['window']} done; waiting for bronze to drain")
    wait_for_drain(t_end)
    lag = bronze_lag(start.replace(tzinfo=None), end.replace(tzinfo=None))
    result["lag"] = lag["all"]
    result["growth_s"] = (lag["last_3_min"]["p95"] or 0) - (
        lag["first_3_min"]["p95"] or 0
    )
    generator_ok = (
        result["achieved"] >= 0.95 * float(rate) and result["dropped_per_min"] < 1
    )
    result["generator_ok"] = generator_ok
    result["sustained"] = (
        generator_ok and result["lag"]["p95"] < 120 and result["growth_s"] <= TRIGGER
    )
    log("result " + json.dumps(result))
    return result


def main() -> int:
    LOG.parent.mkdir(exist_ok=True)
    levels = sys.argv[1:] or LEVELS
    if prom('up{job="bronze-stream"}') != 1:
        log("bronze-stream is not scraped: start core, monitoring and stream")
        return 1
    rows = []
    for level in levels:
        r = run_level(level)
        rows.append(r)
        if not r["generator_ok"]:
            log(
                f"level {level}: the generator did not keep up; the level does not count"
            )
        if not r["sustained"] or r["lag"]["p95"] > 600:
            break
    log(
        "| level it/s | achieved | events/s | Debezium lag max | bronze batch mean/max | lag p50 / p95 / max (s) | growth (s) | sustained | CPU |"
    )
    log("|---|---|---|---|---|---|---|---|---|")
    for r in rows:
        lag = r["lag"]
        log(
            f"| {r['level']} | {r['achieved']:.1f} | {r['events_s']:.0f} | {r['debezium_lag_max_s']:.2f} s "
            f"| {r['batch_s_mean']:.0f} / {r['batch_s_max']:.0f} s | {lag['p50']:.0f} / {lag['p95']:.0f} / {lag['max']:.0f} "
            f"| {r['growth_s']:+.0f} | {'yes' if r['sustained'] else 'NO'} | {r['cpu']} |"
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
