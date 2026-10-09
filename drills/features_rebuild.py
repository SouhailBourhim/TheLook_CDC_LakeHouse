# /// script
# requires-python = ">=3.12"
# dependencies = [
#   "redis==8.1.0",
# ]
# ///
"""Rebuild drill (P5, ADR 018): features rebuilt from Kafka must equal the
features the live stream built, TTLs included.

1. Pause the generator; wait until the features stream is idle (no batch
   for IDLE seconds: Structured Streaming skips triggers with no data).
2. Snapshot every user:* key: content and absolute expiry (PEXPIRETIME).
3. Stop the stream, flush Redis, delete its checkpoint volume, start it:
   it reads the topic from the earliest offset (the last ~72 hours).
   Memory is sampled meanwhile (the catch-up peak, for mem_limit).
4. Wait until idle again; snapshot; compare; restart the generator
   (always, even if a step fails).

Compared: what the API would serve at the second snapshot (T2). Keys
expired by T2 drop out on both sides, and so do sorted-set members older
than their family's window (Redis may still hold them until the next write
trims them, but nothing reads them). Everything else must be identical:
members, scores, hash fields, expiry to the millisecond.

Needs the core and serving profiles up. Usage:
  uv run drills/features_rebuild.py
"""

import calendar
import subprocess
import sys
import threading
import time
from pathlib import Path

import redis

ROOT = Path(__file__).resolve().parent.parent
COMPOSE = ["docker", "compose", "-f", str(ROOT / "onprem" / "compose.yaml")]
IDLE = 30  # seconds without a batch = caught up
HOUR_MS = 3_600_000
WINDOW_MS = {"viewed": 72 * HOUR_MS, "events": HOUR_MS}  # as lakehouse.features


def load_env(path: Path) -> dict:
    env = {}
    for line in path.read_text().splitlines():
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            env[k] = v
    return env


env = load_env(ROOT / "onprem" / ".env")
r = redis.Redis(
    host="localhost",
    username="admin",
    password=env["REDIS_ADMIN_PASSWORD"],
    decode_responses=True,
)


def compose(*args: str) -> str:
    return subprocess.run(
        [*COMPOSE, *args], capture_output=True, text=True, check=False
    ).stdout


def last_batch_age() -> float:
    """Seconds since the features stream last finished a batch (inf if
    none in the last 10 minutes)."""
    lines = [
        line
        for line in compose("logs", "--since", "10m", "features-stream").splitlines()
        if "INFO: batch" in line
    ]
    if not lines:
        return float("inf")
    stamp = lines[-1].split("[", 1)[1].split(",", 1)[0]  # 2026-10-10 00:01:02
    # The container logs in UTC: timegm, not mktime (which reads local time).
    finished = calendar.timegm(time.strptime(stamp, "%Y-%m-%d %H:%M:%S"))
    return time.time() - finished


def wait_idle(after: float = 0.0) -> None:
    """Until a batch has finished after `after` and none for IDLE seconds."""
    while True:
        age = last_batch_age()
        if age >= IDLE and time.time() - age > after:
            return
        time.sleep(5)


def snapshot() -> dict:
    """{key: (content, expire_at_ms)} for every user:* key."""
    keys = sorted(r.scan_iter("user:*", count=1000))
    pipe = r.pipeline(transaction=False)
    for key in keys:
        if ":cart:" in key:
            pipe.hgetall(key)
        else:
            pipe.zrange(key, 0, -1, withscores=True)
        pipe.pexpiretime(key)
    values = pipe.execute()
    return {key: (values[2 * i], values[2 * i + 1]) for i, key in enumerate(keys)}


def served(snap: dict, now_ms: int) -> dict:
    """What the API could serve at now_ms: live keys, members in window."""
    out = {}
    for key, (content, expire_at) in snap.items():
        if expire_at <= now_ms:
            continue
        family = key.split(":")[2]
        if family in WINDOW_MS:
            content = [(m, s) for m, s in content if s >= now_ms - WINDOW_MS[family]]
            if not content:
                continue
        out[key] = (content, expire_at)
    return out


def peak_memory(stop: threading.Event, peak: list) -> None:
    while not stop.is_set():
        out = (
            subprocess.run(
                [
                    "docker",
                    "stats",
                    "--no-stream",
                    "--format",
                    "{{.MemUsage}}",
                    "thelook-features-stream-1",
                ],
                capture_output=True,
                text=True,
                check=False,
            )
            .stdout.split("/")[0]
            .strip()
        )
        if out.endswith("GiB"):
            peak[0] = max(peak[0], float(out[:-3]) * 1024)
        elif out.endswith("MiB"):
            peak[0] = max(peak[0], float(out[:-3]))
        stop.wait(2)


def main() -> int:
    compose("stop", "generator")
    try:
        print("generator paused; waiting for the features stream to go idle")
        wait_idle()
        live = snapshot()
        print(f"live snapshot: {len(live)} keys")

        # Removed, not only stopped: a stopped container still holds its
        # volume, and `docker volume rm` refuses it.
        compose("rm", "-s", "-f", "features-stream")
        r.flushdb()
        subprocess.run(
            ["docker", "volume", "rm", "thelook_features-checkpoints"],
            capture_output=True,
            check=True,
        )
        rebuild_start = time.time()
        stop, peak = threading.Event(), [0.0]
        sampler = threading.Thread(target=peak_memory, args=(stop, peak))
        compose("up", "-d", "features-stream")
        sampler.start()
        print("Redis flushed, checkpoint deleted, stream restarted: rebuilding")
        wait_idle(after=rebuild_start)
        stop.set()
        sampler.join()
        rebuild_s = time.time() - rebuild_start - IDLE
        rebuilt = snapshot()
        print(
            f"rebuilt snapshot: {len(rebuilt)} keys, in ~{rebuild_s:.0f} s, "
            f"peak memory {peak[0]:.0f} MiB"
        )
    finally:
        compose("start", "generator")
        print("generator restarted")

    now_ms = int(time.time() * 1000)
    a, b = served(live, now_ms), served(rebuilt, now_ms)
    only_live, only_rebuilt = a.keys() - b.keys(), b.keys() - a.keys()
    differ = [k for k in a.keys() & b.keys() if a[k] != b[k]]
    print(
        f"\ncompared at T2: live {len(a)} keys, rebuilt {len(b)} keys; "
        f"identical {len(a.keys() & b.keys()) - len(differ)}, "
        f"differ {len(differ)}, only live {len(only_live)}, "
        f"only rebuilt {len(only_rebuilt)}"
    )
    for label, keys in (
        ("differ", differ),
        ("only live", only_live),
        ("only rebuilt", only_rebuilt),
    ):
        for key in sorted(keys)[:3]:
            print(
                f"  {label}: {key}\n    live:    {a.get(key)}\n    rebuilt: {b.get(key)}"
            )
    return 0 if not (differ or only_live or only_rebuilt) else 1


if __name__ == "__main__":
    sys.exit(main())
