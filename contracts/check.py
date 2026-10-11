"""Run contract tests in Athena (ADR 020), several contracts at a time.

datacontract-cli 1.2.2 needs each server's `stagingDir`, and that path holds
the AWS account ID, which the repository never commits: each contract is
copied to a temporary file with `stagingDir` added from the environment
(`DATACONTRACT_STAGING_DIR`), then tested there.

The checks run as the read-only quality user. Its key is passed to each
datacontract process explicitly (`DATACONTRACT_S3_*`), so they never use the
process's own AWS identity (in the Airflow scheduler, the batch user, which
can write silver and gold).

Usage: python check.py [--workers N] contracts/gold/*.odcs.yaml
Exit 1 if any contract fails (or cannot be tested).
"""

import argparse
import os
import subprocess
import sys
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import yaml

DATACONTRACT = os.environ.get("DATACONTRACT_BIN", "datacontract")


def with_staging_dir(contract: dict, staging_dir: str) -> dict:
    """The contract with every Athena server's stagingDir set."""
    out = dict(contract)
    out["servers"] = [
        {**s, "stagingDir": staging_dir} if s.get("type") == "athena" else s
        for s in contract["servers"]
    ]
    return out


def quality_env() -> dict:
    """The environment of a datacontract process: the quality user's key."""
    env = dict(os.environ)
    env.update(
        DATACONTRACT_S3_REGION=os.environ.get("AWS_REGION", "us-east-1"),
        DATACONTRACT_S3_ACCESS_KEY_ID=os.environ["QUALITY_AWS_ACCESS_KEY_ID"],
        DATACONTRACT_S3_SECRET_ACCESS_KEY=os.environ["QUALITY_AWS_SECRET_ACCESS_KEY"],
    )
    return env


# Messages of a check that could not run (Athena throttled, a connection
# dropped), as opposed to a check that ran and found bad data.
NOT_EVALUATED = ("Error evaluating check", "Could not read model")


def errors(report: str) -> list[str]:
    """The numbered lines of datacontract's error summary ("1) ...")."""
    lines = report.split("found the following errors:", 1)[-1].splitlines()
    return [
        line.strip() for line in lines if line.strip()[:1].isdigit() and ")" in line
    ]


def only_not_evaluated(found: list[str]) -> bool:
    """True if every error is a check that could not run: worth one retry."""
    return bool(found) and all(any(m in e for m in NOT_EVALUATED) for e in found)


def run_test(path: Path, staging_dir: str, env: dict) -> tuple[int, str]:
    contract = with_staging_dir(yaml.safe_load(path.read_text()), staging_dir)
    with tempfile.NamedTemporaryFile("w", suffix=".odcs.yaml") as f:
        yaml.safe_dump(contract, f, sort_keys=False)
        f.flush()
        run = subprocess.run(
            [DATACONTRACT, "test", f.name],
            capture_output=True,
            text=True,
            env={**env, "COLUMNS": "200"},
            check=False,
        )
    return run.returncode, run.stdout + run.stderr


def test_one(
    path: Path, staging_dir: str, env: dict
) -> tuple[Path, int, float, list[str]]:
    """(contract, exit code, seconds, errors). A contract whose only errors
    are checks that could not run is tried once more; a check that ran and
    failed is reported at once, never retried."""
    start = time.monotonic()
    code, report = run_test(path, staging_dir, env)
    found = errors(report)
    if code and only_not_evaluated(found):
        time.sleep(10)
        code, report = run_test(path, staging_dir, env)
        found = ["(retried once)"] + errors(report)
    if code and not found:
        found = [report.strip().splitlines()[-1] if report.strip() else "no output"]
    return path, code, time.monotonic() - start, found


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("contracts", nargs="+", type=Path)
    args = parser.parse_args(argv)
    staging_dir = os.environ["DATACONTRACT_STAGING_DIR"]
    env = quality_env()
    start = time.monotonic()
    with ThreadPoolExecutor(args.workers) as pool:
        results = list(
            pool.map(lambda p: test_one(p, staging_dir, env), args.contracts)
        )
    failed = 0
    for path, code, seconds, report in results:
        print(f"{'ok  ' if code == 0 else 'FAIL'} {path} ({seconds:.0f} s)")
        if code:
            failed += 1
            print(
                report or "  (no check report: the test could not run, see its output)"
            )
    print(
        f"{failed} of {len(results)} contracts failed, "
        f"{time.monotonic() - start:.0f} s with {args.workers} workers"
    )
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
