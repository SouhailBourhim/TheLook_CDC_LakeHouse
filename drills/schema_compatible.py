# /// script
# requires-python = ">=3.12"
# dependencies = [
#   "boto3==1.40.40",
#   "pyyaml==6.0.3",
# ]
# ///
"""Schema drill (a), P7 acceptance part 2 (spec FR14, ADR 020): a compatible
column addition flows end to end, and the contracts notice it.

On shop.dist_centers (10 rows the generator writes only at start-up):

1. ALTER TABLE ... ADD COLUMN manager text (nullable), then one UPDATE, so
   Debezium sends a row with the new schema.
2. Schema Registry accepts the new Avro version (BACKWARD: a new optional
   field is compatible).
3. Bronze stores it (the stream appends with mergeSchema).
4. Silver gains the column on the next scheduled transform run
   (add_new_columns), the value included.
5. The drift check fails on silver.dist_centers: data flows, and the drift
   is still caught. The contract is then updated (manager, optional, not
   PII) and the drift check passes.

Needs core, stream, airflow (transform unpaused) up. Run from the repo root:
  uv run drills/schema_compatible.py
"""

import json
import os
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

import boto3

ROOT = Path(__file__).resolve().parent.parent
COMPOSE = ["docker", "compose", "-f", str(ROOT / "onprem" / "compose.yaml")]
SUBJECT = "thelook.shop.dist_centers-value"
athena = boto3.Session(profile_name="thelook", region_name="us-east-1").client("athena")


def psql(sql: str) -> str:
    return subprocess.run(
        [
            *COMPOSE,
            "exec",
            "-T",
            "postgres",
            "sh",
            "-c",
            f'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -Atc "{sql}"',
        ],
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()


def versions() -> list[int]:
    with urllib.request.urlopen(
        f"http://localhost:8081/subjects/{SUBJECT}/versions"
    ) as r:
        return json.load(r)


def athena_rows(sql: str) -> list[list[str]]:
    qid = athena.start_query_execution(QueryString=sql, WorkGroup="thelook")[
        "QueryExecutionId"
    ]
    while True:
        status = athena.get_query_execution(QueryExecutionId=qid)["QueryExecution"][
            "Status"
        ]
        if status["State"] in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(1)
    if status["State"] != "SUCCEEDED":
        return [["ERROR", status.get("StateChangeReason", "")]]
    rows = athena.get_query_results(QueryExecutionId=qid)["ResultSet"]["Rows"][1:]
    return [[c.get("VarCharValue") for c in r["Data"]] for r in rows]


def drift() -> subprocess.CompletedProcess:
    return subprocess.run(
        [
            "uv",
            "run",
            "-q",
            "--no-project",
            "--with-requirements",
            str(ROOT / "contracts" / "requirements.txt"),
            "python",
            "drift.py",
            "silver/dist_centers.odcs.yaml",
            "bronze/shop_dist_centers.odcs.yaml",
        ],
        cwd=ROOT / "contracts",
        capture_output=True,
        text=True,
        env={**os.environ, "AWS_PROFILE": "thelook", "AWS_DEFAULT_REGION": "us-east-1"},
        check=False,
    )


def wait(what: str, check, timeout_s: int, every: int = 15):
    start = time.monotonic()
    while time.monotonic() - start < timeout_s:
        result = check()
        if result:
            print(f"  {what}: after {time.monotonic() - start:.0f} s")
            return result
        time.sleep(every)
    sys.exit(f"  {what}: not within {timeout_s} s")


def main() -> int:
    if "manager" in psql(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema = 'shop' AND table_name = 'dist_centers'"
    ):
        sys.exit("shop.dist_centers already has manager: the drill ran before")
    before = versions()
    print(f"registry versions of {SUBJECT}: {before}")

    t0 = time.monotonic()
    psql("ALTER TABLE shop.dist_centers ADD COLUMN manager text")
    psql("UPDATE shop.dist_centers SET manager = 'schema drill' WHERE id = 1")
    print("1. column added, row 1 updated")

    after = wait(
        "2. new Avro version registered",
        lambda: versions() if versions() != before else None,
        300,
        5,
    )
    print(f"     versions now {after}")

    wait(
        "3. in bronze (after.manager)",
        lambda: (
            athena_rows(
                "SELECT count(*) FROM thelook_bronze.shop_dist_centers "
                "WHERE after.manager = 'schema drill'"
            )[0][0]
            not in ("0", "ERROR")
        ),
        600,
    )
    wait(
        "4. in silver (next transform run)",
        lambda: (
            athena_rows("SELECT manager FROM thelook_silver.dist_centers WHERE id = 1")[
                0
            ][0]
            == "schema drill"
        ),
        2700,
        60,
    )
    print(f"   source -> silver: {time.monotonic() - t0:.0f} s")

    found = drift()
    print(f"5. drift check (exit {found.returncode}):")
    print("     " + found.stdout.strip().replace("\n", "\n     "))
    if found.returncode == 0:
        sys.exit("the drift check should have caught the new column")
    print(
        "   Now add manager to contracts/silver/dist_centers.odcs.yaml and rerun the drift check."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
