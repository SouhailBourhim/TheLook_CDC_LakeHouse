# /// script
# requires-python = ">=3.12"
# dependencies = [
#   "pyiceberg[glue,pyarrow]==0.12.0",
#   "psycopg[binary]==3.3.6",
#   "pymongo==4.18.2",
# ]
# ///
"""Prove that silver holds exactly the current state of the sources (P4).

For each silver table, read it with PyIceberg (merge-on-read delete files
applied) and keep a digest of each row's canonical values; then read the
source table (PostgreSQL) or collection (MongoDB) and compare every row,
column by column, through the same canonical form:

- timestamps compared in UTC to the microsecond (MongoDB dates are
  millisecond values on both sides);
- money: the source double rounded half-up to 2 decimals, the rule silver
  applies (ADR 014), against silver's decimal(10,2);
- MongoDB fields absent from a document compare equal to silver's nulls.

Run it when the sources are quiet and both bronze and silver have caught
up: stop the generator and the review simulator, wait for the stream's
batches to be empty, run `make spark-run JOB=jobs/silver.py`, then this.

Every key that is missing, differs or is extra is written to
drills/logs/silver-diff-<UTC time>.json (git-ignored: keys identify people):
the input of the repair procedure (runbook, "Repair silver after a loss").

Usage: uv run drills/verify_silver.py [table ...]   (exit code 0 = identical)
"""

import datetime
import hashlib
import json
import os
import sys
import time
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path
from urllib.parse import quote_plus

import psycopg
from pyiceberg.catalog import load_catalog
from pymongo import MongoClient

POSTGRES = ["users", "orders", "order_items", "products", "dist_centers"]
MONGO = ["events", "reviews"]
MONEY = {"sale_price", "cost", "retail_price", "price"}
METADATA = {"_position", "_source_ts", "_row_hash", "_merged_at"}
CENT = Decimal("0.01")


def load_env(path: Path) -> dict:
    env = {}
    for line in path.read_text().splitlines():
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            env[k] = v
    return env


def canon(column: str, value) -> str:
    if value is None:
        return "null"
    if isinstance(value, datetime.datetime):
        if value.tzinfo is not None:
            value = value.astimezone(datetime.UTC).replace(tzinfo=None)
        return value.isoformat(timespec="microseconds")
    if column in MONEY:
        return str(
            Decimal(repr(value) if isinstance(value, float) else value).quantize(
                CENT, rounding=ROUND_HALF_UP
            )
        )
    if isinstance(value, list):
        return json.dumps(value)
    return str(value)


def digest(columns: list[str], values: list) -> bytes:
    text = "\x1f".join(canon(c, v) for c, v in zip(columns, values, strict=True))
    return hashlib.blake2b(text.encode(), digest_size=16).digest()


def silver_digests(table) -> tuple[list[str], dict]:
    columns = [f.name for f in table.schema().fields if f.name not in METADATA]
    out = {}
    for batch in table.scan(selected_fields=tuple(columns)).to_arrow_batch_reader():
        cols = [batch.column(c).to_pylist() for c in columns]
        for row in zip(*cols, strict=True):
            out[row[0] if columns[0] == "id" else row[columns.index("id")]] = digest(
                columns, list(row)
            )
    return columns, out


def compare(name: str, columns: list[str], silver: dict, source_rows) -> dict:
    """Keys missing from silver, differing, or in silver only."""
    missing, differ, seen = [], [], 0
    for row in source_rows:
        seen += 1
        got = silver.pop(row["id"], None)
        if got is None:
            missing.append(str(row["id"]))
        elif got != digest(columns, [row.get(c) for c in columns]):
            differ.append(str(row["id"]))
    keys = {"missing": missing, "differ": differ, "extra": [str(k) for k in silver]}
    ok = not any(keys.values())
    print(
        f"{name:13} source={seen:>8} missing={len(missing)} extra={len(keys['extra'])} "
        f"differ={len(differ)}  {'OK' if ok else 'MISMATCH'}"
    )
    return keys


def main() -> int:
    env = load_env(Path(__file__).resolve().parent.parent / "onprem" / ".env")
    os.environ.setdefault("AWS_PROFILE", "thelook")
    os.environ.setdefault("AWS_REGION", "us-east-1")
    catalog = load_catalog(
        "glue",
        type="glue",
        **{
            "glue.region": "us-east-1",
            # PyArrow's S3 default (3.1 s to connect, DNS included) is shorter
            # than this laptop's resolver sometimes takes; one slow lookup
            # used to abort the whole drill.
            "s3.connect-timeout": "30",
            "s3.request-timeout": "60",
        },
    )
    only = set(sys.argv[1:])
    start, diff = time.monotonic(), {}

    with psycopg.connect(
        host="localhost",
        dbname=env.get("POSTGRES_DB", "thelook"),
        user=env.get("POSTGRES_USER", "postgres"),
        password=env["POSTGRES_PASSWORD"],
    ) as pg:
        for name in POSTGRES:
            if only and name not in only:
                continue
            columns, silver = silver_digests(
                catalog.load_table(f"thelook_silver.{name}")
            )
            cur = pg.cursor(name=f"verify_{name}")  # server-side cursor
            cur.itersize = 50_000
            cur.execute(f"SELECT * FROM shop.{name}")
            names = [d.name for d in cur.description]
            rows = (dict(zip(names, r, strict=True)) for r in cur)
            diff[name] = compare(name, columns, silver, rows)
            cur.close()

    mongo = MongoClient(
        f"mongodb://{env.get('MONGO_ROOT_USER', 'root')}:{quote_plus(env['MONGO_ROOT_PASSWORD'])}"
        "@localhost:27017/?directConnection=true&authSource=admin",
        tz_aware=False,
    )
    for name in MONGO:
        if only and name not in only:
            continue
        columns, silver = silver_digests(catalog.load_table(f"thelook_silver.{name}"))
        docs = ({**d, "id": d["_id"]} for d in mongo["web"][name].find())
        diff[name] = compare(name, columns, silver, docs)

    ok = not any(any(keys.values()) for keys in diff.values())
    if not ok:
        logs = Path(__file__).resolve().parent / "logs"
        logs.mkdir(exist_ok=True)
        stamp = datetime.datetime.now(datetime.UTC).strftime("%Y%m%dT%H%M%SZ")
        out = logs / f"silver-diff-{stamp}.json"
        out.write_text(json.dumps(diff, indent=1))
        print("keys written to", out)
    print(f"{'ALL OK' if ok else 'MISMATCH'} in {time.monotonic() - start:.0f} s")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
