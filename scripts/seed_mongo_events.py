# /// script
# requires-python = ">=3.12"
# dependencies = [
#   "psycopg[binary]==3.3.6",
#   "pymongo==4.18.2",
# ]
# ///
"""One-off copy of the PostgreSQL events table into MongoDB web.events (ADR 008).

Run once, after the patched generator writes new events to MongoDB (so the
table is frozen) and before the Debezium MongoDB connector is registered (so
its initial snapshot reads these documents instead of 1.75 M inserts
streaming through the oplog).

Documents have the same shape as the generator's (src/mongo_writer.py):
_id = event id, NULL columns omitted. Historical cart events have no
product_id or price (the synthetic fields start with the patch).

Idempotent: documents are upserted by _id, so a rerun after a crash only
rewrites what is already there. Exit code 0 = Mongo holds every Postgres id.

Usage: uv run scripts/seed_mongo_events.py
"""

import sys
import time
from pathlib import Path
from urllib.parse import quote_plus

import psycopg
from psycopg.rows import dict_row
from pymongo import MongoClient, ReplaceOne

BATCH = 5000
ENV_FILE = Path(__file__).resolve().parent.parent / "onprem" / ".env"


def load_env(path: Path) -> dict:
    env = {}
    for line in path.read_text().splitlines():
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            env[k] = v
    return env


def to_document(row: dict) -> dict:
    doc = {k: v for k, v in row.items() if v is not None}
    doc["_id"] = doc.pop("id")
    return doc


def main() -> int:
    env = load_env(ENV_FILE)
    # The generator's MongoDB user: find/insert/update on web.events only.
    # directConnection: from the host, the replica set member name "mongo"
    # does not resolve (see onprem/mongo/setup.js).
    mongo = MongoClient(
        f"mongodb://generator:{quote_plus(env['MONGO_GENERATOR_PASSWORD'])}@localhost:27017/"
        "?directConnection=true&authSource=web"
    )
    events = mongo["web"]["events"]

    pg = psycopg.connect(
        host="localhost",
        dbname=env.get("POSTGRES_DB", "thelook"),
        user=env.get("POSTGRES_USER", "postgres"),
        password=env["POSTGRES_PASSWORD"],
    )
    total = pg.execute("SELECT count(*) FROM shop.events").fetchone()[0]
    print(f"copying {total:,} events from PostgreSQL to MongoDB")

    copied, start = 0, time.monotonic()
    # A named cursor is a server-side cursor: rows arrive in chunks of
    # itersize instead of loading 1.75 M rows into memory at once.
    with pg.cursor(name="seed_events", row_factory=dict_row) as cur:
        cur.itersize = BATCH
        cur.execute("SELECT * FROM shop.events")
        batch = []
        for row in cur:
            doc = to_document(row)
            batch.append(ReplaceOne({"_id": doc["_id"]}, doc, upsert=True))
            if len(batch) == BATCH:
                events.bulk_write(batch, ordered=False)
                copied += len(batch)
                batch = []
                if copied % 100_000 == 0:
                    print(
                        f"  {copied:,} ({copied / (time.monotonic() - start):,.0f}/s)"
                    )
        if batch:
            events.bulk_write(batch, ordered=False)
            copied += len(batch)
    print(f"copied {copied:,} in {time.monotonic() - start:.0f} s")

    # Check: every Postgres id is in Mongo. Mongo may hold more (the patched
    # generator keeps adding events), never fewer.
    missing = 0
    with pg.cursor(name="check_ids") as cur:
        cur.itersize = 50_000
        cur.execute("SELECT id FROM shop.events")
        ids = []
        for (event_id,) in cur:
            ids.append(event_id)
            if len(ids) == 50_000:
                missing += len(ids) - events.count_documents({"_id": {"$in": ids}})
                ids = []
        if ids:
            missing += len(ids) - events.count_documents({"_id": {"$in": ids}})
    print(f"Postgres ids missing from MongoDB: {missing}")
    return 0 if missing == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
