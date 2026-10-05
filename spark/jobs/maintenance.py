"""Batch job: daily upkeep of the silver and gold tables (ADR 017).

Compacts data and delete files, expires snapshots older than a day and,
on a few tables at a time, removes orphan files (lakehouse.upkeep). Runs as
the batch IAM user, which already has these rights on silver and gold and
none on bronze (the stream maintains bronze itself).

Usage: make spark-run JOB=jobs/maintenance.py
"""

import datetime
import logging
import sys
import time

from lakehouse.bronze import with_retries
from lakehouse.session import lake_session
from lakehouse.upkeep import maintain_tables

logging.basicConfig(
    level=logging.INFO, format="[%(asctime)s] %(levelname)s: %(message)s"
)
log = logging.getLogger("maintenance")

DATABASES = ["lake.thelook_silver", "lake.thelook_gold"]

spark = lake_session(
    "maintenance",
    cores_max=2,
    conf={"spark.executor.memory": "1500m", "spark.sql.shuffle.partitions": "8"},
)

failed = []
for database in DATABASES:
    tables = [f"{database}.{t.name}" for t in spark.catalog.listTables(database)]
    start = time.monotonic()
    try:
        # Safe to retry: compaction and expiry are idempotent.
        report = with_retries(
            lambda tables=tables: maintain_tables(
                spark, tables, datetime.datetime.now(datetime.timezone.utc)
            )
        )
        log.info("%s: %s in %.1f s", database, report, time.monotonic() - start)
    except Exception:
        log.exception("maintenance of %s failed", database)
        failed.append(database)
spark.stop()
if failed:
    log.error("failed: %s", ", ".join(failed))
    sys.exit(1)
