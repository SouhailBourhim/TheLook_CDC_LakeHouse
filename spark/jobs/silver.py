"""Batch job: bronze -> silver for every table, incrementally (ADR 014).

Each table reads only the bronze snapshots added since its last run, keeps
the latest event per key and MERGEs it into lake.thelook_silver.<table>.
The first run reads all of bronze. Runs as the batch IAM user
(thelook-spark-batch). Exit code 1 if any table failed (Airflow shows the
task red); the others are still processed.

Usage: make spark-run JOB=jobs/silver.py            (all tables)
       make spark-run JOB="jobs/silver.py orders"   (some tables)
"""

import logging
import sys
import time

from lakehouse.bronze import with_retries
from lakehouse.session import lake_session
from lakehouse.silver import SPECS, consistent_cut, process_table

logging.basicConfig(
    level=logging.INFO, format="[%(asctime)s] %(levelname)s: %(message)s"
)
log = logging.getLogger("silver")

spark = lake_session(
    "silver",
    cores_max=2,
    conf={
        # The worker has 3 GB; the stream's executor uses 1 GB of it.
        "spark.executor.memory": "1500m",
        # Each shuffle partition writes its own file: Spark's default of 200
        # would turn every MERGE into 200 tiny files (small-files problem).
        # Adaptive execution (on by default) merges partitions further.
        "spark.sql.shuffle.partitions": "8",
    },
)

names = sys.argv[1:] or list(SPECS)
# One cut for the whole run (and its retries): every table as of the end of
# the same stream batch (ADR 014).
cut = with_retries(lambda: consistent_cut(spark))
log.info("bronze cut: %s", cut)
failed = []
for name in names:
    start = time.monotonic()
    try:
        # Safe to retry: the MERGE is idempotent (ADR 014).
        result = with_retries(
            lambda name=name: process_table(spark, SPECS[name], cut=cut)
        )
        log.info("%s in %.1f s", result, time.monotonic() - start)
    except Exception:
        log.exception("silver %s failed", name)
        failed.append(name)
spark.stop()
if failed:
    log.error("failed tables: %s", ", ".join(failed))
    sys.exit(1)
