"""Batch job: recompute gold for a range of days from silver (FR10, ADR 021).

The facts of the orders created and the sessions started on those days,
then the daily marts, replace what gold holds for the range
(lakehouse.gold.rebuild_days): rerunning a day gives the same rows, and
rows silver no longer has leave gold. The backfill DAG runs it one day at
a time; run by hand to repair a range.

Runs as the batch IAM user, in the Airflow pool "lake" (silver is not
written meanwhile).

Usage: make spark-run JOB="jobs/backfill.py 2026-10-04 2026-10-06"
       (start and end inclusive, YYYY-MM-DD; end defaults to start)
"""

import datetime
import logging
import sys
import time

from lakehouse.bronze import with_retries
from lakehouse.gold import rebuild_days
from lakehouse.session import lake_session

logging.basicConfig(
    level=logging.INFO, format="[%(asctime)s] %(levelname)s: %(message)s"
)
log = logging.getLogger("backfill")

if len(sys.argv) not in (2, 3):
    sys.exit(__doc__)
start = datetime.date.fromisoformat(sys.argv[1])
end = datetime.date.fromisoformat(sys.argv[-1])

spark = lake_session(
    "backfill",
    cores_max=2,
    conf={"spark.executor.memory": "1500m", "spark.sql.shuffle.partitions": "8"},
)
began = time.monotonic()
# Each table's replacement is retried on its own: safe, since it replaces
# the range (MERGE on the grain keys).
counts = rebuild_days(spark, start, end, retry=with_retries)
log.info("%s..%s: %s in %.1f s", start, end, counts, time.monotonic() - began)
spark.stop()
