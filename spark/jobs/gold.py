"""Batch job: silver (and bronze history) -> gold (ADR 015).

Stages, run in order (all by default):
  dims    rebuild dim_date, dim_product, dim_distribution_center, dim_user
  facts   recompute and MERGE the changed rows of fct_order_items,
          fct_orders and fct_sessions (marts come in a later step)

Runs as the batch IAM user. Exit code 1 if a stage failed.

Usage: make spark-run JOB=jobs/gold.py           (all stages)
       make spark-run JOB="jobs/gold.py dims"
"""

import logging
import sys
import time

from lakehouse.bronze import with_retries
from lakehouse.gold import build_dimensions, build_facts
from lakehouse.session import lake_session

logging.basicConfig(
    level=logging.INFO, format="[%(asctime)s] %(levelname)s: %(message)s"
)
log = logging.getLogger("gold")

STAGES = {"dims": build_dimensions, "facts": build_facts}

spark = lake_session(
    "gold",
    cores_max=2,
    conf={"spark.executor.memory": "1500m", "spark.sql.shuffle.partitions": "8"},
)

failed = []
for name in sys.argv[1:] or list(STAGES):
    start = time.monotonic()
    try:
        # Safe to retry: every gold write is a replace or an idempotent MERGE.
        result = with_retries(lambda name=name: STAGES[name](spark))
        log.info("%s: %s in %.1f s", name, result, time.monotonic() - start)
    except Exception:
        log.exception("gold stage %s failed", name)
        failed.append(name)
spark.stop()
if failed:
    log.error("failed stages: %s", ", ".join(failed))
    sys.exit(1)
