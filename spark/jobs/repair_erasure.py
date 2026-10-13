"""Operator job: delete bronze rows a purge could no longer find (ADR 021).

Runbook "Repair a partial bronze purge": rows of one bronze user table,
named by key (e.g. order items whose orders were already purged), are
deleted, then the table's history is dropped, keeping the stream's newest
batch marks (lakehouse.erasure.repair_keys).

Runs as the bronze writer (the stream's identity, ADR 017), never from
Airflow, and only while the stream is stopped, so that nothing commits on
the table meanwhile:

  docker compose -f onprem/compose.yaml stop bronze-stream
  docker compose -f onprem/compose.yaml --profile stream run --rm --no-deps \\
    bronze-stream /opt/spark/bin/spark-submit --master spark://spark-master:7077 \\
    /opt/lakehouse/jobs/repair_erasure.py <bronze table> <key> [<key> ...]
  docker compose -f onprem/compose.yaml --profile stream up -d bronze-stream

Example table: lake.thelook_bronze.shop_order_items. Keys are UUIDs.
"""

import logging
import sys

from lakehouse.bronze import with_retries
from lakehouse.erasure import repair_keys
from lakehouse.session import lake_session

logging.basicConfig(
    level=logging.INFO, format="[%(asctime)s] %(levelname)s: %(message)s"
)
log = logging.getLogger("repair-erasure")

if len(sys.argv) < 3:
    sys.exit(__doc__)
table, keys = sys.argv[1], sys.argv[2:]
spark = lake_session("repair-erasure", cores_max=2)
done = with_retries(lambda: repair_keys(spark, table, keys))
log.info("%s: %s keys given, %s", table, len(keys), done)
spark.stop()
