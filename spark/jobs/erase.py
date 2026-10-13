"""Batch job: erase one user from silver and gold (GDPR, ADR 021).

For each table of the erasure map (lakehouse.erasure.LAKE_TABLES): delete
the user's rows, rewrite every file that has deletes, expire every older
snapshot, and write an audit row to lake.thelook_silver.erasure_audit
(sha256 of the user id, never the id). Bronze is purged by the stream
before this job runs; the marts are aggregates and are kept (FR9).

Runs as the batch IAM user, in the Airflow pool "lake" (no MERGE or
compaction commits on the same tables meanwhile). Safe to rerun: a second
run deletes nothing and keeps the first run's audit rows.

Usage: make spark-run JOB="jobs/erase.py <user_id> <request_id> [<upstream>]"
  upstream: JSON {step: rows} from the DAG's earlier steps (sources, bronze,
  Redis), counts only, audited first.
"""

import json
import logging
import sys
import time

from lakehouse.bronze import with_retries
from lakehouse.erasure import Request, erase_lake, is_user_id
from lakehouse.session import lake_session

logging.basicConfig(
    level=logging.INFO, format="[%(asctime)s] %(levelname)s: %(message)s"
)
log = logging.getLogger("erase")

if len(sys.argv) not in (3, 4) or not is_user_id(sys.argv[1]):
    # The arguments are not echoed: one may be a user id.
    sys.exit("usage: erase.py <user_id (canonical UUID)> <request_id> [<upstream>]")
request = Request(request_id=sys.argv[2], user_id=sys.argv[1])
upstream = (
    {k: int(v) for k, v in json.loads(sys.argv[3]).items()}
    if len(sys.argv) == 4
    else {}
)

spark = lake_session(
    "erase",
    cores_max=2,
    conf={"spark.executor.memory": "1500m", "spark.sql.shuffle.partitions": "8"},
)
start = time.monotonic()
# Each table's counts are logged as it is done (lakehouse.erasure).
report = erase_lake(spark, request, retry=with_retries, upstream=upstream)
log.info(
    "request %s: %s tables in %.1f s",
    request.request_id,
    len(report),
    time.monotonic() - start,
)
spark.stop()
