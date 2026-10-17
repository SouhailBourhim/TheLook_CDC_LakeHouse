"""Source-to-bronze lag of every bronze row from a time window (P9 load
test, O8; lakehouse.lag). Prints one JSON line: rows, p50, p95 and max
over all 7 tables, per table, and the p95 of the window's first and last
3 minutes (a growing lag means a growing backlog).

Run within an hour or so of the window: bronze keeps 1 to 2 hours of
snapshots, and the lag needs the one that added each file.

Usage: make spark-run JOB="jobs/bronze_lag.py 2026-10-17T10:03:00 2026-10-17T10:13:00"
(times in UTC)
"""

import datetime
import functools
import json
import sys

from lakehouse.bronze import TOPICS, table_for
from lakehouse.lag import lag_summary, row_lags
from lakehouse.session import lake_session

start, end = (datetime.datetime.fromisoformat(a) for a in sys.argv[1:3])
EDGE = datetime.timedelta(minutes=3)

spark = lake_session("bronze-lag", cores_max=2)
per_table = {}
lags = []
for topic in TOPICS:
    table = table_for(topic)
    t = row_lags(spark, table, start, end).cache()
    per_table[table.rsplit(".", 1)[1]] = lag_summary(t)
    lags.append(t)
every = functools.reduce(lambda a, b: a.unionByName(b), lags)
result = {
    "window": [start.isoformat(), end.isoformat()],
    "all": lag_summary(every),
    "first_3_min": lag_summary(every.where(every.source_ts < start + EDGE)),
    "last_3_min": lag_summary(every.where(every.source_ts >= end - EDGE)),
    "per_table": per_table,
}
print("LAG " + json.dumps(result, default=str))
spark.stop()
