"""One-off migration (P9): cluster the gold order facts by day.

Applies gold.DAY_ORDER (sort order on order_date_key, 32 MB files, 8 MB row
groups) to the existing fct_orders and fct_order_items, then rewrites every
data file sorted (rewrite-all: the daily compaction only rewrites small or
deleted-heavy files, and the tables were just compacted into one file each).
Afterwards, writes and the daily maintenance keep the order (upkeep.compact
uses the sort strategy for sorted tables). Idempotent; safe to rerun.

Usage: make spark-run JOB=jobs/sort_gold.py   (holds no Airflow pool: run it
between transform runs, or pause transform)
"""

import logging

from lakehouse.bronze import with_retries
from lakehouse.gold import DAY_ORDER, apply_day_order
from lakehouse.session import lake_session

logging.basicConfig(
    level=logging.INFO, format="[%(asctime)s] %(levelname)s: %(message)s"
)
log = logging.getLogger("sort-gold")

spark = lake_session(
    "sort-gold",
    cores_max=2,
    conf={"spark.executor.memory": "1500m", "spark.sql.shuffle.partitions": "8"},
)
for name in DAY_ORDER:
    table = f"lake.thelook_gold.{name}"
    apply_day_order(spark, table)
    rewritten = with_retries(
        lambda table=table: spark.sql(
            f"CALL lake.system.rewrite_data_files(table => '{table}', "
            "strategy => 'sort', options => map('rewrite-all', 'true', "
            "'remove-dangling-deletes', 'true'))"
        ).first()["rewritten_data_files_count"]
    )
    files = spark.sql(f"SELECT count(*) FROM {table}.data_files").first()[0]
    log.info("%s: %s files rewritten, %s data files now", table, rewritten, files)
spark.stop()
