"""Athena bytes scanned by a fixed set of analyst queries (P9 scan
benchmark; method in docs/results.md). Runs each query in the `thelook`
workgroup with the operator's AWS profile and prints, per query, the bytes
Athena scanned (what it bills, $5/TB) and the engine time.

Usage: python3 scripts/scan_benchmark.py [--sql "SELECT ..."]
  no argument: the benchmark set below; --sql: one ad hoc query, its rows
  printed too. Needs the AWS CLI and the `thelook` profile (Makefile).
"""

import datetime
import json
import subprocess
import sys
import time

PROFILE = "thelook"
WORKGROUP = "thelook"

# Questions an analyst asks of gold, each with the filters a dashboard would
# use. Fixed text, with dates relative to the newest order day, filled in as
# literals ({day}, {week}, {month}): a literal lets the engine prune
# partitions at planning, as a dashboard's date picker would.
QUERIES = {
    "q1_revenue_last_7_days": """
        SELECT order_date_key, sum(gross_amount) AS revenue, count(*) AS orders
        FROM thelook_gold.fct_orders
        WHERE order_date_key >= {week}
        GROUP BY 1 ORDER BY 1""",
    "q2_country_revenue_last_month": """
        SELECT u.country, sum(f.gross_amount) AS revenue
        FROM thelook_gold.fct_order_items f
        JOIN thelook_gold.dim_user u ON f.user_sk = u.user_sk
        WHERE f.order_date_key >= {month}
        GROUP BY 1 ORDER BY 2 DESC LIMIT 10""",
    "q3_one_day_items": """
        SELECT count(*), sum(sale_price)
        FROM thelook_gold.fct_order_items
        WHERE order_date_key = {day}""",
    "q4_mart_last_30_days": """
        SELECT * FROM thelook_gold.mart_daily_revenue
        WHERE date_key >= {month} ORDER BY date_key""",
    "q5_all_time_by_category": """
        SELECT p.category, sum(f.sale_price) AS revenue
        FROM thelook_gold.fct_order_items f
        JOIN thelook_gold.dim_product p ON f.product_id = p.product_id
        GROUP BY 1 ORDER BY 2 DESC LIMIT 10""",
}


def aws(*args: str) -> dict:
    out = subprocess.run(
        [
            "aws",
            "--profile",
            PROFILE,
            "--region",
            "us-east-1",
            "--output",
            "json",
            *args,
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    return json.loads(out.stdout) if out.stdout.strip() else {}


def run(sql: str) -> tuple[dict, str]:
    qid = aws(
        "athena",
        "start-query-execution",
        "--work-group",
        WORKGROUP,
        "--query-string",
        sql,
    )["QueryExecutionId"]
    while True:
        execution = aws("athena", "get-query-execution", "--query-execution-id", qid)[
            "QueryExecution"
        ]
        state = execution["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            return execution, qid
        time.sleep(1)


def main() -> int:
    if len(sys.argv) > 2 and sys.argv[1] == "--sql":
        execution, qid = run(sys.argv[2])
        if execution["Status"]["State"] != "SUCCEEDED":
            print(execution["Status"].get("StateChangeReason"))
            return 1
        rows = aws("athena", "get-query-results", "--query-execution-id", qid)
        for row in rows["ResultSet"]["Rows"]:
            print("\t".join(c.get("VarCharValue", "") for c in row["Data"]))
        stats = execution["Statistics"]
        print(f"-- scanned {stats['DataScannedInBytes']:,} bytes")
        return 0
    execution, qid = run("SELECT max(order_date_key) FROM thelook_gold.fct_orders")
    rows = aws("athena", "get-query-results", "--query-execution-id", qid)
    newest = datetime.datetime.strptime(
        rows["ResultSet"]["Rows"][1]["Data"][0]["VarCharValue"], "%Y%m%d"
    )
    keys = {
        "day": newest.strftime("%Y%m%d"),
        "week": (newest - datetime.timedelta(days=6)).strftime("%Y%m%d"),
        "month": (newest - datetime.timedelta(days=29)).strftime("%Y%m%d"),
    }
    print(
        time.strftime("%Y-%m-%d %H:%M UTC", time.gmtime()),
        "newest order day",
        keys["day"],
    )
    print("| query | bytes scanned | engine ms |")
    print("|---|---|---|")
    failed = 0
    for name, sql in QUERIES.items():
        execution, _ = run(sql.format(**keys))
        if execution["Status"]["State"] != "SUCCEEDED":
            print(
                f"| {name} | FAILED: {execution['Status'].get('StateChangeReason', '')[:120]} | |"
            )
            failed += 1
            continue
        stats = execution["Statistics"]
        print(
            f"| {name} | {stats['DataScannedInBytes']:,} | {stats['EngineExecutionTimeInMillis']:,} |"
        )
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
