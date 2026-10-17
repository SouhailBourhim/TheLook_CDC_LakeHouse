"""Inventory of the lake from Iceberg's metadata (P9 wrap-up): every table
of bronze, silver and gold, its live rows, data files and size, written to
docs/lake-inventory.md: the record of what the lake held, kept in the
repository even if the AWS side is ever destroyed.

Reads only the metadata tables (`$files`): a few KB per table, not the
data. Live rows = rows in data files minus position deletes (merge-on-read).

Usage: python3 scripts/lake_inventory.py   (the `thelook` AWS profile)
"""

import datetime
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import scan_benchmark as sb  # noqa: E402  Athena helpers

OUT = Path(__file__).resolve().parent.parent / "docs" / "lake-inventory.md"
DATABASES = ["thelook_bronze", "thelook_silver", "thelook_gold"]


def rows(sql: str) -> list[list[str]]:
    execution, qid = sb.run(sql)
    if execution["Status"]["State"] != "SUCCEEDED":
        raise RuntimeError(execution["Status"].get("StateChangeReason"))
    result = sb.aws("athena", "get-query-results", "--query-execution-id", qid)
    return [
        [c.get("VarCharValue", "") for c in r["Data"]]
        for r in result["ResultSet"]["Rows"][1:]
    ]


def main() -> int:
    lines = [
        "# Lake inventory",
        "",
        f"Taken {datetime.datetime.now(datetime.UTC):%Y-%m-%d %H:%M} UTC from Iceberg's "
        "metadata (`scripts/lake_inventory.py`), at the end of P9: what the lake",
        "held. Live rows = rows in data files minus position",
        "deletes (merge-on-read). Sizes are the current snapshot's files.",
        "",
    ]
    grand_rows = grand_bytes = 0
    for db in DATABASES:
        # information_schema, not SHOW TABLES: SHOW TABLES returns no header
        # row, and rows() skips the first row of a result as its header.
        tables = [
            r[0]
            for r in rows(
                "SELECT table_name FROM information_schema.tables "
                f"WHERE table_schema = '{db}'"
            )
        ]
        lines += [
            f"## {db}",
            "",
            "| Table | Live rows | Data files | Size (MB) |",
            "|---|---|---|---|",
        ]
        db_rows = db_bytes = 0
        for t in sorted(tables):
            (data_rows, deletes, files, size) = rows(
                "SELECT coalesce(sum(CASE WHEN content = 0 THEN record_count END), 0), "
                "coalesce(sum(CASE WHEN content = 1 THEN record_count END), 0), "
                "count_if(content = 0), coalesce(sum(file_size_in_bytes), 0) "
                f'FROM {db}."{t}$files"'
            )[0]
            live = int(data_rows) - int(deletes)
            db_rows += live
            db_bytes += int(size)
            lines.append(
                f"| {t} | {live:,} | {int(files):,} | {int(size) / 1e6:,.1f} |"
            )
            print(f"{db}.{t}: {live:,} rows, {int(size) / 1e6:.1f} MB", flush=True)
        lines += [f"| **total** | **{db_rows:,}** | | **{db_bytes / 1e6:,.1f}** |", ""]
        grand_rows += db_rows
        grand_bytes += db_bytes
    lines += [
        f"**Lake total:** {grand_rows:,} rows, {grand_bytes / 1e9:.2f} GB in the current snapshots.",
        "",
    ]
    bucket = sb.aws("sts", "get-caller-identity")["Account"]
    listing = (
        subprocess.run(
            [
                "aws",
                "--profile",
                sb.PROFILE,
                "s3",
                "ls",
                f"s3://thelook-lake-{bucket}/",
                "--recursive",
                "--summarize",
            ],
            capture_output=True,
            text=True,
            check=True,
        )
        .stdout.strip()
        .splitlines()[-2:]
    )
    lines += [
        "**The bucket as a whole** (every object: current files, files kept for",
        "time travel, Athena results), `aws s3 ls --summarize`:",
        "",
        *[f"- {line.strip()}" for line in listing],
        "",
    ]
    OUT.write_text("\n".join(lines))
    print(f"written {OUT}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
