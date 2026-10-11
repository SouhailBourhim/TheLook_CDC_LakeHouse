"""Drift check (ADR 020): each contract's fields vs its table's columns in Glue.

datacontract-cli checks that every field of a contract exists with its type,
but ignores columns the contract does not list (measured in P7 step 0). The
pipeline adds new nullable columns on its own (bronze `mergeSchema`, silver
`add_new_columns`), so without this check a new column, possibly personal
data, would reach the lake with no contract, no PII tag and no rule.

Iceberg's Glue catalog rewrites a table's column list on every commit; a
dropped column stays listed with `iceberg.field.current = false`.

Usage: python drift.py contracts/*/*.odcs.yaml   (exit 1 on any drift)
Credentials: the read-only quality user's key when QUALITY_AWS_* is set (in
Airflow, whose own AWS identity is the batch user), else AWS's default chain.
"""

import os
import sys
from pathlib import Path

import boto3
import yaml


def contract_table(contract: dict) -> tuple[str, str, set[str]]:
    """(Glue database, table, field names) of a one-table contract."""
    [server] = contract["servers"]
    [model] = contract["schema"]
    return (
        server["schema"],
        model["physicalName"],
        {p["name"] for p in model["properties"]},
    )


def current_columns(columns: list[dict]) -> set[str]:
    """Names of a Glue column list, without the ones Iceberg marks dropped."""
    return {
        c["Name"]
        for c in columns
        if c.get("Parameters", {}).get("iceberg.field.current") != "false"
    }


def drift(fields: set[str], columns: set[str]) -> tuple[set[str], set[str]]:
    """(columns the contract does not list, fields the table does not have)."""
    return columns - fields, fields - columns


def quality_credentials(env: dict) -> dict:
    """boto3 client arguments for the quality user, or {} for the default chain."""
    if "QUALITY_AWS_ACCESS_KEY_ID" not in env:
        return {}
    return {
        "aws_access_key_id": env["QUALITY_AWS_ACCESS_KEY_ID"],
        "aws_secret_access_key": env["QUALITY_AWS_SECRET_ACCESS_KEY"],
        "region_name": env.get("AWS_REGION", "us-east-1"),
    }


def main(paths: list[str]) -> int:
    glue = boto3.client("glue", **quality_credentials(dict(os.environ)))
    failed = 0
    for path in paths:
        database, table, fields = contract_table(yaml.safe_load(Path(path).read_text()))
        columns = glue.get_table(DatabaseName=database, Name=table)["Table"][
            "StorageDescriptor"
        ]["Columns"]
        not_in_contract, not_in_table = drift(fields, current_columns(columns))
        if not_in_contract or not_in_table:
            failed += 1
            print(
                f"DRIFT {database}.{table}: not in the contract {sorted(not_in_contract)}, "
                f"not in the table {sorted(not_in_table)}"
            )
        else:
            print(f"ok    {database}.{table}")
    print(f"{failed} of {len(paths)} contracts drifted")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
