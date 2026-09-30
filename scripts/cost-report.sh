#!/usr/bin/env bash
# Weekly cost check: last 7 full days of spend, grouped by the project tag
# and by service. Cost under "(no tag value)" was not counted by the project
# budget: in this shared account part of it belongs to other projects, so
# read the service column: an untagged line for a service this project uses
# (S3, Athena, Glue, KMS, Config) is a probable leak.
# Negative untagged amounts are credits (applied at account level, never
# tagged); they mirror tagged costs and are not leaks.
# Each Cost Explorer API call costs $0.01. Usage: make cost-report
set -euo pipefail
export AWS_PROFILE=${AWS_PROFILE:-thelook}

end=$(date -u +%F)
start=$(date -u -d "$end - 7 days" +%F)
echo "Spend from $start to $end (exclusive), USD, by project tag and service:"
aws ce get-cost-and-usage \
  --time-period "Start=$start,End=$end" --granularity MONTHLY \
  --metrics UnblendedCost \
  --group-by Type=TAG,Key=project Type=DIMENSION,Key=SERVICE \
  --output json | python3 -c '
import json, sys
rows = []
for period in json.load(sys.stdin)["ResultsByTime"]:
    for g in period["Groups"]:
        tag, service = g["Keys"]
        tag = tag.split("$", 1)[1] or "(no tag value)"
        amount = float(g["Metrics"]["UnblendedCost"]["Amount"])
        rows.append((tag, service, amount))
if not rows:
    print("  no spend at all")
for tag, service, amount in sorted(rows, key=lambda r: (r[0], -r[2])):
    flag = "  <- untagged: check" if tag == "(no tag value)" and amount >= 0.005 else ""
    print(f"  {tag:24} {service:45} {amount:9.4f}{flag}")
'
