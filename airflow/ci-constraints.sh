#!/usr/bin/env bash
# Airflow's constraints for 3.3.2 / Python 3.10, verified by checksum, minus
# the pins we override (as in onprem/spark/Dockerfile). Writes the result to
# the path given as $1.
set -euo pipefail
url=https://raw.githubusercontent.com/apache/airflow/constraints-3.3.2/constraints-3.10.txt
sha=9c78ff08ccbe56f9a8cc0a3171927d53a95bd49d12d6bd7eaa91281b34b654fb
overrides="$(dirname "$0")/../onprem/spark/airflow-overrides.txt"
tmp=$(mktemp)
curl -sSfL --retry 3 "$url" -o "$tmp"
echo "$sha  $tmp" | sha256sum -c --quiet
sed -E "/^($(grep -v '^#' "$overrides" | cut -d= -f1 | paste -sd'|'))==/d" "$tmp" > "$1"
