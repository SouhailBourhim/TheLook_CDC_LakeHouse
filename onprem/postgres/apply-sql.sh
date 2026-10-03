#!/usr/bin/env bash
# Apply an idempotent SQL file to the running database as the superuser,
# in one transaction (all or nothing). Safe to re-run.
# Usage (from anywhere): onprem/postgres/apply-sql.sh onprem/postgres/cdc-setup.sql
set -euo pipefail
sql_file=$(realpath "$1")
cd "$(dirname "$0")/.."

# Load onprem/.env so the role passwords are in this shell's environment.
set -a
# shellcheck disable=SC1091
. ./.env
set +a

# -e NAME without a value forwards this shell's variable into the container,
# so no password is on a command line. The SQL files read them with \getenv.
docker compose exec -T \
  -e DEBEZIUM_DB_PASSWORD -e EXPORTER_DB_PASSWORD -e REVIEWER_DB_PASSWORD \
  postgres \
  sh -c 'psql -X -v ON_ERROR_STOP=1 --single-transaction -U "$POSTGRES_USER" -d "$POSTGRES_DB" -f -' \
  < "$sql_file"
