#!/usr/bin/env bash
# Apply postgres/cdc-setup.sql to the running database. Safe to re-run.
# Usage (from anywhere): onprem/postgres/cdc-setup.sh
# Prerequisite: the core profile is up and the generator has created its tables.
set -euo pipefail
cd "$(dirname "$0")/.."

# Load onprem/.env so DEBEZIUM_DB_PASSWORD is in this shell's environment.
set -a
# shellcheck disable=SC1091
. ./.env
set +a
: "${DEBEZIUM_DB_PASSWORD:?set DEBEZIUM_DB_PASSWORD in onprem/.env}"

# -e NAME without a value forwards this shell's variable into the container,
# so the password is on no command line. --single-transaction: all or nothing.
docker compose exec -T -e DEBEZIUM_DB_PASSWORD postgres \
  sh -c 'psql -X -v ON_ERROR_STOP=1 --single-transaction -U "$POSTGRES_USER" -d "$POSTGRES_DB" -f -' \
  < postgres/cdc-setup.sql
