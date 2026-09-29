#!/usr/bin/env bash
# Runs once, on the first start with an empty data volume
# (docker-entrypoint-initdb.d). Later changes need a new volume or a manual run.
#
# The generator gets its own login role that owns the shop schema: it can
# create and write its tables there, and nothing else. The superuser is
# kept for administration only.
set -euo pipefail

# The password goes in as a psql variable (:'pw' quotes it), never pasted
# into the SQL text, so special characters cannot break the statement.
psql -v ON_ERROR_STOP=1 -v pw="$GENERATOR_DB_PASSWORD" \
     --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<'SQL'
CREATE ROLE generator LOGIN PASSWORD :'pw';
CREATE SCHEMA shop AUTHORIZATION generator;
SQL
