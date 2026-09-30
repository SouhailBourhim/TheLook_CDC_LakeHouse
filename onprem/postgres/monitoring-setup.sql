-- Monitoring role for postgres_exporter. Idempotent; run with apply-sql.sh.

\getenv pw EXPORTER_DB_PASSWORD
\if :{?pw}
\else
  -- Fail with a non-zero exit code (\quit would exit 0 and look like success).
  DO $$ BEGIN RAISE EXCEPTION 'EXPORTER_DB_PASSWORD is not set'; END $$;
\endif

SELECT 'CREATE ROLE exporter'
WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'exporter') \gexec

ALTER ROLE exporter WITH LOGIN
  NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS
  PASSWORD :'pw';

-- pg_monitor: Postgres' built-in read-only role for monitoring. It can read
-- every statistics view (pg_stat_*, pg_replication_slots, WAL positions)
-- but no table data.
GRANT pg_monitor TO exporter;
