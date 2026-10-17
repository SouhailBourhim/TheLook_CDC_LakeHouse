-- Grafana's read-only access to Airflow's metadata database (P9b, ADR 023):
-- the orchestration panels (runs, tasks, durations, retries, the lake pool).
-- Run at each `make up` by the airflow-grafana-reader service, after
-- `airflow db migrate`; idempotent, and the grants are reset each time, so
-- they always match this file.
--
-- Column-level SELECT, never whole tables: dag_run.conf holds the GDPR
-- erasure requests' user ids (P8), task_instance holds hostnames, PIDs and
-- executor settings. Revoking at table level also revokes column grants.
\set ON_ERROR_STOP on

SELECT format('CREATE ROLE grafana_reader LOGIN PASSWORD %L', :'pw')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'grafana_reader') \gexec
ALTER ROLE grafana_reader WITH LOGIN PASSWORD :'pw';
-- Belt and braces: read-only even if a grant is ever too wide, and a
-- dashboard query cannot hold the scheduler's database for long.
ALTER ROLE grafana_reader SET default_transaction_read_only = on;
ALTER ROLE grafana_reader SET statement_timeout = '10s';

REVOKE ALL ON ALL TABLES IN SCHEMA public FROM grafana_reader;
GRANT CONNECT ON DATABASE airflow TO grafana_reader;
GRANT USAGE ON SCHEMA public TO grafana_reader;
GRANT SELECT (dag_id, is_paused) ON dag TO grafana_reader;
GRANT SELECT (dag_id, run_id, run_type, state, queued_at, start_date, end_date)
  ON dag_run TO grafana_reader;
GRANT SELECT (dag_id, run_id, task_id, map_index, state, try_number, max_tries,
              start_date, end_date, duration, pool, queued_dttm)
  ON task_instance TO grafana_reader;
GRANT SELECT (pool, slots) ON slot_pool TO grafana_reader;
