-- Read-only role for the daily quality report (ADR 020): it counts rows in
-- the captured tables to reconcile them with silver. Idempotent; run with
-- apply-sql.sh.

\getenv pw QUALITY_DB_PASSWORD
\if :{?pw}
\else
  -- Fail with a non-zero exit code (\quit would exit 0 and look like success).
  DO $$ BEGIN RAISE EXCEPTION 'QUALITY_DB_PASSWORD is not set'; END $$;
\endif

SELECT 'CREATE ROLE quality'
WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'quality') \gexec

ALTER ROLE quality WITH LOGIN
  NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS
  PASSWORD :'pw';

-- Read the five captured tables, nothing else (not shop.events, no longer
-- captured since P2; not the heartbeat table).
GRANT USAGE ON SCHEMA shop TO quality;
GRANT SELECT ON shop.users, shop.orders, shop.order_items, shop.products,
  shop.dist_centers TO quality;
