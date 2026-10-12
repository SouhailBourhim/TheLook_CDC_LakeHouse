-- Role of the GDPR erasure (ADR 021): deletes a user's rows, nothing else.
-- SELECT is needed by the WHERE clauses (orders of the user). Idempotent;
-- run with apply-sql.sh.

\getenv pw ERASER_DB_PASSWORD
\if :{?pw}
\else
  -- Fail with a non-zero exit code (\quit would exit 0 and look like success).
  DO $$ BEGIN RAISE EXCEPTION 'ERASER_DB_PASSWORD is not set'; END $$;
\endif

SELECT 'CREATE ROLE eraser'
WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'eraser') \gexec

ALTER ROLE eraser WITH LOGIN
  NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS
  PASSWORD :'pw';

-- The tables holding a user's data: the three captured ones and the
-- legacy events table (P1, no longer captured but still holding IPs).
GRANT USAGE ON SCHEMA shop TO eraser;
GRANT SELECT, DELETE ON shop.users, shop.orders, shop.order_items, shop.events
  TO eraser;
