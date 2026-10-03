-- Read-only role for the review simulator. Idempotent; run with apply-sql.sh
-- after the generator has created its tables.
--
-- The simulator writes reviews to MongoDB for order items that were really
-- delivered, so it reads order_items (status, product) and orders (buyer).
-- It never writes to PostgreSQL.

\getenv pw REVIEWER_DB_PASSWORD
\if :{?pw}
\else
  -- Fail with a non-zero exit code (\quit would exit 0 and look like success).
  DO $$ BEGIN RAISE EXCEPTION 'REVIEWER_DB_PASSWORD is not set'; END $$;
\endif

SELECT 'CREATE ROLE reviewer'
WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'reviewer') \gexec

ALTER ROLE reviewer WITH LOGIN
  NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS
  PASSWORD :'pw';

GRANT USAGE ON SCHEMA shop TO reviewer;
GRANT SELECT ON shop.orders, shop.order_items TO reviewer;
