-- CDC setup for Debezium: replication role, grants, publication.
--
-- Idempotent: every statement converges to the same end state, so the
-- script can be re-run after any change. Run it with apply-sql.sh, as the
-- superuser, in one transaction (all or nothing), after the generator has
-- created its tables.

-- The password comes from the environment, never from this file or a
-- command line. \getenv (psql 15+) copies an environment variable into a
-- psql variable; :'pw' then inserts it as a quoted literal.
\getenv pw DEBEZIUM_DB_PASSWORD
\if :{?pw}
\else
  -- Fail with a non-zero exit code (\quit would exit 0 and look like success).
  DO $$ BEGIN RAISE EXCEPTION 'DEBEZIUM_DB_PASSWORD is not set'; END $$;
\endif

-- --- Role --------------------------------------------------------------------
-- CREATE ROLE has no IF NOT EXISTS: build the statement only when the role
-- is missing and run it with \gexec. ALTER ROLE then enforces the attributes
-- and password on every run.
SELECT 'CREATE ROLE debezium'
WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'debezium') \gexec

-- REPLICATION: may open a replication connection and use a slot.
-- Nothing else: no superuser, no table ownership, no CREATE anywhere.
ALTER ROLE debezium WITH LOGIN REPLICATION
  NOSUPERUSER NOCREATEDB NOCREATEROLE NOBYPASSRLS
  PASSWORD :'pw';

-- --- Grants ------------------------------------------------------------------
-- SELECT is needed for the initial snapshot (a consistent read of each
-- table); streaming itself only reads the WAL through the slot.
GRANT USAGE ON SCHEMA shop TO debezium;
GRANT SELECT ON
  shop.users, shop.orders, shop.order_items,
  shop.products, shop.dist_centers, shop.heartbeat
TO debezium;

-- events moved to MongoDB (spec v2.0, ADR 008); the table stays but is no
-- longer written or captured. REVOKE converges older installs (a no-op when
-- the privilege was never granted).
REVOKE ALL ON shop.events FROM debezium;

-- Debezium's heartbeat action query upserts one row here (commit 11).
GRANT INSERT, UPDATE ON shop.heartbeat TO debezium;

-- --- Replica identity --------------------------------------------------------
-- DEFAULT = update/delete events carry only the primary key in "before".
-- FULL would copy whole old rows (PII included) into the WAL, Kafka and
-- bronze, even for the DELETE that erases a user. Set explicitly to record
-- the decision; DEFAULT is also Postgres' default.
ALTER TABLE shop.users        REPLICA IDENTITY DEFAULT;
ALTER TABLE shop.orders       REPLICA IDENTITY DEFAULT;
ALTER TABLE shop.order_items  REPLICA IDENTITY DEFAULT;
ALTER TABLE shop.products     REPLICA IDENTITY DEFAULT;
ALTER TABLE shop.dist_centers REPLICA IDENTITY DEFAULT;
ALTER TABLE shop.heartbeat    REPLICA IDENTITY DEFAULT;

-- --- Publication (B1, B2) ---------------------------------------------------
-- The publication says which tables are decoded for the slot. It is created
-- here by the superuser, so Debezium never needs table ownership
-- (connector setting publication.autocreate.mode=disabled, commit 11).
SELECT 'CREATE PUBLICATION thelook_cdc'
WHERE NOT EXISTS (SELECT FROM pg_publication WHERE pubname = 'thelook_cdc') \gexec

-- SET TABLE replaces the list, so re-running converges to exactly these 6
-- (5 business tables since events moved to MongoDB, plus heartbeat).
-- heartbeat is included (B2): Postgres 15+ skips empty transactions, so if
-- the heartbeat table were not published, its writes would not reach
-- Debezium and the slot could not advance while the captured tables are idle.
-- Its topic is not ingested into the lake.
ALTER PUBLICATION thelook_cdc SET TABLE
  shop.users, shop.orders, shop.order_items,
  shop.products, shop.dist_centers, shop.heartbeat;

-- No TRUNCATE: the lake cannot apply it row by row. Policy: never TRUNCATE
-- a source table; the reconciliation check would reveal it.
ALTER PUBLICATION thelook_cdc SET (publish = 'insert, update, delete');
