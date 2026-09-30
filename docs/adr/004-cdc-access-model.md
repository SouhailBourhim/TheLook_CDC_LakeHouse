# 004. CDC access model: publication, roles, replica identity

- Status: Accepted
- Date: 2026-09-30
- Deciders: Souhail Bourhim (approves), Claude Code (drafts)

## Context

Debezium needs a replication slot, a publication naming the captured
tables, and read access for the initial snapshot. By default it creates the
publication itself, which requires owning the tables. The spec asks for
least privilege everywhere (NFR security). Open questions B1 (who creates
the publication) and B2 (heartbeat table) had to be settled before the
connector. The update/delete events' `before` field depends on each
table's REPLICA IDENTITY, and the users table holds personal data.

## Options considered

- **Publication:** (a) Debezium auto-creates it (`all_tables` or
  `filtered`), so its role must own the tables or be superuser; (b) created
  by the superuser in a versioned script, `publication.autocreate.mode=disabled`.
- **Replica identity:** DEFAULT (primary key in `before`), FULL (whole old
  row in `before`), USING INDEX (only for tables without a primary key).

## Decision

- Publication `thelook_cdc`, **pre-created** by `cdc-setup.sql` (B1), for the
  6 tables **plus `shop.heartbeat`** (B2): Postgres 15+ skips empty
  transactions, so heartbeat writes must reach Debezium through a published
  table for the slot to advance while business tables are idle.
  `publish = insert, update, delete` (no TRUNCATE; policy: never truncate a
  source table).
- Role `debezium`: LOGIN + REPLICATION, SELECT on the 7 tables, INSERT/UPDATE
  on `heartbeat` only. No ownership, no CREATE.
- Role `generator` owns schema `shop`; role `exporter` has `pg_monitor` only.
- **REPLICA IDENTITY DEFAULT**, set explicitly on every table.
- Passwords reach psql through `\getenv` and Connect through
  `${env:…}`; none is stored in a file, a command line or `_connect-configs`.

## Consequences

- ✅ A compromised Debezium credential can read the captured tables and the
  WAL, but cannot change data or schema.
- ✅ The GDPR erasure DELETE carries only the key: FULL would copy the erased
  user's whole row into Kafka and bronze during the erasure itself.
- ✅ The setup script is idempotent and runs in one transaction.
- ❌ Adding a table to the capture is a manual step (script + connector
  `table.include.list`), not automatic.
- ❌ `before` has no old values, so "what changed" must be derived by
  comparing versions in the lake (bronze keeps every `after` in LSN order).
- ❌ One extra topic (`thelook.shop.heartbeat`) that the sink must exclude.

## References

- Debezium PostgreSQL connector: publication.autocreate.mode, heartbeat.action.query
- PostgreSQL 17: ALTER TABLE … REPLICA IDENTITY; CREATE PUBLICATION
- onprem/postgres/cdc-setup.sql, onprem/connect/connectors/README.md
