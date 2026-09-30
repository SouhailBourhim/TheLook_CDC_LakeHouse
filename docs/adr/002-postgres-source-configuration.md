# 002. PostgreSQL 17 source configuration

- Status: Accepted
- Date: 2026-09-30
- Deciders: Souhail Bourhim (approves), Claude Code (drafts)

## Context

The source database must support logical decoding for Debezium (FR1) and
must never fill its disk if the consumer stops (FR13). The spec allows
PostgreSQL 16 or 17 (5.3). Settings must be versioned and changeable during
an incident, and the slot runbook relies on changing
`max_slot_wal_keep_size` without a restart.

## Options considered

1. **Settings as `-c` flags in the compose `command`.** Visible in one file,
   but command-line flags have the highest priority: `ALTER SYSTEM` +
   `pg_reload_conf()` is silently ignored (tested: the value stayed at 10 GB
   while `pg_file_settings` reported the new line as applied).
2. **`ALTER SYSTEM` in an init script.** Settings live in
   `postgresql.auto.conf` inside the volume, not in the repository; init
   scripts only run on an empty volume, so the database drifts from the code.
3. **A versioned `postgresql.conf` mounted read-only** and selected with
   `-c config_file=…`, restating the few image settings we depend on.

## Decision

PostgreSQL **17.11**, image pinned by tag and digest, configured by option 3
(`onprem/postgres/postgresql.conf`):

- `wal_level=logical`; `max_replication_slots=4`, `max_wal_senders=4`
  (one for Debezium, headroom for reconnects and drills; low so a leaked slot
  shows up);
- `max_slot_wal_keep_size=10GB`: past it the slot is invalidated instead of
  filling the disk;
- healthcheck `pg_isready` **over TCP**: the entrypoint's first-start
  temporary server listens on the Unix socket only.

Roles and grants are applied by idempotent SQL files run with
`apply-sql.sh`, not by init scripts (except the generator role, needed
before the generator's first start).

## Consequences

- ✅ `ALTER SYSTEM` works for incidents and is visible in `pg_settings.source`.
- ✅ Every setting is in the repository with its reason.
- ✅ The slot cap turns "disk full, source down" into "slot lost, re-snapshot".
- ❌ Replacing the image's config file means restating its useful settings
  (`listen_addresses`, timezone) by hand.
- ❌ The cap is sized from measurements that change: the WAL rate grew 5×
  with table size (106 → 547 MB/hour at 5 iterations/s, full-page images).
  It must be re-checked when the load or data volume changes.
- ❌ An invalidated slot loses the deletes that happened in the gap (a
  re-snapshot cannot see them); the runbook covers the reconciliation.

## References

- PostgreSQL 17 docs: 19.1 Setting Parameters (priority of sources),
  `max_slot_wal_keep_size`, `pg_replication_slots.safe_wal_size`
- docs/results.md (slot drill), docs/runbook.md (replication slot)
