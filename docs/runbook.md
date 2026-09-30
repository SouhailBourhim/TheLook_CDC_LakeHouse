# Runbook

What to do when an alert fires. Commands run from the repository root unless
stated; `dc` means `(cd onprem && docker compose ...)`.

## Replication slot

The slot `thelook_debezium` makes Postgres keep every WAL file Debezium has
not confirmed. `max_slot_wal_keep_size` (10 GiB) caps it: past the cap the
slot is invalidated instead of filling the disk.

**Always check the effective state where it takes effect, not where you
wrote it:**

```sql
SELECT slot_name, active, wal_status,
       pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) AS retained,
       pg_size_pretty(safe_wal_size) AS safe_left
FROM pg_replication_slots;
SHOW max_slot_wal_keep_size;   -- effective value, not the file
```

### ReplicationSlotInactive / ReplicationSlotRetainedWalHigh

The consumer is stopped, failing or slow.

1. `make connector-status`. If a task is FAILED, read the trace:
   `curl -s localhost:8083/connectors/postgres-source/status`.
2. Connect container down: `dc --profile core up -d --wait connect`.
3. Task failed on a schema rejected by the registry (HTTP 409): the source
   table got an incompatible change (e.g. NOT NULL column without a literal
   default). Fix the schema at the source or register a compatible one; the
   connector cannot skip it (fail-closed, open question A5). Then
   `curl -X POST 'localhost:8083/connectors/postgres-source/restart?includeTasks=true&onlyFailed=true'`.
4. Watch the slot catch up: `retained` falls, `active = t`. The drill caught
   up one hour of changes in 44 s.

### ReplicationSlotNearInvalidation

Less than 2 GiB left. If the consumer cannot be fixed before the cap is
reached, buy time (no restart needed, `sighup` setting):

```sql
ALTER SYSTEM SET max_slot_wal_keep_size = '20GB';
SELECT pg_reload_conf();
SHOW max_slot_wal_keep_size;   -- verify it took effect
```

Check disk space first (`df -h` in the Postgres container). Revert with
`ALTER SYSTEM RESET max_slot_wal_keep_size` once the slot has caught up.

### ReplicationSlotLost (wal_status = unreserved or lost)

WAL the connector needs is gone. The stream cannot resume; the connector
fails when it restarts. Recovery = new slot + new snapshot:

*Not yet exercised in a drill; test it before relying on it.*

1. Stop the connector: `curl -X PUT localhost:8083/connectors/postgres-source/stop`.
2. Drop the invalid slot: `SELECT pg_drop_replication_slot('thelook_debezium');`
3. Reset the connector's stored offsets so it snapshots again:
   `curl -X DELETE localhost:8083/connectors/postgres-source/offsets`
   (the connector must be STOPPED).
4. Resume: `curl -X PUT localhost:8083/connectors/postgres-source/resume`.
   Debezium creates a new slot and runs a full snapshot (`op = r`).
5. **Deletes in the gap are not replayed.** A snapshot only reads rows that
   still exist; a row deleted while the slot was lost produces no event, so
   silver keeps it. After the snapshot, reconcile: keys present in silver
   but absent from the new snapshot must be deleted in silver (and, for an
   erasure request, in every layer).
6. Run `uv run drills/verify_cdc.py` with the source quiet.

### ReplicationSlotMissing

No slot metric. If PostgresExporterDown is also firing, fix that first (the
missing-slot alert is inhibited while it fires). Otherwise the slot was
dropped: follow ReplicationSlotLost from step 3.

### PostgresExporterDown

`dc --profile monitoring ps postgres-exporter` and its logs. While it lasts,
no slot alert can fire: check the slot by hand with the query above.

## Kafka Connect and Debezium

Metrics come from the JMX exporter agent in the worker (`connect:9404`).

### KafkaConnectDown

The worker is down or its agent is not reachable. All connector alerts are
silent meanwhile; the slot alerts still work (they come from Postgres).
`dc --profile core ps connect` and `dc logs connect`. A JVM that fails before
start-up (e.g. an unreadable `-javaagent` jar) exits with code 1 and prints
the reason on the first lines of the log.

### ConnectorNotRunning / ConnectorTaskFailed

`make connector-status`, then the task trace:
`curl -s localhost:8083/connectors/postgres-source/status`. A failed task
does not restart on its own. After fixing the cause:
`curl -X POST 'localhost:8083/connectors/postgres-source/restart?includeTasks=true&onlyFailed=true'`.
For a schema rejected by the registry, see ReplicationSlotInactive, step 3.

### DebeziumNotConnected

The task runs but its replication connection is down. Check that Postgres
is up and that the `debezium` role can log in (password in `onprem/.env`
matches the database: re-run `onprem/postgres/apply-sql.sh
onprem/postgres/cdc-setup.sql`).

### DebeziumLagHigh

Changes are committed faster than Debezium processes them. Check the
worker's CPU (`docker stats`) and the generator rate (`GENERATOR_QPS`).
`MilliSecondsBehindSource` is measured on the last processed event; the
heartbeat keeps it fresh when the tables are idle.
