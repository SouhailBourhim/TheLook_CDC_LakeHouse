# Connector configurations

One JSON file per connector; the file name (without `.json`) is the
connector name. `make register-connectors` sends each file with
`PUT /connectors/<name>/config`, which creates the connector or updates
it, so it can be re-run safely.

## postgres-source.json (Debezium)

| Setting | Value | Why |
|---|---|---|
| `database.user` | `debezium` | Least-privilege role from `postgres/cdc-setup.sql` (REPLICATION + SELECT). |
| `database.password` | `${env:DEBEZIUM_DB_PASSWORD}` | Resolved by the worker's EnvVarConfigProvider; only the placeholder is stored in `_connect-configs` and shown by the REST API. |
| `topic.prefix` | `thelook` | Topics are `<prefix>.<schema>.<table>`, e.g. `thelook.shop.orders`. Also names the connector's metrics. Never change it: it is part of the stored offsets' identity. |
| `plugin.name` | `pgoutput` | Postgres' built-in logical decoding plugin; nothing to install on the server. |
| `slot.name` | `thelook_debezium` | The slot that keeps WAL until Debezium confirms it (FR13). |
| `publication.name` / `publication.autocreate.mode` | `thelook_cdc` / `disabled` | Use the publication created by `cdc-setup.sql` (B1); Debezium never creates or alters it. |
| `table.include.list` | 5 tables + `shop.heartbeat` | Matches the publication. `shop.events` left in spec v2.0: clickstream events now live in MongoDB (ADR 008). The heartbeat topic is not ingested into the lake (B2). |
| `snapshot.mode` | `initial` | First start: read every table once (events with `op = r`), then stream from the slot's position. Later starts resume from the stored LSN. |
| `exactly.once.support` | `required` | Refuse to start unless the worker can write records and offsets in one transaction (KIP-618). |
| `heartbeat.interval.ms` | `30000` | Every 30 s, emit a heartbeat and commit the current offset. |
| `heartbeat.action.query` | upsert into `shop.heartbeat` | Creates a real change in a published table, so the slot advances even when the business tables are idle. |
| `topic.creation.default.*` | 1 partition, RF 1, delete, 3 days, 1-day segments | Topic-level settings override the broker's, so they are set explicitly and match `kafka/server.properties`. One partition is enough at this volume; silver deduplicates by key and LSN, so adding partitions later does not break correctness. |

Left at their defaults on purpose (to revisit in P2 with the Iceberg sink):
`time.precision.mode=adaptive` (timestamps as microseconds, `io.debezium.time.MicroTimestamp`),
`tombstones.on.delete=true` (a null-value record after each delete),
`decimal.handling.mode=precise` (no numeric columns today). Changing any of
them later changes the Avro schemas, so it must be decided before data
reaches the lake.
