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

## MongoDB

Single-node replica set `rs0`, oplog fixed at 2 GiB (`--oplogSize`), set up
by the one-shot `mongo-init` service (`onprem/mongo/setup.js`) on every
`make up`. Root shell, from the repository root:

```bash
set -a; . onprem/.env; set +a
docker compose -f onprem/compose.yaml exec -e P="$MONGO_ROOT_PASSWORD" mongo \
  bash -c 'mongosh "mongodb://root:$P@localhost:27017/admin?directConnection=true"'
```

```js
rs.status().members[0].stateStr        // PRIMARY
db.getReplicationInfo()                // usedMB, timeDiffHours = oplog window
```

### `make up` fails on mongo or mongo-init

`docker compose -f onprem/compose.yaml logs mongo-init`. The script is
idempotent: fix the cause and rerun `make up`. `mongo` stays unhealthy
until the replica set is initiated (its healthcheck asks for a writable
primary), so a failed `mongo-init` usually shows as an unhealthy `mongo`.

### Connector fails with ChangeStreamHistoryLost (error 286) or InvalidResumeToken

The oplog no longer holds the connector's resume position: it was stopped
longer than the oplog window. Nothing is lost in MongoDB, but the changes
in the gap cannot be streamed. Recovery (not yet exercised):

1. Stop the connector: `curl -X PUT localhost:8083/connectors/mongo-source/stop`.
2. Delete its offsets (Kafka Connect 3.6+, connector must be STOPPED):
   `curl -X DELETE localhost:8083/connectors/mongo-source/offsets`.
3. Resume: `curl -X PUT localhost:8083/connectors/mongo-source/resume`.
   With no offsets, `snapshot.mode=initial` re-reads both collections.
4. Consequences: bronze receives every document again (silver deduplicates
   by `_id` and position); documents deleted during the gap leave no delete
   event, so silver may keep them until a reconciliation finds them (same
   limit as a lost Postgres slot).

**Prevention:** the window is ~30 h at the demo rate (69 MiB/h with the
generator at 5/s and reviews at 1/s, measured in P2, `docs/results.md`);
keep connector outages shorter than that. **A bulk load eats the window:**
copying the 1.75 M historical events used 1.16 GiB of the 2 GiB. Run bulk
loads while the connector is caught up, and check `getReplicationInfo()`
first. Monitoring the window with an alert is planned for P7.

### Before a reconciliation (`drills/verify_cdc.py`)

Stop the writers, then check both connectors have caught up:

```bash
docker compose -f onprem/compose.yaml stop generator review-simulator
docker compose -f onprem/compose.yaml exec connect \
  wget -qO- localhost:9404/metrics | grep -E '^debezium_streaming_(millisecondssincelastevent|millisecondsbehindsource)'
```

MongoDB: `millisecondssincelastevent` above ~10 000 means nothing new is
arriving. PostgreSQL: its heartbeat table is captured, so "since last
event" resets every 30 s; use `millisecondsbehindsource` (near 0) instead.
The Connect image has `wget`, not `curl`.

## Docker Compose

### A config file edit does not take effect, or a container fails with "no such file or directory" on a mount

A single-file bind mount (`postgresql.conf`, `server.properties`,
`connect-distributed.properties`, Prometheus files) follows the file's
inode. Editors that save by writing a new file and renaming it leave the
running container on the old, deleted copy; on Docker Desktop the next
start can fail outright. After editing such a file, recreate the service:
`docker compose -f onprem/compose.yaml up -d --force-recreate <service>`.
Newer services mount folders instead (`onprem/mongo/`), which are not
affected.

## Spark AWS credentials

The streaming job authenticates as IAM user `thelook-spark-stream`
(`infra/terraform/lake/iam.tf`): read/write `s3://thelook-lake-<account>/bronze/`
and the Glue database `thelook_bronze`, nothing else (verified in P3: silver,
gold, bucket root, other Glue databases, DeleteTable and Athena are denied).
Terraform creates the user and policy; the access key is created with the
CLI so the secret never enters the Terraform state. Keys live in
`onprem/.env` (`SPARK_STREAM_AWS_ACCESS_KEY_ID`, `..._SECRET_ACCESS_KEY`).

**Create** (first time, or after deleting the old key), with the
operator profile:

```bash
aws --profile thelook iam create-access-key --user-name thelook-spark-stream
# copy AccessKeyId and SecretAccessKey into onprem/.env
```

**Rotate** (every 90 days, or at once if the key may have leaked). A user
can hold two keys, so rotation has no downtime:

1. Create a second key (above) and put it in `onprem/.env`.
2. Recreate the Spark containers so they read the new key.
3. Check the job commits again, then disable the old key:
   `aws --profile thelook iam update-access-key --user-name thelook-spark-stream --access-key-id <old> --status Inactive`
4. After a day without errors, delete it:
   `aws --profile thelook iam delete-access-key --user-name thelook-spark-stream --access-key-id <old>`

**Revoke now** (leak): step 3 with the leaked key, then rotate. The policy
limits a leaked key to bronze; a leaked key could still overwrite or delete
bronze files, which are rebuildable from a Debezium snapshot.

Last used: `aws --profile thelook iam get-access-key-last-used --access-key-id <id>`.

## Cost

### Weekly cost check (`make cost-report`)

Last 7 days of spend by `project` tag and service. The project budget only
sees resources tagged `project=thelook-cdc-lakehouse`, so read the
`(no tag value)` lines:

- **Negative amounts** are credits (account-level, never tagged): not leaks.
- **Positive amounts for a service this project uses** (S3, Athena, Glue,
  KMS, Config): probably a resource created without the tag. Find it (Cost
  Explorer, group by resource where available, or Config below), tag it in
  Terraform or delete it.
- Other services belong to other projects in this shared account (other tag
  values, e.g. `signal`).

Each run costs $0.01 (Cost Explorer API). To be scheduled in Airflow in P3.

### Budget alerts

- `thelook-cdc-lakehouse-monthly` (tag-filtered, $15 = O6): $1 and $5
  actual, $15 forecast.
- `thelook-account-total-monthly` (whole account, $5): $1 actual, $5
  forecast. Fires on any account spend, including other projects': check
  `make cost-report` to see whose it is.
- Both **exclude credits and refunds**: they measure usage, so they fire
  even while credits pay the bill.

### Config rule `thelook-s3-required-project-tag`

S3 buckets without `project=thelook-cdc-lakehouse` are NON_COMPLIANT.
Other projects' buckets are expected there; any `thelook-*` bucket must be
COMPLIANT:
`aws configservice get-compliance-details-by-config-rule --config-rule-name thelook-s3-required-project-tag`.
