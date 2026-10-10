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

## Bronze stream (Spark)

`bronze-stream` (profile `stream`) reads the 7 CDC topics and appends to
`lake.thelook_bronze.*` every 60 s (`spark/jobs/bronze_stream.py`).

```bash
make up PROFILES="core stream"
docker compose -f onprem/compose.yaml logs -f bronze-stream | grep 'batch '
```

Driver UI `http://localhost:4040` (Structured Streaming tab: input rate,
processing rate, batch duration); cluster UI `http://localhost:8080`.

### The stream stopped with `failOnDataLoss` / "Some data may have been lost"

The checkpoint points at offsets that Kafka has already deleted: the job
was down longer than topic retention (3 days). Those changes are gone
from Kafka. Recovery: trigger a new snapshot (`make snapshot-postgres`;
for MongoDB, the "ChangeStreamHistoryLost" procedure above re-snapshots),
then restart the stream with a fresh checkpoint (stop `bronze-stream`,
`docker volume rm thelook_spark-checkpoints`, `make up`). Bronze then holds
duplicates of what it had already; silver deduplicates by log position.
Intermediate versions and deletes from the gap are lost (as for a lost
slot).

### Restarting, and what a replay does

`docker compose -f onprem/compose.yaml restart bronze-stream` resumes from
the checkpoint. A batch that was interrupted is replayed with the same
batch id: tables it had already committed are skipped (their Iceberg
snapshot carries the query id and batch id, `thelook.query-id` /
`thelook.batch-id`); the others are written. Check a table's commits:

```sql
SELECT committed_at, summary['thelook.batch-id'], summary['added-records']
FROM thelook_bronze."shop_orders$snapshots" ORDER BY committed_at DESC LIMIT 5
```

### Bronze maintenance (inside the stream, ADR 017)

At the first batch after a start, then hourly, a background thread of the
stream expires bronze snapshots older than 1 hour (keeping the last 5),
compacts its ledger (`thelook_bronze.stream_batches`), and does at most one
heavier task: compacting one table's past days (daily per table) or
removing one table's orphan files (weekly per table). Batches keep running
meanwhile. Check:

```bash
docker compose -f onprem/compose.yaml logs bronze-stream | grep "INFO: maintenance"
```

Normal: 30-40 s, a few minutes with a compaction. A failure is logged
("maintenance failed; next attempt in an hour") and never stops the stream;
the table stays due. When a table was last compacted or cleaned:
`SHOW TBLPROPERTIES lake.thelook_bronze.<table>` (`thelook.compacted-at`,
`thelook.orphans-removed-at`). After the stream has been
down for a long time, the first run expires everything at once (still
seconds with the Java API; it took 41 min with the SQL procedure). Do not
replace it with `CALL ... expire_snapshots`: that procedure costs ~2.5 min
per table whatever the amount to expire.

### Silver and gold maintenance (Airflow DAG `maintenance`, ADR 017)

Daily at 03:00 UTC (or at the next start of the `airflow` profile if the
slot was missed), `jobs/maintenance.py` runs as the batch user: compaction
(data files, delete files, dangling deletes removed), expiry of snapshots
older than a day, and orphan removal on at most 3 tables per run. Its task
and every `transform` task share the one-slot pool `lake` (created by
`airflow-init`), so they never commit at the same time. Run it by hand:
`make spark-run JOB=jobs/maintenance.py` (only when `transform` is paused or
idle: outside Airflow, the pool does not protect it).

If orphan removal fails with `No FileSystem for scheme "s3"`: the call lost
`prefix_listing => true` (listing must go through Iceberg's S3FileIO; the
image has no Hadoop S3 connector).

### Re-snapshot the PostgreSQL tables

`make snapshot-postgres` sends a **blocking** snapshot signal on
`thelook.signals`: the connector pauses streaming, re-reads the 5 tables
like its initial snapshot (`op=r`), then resumes streaming where it
paused. Measured in P3: 964,802 rows in 24 s. Progress:
`docker compose -f onprem/compose.yaml logs connect | grep -E "Snapshot step|Finished exporting|Snapshot ended"`.

**Do not send an INCREMENTAL snapshot.** In Debezium 3.7.0 the read-only
incremental snapshot (`read.only=true`) kills the task under live traffic:
`ConcurrentModificationException` in
`AbstractIncrementalSnapshotChangeEventSource.sendWindowEvents`, called from
`PostgresReadOnlyIncrementalSnapshotChangeEventSource.processMessage`
(a streamed change closes the window while the window's rows are being
emitted, and emitting them deduplicates the same map). No later release
exists yet (checked 2026-10-04). The non-read-only variant would need a
signal table and an INSERT grant (ADR 004), so blocking snapshots are used.

**If the task fails after a snapshot signal** (seen once in P3):

1. `make connector-status`, then read the trace:
   `curl -s localhost:8083/connectors/postgres-source/status`.
2. Check the stored offset: `curl -s localhost:8083/connectors/postgres-source/offsets`.
   With exactly-once, the failed transaction was aborted, so the offset is
   still from before the signal (no `incremental_snapshot_*` fields) and the
   aborted records are invisible to `read_committed` readers.
3. Remove the signal so a restart does not replay it: delete the topic
   (`kafka-topics.sh --delete --topic thelook.signals`), then `make signal-topic`.
4. `curl -X POST "localhost:8083/connectors/postgres-source/restart?includeTasks=true&onlyFailed=true"`.
   The slot kept the WAL meanwhile; streaming resumes from the offset.
5. If the offset *does* hold `incremental_snapshot_*` fields: stop the
   connector (`PUT .../stop`), write the offset back without them
   (`PATCH .../offsets`, Kafka Connect 3.6+), then resume.

### After an unclean Kafka shutdown: verify, then repair silver

**Trigger.** Kafka's log at start says `Recovering N logs ... since no clean
shutdown file was found` (`docker compose -f onprem/compose.yaml logs kafka
| grep "no clean shutdown"`): the broker was killed (Docker VM crash, power
loss, `docker kill`), not stopped. Before ADR 016, records Kafka had
acknowledged could be lost while Connect's offsets past them survived:
Connect then resumes after the gap and nothing reports it (2026-10-04:
~16 s lost). With the fsync setting this should not happen; the procedure
proves it each time.

1. **Quiesce.** `docker compose -f onprem/compose.yaml stop generator
   review-simulator` (`make up` restarts the generator: stop it again).
   Keep `stream` up until its log says "idle and waiting for new data".
2. **Bring silver up to bronze:** `make spark-run JOB=jobs/silver.py`
   (tee the output to a file: `--rm` containers lose their logs).
3. **Compare:** `uv run drills/verify_silver.py` (reads all of silver,
   ~0.7 GB of S3 transfer). Every missing / differing / extra key goes to
   `drills/logs/silver-diff-<time>.json` (git-ignored: keys identify people).
   All OK: stop here.
4. **Re-send only those keys:** `python3 drills/resnapshot.py
   drills/logs/silver-diff-<time>.json` prints one BLOCKING snapshot signal
   per connector, filtered to the keys; add `--send` to send it. Check
   `docker compose -f onprem/compose.yaml logs connect | grep -E
   "Finished (exporting|snapshotting)"`: the counts must equal the keys.
   The `filter` field differs per connector: PostgreSQL runs it as the whole
   SELECT (a bare `id IN (...)` fails with `syntax error at or near "id"`,
   logged as a WARN, the task keeps running); MongoDB parses a query
   document (`{"_id": {"$in": [...]}}`). Blocking snapshots only read.
5. **Wait for bronze** (a batch with those row counts in the stream's
   progress), run silver for the affected tables
   (`make spark-run JOB="jobs/silver.py users order_items events"`), then
   verify them again: all OK. Restart the writers. The next gold run
   recomputes the affected facts (their silver `_merged_at` moved).

Measured on 2026-10-05 for the 2026-10-04 loss: users 6 missing + 4 stale,
order_items 69 missing + 27 stale, events 314 missing (all created in the
crash window); orders, products, dist_centers and reviews intact.
Snapshots: 10 + 96 rows in 46 ms (PostgreSQL), 314 documents in 41 ms
(MongoDB).

## Online features (Redis, P5)

Profile `serving`: `redis`, `features-stream`, `api` (ADR 018). Redis is a
cache rebuilt from Kafka, never a source of truth. An admin shell:

```bash
docker compose -f onprem/compose.yaml exec redis sh -c \
  'REDISCLI_AUTH="$REDIS_ADMIN_PASSWORD" redis-cli --user admin'
```

### The API answers 503, or `features-stream` keeps restarting

Redis is unreachable. `make ps`: is `redis` up and healthy? The API answers
503 within a second while Redis is down and recovers on its own. The stream
retries for ~90 s, then fails its batch and restarts (uncapped restart
policy); once Redis is back it resumes from its checkpoint and catches up,
and the replayed batch changes nothing (idempotent writes). Nothing to
repair (drill: `uv run drills/redis_outage.py`, results.md P5).

If the stream restarts with Redis healthy, read its log: an
`OutOfMemory`/exit 137 means the 2 GB `mem_limit` (measured peak 1.4 GB);
a Schema Registry or Kafka error means the core profile is not up.

### Rebuild the features (Redis lost data, or after a logic change)

A hard crash can lose up to one second of writes (AOF `everysec`), and
Redis would then lag its checkpoint silently. Rebuild from Kafka (~2-3 min
for 72 hours of events):

```bash
uv run drills/features_rebuild.py   # pauses the generator, rebuilds, compares
```

or by hand: `docker compose -f onprem/compose.yaml rm -s -f features-stream`,
`FLUSHDB` as admin, `docker volume rm thelook_features-checkpoints`, then
`docker compose -f onprem/compose.yaml up -d features-stream`. It reads the
topic from the earliest offset; features older than Kafka's retention have
expired anyway (TTLs = 72 h).

### "Some data may have been lost" in the features stream log

Expected after a long downtime: offsets that Kafka's retention deleted are
skipped (`failOnDataLoss=false`, unlike bronze), and empty batches are
logged as "nothing to decode" until the stream reaches the earliest
offset still kept. Everything skipped is older than 72 hours, so its
features would have expired. No action.

### Rotate a Redis password

Change it in `onprem/.env`, then recreate the services that use it:
`docker compose -f onprem/compose.yaml up -d redis features-stream api`.
`start-redis.sh` writes the ACL file from `.env` on every start, so the old
password stops working when Redis restarts.

### Memory

`INFO memory` as admin: `used_memory_human` against `maxmemory` (256 MB;
~35 MB for ~200k keys measured). Above the cap Redis evicts the keys closest
to expiring (`volatile-ttl`): features would go missing, not stale. Raise
`maxmemory` in `onprem/redis/redis.conf` (and the container's 512 MB limit,
which leaves room for the AOF rewrite's fork).

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
limits a leaked key to bronze, but within bronze it can read everything
(raw change events, PII included), overwrite or delete data and metadata
files, and repoint or create tables in `thelook_bronze` (bogus Iceberg
commits). A Debezium snapshot restores the **current state** only: the
history (intermediate versions, deletes) that bronze alone holds would be
lost. Accepted for a laptop project (no S3 versioning, on purpose: GDPR);
a production design would add S3 Object Lock or a replicated backup of
bronze, and keep long-lived keys off workloads (IAM Roles Anywhere).

Last used: `aws --profile thelook iam get-access-key-last-used --access-key-id <id>`.

**Batch jobs** (silver, gold; P4) use IAM user `thelook-spark-batch`, keys
`SPARK_BATCH_AWS_ACCESS_KEY_ID` / `..._SECRET_ACCESS_KEY` in `onprem/.env`:
read bronze; read/write/delete objects in silver and gold; Glue get on
`thelook_bronze`, get/create/update tables in `thelook_silver` and
`thelook_gold`. Verified in P4: writing or deleting in bronze, creating a
bronze table, DeleteTable, DeleteDatabase, IAM and Athena are denied.
Create, rotate and revoke exactly as above, with
`--user-name thelook-spark-batch`. Only this key (never the stream key) goes
into Airflow's environment (ADR 013).

## Analyst access (DBeaver or any Athena client)

IAM user `thelook-analyst` (`infra/terraform/lake/iam.tf`): read-only SQL on
the lake through Athena, in the `thelook` workgroup only (results location,
encryption and the 1 GB scan cutoff enforced). Verified with real calls on
2026-10-07: queries on gold and on Iceberg metadata tables and catalog
listings work; the default workgroup, `CREATE TABLE`, S3 writes and deletes
in the layers, Glue `UpdateTable` and the Terraform state bucket are denied.

**Create the key** (operator profile; the key goes into DBeaver only, not
into `onprem/.env`, which the Spark jobs read):

```bash
aws --profile thelook iam create-access-key --user-name thelook-analyst
```

**DBeaver**: New Database Connection -> Athena. Region `us-east-1`; S3
location `s3://thelook-lake-<account>/athena-results/`; access key and
secret from the command above; driver property `WorkGroup` = `thelook`
(older driver versions: `Workgroup`). Without the workgroup property,
queries go to the default workgroup and are denied.

**Revoke**: `aws --profile thelook iam delete-access-key --user-name
thelook-analyst --access-key-id <id>` (DBeaver keeps the secret in its own
settings: delete the key if the laptop or the DBeaver profile is shared).

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
