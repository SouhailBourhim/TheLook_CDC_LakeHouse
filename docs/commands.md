# Command reference

Every command this project uses, grouped by what you want to do. Run them
from the repository root. Each was taken from the Makefile, a script's usage
line or the runbook, and the shells were run against the live stack on
2026-10-10. The *why* behind a procedure is in the [runbook](runbook.md);
measured results are in [results](results.md).

**Conventions**

- `C` stands for `docker compose -f onprem/compose.yaml`. Define it once per
  shell: `C="docker compose -f onprem/compose.yaml"`.
- Passwords live in `onprem/.env` (copied from `.env.example`). Commands that
  need one on the host load it first: `set -a; . onprem/.env; set +a`.
- ⚠️ marks a command that deletes data or state: read the runbook section
  before running it.
- `uv run drills/<x>.py` installs a drill's pinned dependencies on the fly
  (declared at the top of the script).

## Contents

1. [First-time setup](#1-first-time-setup)
2. [Start, stop and inspect the stack](#2-start-stop-and-inspect-the-stack)
3. [The workload (generators)](#3-the-workload-generators)
4. [Capture: Debezium, Kafka, Schema Registry](#4-capture-debezium-kafka-schema-registry)
5. [The lake: Spark jobs and streams](#5-the-lake-spark-jobs-and-streams)
6. [Orchestration: Airflow](#6-orchestration-airflow)
7. [Querying the data](#7-querying-the-data)
8. [Serving: API, Redis, Neo4j](#8-serving-api-redis-neo4j)
9. [Monitoring](#9-monitoring)
10. [Drills and reconciliations](#10-drills-and-reconciliations)
11. [Tests and code quality](#11-tests-and-code-quality)
12. [AWS: Terraform, keys, cost](#12-aws-terraform-keys-cost)
13. [Recovery procedures (pointers)](#13-recovery-procedures-pointers)
14. [Ports and UIs](#14-ports-and-uis)

---

## 1. First-time setup

```bash
git config core.hooksPath .githooks       # pre-push hook: gitleaks scan (needs Docker)
cp onprem/.env.example onprem/.env        # then set every password and key

# AWS (once per account, operator profile "thelook")
make tf-bootstrap                         # Terraform state bucket (local state)
make tf-init                              # backend = thelook-tfstate-<account id>
make tf-plan && make tf-apply             # S3, Glue, Athena, IAM, budgets
aws --profile thelook iam create-access-key --user-name thelook-spark-stream  # -> onprem/.env
aws --profile thelook iam create-access-key --user-name thelook-spark-batch   # -> onprem/.env

# On-prem stack
make up PROFILES="core monitoring"
onprem/postgres/apply-sql.sh onprem/postgres/cdc-setup.sql         # Debezium role + publication
onprem/postgres/apply-sql.sh onprem/postgres/monitoring-setup.sql  # exporter role
onprem/postgres/apply-sql.sh onprem/postgres/reviewer-setup.sql    # review simulator role
make register-connectors                  # creates or updates both connectors, then prints status
make up PROFILES="core stream"            # Spark cluster + bronze stream
make spark-run JOB=jobs/smoke_test.py     # Kafka, Avro and Glue checks from the cluster
make snapshot-postgres                    # blocking re-snapshot so bronze starts complete
make up PROFILES="core stream airflow"    # then unpause the DAGs (section 6)
make up PROFILES="core serving"           # Redis, features stream, Neo4j, API
```

The SQL scripts need the generator's tables: if one reports a missing
table, run it again a few seconds later (all are idempotent). MongoDB needs
no manual step: `mongo-init` initiates the replica set and creates its
users on every `up`.

## 2. Start, stop and inspect the stack

| Command | What it does |
|---|---|
| `make up` | Starts the `core` profile and waits until every service is healthy |
| `make up PROFILES="core stream airflow serving monitoring"` | Starts any set of profiles (RAM per profile in the README) |
| `make ps` | Every service of every profile, with its status |
| `make down` | Stops everything, keeps the data (volumes). **Always stop this way**: a killed Kafka triggers the verify procedure (section 13) |
| `$C logs -f <service>` | Follow a service's log |
| `$C restart <service>` | Restart one service (a stream resumes from its checkpoint) |
| `$C stop <service>` / `$C start <service>` | Stop or start one service without touching the others |
| `$C up -d --force-recreate <service>` | Recreate after editing a single-file bind mount (`postgresql.conf`, `server.properties`, Prometheus files) |
| `docker stats --no-stream` | Memory and CPU per container |

| Profile | Services | Needs AWS |
|---|---|---|
| `core` | postgres, mongo, mongo-init, generator, review-simulator, kafka, schema-registry, connect | no |
| `monitoring` | postgres-exporter, prometheus, alertmanager | no |
| `stream` | spark-master, spark-worker, bronze-stream | yes (bronze) |
| `airflow` | airflow-db, -init, -apiserver, -scheduler, -dag-processor | yes (batch jobs) |
| `serving` | redis, features-stream, neo4j, api | no |

`make up` starts *every* service of the listed profiles: a service you
stopped by hand (for example `features-stream` to save 1.4 GB) comes back,
so stop it again afterwards.

## 3. The workload (generators)

| Command | What it does |
|---|---|
| `GENERATOR_QPS=10` in `onprem/.env`, then `$C up -d generator` | Generator rate (default 5 iterations/s) |
| `$C stop generator review-simulator` | Stop the writers (before any reconciliation) |
| `$C start generator review-simulator` | Resume them |
| `$C logs -f generator` | Watch inserts and updates as they happen |

The generator's other settings (basket affinity, companions, popularity
skew, update probabilities) are explicit flags under `generator.command`
in `onprem/compose.yaml`.

## 4. Capture: Debezium, Kafka, Schema Registry

**Connectors** (Kafka Connect REST API, `localhost:8083`)

| Command | What it does |
|---|---|
| `make register-connectors` | Create or update every connector in `onprem/connect/connectors/` (idempotent PUT) |
| `make connector-status` | State of each connector and task (RUNNING, PAUSED, FAILED) |
| `curl -s localhost:8083/connectors/postgres-source/status` | Full status, including a failed task's stack trace |
| `curl -s localhost:8083/connectors/postgres-source/offsets` | The stored source offset (LSN, or MongoDB resume token) |
| `curl -X PUT localhost:8083/connectors/<name>/pause` / `.../resume` | Pause or resume a connector |
| `curl -X PUT localhost:8083/connectors/<name>/stop` | Stop it (required before changing offsets) |
| `curl -X POST "localhost:8083/connectors/<name>/restart?includeTasks=true&onlyFailed=true"` | Restart failed tasks |
| ⚠️ `curl -X DELETE localhost:8083/connectors/mongo-source/offsets` | Forget the offsets (connector stopped): the next start re-snapshots |

**Snapshots and signals**

| Command | What it does |
|---|---|
| `make snapshot-postgres` | Blocking re-snapshot of the 5 PostgreSQL tables (never INCREMENTAL, see runbook) |
| `make signal-topic` | Create the `thelook.signals` topic if missing |
| `$C logs connect \| grep -E "Snapshot step\|Finished exporting\|Snapshot ended"` | Snapshot progress |
| `python3 drills/resnapshot.py drills/logs/silver-diff-<time>.json [--send] [--only=postgres\|--only=mongo]` | Re-send only the keys a reconciliation found (prints the signal; `--send` sends it) |

**Kafka and Schema Registry**

```bash
K="$C exec -T kafka env KAFKA_HEAP_OPTS=-Xmx128m /opt/kafka/bin"
$K/kafka-topics.sh --bootstrap-server kafka:29092 --list
$K/kafka-topics.sh --bootstrap-server kafka:29092 --describe --topic thelook.shop.orders
$K/kafka-configs.sh --bootstrap-server kafka:29092 --entity-type topics \
  --entity-name thelook.shop.orders --describe --all | grep retention   # 3 days

# Read change events, decoded from Avro:
$C exec schema-registry kafka-avro-console-consumer --bootstrap-server kafka:29092 \
  --topic thelook_mongo.web.reviews --property schema.registry.url=http://localhost:8081

curl -s localhost:8081/subjects                 # one key and one value subject per topic
curl -s localhost:8081/subjects/thelook.shop.orders-value/versions
curl -s localhost:8081/config                   # default compatibility: BACKWARD
```

Topics: `thelook.shop.<table>` (PostgreSQL: users, orders, order_items,
products, dist_centers, heartbeat), `thelook_mongo.web.<collection>`
(events, reviews), `thelook.signals`. `thelook.shop.events` is left over
from P1: events moved to MongoDB in P2 (ADR 008) and the table is no longer
captured.

**Debezium lag** (the Connect image has `wget`, not `curl`):

```bash
$C exec connect wget -qO- localhost:9404/metrics \
  | grep -E '^debezium_streaming_(millisecondssincelastevent|millisecondsbehindsource)'
```

## 5. The lake: Spark jobs and streams

**Batch jobs** (`make spark-run` needs the `stream` profile; without Airflow
the `lake` pool does not protect them, so pause `transform` first)

| Command | What it does |
|---|---|
| `make spark-run JOB=jobs/smoke_test.py` | Kafka, Avro decoding and Glue access from the cluster; creates nothing |
| `make spark-run JOB=jobs/silver.py` | Bronze -> silver, every table (incremental MERGE) |
| `make spark-run JOB="jobs/silver.py orders users"` | Only some silver tables |
| `make spark-run JOB=jobs/gold.py` | Every gold stage: dims, facts, marts |
| `make spark-run JOB="jobs/gold.py dims"` | One stage (`dims`, `facts` or `marts`) |
| `make spark-run JOB=jobs/maintenance.py` | Silver and gold upkeep: compaction, snapshot expiry, orphan removal |
| `$C exec airflow-scheduler spark-submit --master spark://spark-master:7077 /opt/lakehouse/jobs/graph.py` | Rebuild the Neo4j graph by hand (its Neo4j client exists only in the Airflow image) |

`--rm` containers lose their logs: append `2>&1 | tee spark-run.log` to keep them.

**Streams**

| Command | What it does |
|---|---|
| `$C logs -f bronze-stream \| grep 'batch '` | One line per micro-batch: rows per table and duration (`skipped(replay)` on a replay) |
| `$C logs bronze-stream \| grep "INFO: maintenance"` | Bronze upkeep runs inside the stream (hourly) |
| `$C restart bronze-stream` | Resume from the checkpoint (a replayed batch skips what it committed) |
| `$C logs -f features-stream` | Features batches into Redis (every 10 s) |

Settings, set as environment variables on the service in `compose.yaml`:
`BRONZE_TRIGGER` (default `60 seconds`), `BRONZE_MAX_OFFSETS` (200,000),
`BRONZE_CHECKPOINT`; `FEATURES_TRIGGER` (`10 seconds`),
`FEATURES_MAX_OFFSETS` (50,000), `FEATURES_CHECKPOINT`.

⚠️ `docker volume rm thelook_spark-checkpoints` starts bronze from scratch
(a new query id: bronze then holds duplicates, which silver removes). Only
in the "failOnDataLoss" procedure of the runbook.

## 6. Orchestration: Airflow

UI: `http://localhost:8088` (user `admin`, password `AIRFLOW_ADMIN_PASSWORD`).
DAGs start paused on a fresh Airflow database.

```bash
A="$C exec -T airflow-scheduler airflow"
$A dags list                                   # paused or not
$A dags unpause transform                      # also: maintenance, graph
$A dags pause transform
$A dags trigger graph                          # run now (waits for the lake pool slot)
$A dags list-runs transform -o json | grep '^\['   # newest first; the grep drops a graphviz warning printed on stdout
$A tasks states-for-dag-run transform "scheduled__2026-10-10T18:00:00+00:00"
$C exec airflow-scheduler ls /opt/airflow/logs/dag_id=graph   # task logs, one folder per run
```

| DAG | Schedule (UTC) | Tasks |
|---|---|---|
| `transform` | every 30 min | silver -> gold_dims -> gold_facts -> gold_marts |
| `maintenance` | daily 03:00 | silver_gold_upkeep |
| `graph` | daily 04:00 | rebuild_graph |

All three share the one-slot pool `lake`; `max_active_runs=1`,
`catchup=False`.

## 7. Querying the data

**Athena** (workgroup `thelook`; databases `thelook_bronze`,
`thelook_silver`, `thelook_gold`). Analysts use DBeaver with the
`thelook-analyst` key (runbook, "Analyst access"). From the CLI:

```bash
q=$(aws --profile thelook athena start-query-execution --work-group thelook \
  --query-string "SELECT count(*) FROM thelook_gold.fct_orders" \
  --query QueryExecutionId --output text)
aws --profile thelook athena get-query-execution --query-execution-id "$q" --query QueryExecution.Status.State
aws --profile thelook athena get-query-results --query-execution-id "$q"
```

Example SQL:

```sql
-- Revenue by country, as the customer's address was when they ordered (SCD2)
SELECT u.country, sum(f.gross_amount) AS gross
FROM thelook_gold.fct_order_items f
JOIN thelook_gold.dim_user u ON f.user_sk = u.user_sk
GROUP BY 1 ORDER BY 2 DESC;

-- A bronze table's last commits, with the replay guard's batch id
SELECT committed_at, summary['thelook.batch-id'], summary['added-records']
FROM thelook_bronze."shop_orders$snapshots" ORDER BY committed_at DESC LIMIT 5;
```

From Spark SQL, Iceberg table properties:
`SHOW TBLPROPERTIES lake.thelook_bronze.<table>` (`thelook.compacted-at`,
`thelook.orphans-removed-at`).

**PostgreSQL** (the source)

```bash
$C exec postgres sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB"'
$C exec -T postgres sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -Atc "select count(*) from shop.orders"'
onprem/postgres/apply-sql.sh <file.sql>        # run an idempotent SQL file in one transaction
```

**MongoDB** (the source)

```bash
set -a; . onprem/.env; set +a
$C exec -e P="$MONGO_ROOT_PASSWORD" mongo \
  bash -c 'mongosh "mongodb://root:$P@localhost:27017/admin?directConnection=true"'
```

Inside mongosh: `rs.status().members[0].stateStr` (PRIMARY),
`db.getReplicationInfo()` (oplog window), `use web`,
`db.events.estimatedDocumentCount()`.

## 8. Serving: API, Redis, Neo4j

**API** (`http://localhost:8000`, interactive docs at `/docs`)

```bash
curl -s localhost:8000/health                  # liveness: 200 while the process answers
curl -s localhost:8000/ready                   # {"redis":"ok","neo4j":"ok"}, 503 naming a store that is down
curl -s localhost:8000/users/<user id>/features | python3 -m json.tool
curl -s "localhost:8000/products/27809/recommendations?limit=5" | python3 -m json.tool
curl -s localhost:8000/openapi.json            # the OpenAPI schema
```

Recommendations: `limit` 1 to 20 (422 outside); 404 for an unknown
product, `[]` when it was never bought with another; 503 within about a
second when Neo4j is down.

**Redis** (admin shell; the API and the stream have their own ACL users)

```bash
$C exec redis sh -c 'REDISCLI_AUTH="$REDIS_ADMIN_PASSWORD" redis-cli --user admin'
```

Inside: `DBSIZE`, `SCAN 0 MATCH user:*:viewed COUNT 100` (find a user id),
`ZREVRANGE user:<id>:viewed 0 9 WITHSCORES`, `HGETALL user:<id>:cart:<session>`,
`TTL user:<id>:viewed`, `INFO memory`. ⚠️ `FLUSHDB` only in the rebuild
procedure (`uv run drills/features_rebuild.py` does it for you).

**Neo4j** (Browser at `http://localhost:7474`, user `neo4j`, `NEO4J_PASSWORD`)

```bash
$C exec neo4j sh -c 'cypher-shell -u neo4j -p "${NEO4J_AUTH#neo4j/}"'
```

Example Cypher:

```cypher
MATCH (p:Product) RETURN count(p);
MATCH ()-[r:BOUGHT_WITH]->() RETURN count(r), max(r.weight);
// A product's top 5, read in both directions (one relationship per pair)
MATCH (p:Product {id: 27809})-[r:BOUGHT_WITH]-(o:Product)
RETURN o.id, o.name, r.weight ORDER BY r.weight DESC, o.id LIMIT 5;
// Which run wrote the graph (every node and relationship carries it)
MATCH (p:Product) RETURN DISTINCT p.run;
```

## 9. Monitoring

| What | Where or how |
|---|---|
| Prometheus (targets, alerts, queries) | `http://localhost:9090` |
| Alertmanager | `http://localhost:9093` |
| Spark cluster UI / bronze and features driver UIs | `http://localhost:8080` / `http://localhost:4040`, `http://localhost:4041` |

Start only the monitoring services (`make up PROFILES=monitoring` would also
restart anything you stopped in the profiles already running):
`$C --profile monitoring up -d --wait postgres-exporter prometheus alertmanager`.
Prometheus scrapes two targets every 15 s: `postgres` (postgres-exporter,
the replication slot) and `connect` (the JMX exporter in the Connect worker:
connectors, tasks, Debezium). Spark, Airflow, Redis, Neo4j and the API are
not scraped yet (P7). In the UI: **Status → Targets** (is each target UP?),
**Alerts** (inactive, pending, firing), **Query** (PromQL, then the Graph
tab for history).

**PromQL** (healthy values measured on 2026-10-10; the alert column is the
rule in `onprem/monitoring/alerts.yml` that watches it):

| Query | Meaning | Healthy | Alert |
|---|---|---|---|
| `up` | Last scrape of each target succeeded | 1 for both jobs | `PostgresExporterDown`, `KafkaConnectDown` (0 for 2 min) |
| `pg_replication_slots_slot_is_active` | Debezium is connected to its slot | 1 | `ReplicationSlotInactive` (0 for 30 min) |
| `pg_replication_slots_pg_wal_lsn_diff / 1024^2` | MB of WAL PostgreSQL keeps for the slot | < 1 MB | `ReplicationSlotRetainedWalHigh` (> 1 GB for 5 min) |
| `pg_replication_slots_safe_wal_size_bytes / 1024^3` | GB left before the slot is invalidated | ~10 GB (the cap) | `ReplicationSlotNearInvalidation` (< 2 GB) |
| `debezium_streaming_connected` | Each Debezium source connected | 1 for both | `DebeziumNotConnected` (0 for 2 min) |
| `debezium_streaming_millisecondsbehindsource / 1000` | Capture lag in seconds, per source | 0.07-0.3 s | `DebeziumLagHigh` (> 60 s for 10 min) |
| `max_over_time(debezium_streaming_millisecondsbehindsource[1h]) / 1000` | Worst capture lag over the last hour | < 0.5 s | |
| `kafka_connect_connector_status == 1` | Each connector's state | `running` for both | `ConnectorNotRunning` (5 min) |
| `kafka_connect_task_status{status="failed"} == 1` | Failed tasks | empty | `ConnectorTaskFailed` (1 min) |

The same from a terminal, through the HTTP API:

```bash
curl -s localhost:9090/api/v1/query --data-urlencode 'query=debezium_streaming_millisecondsbehindsource / 1000'
curl -s localhost:9090/api/v1/targets          # scrape health per target
curl -s localhost:9090/api/v1/alerts           # pending and firing alerts
```

Alertmanager (`http://localhost:9093`) groups firing alerts by `alertname`
and `slot_name` and lets you silence one during planned work. No delivery
channel (email, Slack) is configured yet: firing alerts appear only there.
To see the whole cycle (Connect stopped, WAL grows, alert fires, recovery):
`drills/slot-drill.sh` (`core monitoring`).

Alert rule tests, as CI runs them (the stack's Prometheus image, so promtool
matches the server):

```bash
docker run --rm -v "$PWD/onprem/monitoring:/m:ro" -w /m --entrypoint promtool \
  prom/prometheus:v3.15.0@sha256:efd719c99d83b060d9daefdcf00360461adf279f45ef5391f8d111892118753e \
  test rules alerts_test.yml
```

Replication slot by hand (in psql):

```sql
SELECT slot_name, active, wal_status,
       pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) AS retained,
       pg_size_pretty(safe_wal_size) AS safe_left
FROM pg_replication_slots;
SHOW max_slot_wal_keep_size;
```

## 10. Drills and reconciliations

| Command | Checks | Needs |
|---|---|---|
| `uv run drills/verify_cdc.py [source ...]` | Kafka = both databases, every table (exit 0 = identical) | writers stopped, connectors caught up |
| `uv run drills/verify_silver.py [table ...]` | Silver = both databases, every column | writers stopped, silver run after bronze went idle |
| `uv run drills/freshness.py [samples]` | O1: source commit -> queryable in Athena (< 5 min) | `core stream` |
| `uv run drills/gold_freshness.py [label]` | O2: source commit -> gold (< 1 hour) | `core stream airflow` |
| `uv run drills/features_freshness.py [samples]` | P5: a product view -> API (< 1 min) | `core serving` |
| `uv run drills/features_rebuild.py` | Rebuild Redis from Kafka and compare with the live state | `core serving` |
| `uv run drills/redis_outage.py [seconds]` | Redis down under load: API 503, stream recovers, bronze unaffected | `core stream serving` |
| `uv run drills/recommendations_affinity.py` | P6 acceptance: top 20 products' companions; same-category lift | `core serving`, a recent graph |
| `drills/slot-drill.sh [log file]` | Stop Connect, WAL grows, the slot alert fires, recovery | `core monitoring` |
| `drills/throughput-baseline.sh [log file]` | Raises the generator rate step by step, measures capture lag and WAL | `core monitoring` |
| `uv run scripts/seed_mongo_events.py` | One-off (P2): copy the event history into MongoDB | done once; kept for the record |

## 11. Tests and code quality

```bash
uvx ruff check . && uvx ruff format --check .   # lint and format, from the repo root (CI pins ruff 0.16.10)
make test-spark                                  # PySpark unit tests (Java 17 needed)
make test-graph                                  # graph writer + API query on a throwaway Neo4j

# API, generator, review simulator: from the component's folder, as CI runs them
cd api && uv run --no-project --python 3.12 --with-requirements requirements.txt \
  --with-requirements requirements-test.txt python -m pytest -q
cd onprem/generator && uv run --no-project --python 3.12 \
  --with-requirements requirements.txt --with pytest==9.1.1 python -m pytest -q

# Airflow DAG tests, Airflow installed as in the image
airflow/ci-constraints.sh /tmp/constraints.txt
uv venv --python 3.10 /tmp/af && uv pip install --python /tmp/af \
  -r airflow/requirements-test.txt -c /tmp/constraints.txt --override onprem/spark/airflow-overrides.txt
/tmp/af/bin/python -m pytest -q airflow/tests

gh run list --limit 3                            # CI results after a push
```

`make test-graph` deletes every node of the Neo4j it uses, which is why it
starts its own container (port 17687) and never touches the serving one.

## 12. AWS: Terraform, keys, cost

| Command | What it does |
|---|---|
| `make tf-plan` | Plan the `lake` stack into `tfplan` (profile `thelook`) |
| `make tf-apply` | Apply exactly the reviewed plan |
| `make cost-report` | Last 7 days of spend by project tag and service (each call costs $0.01) |
| `aws --profile thelook iam create-access-key --user-name <user>` | New key for `thelook-spark-stream`, `thelook-spark-batch` or `thelook-analyst` |
| `aws --profile thelook iam update-access-key --user-name <user> --access-key-id <old> --status Inactive` | Rotation step 3, or revoke a leaked key at once |
| ⚠️ `aws --profile thelook iam delete-access-key --user-name <user> --access-key-id <old>` | Delete the old key a day after rotating |
| `aws --profile thelook iam get-access-key-last-used --access-key-id <id>` | When a key was last used |

After changing a key in `onprem/.env`, recreate the containers that read
it (`$C up -d <service>`). S3 transfer with `stream` and `airflow` up is
about 0.8 GB an hour: run them while working, `make down` otherwise.

## 13. Recovery procedures (pointers)

Each is a sequence of the commands above; follow the runbook section, in order.

| Symptom | Runbook section |
|---|---|
| Kafka log says "no clean shutdown" | After an unclean Kafka shutdown: verify, then repair silver |
| A connector task FAILED after a snapshot signal | Re-snapshot the PostgreSQL tables |
| MongoDB connector: ChangeStreamHistoryLost / InvalidResumeToken | MongoDB |
| Bronze stream stopped with `failOnDataLoss` | The stream stopped with failOnDataLoss |
| Replication slot alerts | Replication slot |
| API 503, `features-stream` restarting | The API answers 503, or features-stream keeps restarting |
| Redis lost data, or the features logic changed | Rebuild the features |
| Rotate a Redis password | Rotate a Redis password |

## 14. Ports and UIs

All bound to `localhost` only.

| Port | Service |
|---|---|
| 5432 | PostgreSQL |
| 27017 | MongoDB (`mongodb://localhost:27017/?directConnection=true`) |
| 9092 | Kafka (inside Compose: `kafka:29092`) |
| 8081 | Schema Registry |
| 8083 | Kafka Connect REST |
| 8080 / 4040 / 4041 | Spark cluster UI / bronze driver UI / features driver UI |
| 8088 | Airflow UI |
| 6379 | Redis |
| 8000 | Serving API (`/docs`) |
| 7474 / 7687 | Neo4j Browser / Bolt |
| 9090 / 9093 | Prometheus / Alertmanager |
