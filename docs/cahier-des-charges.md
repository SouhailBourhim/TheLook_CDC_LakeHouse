*Cahier des charges*

# theLook CDC Lakehouse on AWS

Real-time change data capture from an operational database to a
governed, cost-controlled analytics lakehouse

| **Item**     | **Value**                                     |
|--------------|-----------------------------------------------|
| Author       | Souhail Bourhim                               |
| Programme    | INE3, Smart-ICT, INPT Rabat                   |
| Project type | Personal portfolio project (data engineering) |
| Version      | 2.0                                           |
| Date         | 3 October 2026                                |
| Status       | In progress: P1 done, P2 to P4 must-have      |

### Revision history

| **Version** | **Date**   | **Change**                                                                                                                                                                                                                                                                                          |
|-------------|------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| 1.0         | 29/09/2026 | Initial specification, based on the Olist dataset                                                                                                                                                                                                                                                   |
| 1.1         | 29/09/2026 | Source switched to theLook eCommerce: live generator, realistic PII, returns, clickstream events. Added optional clickstream extension (P7).                                                                                                                                                        |
| 1.2         | 29/09/2026 | Kinesis and Firehose replaced by self-hosted Kafka (on-prem side): Kafka Connect with Debezium source, Schema Registry with Avro, Apache Iceberg sink connector to S3/Glue. Kinesis kept for P7 only.                                                                                               |
| 1.3         | 29/09/2026 | Orchestration moved from Step Functions + EventBridge to self-hosted Apache Airflow (on-prem side): dbt runs, compaction, snapshot expiry, GDPR erasure and backfills.                                                                                                                              |
| 1.4         | 29/09/2026 | Pre-start review based on online research: local lake dropped (dbt developed on Athena); incremental MERGE silver; replication slot safeguards; Debezium exactly-once; ODCS contracts; dbt unit tests; resilience drills; throughput objective; ADRs; version pinning; generator licence confirmed. |
| 1.5         | 29/09/2026 | Added section 13: working rules for Claude Code as a learning partner (decisions stay with the author), with the matching CLAUDE.md file.                                                                                                                                                           |
| 1.6         | 29/09/2026 | Section 13 revised: Claude Code may design and write everything, but must explain every decision and change so that the author understands all of it.                                                                                                                                               |
| 1.7         | 29/09/2026 | Section 4.2 corrected after reading the generator code: it runs INSERT and UPDATE statements only, never DELETE. All source deletes come from the synthetic erasure scripts (4.3). From this version the Markdown file is the reference; the Word file stays at v1.6. |
| 1.8         | 30/09/2026 | O8 target set from the P1 baseline run (docs/results.md): 200 change events/s. The capture side's breaking point is found in P2, end to end (the P1 load generator is latency-bound near 266 events/s). |
| 1.9         | 30/09/2026 | Open questions A4 and C1 settled at the start of P2: the Iceberg sink creates and evolves the bronze tables, and the contracts validate them (Terraform creates only the Glue databases); the lake stays deployed between sessions, with `terraform destroy` kept as the one-command teardown. |
| 2.0         | 03/10/2026 | Extension for Spark, NoSQL and serving (ADRs 007 to 010): MongoDB becomes a second CDC source and the new home of clickstream `events`, plus a synthetic `reviews` collection; PySpark Structured Streaming replaces the planned Iceberg sink connector as the bronze writer; PySpark batch replaces dbt for silver and gold; new serving layer (Redis online features, Neo4j co-purchase graph, FastAPI); synthetic cart product/price and basket affinity in the generator; milestones renumbered P1 to P10, CI starts in P2. |

## 1. Context and problem

theLook is a fictional online clothing retailer. Its synthetic dataset
was created by Google for Looker and is published as a public BigQuery
dataset. The company runs its website and operations on a PostgreSQL
database, and today analytics depends on a nightly full dump of that
database. This causes five problems:

- **Slow, heavy loads:** the dump takes hours, loads the production
  database, and the data is up to 24 hours old.

- **Inconsistent metrics:** finance and marketing compute "revenue"
  differently (with or without returns and cancellations), so dashboards
  disagree.

- **Lost history:** user records are overwritten. When a user moves,
  past orders get attributed to the new address.

- **No privacy process:** the database holds names, emails, street
  addresses and IP addresses, but there is no way to handle
  data-deletion requests (GDPR in Europe, Law 09-08 supervised by the
  CNDP in Morocco).

- **Silent breakage:** nobody notices when an upstream schema change
  breaks the dashboards.

**Goal:** replace the nightly dump with a change data capture (CDC)
pipeline into an AWS lakehouse. The pipeline must be near-real-time,
trustworthy, governed and cost-controlled.

Since v2.0 the website's clickstream and product reviews live in a
MongoDB document store, next to the PostgreSQL database, and both are
captured. The same change streams also feed a small serving layer: per-user
features for the website (recently viewed products, current cart value,
recent activity) and "frequently bought together" recommendations.

## 2. Measurable objectives

Each objective has a target that is measured at the end of the project.
The measured values are reported on the results page (section 10) and
become the figures used on the CV.

| **\#** | **Objective**                                | **Target**                                                                                                                         |
|--------|----------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------|
| O1     | Freshness from source commit to bronze layer | \< 5 minutes                                                                                                                       |
| O2     | Freshness of analytics tables (gold layer)   | \< 1 hour                                                                                                                          |
| O3     | Extra load on the source database            | Only reading the change log (WAL); no full table scans                                                                             |
| O4     | Data correctness                             | 0 duplicates, 0 lost changes; row counts reconcile with the source                                                                 |
| O5     | Data-deletion request handled                | User erased from every layer and store (PostgreSQL, MongoDB, lake, Redis), including clickstream, in \< 24 hours, with an audit trail; Kafka bounded by topic retention |
| O6     | Monthly AWS cost                             | \< \$15 at demo volume                                                                                                             |
| O7     | Reproducibility                              | Whole stack deployed or destroyed with one command                                                                                 |
| O8     | Throughput                                   | Sustain 200 change events/s (set from the P1 baseline run) with consumer lag \< 30 seconds; find and document the breaking point (in P2) |
| O9     | Resilience                                   | Every failure drill in FR14 ends with 0 lost and 0 duplicated rows after reconciliation                                            |

## 3. Scope

### 3.1 In scope

- CDC ingestion of 5 PostgreSQL tables and 2 MongoDB collections
  (section 4.1), including an initial snapshot, through Apache Kafka.

- A lakehouse organised in three layers: bronze (raw change log), silver
  (current state), gold (star schema).

- A user history dimension (SCD Type 2).

- Data contracts, quality checks and alerting.

- Data-deletion (GDPR) workflow and PII governance.

- Backfills and reprocessing.

- Monitoring, infrastructure as code (Terraform) and CI/CD.

- Stream and batch processing with Apache Spark (PySpark): streaming
  ingestion to bronze, batch silver and gold.

- A serving layer: online user features in Redis, a co-purchase graph in
  Neo4j, and a FastAPI service exposing both.

- Automated tests (pytest, chispa) and continuous integration.

- Optional extension: AWS-managed streaming on Kinesis and Managed Flink
  (phase P10).

### 3.2 Out of scope

- Machine learning models (covered by a separate project). The
  recommendations are co-purchase counts, not a trained model.

- A BI tool beyond basic dashboards.

- Multi-region deployment and disaster recovery.

- A real production data source.

## 4. Data sources

### 4.1 theLook eCommerce schema

The operational data lives in two stores. Column lists are recorded in
`docs/source-schema.md` (checked against the generator's code at the start
of P1) and frozen in the data contracts.

**PostgreSQL** (schema `shop`), transactional data:

| **Table**    | **Content**                                                                                                     | **Change pattern**                 |
|--------------|-----------------------------------------------------------------------------------------------------------------|------------------------------------|
| users        | Name, email, age, gender, street address, city, state, country, postal code, latitude/longitude, traffic source | Inserts (sign-ups); address updates |
| products     | Name, brand, category, department, cost, retail price, distribution center                                      | Mostly static                      |
| dist_centers | Name, latitude/longitude                                                                                        | Static                             |
| orders       | User, status (Processing, Shipped, Delivered, Cancelled, Returned), created/shipped/delivered/returned timestamps | Frequent updates as status changes |
| order_items  | Order, product, item status, sale price, lifecycle timestamps                                                   | Frequent updates                   |

**MongoDB** (database `web`), website data, one document per record:

| **Collection** | **Content**                                                                                                                                         | **Change pattern**                          |
|----------------|-----------------------------------------------------------------------------------------------------------------------------------------------------|---------------------------------------------|
| events         | Session, sequence number, event type (home, department, product, cart, purchase, cancel, return), URI, browser, IP address, traffic source; cart events also carry product and price (4.3) | Append-only, high volume                    |
| reviews        | Product, user, order item, rating, title, text, helpful votes (synthetic, 4.3)                                                                     | Inserts, edits, helpful-vote updates, deletes |

Until v2.0, `events` was a PostgreSQL table; the P1 results (including the
throughput baseline) were measured with it there. At the start of P2 its
rows are copied once into MongoDB, the generator writes new events to
MongoDB, and the table leaves the PostgreSQL publication (ADR 008).

This mix is deliberate: static reference data, frequently updated
transactional data, a high-volume append-only log and schemaless documents
each need a different handling strategy (see section 8).

### 4.2 Live data generator

Factor House publishes an open-source Python generator that writes live
theLook traffic: new sign-ups, browsing sessions, purchases, cancellations
and returns. It produces genuine INSERT and UPDATE statements (order status
changes, and address changes when enabled). It never deletes rows. From
v2.0 a patch in this repository makes it write clickstream events to
MongoDB instead of PostgreSQL.

- Only the generator is reused. The surrounding Factor House demo uses
  their commercial monitoring tools (Kpow, Flex), which are not part of
  this project.

- The Factor House examples repository is published under the **Apache
  License 2.0**, which allows reuse with attribution. Check that the
  generator folder does not carry a different licence, and keep the
  notice in the repository.

- The generator runs as a container next to PostgreSQL and MongoDB, with a
  configurable event rate.

### 4.3 Synthetic additions

Some behaviours are not produced by the generator. They are added by small
scripts or by labelled patches to the vendored generator, and clearly
marked as synthetic in the documentation.

- Data-deletion requests. In PostgreSQL they are the only source of DELETE
  statements, so they also produce the Debezium delete events and
  tombstones that the silver MERGE must handle.

- Injected schema changes and malformed records, to exercise the data
  contracts.

- **Cart product and price** (generator patch): theLook cart events only
  have the URI `/cart`. Each cart event gets the `product_id` of the product
  viewed just before it in the session and that product's `retail_price`,
  so the online cart-value feature (FR15) has data.

- **Basket affinity** (generator patch): the generator picks every product
  uniformly at random, so almost no product pair is bought together twice
  and co-purchase recommendations (FR16) would be noise. With a set
  probability, the second and later items of an order come from the same
  category as the first.

- **Reviews** (review simulator): a small service writes reviews for
  delivered order items (real users and products), edits some, adds
  helpful votes and deletes a few. It is the only source of MongoDB
  deletes besides erasure requests.

The generator already updates user addresses, so no synthetic
address-change script is needed (see `docs/source-schema.md`).

## 5. Target architecture

### 5.1 Data flow

```
--- On-prem side (Docker) ---
theLook generator --> PostgreSQL (orders, users, products...) --WAL-----------+
                  --> MongoDB (events)  <-- review simulator (reviews)         |
                        --change streams--+                                    |
                                          v                                    v
                   Kafka Connect [Debezium MongoDB + PostgreSQL sources]
                   --> Kafka (KRaft) + Schema Registry (Avro)
                   --> Spark Structured Streaming --+--> bronze (below)
                                                    +--> Redis (online user features)
--- AWS side ---
   --> Iceberg bronze tables (S3 + Glue Catalog, one table per source table/collection)
   --> PySpark batch: silver (current state, SCD2) --> gold (star schema, marts)
   --> Athena (SQL, dashboards)
--- Serving (on-prem) ---
   gold order items --> Neo4j co-purchase graph
   FastAPI: /users/{id}/features (Redis), /products/{id}/recommendations (Neo4j)

Orchestration : Apache Airflow (self-hosted, Docker, on-prem side), spark-submit
Monitoring    : Prometheus/Alertmanager on-prem; CloudWatch for AWS
IaC / CI      : Terraform, GitHub Actions (ruff, pytest, terraform validate; OIDC to AWS)
Development   : everything heavy runs locally in Docker; AWS holds only S3, Glue, Athena
```

The source databases, Kafka and the processing engines run locally to
represent a company that keeps its operational systems and event platform
in its own data centre and lands data in a cloud lakehouse. This hybrid
set-up is a common real-world pattern, and it avoids paying for managed
databases, a managed Kafka cluster or a managed Spark service.

There is deliberately **no local copy of the lake**. The Spark jobs run
locally but read and write the Iceberg tables on S3 through the Glue
catalog, so there is one lake and Athena sees every commit at once.
Transformations are tested with pytest and chispa on small fixed
DataFrames built in memory, never on a second lakehouse.

### 5.2 Technology choices

| **Component**    | **Choice**                        | **Justification**                                                                                                  | **Rejected alternative**                                                                                |
|------------------|-----------------------------------|--------------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------|
| Event platform   | Apache Kafka (KRaft), self-hosted | Industry-standard event platform, widely requested by employers; \$0 when run locally                              | MSK Serverless: about \$0.75/hour (~\$540/month); Kinesis: AWS-only, less common in industry            |
| CDC              | Debezium on Kafka Connect         | The standard production deployment of Debezium; offsets and restarts managed by Connect; one platform for both sources | Debezium Server: simpler, but less representative of production                                         |
| Document store   | MongoDB, single-node replica set  | Natural home for clickstream and reviews; change streams (replica set required) give Debezium a CDC log            | A second PostgreSQL schema: no NoSQL practice, no schemaless-data problem                               |
| Schemas          | Schema Registry + Avro            | Schemas enforced when data is written; compatibility rules block breaking changes                                  | Schemaless JSON: breakage only detected downstream                                                      |
| Delivery to lake | Spark Structured Streaming        | One engine for streaming and batch; full control of envelope parsing; the same stream also feeds Redis (ADR 007)   | Iceberg Kafka Connect sink: configuration only and exactly-once, but must be built from source and cannot feed Redis |
| Storage          | Apache Iceberg on S3              | ACID tables, row-level deletes (needed for GDPR), time travel; Athena reads and writes it (MERGE, OPTIMIZE, VACUUM); the Glue catalog makes Spark commits visible to Athena at once | Delta Lake: Athena can only read it, and open-source Spark needs a separate Glue registration step; plain Parquet: no deletes or ACID |
| Query            | Athena                            | Serverless, pay per query                                                                                          | Redshift: fixed monthly cost                                                                            |
| Transformation   | PySpark batch (Spark SQL MERGE on Iceberg) | Same engine as ingestion; transformations unit-tested with pytest and chispa (ADR 009)                    | dbt on Athena: strong SQL tooling, but a second engine and a second testing stack                       |
| Online store     | Redis                             | Sub-millisecond reads of small per-user structures; TTLs bound how long features and personal data live (ADR 010) | Querying the lake per request: seconds per query and Athena cost per call                               |
| Graph store      | Neo4j Community                   | Co-purchase pairs are a graph; Cypher expresses "top-N neighbours by weight" directly                             | Pre-computed table in Redis: works, but no graph practice and no traversal beyond one hop               |
| Serving API      | FastAPI                           | Typed request/response models, automatic OpenAPI docs, easy to test with TestClient                                | Flask: no built-in validation or schema                                                                 |
| Orchestration    | Apache Airflow, self-hosted       | Industry standard; native backfills over date ranges; \$0 when run locally next to Kafka                           | MWAA (managed Airflow): hundreds of dollars/month idle; Step Functions: AWS-only, weak backfill support |
| Tests and CI     | pytest, chispa, ruff, GitHub Actions | chispa compares DataFrames with readable diffs; ruff lints and formats (black style) in one tool; Actions is free for public repositories | Great Expectations for unit tests: built for data validation, not code tests                           |

### 5.3 Version pinning

Versions current at the time of writing. All images and packages are
pinned in the repository; upgrades are deliberate and documented. Exact
versions for the components added in v2.0 are chosen at the start of the
phase that introduces them, after checking their compatibility, and
recorded in the phase's ADR or commit.

| **Component**        | **Version line**                   | **Notes**                                                          |
|----------------------|------------------------------------|--------------------------------------------------------------------|
| Apache Kafka         | 4.x (4.3 released May 2026)        | KRaft only since 4.0 (no ZooKeeper); single broker locally         |
| Debezium             | 3.x (3.7 in use)                   | PostgreSQL connector with pgoutput; MongoDB connector with change streams |
| PostgreSQL           | 17                                 | max_slot_wal_keep_size available since 13; failover slots since 17 |
| MongoDB              | 8.x                                | Single-node replica set; pre- and post-images available since 6.0 |
| Apache Spark         | 4.x                                | Standalone master + one worker; Python API                         |
| Apache Iceberg       | Spark runtime and AWS bundle matching the Spark line | GlueCatalog and S3FileIO                         |
| Apache Airflow       | 3.x (3.3 released July 2026)       | Asset-aware scheduling; OpenLineage provider                       |
| Redis, Neo4j, FastAPI | Current stable lines              | Pinned when introduced (P5, P6)                                     |

## 6. Functional requirements

### FR1: Capture

Capture every insert, update and delete on the 5 PostgreSQL tables and the
2 MongoDB collections, starting with an initial snapshot. Changes are
published to one Kafka topic per table or collection, keyed by primary key
(`id`, or `_id` in MongoDB) so that all changes to a row stay in order
within one partition. Each event is serialised in Avro and carries its
operation type, log position (LSN in PostgreSQL; cluster time and order
within it in MongoDB), commit timestamp and source table or collection.

Debezium PostgreSQL configuration requirements:

- **pgoutput** logical decoding plugin, with a pre-created publication
  limited to the captured tables, so the WAL decoded for other tables is
  not streamed.

- **Heartbeats** (heartbeat.interval.ms of about 30 seconds, with a
  heartbeat.action.query writing to a small heartbeat table) so the
  replication slot keeps advancing even when captured tables are idle.

- **Exactly-once source delivery**: Kafka Connect in distributed mode
  (3.3 or later) with exactly.once.source.support = enabled on the
  workers and exactly.once.support = required on the connector. The
  Debezium documentation itself notes open questions about Kafka
  transaction correctness, so deduplication in silver stays as the
  safety net.

- lz4 compression on the connector producer to reduce network and
  storage volume.

Debezium MongoDB configuration requirements:

- MongoDB runs as a replica set (a single node locally): change streams,
  which Debezium reads, only exist on replica sets and sharded clusters.

- Updates carry the full document after the change, so bronze holds
  complete versions, not partial update descriptions.

- A topic prefix distinct from the PostgreSQL connector's (Debezium
  requires one prefix per connector); topics keep the
  `<prefix>.<database>.<collection>` shape.

- The same exactly-once, heartbeat and compression settings as the
  PostgreSQL connector, where the connector supports them.

### FR2: Bronze layer

Store an append-only, immutable change log per source table and
collection. Nothing in bronze is ever updated in place (erasure requests
are the documented exception, FR9). A PySpark Structured Streaming job
reads the CDC topics and appends to the bronze Iceberg tables:

- one row per change event, with the operation, the before and after
  images, the log position, the source commit timestamp and the Kafka
  coordinates (topic, partition, offset);

- Confluent-framed Avro is decoded with the writer schema fetched from
  Schema Registry by schema ID, so several schema versions in one topic
  are handled;

- MongoDB documents are schemaless, so their images are kept as JSON text
  in bronze and parsed with explicit schemas in silver (schema-on-read);

- Kafka is read with read_committed isolation, since Debezium writes in
  transactions;

- progress is checkpointed, and each micro-batch commit records its batch
  ID in the Iceberg snapshot so that a replayed batch is not appended
  twice;

- the job creates the bronze tables and evolves them with new nullable
  columns; the data contracts validate them (ADR 006).

### FR3: Silver layer

Build the current state of each entity from the change log:

- keep only the latest version of each row, ordered by log position
  (LSN for PostgreSQL; cluster time then order for MongoDB);

- remove rows that were deleted at the source;

- ignore updates that change no business column (the generator rewrites
  unchanged rows, see `docs/source-schema.md`);

- be idempotent, so that a rerun always gives the same result;

- be **incremental**: PySpark jobs read only bronze rows committed after
  the last processed point, deduplicate them, and apply them with Spark
  SQL `MERGE INTO` on the Iceberg silver table (update, insert, delete).

The events collection is append-only, so its silver version only needs
deduplication, not current-state logic.

### FR4: User history (SCD2)

Build dim_user with one row per version of each user, with valid_from
and valid_to taken from source commit timestamps. Each order joins to
the user's address as it was at purchase time, not the current one.

Periodic snapshots of the current state (daily copies, or dbt-style
snapshots) are deliberately not used: they only record the state seen at
each run, whereas the change log contains every intermediate version.

### FR5: Gold star schema

| **Type**   | **Tables**                                                      |
|------------|-----------------------------------------------------------------|
| Facts      | fct_orders, fct_order_items, fct_sessions (built from events)   |
| Dimensions | dim_user (SCD2), dim_product, dim_distribution_center, dim_date |
| Marts      | mart_daily_revenue, mart_session_funnel, mart_product_ratings (from reviews) |

Gold tables are written by PySpark batch jobs as Iceberg tables in the
`thelook_gold` Glue database, so Athena can query them directly.

### FR6: Metrics layer

One agreed definition for each business metric, documented and tested:

- gross revenue and net revenue (after cancellations and returns);

- gross margin, using product cost versus sale price;

- average order value;

- cancellation rate and return rate;

- time to ship and time to deliver;

- session-to-purchase conversion rate, from the events collection
  (ghost sessions excluded, see `docs/source-schema.md`);

- average product rating and review count, from the reviews collection.

### FR7: Data contracts

- One contract per table, written in the **Open Data Contract Standard
  (ODCS)**, defines the schema, types, primary key, quality rules, PII
  flags and owner.

- Contracts are checked automatically with **datacontract-cli**, which
  can test Athena, S3 and Iceberg data, and Avro Kafka topics through
  Schema Registry (its Kafka support is still marked experimental).

- Schema Registry enforces the schema on the wire, with a compatibility
  rule (for example BACKWARD) that rejects breaking changes at the
  producer.

- Records that violate a contract go to a dead-letter area and raise an
  alert.

- The contract files are the single source of truth for what each table
  must look like. The Spark streaming job creates the bronze tables and
  evolves them (new nullable columns); the contracts validate the resulting tables
  and topics, so a drift between data and contract fails a check instead
  of being hidden by a Terraform-managed schema.

- The contracts also protect the pipeline against schema changes in the
  generator itself.

- MongoDB documents have no schema at the source; their contracts apply
  to the parsed silver tables, and documents that do not parse go to the
  dead-letter area.

### FR8: Data quality

- Checks on silver and gold: unique, not null, relationships, accepted
  values (for example, order status), run by datacontract-cli from the
  contracts (FR7).

- Freshness checks on every source.

- Row-count reconciliation between the sources (PostgreSQL and MongoDB)
  and the silver layer.

- **Unit tests** with pytest and chispa on fixed input DataFrames for the
  hardest logic: Debezium envelope parsing, log-position deduplication,
  delete handling, no-op updates, SCD2 validity ranges, and net revenue
  after returns. Incremental jobs are tested both on an empty target and
  on an existing one.

### FR9: Data-deletion (GDPR) workflow

- A deletion request removes the user from every layer and store: users
  and orders in PostgreSQL, events and reviews in MongoDB, every lake
  layer, and the user's keys in Redis. The Neo4j graph holds products
  only, so it contains no personal data.

- Old Iceberg snapshots are then expired so the data is physically gone,
  including from time travel. On Athena this means lowering
  vacuum_max_snapshot_age_seconds on the affected tables and running
  VACUUM, which expires snapshots and removes orphan files.

- Kafka topics also hold the personal data until retention expires.
  Topic retention is therefore kept short (a few days) and documented as
  the upper bound of the erasure delay.

- Every step is recorded in an audit table.

- Aggregated metrics are kept, since they no longer identify the person.

### FR10: Backfill

Rebuild silver and gold for any date range without creating duplicates.
This is demonstrated by injecting a logic bug (for example, counting
returned orders as revenue), fixing it, and reprocessing the affected
period with an Airflow backfill over that date range.

### FR11: PII governance

PII columns are tagged in the contracts: first and last name, email,
street address, postal code, latitude/longitude, and IP address.
Analysts see them masked; only a privileged role sees them in clear,
enforced with AWS Lake Formation. Lake Formation data cell filters work
on Iceberg tables queried with Athena engine version 3, but not on views
or nested columns, so they are applied to the gold tables themselves.

### FR12: Orchestration

Apache Airflow runs next to Kafka on the on-prem side and schedules
everything that is not streaming. Each DAG is idempotent and
parameterised by date, so any run can be repeated or backfilled safely.

| **DAG**        | **Schedule**                    | **Tasks**                                                                                                                      |
|----------------|---------------------------------|--------------------------------------------------------------------------------------------------------------------------------|
| transform      | Every 30 minutes                | spark-submit silver then gold jobs, then quality checks; fail and alert if a check fails                                      |
| graph          | Daily                           | Rebuild the Neo4j co-purchase graph from gold order items                                                                      |
| maintenance    | Daily                           | OPTIMIZE ... REWRITE DATA USING BIN_PACK on recent partitions; VACUUM to expire snapshots and remove orphan files              |
| gdpr_erasure   | Triggered by a deletion request | Delete the user in every layer, expire snapshots, verify, write the audit record                                               |
| quality_report | Daily                           | Freshness checks, source-to-silver reconciliation, datacontract-cli tests, connector, consumer-lag and replication-slot health |

The OpenLineage provider for Airflow can send lineage events to Marquez,
giving a free lineage graph of every DAG run (optional, P9).

### FR13: Replication slot safety

A Debezium replication slot stops PostgreSQL from recycling WAL files
the connector has not yet confirmed. If Debezium stops, the WAL grows
until the disk is full, which is a classic production outage.
Safeguards:

- max_slot_wal_keep_size set on the source database, so a stalled slot
  is invalidated instead of filling the disk;

- heartbeats (FR1) so idle tables do not freeze the slot;

- alerts when the slot is inactive for more than 30 minutes or retains
  more WAL than a threshold sized for the machine;

- a runbook entry for dropping orphaned slots and re-snapshotting after
  an invalidated slot.

MongoDB has the opposite failure mode. Its change log (the oplog) is a
fixed-size collection, so a stopped connector never fills the disk, but
once the oldest change it still needs is overwritten, its resume position
is lost and it must re-snapshot. The oplog window (hours of changes the
oplog holds) is measured, monitored, and kept well above the longest
expected outage.

### FR14: Resilience drills

Each drill is scripted, repeatable and documented with its measured
result. They are what turns the correctness claims into evidence.

| **Drill**                                                 | **Expected result**                                                                          |
|-----------------------------------------------------------|----------------------------------------------------------------------------------------------|
| Kill the Kafka Connect worker mid-stream, then restart it | Both connectors resume from committed offsets; reconciliation shows 0 lost and 0 duplicated rows |
| Restart the Kafka broker under load                       | Producers and connectors recover automatically; lag returns to normal                        |
| Stop Debezium for one hour while the generator runs       | Retained WAL grows and the alert fires; after restart the slot catches up with no data loss  |
| Reset connector offsets to force a replay                 | Bronze receives duplicates; silver output is unchanged                                       |
| Add a nullable column in PostgreSQL                       | The change flows through Avro, the Iceberg table schema and silver without breaking anything |
| Kill the Spark streaming job mid-batch, then restart it   | Resumes from its checkpoint; the replayed batch is not appended twice                       |
| Attempt an incompatible type change                       | Schema Registry rejects it; alert raised; nothing reaches the lake                           |

### FR15: Online user features (Redis)

A second query in the streaming job keeps per-user features in Redis,
updated within a minute of the event:

- the last 10 products the user viewed;

- the value of the current session's cart (cart events carry the price,
  4.3);

- the number of events in the last hour.

Updates must be idempotent, because a failed micro-batch is replayed.
Every key has a TTL, which bounds how long features (and personal data)
live. Anonymous (ghost) sessions are skipped. Redis is a cache rebuilt
from the stream, never a source of truth.

### FR16: Co-purchase graph (Neo4j)

A batch job builds `(:Product)-[:BOUGHT_WITH {weight}]->(:Product)` from
gold order items, where the weight is the number of orders containing both
products. It is rebuilt idempotently, so a rerun gives the same graph.

### FR17: Serving API (FastAPI)

| **Endpoint**                        | **Source** | **Returns**                                     |
|-------------------------------------|------------|-------------------------------------------------|
| GET /users/{id}/features            | Redis      | Recently viewed products, cart value, events in the last hour |
| GET /products/{id}/recommendations  | Neo4j      | Top-N products bought together, by weight       |

Unknown ids return 404; responses are typed models with OpenAPI
documentation.

### FR18: Tests and continuous integration

- GitHub Actions on every push: ruff (lint and format check), pytest, and
  `terraform fmt -check` and `terraform validate`.

- pytest covers the PySpark transformations (with chispa), the CDC
  parsing, the simulators and the API endpoints.

- CI starts in P2 with lint and the first tests, and grows with each
  phase.

## 7. Non-functional requirements

| **Area**            | **Requirement**                                                                                                                                                                                                                                                            |
|---------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Delivery semantics  | Debezium sources are at-least-once at worst (exactly-once where supported); the Spark bronze writer is exactly-once per micro-batch (checkpoint + batch ID in the Iceberg snapshot); silver deduplicates on log position, which gives exactly-once results end to end; Redis updates are idempotent |
| Kafka configuration | Topic retention long enough to replay a full day; replication factor documented (1 locally, 3 in a production design); consumer lag monitored                                                                                                                              |
| Schema evolution    | A new nullable column is added without breaking the pipeline; an incompatible type change is blocked by the contract                                                                                                                                                       |
| Performance         | Bronze tables partitioned by day; the streaming trigger interval trades freshness against file count; scheduled compaction (the events table matters most); Athena bytes scanned measured before and after optimisation |
| Security            | The Spark jobs use a dedicated IAM user restricted to the lake bucket and the project's Glue databases (never admin keys); Kafka Connect no longer touches AWS; least-privilege IAM for every other component and least-privilege database users (PostgreSQL and MongoDB); S3 encrypted with KMS; no secrets in the repository (`.env` git-ignored, `.env.example` documented); GitHub Actions authenticates with OIDC |
| Cost                | AWS Budget alarm; scan limit per Athena query (workgroup); one-command teardown                                                                                                                                                                                            |
| Observability       | Kafka consumer lag, connector status, replication slot and oplog window monitored (Prometheus and Alertmanager on-prem); Spark streaming progress (batch duration, input and processing rates) monitored; CloudWatch alarms on data freshness, dead-letter volume and Athena usage |
| Laptop resources    | Everything runs in Docker Compose profiles (`make up PROFILES=...`), so a subset can run alone; RAM per profile is measured and documented; JVM heaps sized explicitly |
| Documentation       | README with architecture (Mermaid), design decisions, how to run and demo each phase, cost breakdown, and an operations runbook |

## 8. Key technical challenges

These are the problems that make the project worth discussing in an
interview. Each one should be documented in the README with the chosen
solution and its trade-offs.

1.  **Out-of-order and duplicate events:** Kafka only guarantees order
    within a partition, so topics are keyed by primary key; silver still
    orders by LSN and deduplicates, because a connector restart can
    replay events.

2.  **Deletes in an append-only lake:** a delete is stored as an event
    in bronze and applied when building silver.

3.  **SCD2 from the change log:** more accurate than periodic snapshots,
    because it captures every change rather than the state at snapshot
    time.

4.  **Returns restate the past:** an order counted as revenue last week
    becomes a return today. Metrics must be recomputed for past periods,
    and the dashboards must say which figures can still change.

5.  **Different change patterns per table:** static reference data,
    frequently updated orders and a high-volume append-only log each
    need their own bronze-to-silver strategy.

6.  **Small files:** every streaming micro-batch commit creates new
    files, and the events table generates many of them. The trigger
    interval trades freshness against file count, and compaction is
    still needed.

7.  **WAL retention:** the source database silently keeps every change
    the connector has not confirmed, so a stopped pipeline becomes a
    disk problem on the production database.

8.  **Exactly-once is layered, not absolute:** exactly-once source and
    writer settings reduce duplicates, but correctness still depends on
    idempotent processing in silver.

9.  **Erasure in a versioned lake:** deleting a user means finding them
    across tables and stores (including clickstream IPs in MongoDB and
    keys in Redis) and expiring older Iceberg snapshots that still
    contain them.

10. **Registry-framed Avro in Spark:** Debezium's records start with a
    schema ID that Spark's Avro reader does not understand, and one topic
    can hold several schema versions. Each micro-batch is decoded group
    by group, with the writer schema fetched from Schema Registry.

11. **Schemaless documents into typed tables:** MongoDB documents can
    differ from one another. Bronze keeps them as JSON text; silver parses
    them with an explicit schema and sends what does not parse to the
    dead-letter area.

12. **Side effects in a replayed stream:** a failed micro-batch runs
    again, so the Redis writes it made must be idempotent (sorted sets
    keyed by product or event, not list pushes).

## 9. Milestones

Estimated duration: 8 to 10 weeks part-time for P1 to P9. Each phase ends
with a working, demonstrable result, committed with tests, a README update
and a "how to demo" note.

| **Phase**      | **Content**                                                                                                                                                                                       | **Acceptance criterion**                                                                                                                 |
|----------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------|
| P1 (done)      | On-prem stack: Postgres, theLook generator, Kafka, Schema Registry, Kafka Connect with Debezium; heartbeats and slot safeguards; baseline throughput run                                         | Avro change events from all 6 tables visible in their Kafka topics; slot alert tested                                                    |
| P2             | MongoDB as a second CDC source: replica set, events moved from PostgreSQL, synthetic reviews and generator additions (4.3), Debezium MongoDB connector, `make up/down` with profiles, minimal CI | Change events from both collections in Kafka; Kafka reconciles with MongoDB and PostgreSQL; CI green                                      |
| P3             | Bronze on AWS with Spark Structured Streaming: Terraform IAM for the Spark jobs, Spark cluster, streaming job with checkpointing, envelope parsing and Avro decoding                            | A changed row is queryable in Athena in \< 5 minutes; a killed and restarted streaming job appends no duplicates                         |
| P4             | PySpark silver (MERGE, deletes, SCD2) and gold (star schema, marts); Airflow DAG running them with spark-submit                                                                                  | Row counts reconcile with the sources; all unit tests pass; scheduled runs keep gold \< 1 hour old                                       |
| P5             | Redis online features from the streaming job; FastAPI `GET /users/{id}/features`                                                                                                                 | A product view appears in the user's features within 1 minute; endpoint tests pass                                                       |
| P6             | Neo4j co-purchase graph from gold; FastAPI `GET /products/{id}/recommendations`                                                                                                                  | Recommendations follow the synthetic basket affinity (same-category products ranked first); endpoint tests pass                          |
| P7             | ODCS contracts with datacontract-cli, quality checks, alerting, schema evolution drills                                                                                                           | An injected schema break is caught and an alert is raised; a compatible column addition flows end to end                                 |
| P8             | GDPR erasure and backfill, both as Airflow DAGs; failure drills (FR14)                                                                                                                            | Erasure verified in every layer and store, including clickstream, Redis and time travel; backfill leaves no duplicates; every drill passes reconciliation |
| P9             | Hardening: optimisation, load test, CI/CD completed (terraform validate, OIDC), ADRs, documentation; optional OpenLineage lineage                                                                 | Scan cost reduction and throughput limit measured; README, ADRs and runbook complete                                                     |
| P10 (optional) | AWS-managed streaming: clickstream events forwarded to a Kinesis stream and processed by Managed Flink (sessions, live funnel, late events), for the AWS Data Engineer exam                      | Live conversion funnel updated in \< 1 minute; late events handled with watermarks                                                       |

**Rule:** finish P1 to P4 before starting anything else. They already form
a complete, presentable project (the must-have for job applications). P5
and P6 add the serving layer; P10 only makes sense once P9 is done.

## 10. Deliverables

- A public GitHub repository with the code, Terraform, PySpark jobs and
  their tests, Airflow DAGs, the serving API, data contracts and CI
  pipeline.

- A README with the architecture diagram (Mermaid), design decisions,
  cost breakdown, and how to run and demo each phase.

- A demo video under 3 minutes: the generator creates and returns an
  order, and the change reaches the dashboard.

- A results page with the real measurements: freshness, throughput
  limit, scan savings, number of tests, drill results, cost per month.

- Architecture decision records (ADRs), one page each, in `docs/adr/`
  (for example: Kafka over Kinesis, Spark streaming bronze, MongoDB as a
  second source, PySpark instead of dbt, the serving layer).

### 10.1 CV bullets (to complete with measured values)

Only use numbers that were actually measured; interviewers will ask how
each one was obtained.

- Built a CDC pipeline from PostgreSQL and MongoDB (Debezium, Kafka
  Connect, Schema Registry, PySpark Structured Streaming, Iceberg on S3)
  that replaced full nightly reloads, with end-to-end freshness of \[X\]
  minutes.

- Modelled a star schema with SCD Type 2 user history in PySpark (MERGE
  on Iceberg), validated by \[N\] automated tests (pytest, chispa).

- Served real-time user features from Redis and co-purchase
  recommendations from Neo4j through a FastAPI service, with features
  \[X\] seconds behind the source.

- Implemented data contracts that caught \[N\] injected schema-breaking
  changes before they reached analytics.

- Implemented right-to-erasure across transactional and clickstream data
  in Iceberg, including snapshot expiry.

- Reduced Athena scan cost by \[X\]% through incremental MERGE models,
  partitioning and compaction.

- Validated fault tolerance with \[N\] scripted failure drills
  (connector crash, broker restart, offset replay), all reconciling with
  0 lost and 0 duplicated rows.

- Sustained \[X\] change events per second with consumer lag under \[Y\]
  seconds.

## 11. Budget

Estimates at demo volume, based on AWS prices in us-east-1 as of
September 2026. Real costs must be checked in AWS Cost Explorer.

| **Service**                           | **Usage**                                                          | **Estimated cost**                                               |
|---------------------------------------|--------------------------------------------------------------------|------------------------------------------------------------------|
| Kafka, Kafka Connect, Schema Registry | Self-hosted in Docker on the local machine                         | \$0                                                              |
| S3 requests from Spark streaming      | One commit per table per minute during work sessions (~8 tables x ~5 PUTs) | About \$0.01 per running hour                             |
| Data transfer out of S3               | Local Spark jobs read the lake over the internet                   | \$0 under the 100 GB/month free allowance; \$0.09/GB above      |
| S3 + Glue Catalog                     | A few GB stored                                                    | Under \$1                                                        |
| Athena                                | \$5 per TB scanned, with a per-query scan limit                    | Under \$1                                                        |
| Airflow, Spark, MongoDB, Redis, Neo4j | Self-hosted in Docker on the local machine                         | \$0                                                              |
| CloudWatch                            | A few alarms and metrics                                           | Near zero                                                        |
| Kinesis + Managed Flink (P10 only)    | 1 shard (\$0.015/hour) + 2 KPUs at \$0.11/hour, only while running | About \$0.24 per hour of use; must be stopped after each session |
| MSK (not used)                        | Smallest provisioned cluster, for reference                        | Under \$2.50/day; MSK Serverless about \$0.75/hour               |
| Development                           | On-prem stack in Docker; unit tests on in-memory DataFrames         | \$0                                                              |
| GitHub Actions                        | Public repository                                                  | \$0                                                              |

The generator's event rate drives volume and cost. Keep it low on AWS
and raise it only for load tests.

## 12. Risks and mitigations

| **Risk**                                                                      | **Mitigation**                                                                                           |
|-------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------|
| Unexpected AWS bill                                                           | Develop locally; low generator rate on AWS; budget alarm; lake kept deployed (idle cost near zero), `terraform destroy` as one-command teardown |
| Scope creep                                                                   | Finish P1 to P4 before adding anything else; each phase demo-able on its own                             |
| Generator folder carries a different licence from the repository (Apache 2.0) | Check before reuse; otherwise write an equivalent generator on the same schema (about one week)          |
| Generator schema changes or stops being maintained                            | Pin a specific version; data contracts detect any schema drift                                           |
| Kafka Connect configuration friction (converters, Avro, plugins)              | Validate topics, Avro schemas and offsets on the on-prem stack before the Spark job writes to AWS        |
| Spark cannot read registry-framed Avro natively                               | Decode per schema ID in `foreachBatch` with schemas fetched from Schema Registry; covered by unit tests  |
| Spark, Iceberg, Kafka and AWS jar versions incompatible                       | Check the compatibility matrix at the start of P3; bake pinned, checksummed jars into one Spark image    |
| Laptop resources (16 GB for WSL; Kafka, Connect, Spark, Airflow, MongoDB, Neo4j) | Compose profiles started as needed, single-broker KRaft, explicit JVM heaps and MongoDB cache size, Airflow with LocalExecutor; RAM measured per profile |
| Kafka Connect exactly-once has known caveats                                  | Keep LSN deduplication in silver; prove correctness with the replay drill                                |
| Synthetic data gives meaningless business insights                            | Present the project on its engineering results (freshness, correctness, cost), not on business findings; synthetic basket affinity makes recommendations testable, and is labelled as such |
| Debezium MongoDB connector may not support exactly-once source delivery       | Checked in P2: supported (Debezium 3.7 lists it; the connector runs with `exactly.once.support=required`); silver deduplication stays the safety net |
| Kafka retention (3 days) is shorter than the life of most rows, so the topics no longer hold a full snapshot of the PostgreSQL tables (found in P2) | Bronze cannot be built from Kafka alone: P3 starts with a Debezium incremental snapshot of the PostgreSQL tables (signal), so bronze begins complete; afterwards bronze, not Kafka, is the full history |
| Local Spark reading S3 incurs data-transfer charges                           | Demo volume stays far below the 100 GB/month free allowance; warn before any bulk backfill               |

## 13. Working with Claude Code

The project is built with the help of Claude Code. Its purpose is for
the author to **learn** data engineering. Claude Code may design and
write all of the code, but Souhail must understand everything it did and
why it did it. This section is written for Claude Code and sets the
rules it must follow. The same rules are saved in a CLAUDE.md file at
the root of the repository, which Claude Code reads automatically at the
start of every session.

### 13.1 Principle

**Claude Code may make the decisions and write the code. Souhail must be
able to explain every decision and every line.** Work that Souhail
cannot explain in an interview does not count as done, even if it works.

### 13.2 Rules for Claude Code

1.  **Explain every decision.** For each design choice (tool,
    configuration value, schema, data model, partitioning, naming,
    trade-off), state what was decided, which alternatives were
    considered, and why this option was chosen. Do this at the moment
    the decision is made, not at the end.

2.  **Ask only when the cahier des charges would change.** Decisions
    inside the specification can be taken directly and explained. A
    decision that changes the architecture, scope or objectives in this
    document is proposed with its options and reasoning, and waits for
    Souhail's approval.

3.  **Explain every change.** After each step, summarise what was built,
    how the pieces connect, and what each non-trivial configuration
    setting or piece of logic does, why it is needed, and what would
    break without it.

4.  **Check understanding, not just delivery.** After each important
    step, ask Souhail one or two short questions or ask him to explain
    the step back in his own words. Correct misunderstandings before
    moving on.

5.  **Go deeper on request.** Whenever Souhail asks "why", give the full
    reasoning, including the underlying concept, and point to the
    relevant documentation.

6.  **Record decisions as ADRs.** After each significant decision, write
    an architecture decision record in docs/adr/ (context, options,
    decision, consequences). Souhail reads and approves each ADR, and
    asks about anything unclear.

7.  **Explain failures.** When something breaks, show the symptom, how
    the cause was found, and why the fix works, so that Souhail learns
    the debugging method and not only the fix.

8.  **Checkpoint at the end of each phase.** Ask three to five
    interview-style questions about the phase just finished (section
    13.4). Gaps are closed before the next phase starts.

9.  **Keep a learning log.** Maintain docs/learning-log.md with the
    concepts covered, decisions taken and open questions from each
    session.

10. **One step at a time.** Work in small increments and small commits
    with clear messages. Souhail must be able to explain every commit in
    an interview.

### 13.3 Who does what

| **Type of work**                                 | **Claude Code**                         | **Souhail**                              |
|--------------------------------------------------|-----------------------------------------|------------------------------------------|
| Architecture within the specification            | Decides and explains the reasoning      | Understands and can challenge any choice |
| Changes to the cahier des charges                | Proposes options with trade-offs        | Approves or rejects                      |
| Code, configuration, Spark jobs, DAGs, Terraform | Writes and explains                     | Reads, asks questions, explains it back  |
| ADRs and learning log                            | Writes                                  | Reviews and approves                     |
| Debugging                                        | Diagnoses and fixes, showing the method | Follows the reasoning                    |
| Phase checkpoints                                | Asks the questions                      | Answers without looking at the code      |

### 13.4 Phase checkpoints

Examples of questions Claude Code asks at the end of each phase. Souhail
answers without looking at the code.

| **Phase** | **Example questions**                                                                                                                                      |
|-----------|------------------------------------------------------------------------------------------------------------------------------------------------------------|
| P1        | Why must topics be keyed by primary key? What happens to Postgres if Debezium stops for a day? What does a heartbeat actually write, and why?              |
| P2        | Why does Debezium need a MongoDB replica set? How does the oplog's failure mode differ from a replication slot's? Why one document per event rather than one per session? |
| P3        | Why is bronze append-only? How does the streaming job avoid appending a replayed batch twice? Why can Spark not read Debezium's Avro directly? What does the trigger interval trade off? |
| P4        | How does the MERGE handle a row updated twice in the same batch? How are deletes applied? Why SCD2 from the log rather than periodic snapshots?            |
| P5        | Why sorted sets rather than lists in Redis? What does each TTL protect? What happens to Redis when a micro-batch is replayed?                                |
| P6        | How is the edge weight computed, and how do you make the rebuild idempotent? Why does the graph hold no personal data?                                      |
| P7        | What is the difference between a Schema Registry compatibility rule and a data contract? Which change is blocked where?                                    |
| P8        | Where can a deleted user's data still exist after the DELETE statements, and how is each place cleaned? How do you prove a backfill created no duplicates? |
| P9        | Where did the scan-cost savings come from? Where is the throughput limit, and which component hits it first?                                               |

### 13.5 CLAUDE.md

The rules above are saved as CLAUDE.md at the root of the repository.
Keep it short and update it when the working agreement changes.

## 14. Sources consulted

Online research carried out for version 1.4 (September 2026).

- Gunnar Morling, Mastering Postgres Replication Slots:
  morling.dev/blog/mastering-postgres-replication-slots

- BigData Boutique, Debezium in Production: PostgreSQL to Kafka to
  Iceberg: bigdataboutique.com/blog/debezium-production-cdc-patterns

- DBA Globe, Preventing PostgreSQL Replication Lag with Debezium
  Heartbeat (March 2026): dbaglobe.com

- Debezium documentation, Exactly once delivery:
  debezium.io/documentation/reference/stable/configuration/eos.html

- Apache Iceberg documentation, Kafka Connect:
  iceberg.apache.org/docs/latest/kafka-connect

- dbt documentation, Amazon Athena configurations and Unit tests:
  docs.getdbt.com

- Amazon Athena documentation, Optimize Iceberg tables:
  docs.aws.amazon.com/athena/latest/ug/querying-iceberg-data-optimization.html

- AWS Lake Formation documentation, Data filtering limitations:
  docs.aws.amazon.com/lake-formation/latest/dg/data-filtering-notes.html

- Data Contract CLI documentation (ODCS, Kafka testing):
  docs.datacontract.com

- Apache Kafka 4.2.0 release announcement and Factor House guide to
  Kafka 4.3.0

- Apache Airflow 3.3.0 release announcement:
  airflow.apache.org/blog/airflow-3.3.0

- Factor House, theLook eCommerce CDC example (Apache License 2.0):
  github.com/factorhouse/examples

Added for version 2.0 (October 2026):

- Debezium documentation, MongoDB connector (replica sets, change
  streams, capture modes)

- Apache Spark documentation, Structured Streaming + Kafka integration
  guide and Avro data source

- Apache Iceberg documentation, Spark (structured streaming writes,
  MERGE INTO) and AWS (GlueCatalog, S3FileIO)

- MongoDB documentation, replica set oplog and change streams

- Redis documentation, sorted sets and key expiration

- Neo4j documentation, Cypher MERGE and UNWIND for batched writes

- chispa: github.com/MrPowers/chispa
