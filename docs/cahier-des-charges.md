*Cahier des charges*

# theLook CDC Lakehouse on AWS

Real-time change data capture from an operational database to a
governed, cost-controlled analytics lakehouse

| **Item**     | **Value**                                     |
|--------------|-----------------------------------------------|
| Author       | Souhail Bourhim                               |
| Programme    | INE3, Smart-ICT, INPT Rabat                   |
| Project type | Personal portfolio project (data engineering) |
| Version      | 1.9                                           |
| Date         | 29 September 2026                             |
| Status       | Specification, not yet started                |

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
| O5     | Data-deletion request handled                | User erased from every layer, including clickstream, in \< 24 hours, with an audit trail                                           |
| O6     | Monthly AWS cost                             | \< \$15 at demo volume                                                                                                             |
| O7     | Reproducibility                              | Whole stack deployed or destroyed with one command                                                                                 |
| O8     | Throughput                                   | Sustain 200 change events/s (set from the P1 baseline run) with consumer lag \< 30 seconds; find and document the breaking point (in P2) |
| O9     | Resilience                                   | Every failure drill in FR14 ends with 0 lost and 0 duplicated rows after reconciliation                                            |

## 3. Scope

### 3.1 In scope

- CDC ingestion of the 6 source tables (section 4.1), including an
  initial snapshot, through Apache Kafka.

- A lakehouse organised in three layers: bronze (raw change log), silver
  (current state), gold (star schema).

- A user history dimension (SCD Type 2).

- Data contracts, quality checks and alerting.

- Data-deletion (GDPR) workflow and PII governance.

- Backfills and reprocessing.

- Monitoring, infrastructure as code (Terraform) and CI/CD.

- Optional extension: real-time clickstream processing on Kinesis and
  Managed Flink (phase P7).

### 3.2 Out of scope

- Machine learning models (covered by a separate project).

- A BI tool beyond basic dashboards.

- Multi-region deployment and disaster recovery.

- A real production data source.

## 4. Data sources

### 4.1 theLook eCommerce schema

The source database contains the following tables. Column lists must be
checked against the generator's actual schema at the start of P1, then
frozen in the data contracts.

| **Table**            | **Content**                                                                                                                        | **Change pattern**                 |
|----------------------|------------------------------------------------------------------------------------------------------------------------------------|------------------------------------|
| users                | Name, email, age, gender, street address, city, state, country, postal code, latitude/longitude, traffic source                    | Inserts (sign-ups); rare updates   |
| products             | Name, brand, category, department, cost, retail price, distribution center                                                         | Mostly static                      |
| distribution_centers | Name, latitude/longitude                                                                                                           | Static                             |
| orders               | User, status (Processing, Shipped, Complete, Cancelled, Returned), created/shipped/delivered/returned timestamps                   | Frequent updates as status changes |
| order_items          | Order, product, item status, sale price, lifecycle timestamps                                                                      | Frequent updates                   |
| events               | Session, sequence number, event type (home, department, product, cart, purchase, cancel), URI, browser, IP address, traffic source | Append-only, high volume           |

This mix is deliberate: static reference data, frequently updated
transactional data and a high-volume append-only log each need a
different handling strategy (see section 8).

### 4.2 Live data generator

Factor House publishes an open-source Python generator that writes live
theLook traffic into PostgreSQL: new sign-ups, browsing sessions,
purchases, cancellations and returns. It produces genuine INSERT and
UPDATE statements (order status changes, and address changes when
enabled). It never deletes rows: every DELETE in the source comes from the
synthetic erasure scripts described in 4.3.

- Only the generator is reused. The surrounding Factor House demo uses
  their commercial monitoring tools (Kpow, Flex), which are not part of
  this project.

- The Factor House examples repository is published under the **Apache
  License 2.0**, which allows reuse with attribution. Check that the
  generator folder does not carry a different licence, and keep the
  notice in the repository.

- The generator runs as a container next to PostgreSQL, with a
  configurable event rate.

### 4.3 Synthetic additions

Some behaviours may not be produced by the generator. They are added by
small scripts and clearly labelled as synthetic in the documentation.

- User address changes, to exercise the SCD2 dimension (only if the
  generator does not already update addresses).

- Data-deletion requests. They are the only source of DELETE statements,
  so they also produce the Debezium delete events and tombstones that the
  silver MERGE must handle.

- Injected schema changes and malformed records, to exercise the data
  contracts.

## 5. Target architecture

### 5.1 Data flow

```
--- On-prem side (Docker) ---
theLook generator --> Postgres
   --WAL--> Kafka Connect [Debezium source] --> Kafka (KRaft) + Schema Registry (Avro)
   --> Kafka Connect [Apache Iceberg sink]
--- AWS side ---
   --> Iceberg bronze tables (S3 + Glue Catalog, one table per source table)
   --> dbt on Athena --> silver (current state) --> gold (star schema)
   --> dashboards (QuickSight or Grafana)

Orchestration : Apache Airflow (self-hosted, Docker, on-prem side)
Monitoring    : CloudWatch metrics and alarms
IaC / CI      : Terraform, GitHub Actions (OIDC to AWS)
Development   : on-prem stack in Docker; dbt developed directly on Athena in a dev schema (cost: cents)
```

The source database and Kafka run locally to represent a company that
keeps its operational systems and event platform in its own data centre
and lands data in a cloud lakehouse. This hybrid set-up is a common
real-world pattern, and it avoids paying for a managed database or a
managed Kafka cluster.

There is deliberately **no local copy of the lake**. Maintaining two
lakehouses (for example MinIO with DuckDB locally and Glue with Athena
on AWS) would mean two dbt adapters, SQL dialect differences and bugs
that only appear on one side. dbt is developed directly on Athena in a
separate dev schema, which costs cents at this volume.

### 5.2 Technology choices

| **Component**    | **Choice**                        | **Justification**                                                                                                  | **Rejected alternative**                                                                                |
|------------------|-----------------------------------|--------------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------|
| Event platform   | Apache Kafka (KRaft), self-hosted | Industry-standard event platform, widely requested by employers; \$0 when run locally                              | MSK Serverless: about \$0.75/hour (~\$540/month); Kinesis: AWS-only, less common in industry            |
| CDC              | Debezium on Kafka Connect         | The standard production deployment of Debezium; offsets and restarts managed by Connect                            | Debezium Server: simpler, but less representative of production                                         |
| Schemas          | Schema Registry + Avro            | Schemas enforced when data is written; compatibility rules block breaking changes                                  | Schemaless JSON: breakage only detected downstream                                                      |
| Delivery to lake | Apache Iceberg sink connector     | Official Iceberg connector; exactly-once delivery; routes records to tables; writes to S3 through the Glue catalog | Firehose: requires Kinesis; custom consumer: more code to maintain                                      |
| Storage          | Apache Iceberg on S3              | ACID tables, row-level deletes (needed for GDPR), time travel                                                      | Plain Parquet: no deletes or ACID guarantees                                                            |
| Query            | Athena                            | Serverless, pay per query                                                                                          | Redshift: fixed monthly cost                                                                            |
| Transformation   | dbt (dbt-athena)                  | Tests, documentation and lineage built in; industry standard                                                       | Hand-written SQL scripts                                                                                |
| Orchestration    | Apache Airflow, self-hosted       | Industry standard; native backfills over date ranges; \$0 when run locally next to Kafka                           | MWAA (managed Airflow): hundreds of dollars/month idle; Step Functions: AWS-only, weak backfill support |

### 5.3 Version pinning

Versions current at the time of writing. All images and packages are
pinned in the repository; upgrades are deliberate and documented.

| **Component**       | **Version line**                   | **Notes**                                                          |
|---------------------|------------------------------------|--------------------------------------------------------------------|
| Apache Kafka        | 4.x (4.3 released May 2026)        | KRaft only since 4.0 (no ZooKeeper); single broker locally         |
| Debezium            | 3.x (documentation at 3.6)         | PostgreSQL connector with pgoutput                                 |
| PostgreSQL          | 16 or 17                           | max_slot_wal_keep_size available since 13; failover slots since 17 |
| Apache Airflow      | 3.x (3.3 released July 2026)       | Asset-aware scheduling; OpenLineage provider                       |
| Apache Iceberg sink | Pinned Iceberg release             | Built from the Iceberg repository in a Dockerfile                  |
| dbt                 | dbt-core 1.8 or later + dbt-athena | Unit tests need 1.8+; MERGE needs Athena engine version 3          |

## 6. Functional requirements

### FR1: Capture

Capture every insert, update and delete on the 6 source tables, starting
with an initial snapshot. Changes are published to one Kafka topic per
table, keyed by primary key so that all changes to a row stay in order
within one partition. Each event is serialised in Avro and carries its
operation type, log position (LSN), commit timestamp and source table
name.

Debezium configuration requirements:

- **pgoutput** logical decoding plugin, with a publication limited to
  the captured tables (publication.autocreate.mode = filtered), so the
  WAL decoded for other tables is not streamed.

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

### FR2: Bronze layer

Store an append-only, immutable change log per source table. Nothing in
bronze is ever updated in place. The Iceberg sink runs in append mode
only; its upsert/CDC mode is deliberately not used, because
deduplication belongs in silver. This matches common production
practice: the Iceberg Kafka Connect sink is used for append-only bronze,
and upserts happen in a later layer.

### FR3: Silver layer

Build the current state of each entity from the change log:

- keep only the latest version of each row, ordered by LSN;

- remove rows that were deleted at the source;

- be idempotent, so that a rerun always gives the same result;

- be **incremental**: dbt incremental models with the Iceberg MERGE
  strategy (dbt-athena, Athena engine version 3). Each run only reads
  bronze rows newer than the last processed commit, uses the primary key
  as unique_key, and applies source deletes through delete_condition.
  incremental_predicates limit the rows scanned in the target table.

The events table is append-only, so its silver version only needs
deduplication, not current-state logic.

### FR4: User history (SCD2)

Build dim_user with one row per version of each user, with valid_from
and valid_to taken from source commit timestamps. Each order joins to
the user's address as it was at purchase time, not the current one.

dbt snapshots are supported on Athena but are deliberately not used:
they only record the state seen at each run, whereas the change log
contains every intermediate version.

### FR5: Gold star schema

| **Type**   | **Tables**                                                      |
|------------|-----------------------------------------------------------------|
| Facts      | fct_orders, fct_order_items, fct_sessions (built from events)   |
| Dimensions | dim_user (SCD2), dim_product, dim_distribution_center, dim_date |

### FR6: Metrics layer

One agreed definition for each business metric, documented and tested:

- gross revenue and net revenue (after cancellations and returns);

- gross margin, using product cost versus sale price;

- average order value;

- cancellation rate and return rate;

- time to ship and time to deliver;

- session-to-purchase conversion rate, from the events table.

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
  must look like. The Iceberg sink creates the bronze tables and evolves
  them (new nullable columns); the contracts validate the resulting tables
  and topics, so a drift between data and contract fails a check instead
  of being hidden by a Terraform-managed schema.

- The contracts also protect the pipeline against schema changes in the
  generator itself.

### FR8: Data quality

- dbt tests: unique, not null, relationships, accepted values (for
  example, order status).

- Freshness checks on every source.

- Row-count reconciliation between the source database and the silver
  layer.

- **dbt unit tests** (dbt 1.8+) on fixed input rows for the hardest
  logic: LSN deduplication, delete handling, SCD2 validity ranges, and
  net revenue after returns. Incremental models are tested in both modes
  using the is_incremental override.

### FR9: Data-deletion (GDPR) workflow

- A deletion request removes the user from every layer: users, orders,
  and the clickstream events linked to them (including IP addresses).

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
| transform      | Every 30 minutes                | dbt build on Athena (silver, gold, tests); fail and alert if a test fails                                                      |
| maintenance    | Daily                           | OPTIMIZE ... REWRITE DATA USING BIN_PACK on recent partitions; VACUUM to expire snapshots and remove orphan files              |
| gdpr_erasure   | Triggered by a deletion request | Delete the user in every layer, expire snapshots, verify, write the audit record                                               |
| quality_report | Daily                           | Freshness checks, source-to-silver reconciliation, datacontract-cli tests, connector, consumer-lag and replication-slot health |

The OpenLineage provider for Airflow can send lineage events to Marquez,
giving a free lineage graph of every DAG run (optional, P6).

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

### FR14: Resilience drills

Each drill is scripted, repeatable and documented with its measured
result. They are what turns the correctness claims into evidence.

| **Drill**                                                 | **Expected result**                                                                          |
|-----------------------------------------------------------|----------------------------------------------------------------------------------------------|
| Kill the Kafka Connect worker mid-stream, then restart it | Processing resumes from committed offsets; reconciliation shows 0 lost and 0 duplicated rows |
| Restart the Kafka broker under load                       | Producers and connectors recover automatically; lag returns to normal                        |
| Stop Debezium for one hour while the generator runs       | Retained WAL grows and the alert fires; after restart the slot catches up with no data loss  |
| Reset connector offsets to force a replay                 | Bronze receives duplicates; silver output is unchanged                                       |
| Add a nullable column in PostgreSQL                       | The change flows through Avro, the Iceberg table schema and dbt without breaking anything    |
| Attempt an incompatible type change                       | Schema Registry rejects it; alert raised; nothing reaches the lake                           |

## 7. Non-functional requirements

| **Area**            | **Requirement**                                                                                                                                                                                                                                                            |
|---------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Delivery semantics  | Debezium source is at-least-once; the Iceberg sink is exactly-once; silver deduplicates on LSN, which gives exactly-once results end to end                                                                                                                                |
| Kafka configuration | Topic retention long enough to replay a full day; replication factor documented (1 locally, 3 in a production design); consumer lag monitored                                                                                                                              |
| Schema evolution    | A new nullable column is added without breaking the pipeline; an incompatible type change is blocked by the contract                                                                                                                                                       |
| Performance         | Bronze tables partitioned by day; scheduled compaction (the events table matters most); Athena bytes scanned measured before and after optimisation                                                                                                                        |
| Security            | The on-prem Kafka Connect worker uses a dedicated IAM user restricted to the lake bucket and Glue databases (never admin keys); least-privilege IAM for every other component; S3 encrypted with KMS; no secrets in the repository; GitHub Actions authenticates with OIDC |
| Cost                | AWS Budget alarm; scan limit per Athena query (workgroup); one-command teardown                                                                                                                                                                                            |
| Observability       | Kafka consumer lag and connector status monitored (for example Prometheus and Grafana on-prem); CloudWatch alarms on data freshness, dead-letter volume and Athena usage                                                                                                   |
| Documentation       | README with architecture, design decisions, cost breakdown and an operations runbook                                                                                                                                                                                       |

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

6.  **Small files:** every Iceberg sink commit creates new files, and
    the events table generates many of them. The commit interval
    (default 5 minutes) trades freshness against file count, and
    compaction is still needed.

7.  **WAL retention:** the source database silently keeps every change
    the connector has not confirmed, so a stopped pipeline becomes a
    disk problem on the production database.

8.  **Exactly-once is layered, not absolute:** exactly-once source and
    sink settings reduce duplicates, but correctness still depends on
    idempotent processing in silver.

9.  **Erasure in a versioned lake:** deleting a user means finding them
    across tables (including clickstream IPs) and expiring older Iceberg
    snapshots that still contain them.

## 9. Milestones

Estimated duration: 6 to 8 weeks part-time for P1 to P6. Each phase ends
with a working, demonstrable result.

| **Phase**     | **Content**                                                                                                                                                                                                                                                                                      | **Acceptance criterion**                                                                                                                 |
|---------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------|
| P1            | On-prem stack: Postgres, theLook generator, Kafka, Schema Registry, Kafka Connect with Debezium; heartbeats and slot safeguards; baseline throughput run                                                                                                                                         | Avro change events from all 6 tables visible in their Kafka topics; slot alert tested                                                    |
| P2            | Bronze on AWS: Terraform (S3, Glue, IAM, Athena), Iceberg sink connector (built from the Iceberg repository), commit interval lowered to about 1 minute                                                                                                                                          | A changed row is queryable in Athena in \< 5 minutes                                                                                     |
| P3            | dbt incremental silver (MERGE) and gold layers, SCD2, unit tests; Airflow DAG running dbt on a schedule                                                                                                                                                                                          | Row counts reconcile with the source; all dbt tests and unit tests pass; scheduled runs keep gold \< 1 hour old                          |
| P4            | ODCS contracts with datacontract-cli, quality checks, alerting, schema evolution drills                                                                                                                                                                                                          | An injected schema break is caught and an alert is raised; a compatible column addition flows end to end                                 |
| P5            | GDPR erasure and backfill, both as Airflow DAGs; failure drills (FR14)                                                                                                                                                                                                                           | Erasure verified in every layer, including clickstream and time travel; backfill leaves no duplicates; every drill passes reconciliation |
| P6            | Optimisation, load test, CI/CD, ADRs, documentation; optional OpenLineage lineage                                                                                                                                                                                                                | Scan cost reduction and throughput limit measured; README, ADRs and runbook complete                                                     |
| P7 (optional) | Real-time clickstream: the generator emits events directly to a Kinesis stream (they stop being written to PostgreSQL, so they are not ingested twice) and Managed Flink processes them (sessions, live funnel, late events). Adds AWS-managed streaming practice for the AWS Data Engineer exam | Live conversion funnel updated in \< 1 minute; late events handled with watermarks                                                       |

**Rule:** finish P1 to P3 before starting anything else. They already
form a complete, presentable project. P7 only makes sense once P6 is
done.

## 10. Deliverables

- A public GitHub repository with the code, Terraform, dbt project, data
  contracts and CI pipeline.

- A README with the architecture diagram, design decisions and cost
  breakdown.

- A demo video under 3 minutes: the generator creates and returns an
  order, and the change reaches the dashboard.

- A results page with the real measurements: freshness, throughput
  limit, scan savings, number of tests, drill results, cost per month.

- Architecture decision records (ADRs), one page each, for example: 001
  Kafka over Kinesis; 002 Airflow over Step Functions; 003 append-only
  bronze with MERGE silver; 004 no local lake; 005 SCD2 from the change
  log instead of dbt snapshots; 006 contracts in ODCS.

### 10.1 CV bullets (to complete with measured values)

Only use numbers that were actually measured; interviewers will ask how
each one was obtained.

- Built a CDC pipeline (Postgres, Debezium, Kafka Connect, Schema
  Registry, Iceberg on S3) that replaced full nightly reloads, with
  end-to-end freshness of \[X\] minutes.

- Modelled a star schema with SCD Type 2 user history in dbt, validated
  by \[N\] automated tests.

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
| S3 requests from the Iceberg sink     | One commit per minute during work sessions                         | Cents                                                            |
| S3 + Glue Catalog                     | A few GB stored                                                    | Under \$1                                                        |
| Athena                                | \$5 per TB scanned, with a per-query scan limit                    | Under \$1                                                        |
| Apache Airflow                        | Self-hosted in Docker on the local machine                         | \$0                                                              |
| CloudWatch                            | A few alarms and metrics                                           | Near zero                                                        |
| Kinesis + Managed Flink (P7 only)     | 1 shard (\$0.015/hour) + 2 KPUs at \$0.11/hour, only while running | About \$0.24 per hour of use; must be stopped after each session |
| MSK (not used)                        | Smallest provisioned cluster, for reference                        | Under \$2.50/day; MSK Serverless about \$0.75/hour               |
| Development                           | On-prem stack in Docker; dbt runs on Athena in a dev schema        | Cents                                                            |

The generator's event rate drives volume and cost. Keep it low on AWS
and raise it only for load tests.

## 12. Risks and mitigations

| **Risk**                                                                      | **Mitigation**                                                                                           |
|-------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------|
| Unexpected AWS bill                                                           | Develop locally; low generator rate on AWS; budget alarm; lake kept deployed (idle cost near zero), `terraform destroy` as one-command teardown |
| Scope creep                                                                   | Finish P1 to P3 before adding anything else                                                              |
| Generator folder carries a different licence from the repository (Apache 2.0) | Check before reuse; otherwise write an equivalent generator on the same schema (about one week)          |
| Generator schema changes or stops being maintained                            | Pin a specific version; data contracts detect any schema drift                                           |
| Kafka Connect configuration friction (converters, Avro, plugins)              | Validate topics, Avro schemas and offsets on the on-prem stack before connecting the Iceberg sink to AWS |
| Iceberg sink must be built from source                                        | Build it once in a Dockerfile and pin the Iceberg version                                                |
| Laptop resources (Kafka, Connect, Registry, Postgres, Airflow at once)        | Single-broker KRaft, small JVM heaps, Airflow with LocalExecutor, stop services not in use               |
| dbt unit tests not supported by the Athena adapter                            | Run unit tests against a DuckDB target, since they only use fixed input rows and no lake data            |
| Kafka Connect exactly-once has known caveats                                  | Keep LSN deduplication in silver; prove correctness with the replay drill                                |
| Synthetic data gives meaningless business insights                            | Present the project on its engineering results (freshness, correctness, cost), not on business findings  |

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
| Code, configuration, dbt models, DAGs, Terraform | Writes and explains                     | Reads, asks questions, explains it back  |
| ADRs and learning log                            | Writes                                  | Reviews and approves                     |
| Debugging                                        | Diagnoses and fixes, showing the method | Follows the reasoning                    |
| Phase checkpoints                                | Asks the questions                      | Answers without looking at the code      |

### 13.4 Phase checkpoints

Examples of questions Claude Code asks at the end of each phase. Souhail
answers without looking at the code.

| **Phase** | **Example questions**                                                                                                                                      |
|-----------|------------------------------------------------------------------------------------------------------------------------------------------------------------|
| P1        | Why must topics be keyed by primary key? What happens to Postgres if Debezium stops for a day? What does a heartbeat actually write, and why?              |
| P2        | Why is bronze append-only? What does the commit interval trade off? Which IAM permissions does the Connect worker need, and why no more?                   |
| P3        | How does the MERGE model handle a row updated twice in the same batch? How are deletes applied? Why SCD2 from the log rather than dbt snapshots?           |
| P4        | What is the difference between a Schema Registry compatibility rule and a data contract? Which change is blocked where?                                    |
| P5        | Where can a deleted user's data still exist after the DELETE statements, and how is each place cleaned? How do you prove a backfill created no duplicates? |
| P6        | Where did the scan-cost savings come from? Where is the throughput limit, and which component hits it first?                                               |

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
