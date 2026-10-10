# Architecture in diagrams

Ten diagrams, from the whole system down to single mechanisms. Each one is
drawn from the code and the ADRs, with the measured figures from
[results](results.md); the reasoning behind each choice is in the
[ADRs](adr/). GitHub renders the Mermaid blocks; the text under each one
says how to read it and what to remember.

1. [The whole system](#1-the-whole-system)
2. [Where everything runs](#2-where-everything-runs)
3. [One change, from commit to Athena](#3-one-change-from-commit-to-athena)
4. [Exactly-once into bronze](#4-exactly-once-into-bronze)
5. [Medallion layers, table by table](#5-medallion-layers-table-by-table)
6. [The gold star schema](#6-the-gold-star-schema)
7. [Orchestration](#7-orchestration)
8. [Online features (Redis)](#8-online-features-redis)
9. [Co-purchase graph (Neo4j)](#9-co-purchase-graph-neo4j)
10. [Roadmap](#10-roadmap)

---

## 1. The whole system

```mermaid
flowchart LR
  subgraph SRC["Sources (on-prem)"]
    gen["theLook generator"]
    rev["review simulator"]
    pg[("PostgreSQL 17<br/>shop.*")]
    mongo[("MongoDB 8<br/>web.events, web.reviews")]
    gen --> pg
    gen --> mongo
    rev --> mongo
  end

  subgraph EVT["Event platform (on-prem)"]
    connect["Kafka Connect<br/>Debezium sources"]
    kafka[("Kafka 4, KRaft<br/>7 CDC topics, 3-day retention")]
    sr["Schema Registry<br/>Avro"]
  end
  pg -- "WAL via replication slot" --> connect
  mongo -- "change streams" --> connect
  connect -- "Avro events" --> kafka
  connect -.- sr

  subgraph LAKE["Lakehouse (AWS: S3 + Glue, Iceberg tables)"]
    bronze[("bronze<br/>every change")]
    silver[("silver<br/>current state")]
    gold[("gold<br/>star schema, marts")]
    bronze -- "Spark batch<br/>every 30 min" --> silver
    silver -- "Spark batch<br/>every 30 min" --> gold
  end
  athena["Athena SQL"]
  kafka -- "Spark stream<br/>every 60 s" --> bronze
  gold --> athena

  subgraph SERVE["Serving (on-prem)"]
    redis[("Redis 8<br/>user features")]
    neo[("Neo4j 2026.08<br/>co-purchase graph")]
    api["FastAPI"]
    redis --> api
    neo --> api
  end
  kafka -- "Spark stream, web.events<br/>every 10 s" --> redis
  gold -- "Spark batch<br/>daily" --> neo

  airflow["Airflow 3<br/>schedules the batch jobs"] -.-> LAKE
  prom["Prometheus + Alertmanager"] -. "slot, connector, lag alerts" .-> EVT
```

**How to read it.** Left to right is the path of the data; each arrow into a
store names the Spark job that writes it and how often. Two operational
databases change all the time; Debezium reads their change logs (never the
tables themselves, objective O3) and publishes every change to Kafka. From
Kafka, two independent Spark streams fan out: one lands every change in the
lake (bronze), the other keeps per-user features in Redis. Batch jobs,
scheduled by Airflow, turn bronze into silver and gold, and a daily job
turns gold into a graph.

**What to remember.**

- **Hybrid on purpose**: sources, Kafka and the engines run locally (a
  company's own data centre); AWS holds only storage, catalog and SQL
  (S3, Glue, Athena), which keeps the bill under O6's \$15 a month.
- **One lake, no local copy**: Spark reads and writes Iceberg on S3 through
  Glue, so Athena sees each commit at once (spec 5.1).
- **Kafka decouples everything**: a slow lake never slows the features, and
  any consumer can be rebuilt by replaying the topic (3 days kept).

---

## 2. Where everything runs

```mermaid
flowchart LR
  subgraph CORE["profile core"]
    generator["generator"]
    reviewsim["review-simulator"]
    postgres[("postgres")]
    mongo[("mongo")]
    connect["connect"]
    registry["schema-registry"]
    kafka[("kafka")]
    generator --> postgres
    generator --> mongo
    reviewsim --> mongo
    postgres --> connect
    mongo --> connect
    connect --> kafka
    connect -.- registry
  end
  subgraph MON["profile monitoring"]
    exporter["postgres-exporter"]
    prometheus["prometheus"]
    alertmanager["alertmanager"]
    prometheus --> exporter
    prometheus --> alertmanager
  end
  subgraph STREAM["profile stream"]
    bronzedrv["bronze-stream<br/>driver"]
    master["spark-master"]
    worker["spark-worker<br/>executors, 3 GB cap"]
    bronzedrv --> master --> worker
  end
  subgraph AIRFLOW["profile airflow"]
    scheduler["airflow-scheduler<br/>hosts the batch drivers"]
    apiserver["airflow-apiserver<br/>UI :8088"]
    dagproc["airflow-dag-processor"]
    afdb[("airflow-db")]
    apiserver --> afdb
    dagproc --> afdb
    scheduler --> afdb
  end
  subgraph SERVING["profile serving"]
    features["features-stream<br/>Spark local mode"]
    redis[("redis")]
    neo4j[("neo4j")]
    api["api :8000"]
    features --> redis
    redis --> api
    neo4j --> api
  end
  subgraph AWS["AWS us-east-1 (Terraform)"]
    s3[("S3 lake bucket")]
    glue["Glue catalog"]
    athena["Athena workgroup"]
    guard["Budgets, Config tag rule"]
    athena --> s3
    athena --> glue
  end

  exporter --> postgres
  prometheus -- "JMX metrics" --> connect
  kafka --> bronzedrv
  kafka --> features
  scheduler -- "spark-submit" --> master
  scheduler -- "graph load, Bolt" --> neo4j
  worker -- "IAM spark_stream or spark_batch" --> s3
  worker --> glue
  analyst["analyst, DBeaver"] -- "IAM analyst" --> athena
```

**How to read it.** Each box is a Compose service on the laptop, grouped by
the profile that starts it (`make up PROFILES="core serving"`); the AWS box
is the only part outside the machine, and the arrows into it are the only
traffic that leaves it.

**What to remember.**

- **Profiles = cost control**: `core` + `serving` demo P5 and P6 with no AWS
  traffic at all; `stream` and `airflow` read and write S3 (~0.8 GB an
  hour), so they run only while working.
- **Least privilege, three identities**: the bronze writer, the batch jobs
  and the analyst each have their own IAM user and policy; no AWS key sits
  in Airflow (each job loads its identity from the scheduler's environment).
- **Drivers vs executors**: the driver plans the job (in `bronze-stream` or
  the Airflow scheduler); executors on `spark-worker` do the reading and
  writing. The graph load is the exception: one writer, from the driver
  (ADR 019).

---

## 3. One change, from commit to Athena

```mermaid
sequenceDiagram
  autonumber
  participant G as generator
  participant PG as PostgreSQL
  participant D as Debezium (Connect)
  participant K as Kafka + Registry
  participant B as bronze stream
  participant L as Iceberg (S3 + Glue)
  participant T as transform DAG
  participant A as Athena

  G->>PG: INSERT order, COMMIT
  PG->>D: decoded WAL record (pgoutput, slot)
  D->>K: Avro change event (schema id in each message)
  D-->>PG: confirm flushed LSN, slot advances
  Note over PG,K: capture lag under 1 s (P1 baseline)
  B->>K: read new offsets (micro-batch every 60 s)
  B->>L: append to bronze, one commit per table
  B->>L: ledger row: every table's snapshot id
  Note over G,L: O1 commit to queryable bronze: median 75 s, max 88 s (target 5 min)
  T->>L: silver: read bronze at the ledger's cut, MERGE latest per key
  T->>L: gold: rebuild dimensions, MERGE facts and marts
  A->>L: SQL on gold
  Note over G,A: O2 commit to gold: 10 to 36 min (target 1 hour)
```

**How to read it.** Time runs downwards. Steps 1-4 are capture, 5-7 the
streaming ingestion, 8-10 the batch refinement.

**What to remember.**

- **Step 4 is why replication slots are dangerous**: PostgreSQL keeps WAL
  until Debezium confirms it. A stopped connector makes WAL pile up, hence
  `max_slot_wal_keep_size` and the slot alerts (P1 drill).
- **Step 7 makes silver consistent**: the ledger row is written only after
  every table of the batch has committed, so silver reads all tables as of
  the same batch (an order item is never merged without its order).
- **Freshness is bounded by the schedules**: ~60 s trigger + batch time for
  bronze; up to one 30-minute interval + one run for gold.

---

## 4. Exactly-once into bronze

```mermaid
flowchart TD
  start(["micro-batch N<br/>offsets from the checkpoint"]) --> topic{"for each topic:<br/>does the table already have a snapshot<br/>stamped with this query id and batch at least N?"}
  topic -- "yes: a replay" --> skip["skip the table<br/>logged as skipped(replay)"]
  topic -- "no" --> append["append the rows, snapshot<br/>stamped with query id + batch N"]
  skip --> more{"more topics?"}
  append --> more
  more -- "yes" --> topic
  more -- "no" --> ledger{"ledger already<br/>holds batch N?"}
  ledger -- "no" --> row["write the ledger row:<br/>each table's snapshot id"]
  ledger -- "yes" --> done
  row --> done(["Spark commits the offsets<br/>to the checkpoint"])
  crash["crash anywhere above"] -. "restart replays batch N<br/>from the checkpoint" .-> start
```

**How to read it.** Spark's checkpoint guarantees that a batch is processed
*at least* once: after a crash, batch N runs again with the same offsets.
The stamps turn that into *exactly once*: each Iceberg commit carries the
query id and batch id in its snapshot summary, so a replayed batch finds
its own earlier commits and skips them, table by table.

**What to remember.**

- **Idempotence lives in the sink**, not in Kafka: the commit and its stamp
  are one atomic Iceberg snapshot, so "written" and "marked written" can
  never disagree.
- A **new checkpoint means a new query id**: the guard no longer matches,
  bronze gets duplicates, and silver removes them by log position (ADR 012).
- Measured in P3: a killed and restarted stream appended no duplicates.

---

## 5. Medallion layers, table by table

```mermaid
flowchart LR
  subgraph BR["bronze: every change, append-only"]
    b_users["shop_users"]
    b_orders["shop_orders"]
    b_items["shop_order_items"]
    b_products["shop_products"]
    b_dc["shop_dist_centers"]
    b_events["web_events"]
    b_reviews["web_reviews"]
    b_ledger["stream_batches<br/>(ledger)"]
  end
  subgraph SV["silver: current state per key"]
    s_users["users"]
    s_orders["orders"]
    s_items["order_items"]
    s_products["products"]
    s_dc["dist_centers"]
    s_events["events"]
    s_reviews["reviews"]
  end
  subgraph GD["gold: star schema and marts"]
    d_user["dim_user (SCD2)"]
    d_product["dim_product"]
    d_dc["dim_distribution_center"]
    d_date["dim_date"]
    f_items["fct_order_items"]
    f_orders["fct_orders"]
    f_sessions["fct_sessions"]
    m_rev["mart_daily_revenue"]
    m_funnel["mart_session_funnel"]
    m_ratings["mart_product_ratings"]
  end
  neo[("Neo4j<br/>Product, BOUGHT_WITH")]

  b_ledger -. "consistent cut" .-> SV
  b_orders --> s_orders
  b_items --> s_items
  b_products --> s_products
  b_dc --> s_dc
  b_events --> s_events
  b_reviews --> s_reviews
  b_users --> s_users
  b_users -- "full history" --> d_user
  s_products --> d_product
  s_dc --> d_dc
  s_items --> f_items
  s_orders --> f_orders
  f_items --> f_orders
  s_events --> f_sessions
  f_orders --> m_rev
  f_items --> m_rev
  f_sessions --> m_funnel
  s_reviews --> m_ratings
  s_products --> m_ratings
  f_items -- "pairs" --> neo
  d_product -- "nodes" --> neo
```

**How to read it.** Bronze keeps every change event as it arrived (inserts,
updates, deletes, with their log position). Silver keeps the latest version
of each row (MERGE on the key, deletes applied). Gold reshapes silver for
analysis.

**What to remember.**

- **`dim_user` reads bronze, not silver**: silver has only the current
  address; SCD2 history needs every change, and bronze is that history.
- **Silver is incremental** (only rows ingested since its watermark);
  dimensions are rebuilt every run (small, always consistent); facts and
  marts are MERGEd for what changed (ADR 014, ADR 015).
- `web_events` is partitioned by day in silver; bronze is partitioned by
  commit day (`source_ts`), hidden partitioning (ADR 012).

---

## 6. The gold star schema

```mermaid
erDiagram
  dim_user ||--o{ fct_order_items : "version valid at order time"
  dim_user ||--o{ fct_orders : "version valid at order time"
  dim_product ||--o{ fct_order_items : "product_id"
  dim_distribution_center ||--o{ fct_order_items : "distribution_center_id"
  dim_date ||--o{ fct_order_items : "order_date_key"
  dim_date ||--o{ fct_orders : "order_date_key"
  fct_orders ||--|{ fct_order_items : "sums its items"

  dim_user {
    long user_sk PK "xxhash64(user_id, lsn); -1 = unknown member"
    string user_id
    timestamp valid_from
    timestamp valid_to "9999-12-31 while current"
    boolean is_current
  }
  dim_product {
    long product_id PK
    string name
    string category
    string brand
    decimal retail_price
  }
  dim_distribution_center {
    long distribution_center_id PK
    string name
  }
  dim_date {
    int date_key PK "yyyymmdd"
    int year
    int month
    boolean is_weekend
  }
  fct_order_items {
    string order_item_id PK
    string order_id
    long user_sk FK
    long product_id FK
    long distribution_center_id FK
    int order_date_key FK
    decimal gross_amount "sale_price x quantity"
    decimal cost_amount
    boolean is_cancelled
    boolean is_returned
  }
  fct_orders {
    string order_id PK
    long user_sk FK
    int order_date_key FK
    int item_count
    decimal net_amount
  }
  fct_sessions {
    string session_id PK
    string user_id "null for ghost sessions"
    boolean is_ghost
    int event_count
    boolean purchased
  }
```

**How to read it.** Facts (events you count and sum) in the middle,
dimensions (who, what, where, when) around them. Only the main columns are
shown; `fct_sessions` stands alone (it joins on `user_id` when it needs to).

**What to remember.**

- **SCD2 join**: an order joins the user version that was valid when it was
  placed (`created_at >= valid_from AND created_at < valid_to`), so revenue
  by city stays right after a user moves.
- **Unknown member** (`user_sk = -1`, Kimball): an order whose user version
  is not there yet still joins, as "Unknown", and is repaired on a later
  run instead of being dropped by an inner join.
- **Metric definitions live in one place** (ADR 015): gross revenue
  excludes cancelled items, net revenue subtracts returns, session
  conversion excludes ghost sessions.

---

## 7. Orchestration

```mermaid
flowchart LR
  subgraph TR["transform: every 30 min"]
    t1["silver"] --> t2["gold_dims"] --> t3["gold_facts"] --> t4["gold_marts"]
  end
  subgraph MA["maintenance: daily 03:00 UTC"]
    m1["silver_gold_upkeep<br/>compaction, snapshot expiry"]
  end
  subgraph GR["graph: daily 04:00 UTC"]
    g1["rebuild_graph"]
  end
  pool{{"pool lake<br/>1 slot"}}
  TR --> pool
  MA --> pool
  GR --> pool
  pool --> cluster["Spark cluster<br/>spark-worker"]
  bronzeup["bronze upkeep<br/>inside the stream, hourly"] -.-> cluster
```

```mermaid
gantt
  title One slot, one job at a time (measured durations, illustrative order)
  dateFormat HH:mm
  axisFormat %H:%M
  section pool lake
  maintenance 2.6 min              :m, 03:00, 3m
  transform 03.00 waits then runs  :t1, after m, 7m
  transform 03.30                  :t2, 03:30, 7m
  graph 64 s                       :g, 04:00, 1m
  transform 04.00 waits then runs  :t3, after g, 7m
```

**How to read it.** Three DAGs, each task one `spark-submit`. All of them
take the single slot of the `lake` pool, so two jobs never run on the lake
at once; a job scheduled while another holds the slot waits its turn.

**What to remember.**

- **Why one slot**: a compaction committing while a MERGE rewrites the same
  table would make one of them fail and retry (ADR 017); it also stops the
  batch jobs from taking the worker's last cores.
- `max_active_runs=1` and `catchup=False`: runs never overlap, and after
  downtime the next run's incremental read covers the missed slots instead
  of replaying them one by one.
- Bronze is maintained by its own stream (it is the only writer), not by
  Airflow (ADR 017).

---

## 8. Online features (Redis)

```mermaid
sequenceDiagram
  autonumber
  participant M as MongoDB web.events
  participant K as Kafka
  participant F as features stream
  participant R as Redis
  participant A as API
  participant C as client

  M->>K: change event (Debezium)
  F->>K: micro-batch every 10 s
  F->>R: one pipeline per batch: ZADD viewed, events, session and HSET cart
  Note over F,R: idempotent: a replayed batch rewrites the same members
  C->>A: GET /users/{id}/features
  A->>R: one pipeline: last 10 views (72 h), events of the last hour, newest session
  A->>R: HGETALL that session's cart
  A-->>C: 200 JSON, or 404 unknown user, or 503 Redis down (in 1 s)
  Note over M,C: P5 freshness, view to API: median 9.6 s, max 10.6 s (target 1 min)
```

| Redis key | Type | Holds |
|---|---|---|
| `user:{id}:viewed` | sorted set | product id -> last view (epoch ms), last 72 h |
| `user:{id}:events` | sorted set | event id -> event time, counted at read time |
| `user:{id}:session` | sorted set | session id -> its last event |
| `user:{id}:cart:{session}` | hash | `item:<event id>` -> price, `purchased` -> 1 |

**What to remember.**

- **Sorted sets keyed by event id** make writes idempotent: replaying a
  batch sets the same members to the same scores.
- **"Events in the last hour" is counted when read**, so it falls as time
  passes even if the user does nothing (a stored counter would not).
- **Least privilege in Redis too**: the stream writes as `features`, the
  API reads as `api` (ACL users), `default` is disabled.

---

## 9. Co-purchase graph (Neo4j)

```mermaid
flowchart TD
  subgraph SPARK["Spark (graph job, daily)"]
    items["gold fct_order_items"] --> dist["distinct order_id, product_id"]
    dist --> join["self-join on order_id<br/>a.product_id < b.product_id"]
    join --> count["count orders per pair = weight"]
  end
  count --> collect["driver: one writer, batches of 10,000"]
  subgraph NEO["Neo4j"]
    upsert["MERGE nodes and relationships<br/>SET weight, run = this run"]
    stale_r["delete BOUGHT_WITH with an older run<br/>IN TRANSACTIONS"]
    stale_n["delete Product nodes with an older run"]
    upsert --> stale_r --> stale_n
  end
  collect --> upsert
```

```mermaid
flowchart TD
  req["GET /products/{id}/recommendations?limit=5"] --> valid{"id and limit valid?"}
  valid -- "no" --> e422["422"]
  valid -- "yes" --> reach{"Neo4j answers<br/>within 1 s connect, 2 s query?"}
  reach -- "no" --> e503["503 graph store unavailable"]
  reach -- "yes" --> known{"product in the graph?"}
  known -- "no" --> e404["404"]
  known -- "yes" --> rel{"any BOUGHT_WITH?"}
  rel -- "no" --> empty["200, empty list"]
  rel -- "yes" --> top["200, top N by weight,<br/>ties by product id"]
  health["GET /health: liveness, no store called"] --> ok["200 while the process answers"]
  ready["GET /ready: pings Redis and Neo4j"] --> both["200, or 503 naming each store down"]
```

**How to read it.** The first diagram builds the graph; the second is what
one request can return.

**What to remember.**

- **Full rebuild, not increments**: weights must be able to go *down*
  (erasure, P8), and `weight += n` counts a replayed batch twice. Recomputing
  every pair from gold is idempotent by construction: same input, same
  graph (measured: same fingerprint on three runs).
- **Write first, delete after**: the old graph stays readable until the new
  run has fully written, so the API never sees an empty graph.
- **One writer**: parallel `MERGE`s on the same popular products would
  deadlock on node locks.
- Measured: 491,014 pairs, 29,120 products, 55 s per run; the top product's
  5 neighbours are exactly its 5 companions (weights 34-43, the next is 2).
- **Fail fast**: 503 in about a second with Neo4j stopped; `/health` stays
  200 so a store outage never gets a working API restarted (ADR 019
  amendment).

---

## 10. Roadmap

```mermaid
flowchart LR
  P1["P1 CDC from PostgreSQL<br/>Kafka, Debezium, slot safety"]:::done
  P2["P2 MongoDB source<br/>Terraform, CI"]:::done
  P3["P3 bronze on AWS<br/>Spark streaming, Iceberg"]:::done
  P4["P4 silver, gold, Airflow"]:::done
  P5["P5 Redis features, API"]:::done
  P6["P6 Neo4j recommendations"]:::active
  P7["P7 data contracts, quality,<br/>schema evolution"]:::planned
  P8["P8 GDPR erasure, backfill,<br/>failure drills"]:::planned
  P9["P9 hardening, load test,<br/>CI/CD, docs"]:::planned
  P10["P10 optional: Kinesis,<br/>Managed Flink"]:::optional
  P1 --> P2 --> P3 --> P4 --> P5 --> P6 --> P7 --> P8 --> P9 --> P10
  classDef done fill:#d4edda,stroke:#2e7d32,color:#1b4d20
  classDef active fill:#fff3cd,stroke:#b8860b,color:#5c4400
  classDef planned fill:#ffffff,stroke:#888888,stroke-dasharray: 5 5,color:#333333
  classDef optional fill:#ffffff,stroke:#bbbbbb,stroke-dasharray: 2 4,color:#777777
```

Green: built and accepted. Yellow: in progress (P6 acceptance run pending).
Dashed: planned. P1 to P4 form the core project; P5 and P6 add serving; P10
only makes sense after P9 (spec section 9).
