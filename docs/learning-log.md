# Learning log

One entry per session: concepts covered, decisions taken, open questions.

---

## Session 1 — 2026-09-29 — Kickoff

No code. Read the spec end to end, challenged it, checked the environment,
agreed the repo layout and the P1 plan, drafted ADR 000 and 001.

### Concepts covered

- **WAL and logical decoding.** Postgres writes every change to its write-ahead
  log for crash recovery. With `wal_level=logical`, the WAL can be decoded into
  row-level changes (plugin: `pgoutput`). A **publication** says which tables are
  decoded; a **replication slot** remembers how far the consumer (Debezium) has
  confirmed, and Postgres keeps all WAL after that point.
- **Debezium envelope.** Each event carries `before`, `after`, `op`, the LSN,
  the commit timestamp and the table. Ops: `c` create, `u` update, `d` delete,
  `r` read during the initial snapshot. Logical decoding only emits committed
  transactions, so there is no "rollback" event.
- **Deletes in an append-only lake.** A delete is stored in bronze as an event;
  the row disappears only in silver, where the dbt MERGE applies it. Bronze and
  Kafka still hold earlier versions, which is why GDPR erasure is hard.
- **Layered exactly-once.** Exactly-once source (Debezium on Connect) + exactly-once
  sink (Iceberg) reduce duplicates; LSN deduplication in silver makes the result
  correct even after a replay.
- **Why a broken source connector fills the Postgres disk.** If the Debezium task
  fails (for example on a schema rejected by Schema Registry), it stops confirming
  LSNs, so the slot makes Postgres keep all WAL. `max_slot_wal_keep_size` caps
  this: the slot is invalidated instead, at the price of a full re-snapshot.
- **Line endings.** Git's `core.autocrlf=true` turns LF into CRLF on checkout;
  shell scripts or SQL files with `\r` fail inside Linux containers
  (`bash^M: bad interpreter`). A repo-level `.gitattributes` (`* text=auto eol=lf`)
  fixes it for every clone and for CI.
- **Resource budget.** Full on-prem stack ≈ 8–10 GB RAM; Kafka Connect and
  Airflow are the largest consumers.
- **ADRs.** One file per decision; accepted ADRs are never rewritten, a new ADR
  supersedes the old one.

### Decisions taken

- The repo moves to WSL2 Ubuntu-24.04 (`~/projects/thelook-cdc-lakehouse`),
  opened with the VS Code WSL extension. Reason: same bash as containers and CI,
  faster bind mounts, no line-ending problems.
- The repo contains the spec `.md` and the v1.6 `.docx` only; v1.3–v1.5 stay outside it.
- Decisions are recorded as ADRs (ADR 000, proposed). ADR 001 records Kafka over
  Kinesis (proposed).
- Repo layout agreed: `onprem/` (Compose stack), `infra/terraform/`, `dbt/`,
  `contracts/`, `drills/`, `scripts/`, `docs/`. Folders are created only in the
  commit that first needs them.
- P1 plan agreed as 16 small commits: repo init → ADRs → vendored generator →
  source schema → Postgres → generator → Kafka → Schema Registry → Connect image →
  replication user/publication → Debezium source → slot alert → slot drill →
  Connect metrics → baseline throughput → wrap-up and checkpoint.

### Open questions (spec unchanged; to settle one at a time)

- **A1** O5 (< 24h erasure in every layer) vs Kafka retention "a few days" vs
  "replay a full day". Recommendation: reword O5, Kafka bounded by retention.
- **A2** Erasure must start with a DELETE in the source, otherwise a re-snapshot
  re-imports the user.
- **A3** FR2 (bronze never updated) needs an explicit GDPR-erasure exception.
- **A4** Contracts creating bronze tables vs P2/P4 ordering and sink schema
  evolution (Terraform drift). Recommendation: sink creates/evolves tables,
  contracts validate them. Settle before P2.
- **A5** A source-side schema break can't go to a dead-letter queue: the connector
  stops and the slot stalls. Document as fail-closed in FR7/FR14 and the runbook.
- **A6** FR1 "exactly-once source" vs NFR "at-least-once": fix the wording.
- **B1** Pre-create the publication and use `publication.autocreate.mode=disabled`
  (least privilege). Settle before P1 commit 10.
- **B2** The heartbeat table must be in the publication (PG 15+ skips empty
  transactions), so 7 tables are captured, one of which never goes to the lake.
- **B3** Airflow needs its own least-privilege IAM identity (the NFR only names the
  Connect worker). Both on-prem identities use long-lived keys; document rotation.
- **B4** Lake Formation cell filters hide columns, they don't mask values (use
  hashed columns such as `email_sha256` in gold if needed). Analysts get no access
  to bronze or silver.
- **B5** Write the measurement method in `docs/results.md` before measuring:
  O1 (freshness up to what?), O4 (counts move while the generator runs), O8 (the
  generator may be the bottleneck).
- **B6** Check version compatibility at P1 start: Debezium 3.x on Connect 4.x,
  the Iceberg sink with Kafka 4 clients, Confluent Avro converter jars (not in
  Debezium's image).
- **C1** Drop "terraform destroy after each session"; keep the lake persistent.
  Idle AWS cost is near zero, and destroying the lake forces a full re-snapshot.
- **C2** Grafana instead of QuickSight (budget O6).
- **C3** Check whether dbt-athena runs unit tests natively before falling back
  to DuckDB (which reintroduces a second adapter).
- **C4** Awareness: Confluent Schema Registry is source-available (Confluent
  Community License); Apicurio is the Apache-2.0 alternative.

Timing: B1 before P1 commit 10; A4 before P2; A1–A3 before P5.

### Environment snapshot

- 31.3 GB RAM, 32 logical CPUs, 567 GB free on C:.
- Docker 29.8.0 + Compose v5.5.1; WSL 2.7.10 with Ubuntu-24.04.
- Windows Git 2.55 has system-level `core.autocrlf=true`; the repo's
  `.gitattributes` makes this irrelevant.
- Python 3.11 (default), 3.12, 3.14 on Windows.
- Done during the session: `.wslconfig` `memory=16GB` (Ubuntu sees 15Gi, Docker
  ~16 GB); Docker WSL integration on for Ubuntu-24.04; Terraform and AWS CLI v2
  installed in Ubuntu (HashiCorp apt key fingerprint checked:
  `D55C 0D1A C78A 8D81 26CB 631C FC9C A96A CA02 6560`, key created 2026-09-09);
  Git 2.43 in Ubuntu with name and email set.
- uv 0.12.20 with Python 3.12.14 installed (checked in session 2).

### Where we stopped

- The repo now lives in WSL: `~/projects/thelook-cdc-lakehouse`. The old Windows
  folder is obsolete (Souhail deletes it when ready).
- `git init -b main` done. **Nothing committed yet.** The working tree contains
  the content of commits 1 and 2. Commit them separately:
  1. `git add .gitattributes .gitignore .editorconfig LICENSE README.md CLAUDE.md docs/cahier-des-charges.md docs/cahier-des-charges-v1.6.docx`
     then `git commit -m "chore: initialise repository"`
  2. `git add docs/adr docs/learning-log.md`
     then `git commit -m "docs: add ADR 000 and 001 and learning log"`
- Pending check question for Souhail: `.gitattributes` and `.editorconfig` both
  mention LF. What does each one control, and why keep both?
- Next: answer the check question, make commits 1 and 2, install uv, then
  continue with commit 3.

## Session 2 — 2026-09-29 — Commits 1–4

### Concepts covered

- **`.gitattributes` vs `.editorconfig`.** Git normalises text to LF on
  `git add` and writes LF on checkout (the guarantee); EditorConfig tells the
  editor what to write (prevention) and also covers indentation, final newline
  and charset. Only `.gitattributes` protects binaries (`*.docx binary`).
  VS Code needs the EditorConfig extension. `git ls-files --eol` shows the
  real state per file.
- **Vendoring a dependency.** Copy unchanged at a pinned SHA (no upstream
  tags), then patch in separate commits: provable provenance (`diff -r` is
  empty), each change isolated and revertable, Apache 2.0 §4(b) satisfied,
  patches re-applicable on a fresh upstream copy. Alternatives rejected:
  submodule (whole repo, `--recursive`), clone at build time (not reproducible).
- **Apache 2.0 obligations.** Keep the licence text, keep notices, mark
  modified files. Upstream has no NOTICE and a blank copyright line, so our
  `NOTICE` names Factor House, the source, the SHA and our changes.
- **Trust the code, not the README.** I first said the update probabilities
  default to 0 (from the README); the argparse defaults are 0.1 / 0.4 / 0.2.
- **No-op updates.** `INSERT ... ON CONFLICT DO UPDATE SET` every column
  writes a new row version even when nothing changed, so Debezium emits `u`
  events with identical values. Silver/SCD2 must ignore them.

### Decisions taken

- Generator vendored in `onprem/generator/` at `4f7537b` (folder unchanged
  since `b1aa968`), `images/` not copied, `world_pop.csv` normalised to LF.
  Licence check passed: the folder is covered by the repo's Apache 2.0 root
  licence.
- **A7 → spec v1.7:** the generator never deletes; all source deletes come
  from the synthetic erasure scripts (option 1, Souhail's choice). The `.md`
  is the reference from v1.7; the `.docx` stays a v1.6 snapshot.
- Remote `origin` = github.com/SouhailBourhim/TheLook_CDC_LakeHouse; pushed
  up to commit 3 after checking the commit contained no stray or secret files.
  Never force-push.

### Findings recorded in `docs/source-schema.md`

UUID text keys, no FKs, `dist_centers` (not `distribution_centers`), status
"Delivered" (not "Complete"), address updates don't bump `updated_at` (SCD2
must use commit time/LSN), every restart re-upserts 29k products and adds
1,000 users, meaningless transaction boundaries, float money,
`ORDER BY RANDOM()` may bottleneck the generator, ghost sessions with fake
purchases, container must run in UTC.

### Open questions added

- **A8** `src/data/*.csv` come from Looker's public BigQuery dataset; no
  separate terms found for the original data. Non-blocking.
- Commit 6 must pass the update probabilities explicitly, pin `TZ=UTC`, and
  fix the Dockerfile `CMD` (`generator.py`).

### Commit 5 and push policy (later in session 2)

- **Postgres settings priority.** Command-line `-c` flags override both
  `postgresql.conf` and `postgresql.auto.conf` (`ALTER SYSTEM`). With `-c`
  flags, `ALTER SYSTEM SET max_slot_wal_keep_size` + `pg_reload_conf()` did
  nothing, while `pg_file_settings` still said `applied = t`. Settings moved
  to a mounted `onprem/postgres/postgresql.conf`; verified that `ALTER SYSTEM`
  now wins. `pg_settings.context`: `postmaster` = restart, `sighup` = reload.
- **Healthcheck trap.** On first start the image's entrypoint runs a
  temporary server with `listen_addresses=''` (socket only) for initdb and
  init scripts. A socket `pg_isready` reports healthy too early; ours uses
  `-h 127.0.0.1` (TCP).
- **Logical decoding smoke test** (`test_decoding`): DDL decodes to an empty
  BEGIN/COMMIT (DDL is not streamed); a DELETE carries only the key (default
  REPLICA IDENTITY).
- **Pinning an image**: tag for humans + digest for identical bytes.
- **Invalidated slot**: past `max_slot_wal_keep_size` the slot is lost; drop
  it and re-snapshot. A re-snapshot can't emit deletes that happened in the
  gap, so silver may keep rows deleted meanwhile (to handle in the runbook).
- Check answers: SCD2 validity from `source.ts_ms` (commit time, not the
  top-level `ts_ms`) with LSN as tie-breaker; no-op updates stay in bronze and
  are skipped in the MERGE by a hash of business columns only.
- **Push policy (Souhail):** push after every commit, guarded by a versioned
  pre-push hook (`.githooks/pre-push`, gitleaks v8.30.1 in Docker, pinned by
  digest, fails closed). Tested: a planted fake token blocks the push.
  Enable per clone with `git config core.hooksPath .githooks`.

### Commits 6–11 (session 2, continued on 2026-09-30)

- **Commit 6, generator.** Pinned Dockerfile + hashed `requirements.lock`,
  non-root; own role/schema (`generator`, `shop`) via a first-start init
  script; the generator logged its DB password in clear: patched to read
  `DB_PASSWORD` and redact it, password rotated. Measured WAL: ~106 MB/h at
  5 iterations/s, so the 10 GB slot cap holds ~4 days (~1 day at 20/s).
  Init scripts only run on an empty volume; later changes need idempotent
  scripts (Souhail's answer). The generator dies after one startup
  connection error with exit code 0, so `restart: on-failure` would not help.
- **Commit 7, Kafka 4.3.1 KRaft.** Mounted `server.properties`. Traps:
  image default `log.dirs=/tmp/...`; RF 3 defaults for internal topics incl.
  the transaction log; the `advertised.listeners` trap (INTERNAL
  `kafka:29092` / EXTERNAL `localhost:9092`); retention deletes closed
  segments only, so `log.roll.hours=24`. Souhail added: topic-level
  overrides beat broker defaults, and a time roll needs a new message.
- **Commit 8, Schema Registry 8.3.2.** Schemas live in the compacted
  `_schemas` topic (registry is stateless). BACKWARD = new schema reads old
  data, consumers upgrade first (my first comment said the opposite; fixed).
  Wire format seen: magic byte 0 + 4-byte schema ID + Avro payload. Adding a
  NOT NULL column without a literal default fails registration and stops
  the connector (A5: needs a DDL policy).
- **Commit 9, Connect worker.** `apache/kafka:4.3.1` + Debezium 3.7.0
  (built against Kafka 4.3.1: **B6 part 1 closed**) + Confluent Avro
  converter 8.3.2, downloaded with `ADD --checksum` (verified: a wrong digit
  fails the build), multi-stage. Distributed mode: state lives in
  `_connect-*` topics. Exactly-once source = records + offsets in one
  transaction. Dedup key in silver is (PK, LSN), never LSN alone (snapshot
  events share one LSN).
- **Commit 10, CDC setup.** Idempotent `cdc-setup.sql` (one transaction,
  password via psql `\getenv`): role `debezium` (REPLICATION + SELECT,
  heartbeat upsert), publication `thelook_cdc` pre-created (**B1**) with the
  6 tables + heartbeat (**B2**), no TRUNCATE, REPLICA IDENTITY DEFAULT
  (FULL would copy an erased user's PII into Kafka during the erasure).
  WSL restarted overnight: Postgres ran crash recovery (19 MB of WAL in
  0.07 s). No restart policy on purpose.
- **Commit 11, Debezium source.** `postgres-source.json` + `make
  register-connectors` (idempotent PUT, waits for RUNNING). Password via
  EnvVarConfigProvider (allowlisted), lz4 on source producers.
  **P1 acceptance part 1 met:** 6 topics (+ heartbeat), 16 subjects,
  snapshot counts = Postgres counts on all 6 tables; streaming shows the
  predicted 29,120 no-op product updates after a generator restart; slot
  active, 238 kB behind. With EOS, end offsets include transaction markers,
  so count records, not offsets.
- **Debugging lessons:** inline client objects garbage-collected before
  their reply; `tr` at the end of a pipe hiding exit codes; "stop when
  quiet" never ends on a live topic (bounded read to frozen end offsets);
  `pkill -f` matched and killed its own shell.
- Deferred to P2 (decide before data reaches the lake):
  `time.precision.mode` (timestamps are microseconds), `tombstones.on.delete`.

### Commits 12–16 (session 2, 2026-09-30, partly run as a /loop)

- **Commit 12, slot alerts.** postgres_exporter as a `pg_monitor` role; its
  built-in slot collector measures retained WAL from `restart_lsn` (checked
  against SQL), so no custom query. Six rules with `promtool` unit tests
  (a weakened rule makes them fail); `absent()` and `up == 0` so missing
  metrics alert; an inhibit rule stops "slot missing" while the exporter is
  down. `apply-sql.sh <file>` replaces `cdc-setup.sh`.
- **Commit 13, slot drill (P1 acceptance part 2 met).** Connect stopped
  60 min: alert pending at minute 1, firing at minute 31; 547 MB retained;
  caught up in 44 s; `verify_cdc.py` (latest version per key from Kafka vs
  Postgres, column by column) mismatched before the restart and matched
  after on all 6 tables. **WAL rate grows with table size** (106 → 547 MB/h
  at 5/s): full-page images after each checkpoint, confirmed with
  `pg_stat_wal`. `docs/runbook.md` written; lost-slot recovery untested.
- **Commit 14, Connect/Debezium metrics.** JMX exporter agent 1.6.0 (GitHub
  release; Maven stops at 1.0.1). Five alerts with tests; the tests caught a
  ms/s unit bug. Crash on first try: `ADD --chmod=644` also applied to the
  directory it created (no execute bit), fixed by adding into `/opt`.
- **Commit 15, throughput.** Method committed before measuring (B5). Capture
  side never limited (lag ≤ 0.5 s up to 266 events/s); the generator caps
  near 25 iterations/s by serial latency (`ORDER BY RANDOM()`, sleep before
  work), not CPU. My 90 %-of-target criterion measured the generator's
  pacing, so no level passed; reported, not rewritten. `wal_compression=lz4`
  saves 19 % and is now on.
- **Commit 16, wrap-up.** ADRs 002–005 **proposed** (Postgres config, Connect
  image, CDC access model, monitoring); README "Run P1".

### Open questions added

- **O8 target: decided** (Souhail): 200 change events/s, capture lag < 1 s
  in P1, re-validated end to end in P2 (spec v1.8).
- **A9: deferred to P2** (Souhail): the capture side's breaking point was
  not found (generator latency-bound); measure it end to end once the sink
  exists.
- Lost-slot recovery (runbook) not yet exercised.

### Where we stopped

- All 16 P1 commits done and pushed. Stack running; generator at 5/s.
- ADRs 002–005 accepted by Souhail; O8 and A9 decided. Stack stopped.
- P1 checkpoint answered: A, 1, 3, 4, 5 correct. **Gap:** B and 2 used the
  commit 6 WAL rate (106 MB/h) instead of the drill's ~550 MB/h, so "the slot
  survives a day" is wrong: at 5/s a day is ~13 GB > the 10 GiB cap (lost
  after ~18 h). **Closed:** Souhail redid question 2 with the measured
  rates (5/s: ~13 GB/day, slot lost after ~16–20 h; max rate: ~1.5 GB/h,
  lost after ~7 h; writes unaffected; same recovery). Added:
  NearInvalidation leaves ~3–4 h (5/s) or ~80 min (max) to raise the cap.
  **P1 checkpoint passed; phase 1 closed on 2026-09-30.**
- **P2 started.** Decided by Souhail: **A4** sink creates and evolves the
  bronze tables, contracts validate (spec v1.9, ADR 006 proposed); **C1**
  lake stays deployed, `terraform destroy` kept as teardown (spec v1.9);
  AWS: a **new project profile** (`thelook`), not the `Signal` admin user.
- AWS profile `thelook` (IAM user `TheLook`, same account as Signal, free
  tier kept; Identity Center avoided because it needs Organizations).

### P2 so far (2026-09-30)

- **Terraform layout + remote state.** `bootstrap/` (local state) creates the
  versioned, encrypted, TLS-only state bucket; `lake/` uses it with S3-native
  locking; the account ID is passed at init, not committed. Saved plans only
  (`make tf-plan` / `tf-apply`). First `init` hung: registry reachable,
  download measured at 8 MB/s, so a transient stall; retried.
- **Budgets.** Project budget filtered on the `project` tag ($15 = O6), plus
  a whole-account budget ($5), a weekly `make cost-report` and a Config
  REQUIRED_TAGS rule (S3 only), added at Souhail's request. The first report
  showed **credits hide usage** (applied untagged, budgets subtract them by
  default): both budgets now exclude credits and refunds. Signal uses the
  same tag key. An untagged ~$0.008/week EC2/ELB leftover (not ours) found.
  The account already had 3 budgets, so each new one costs ~$0.60/month.
- **Lake storage.** Bucket with SSE-KMS (`aws/s3` + Bucket Key), **no
  versioning** (would keep erased data in old versions), abort incomplete
  uploads, results expire after 7 days; Glue `thelook_bronze|silver|gold`;
  Athena workgroup `thelook` (engine v3, enforced settings, 1 GiB scan
  cutoff, KMS results). Verified with a real query: an overridden output
  location was ignored.
- **Open check:** Config has not yet discovered the buckets that existed
  before the recorder (only the 2 new ones are evaluated).
- Next: P2 commit 4, least-privilege IAM user for the Connect worker (B3).
- Before P2: settle A4 (who creates bronze tables) and the connector
  settings deferred to P2 (`time.precision.mode`, `tombstones.on.delete`).

## Session 3 — 2026-10-03 — Spec v2.0, P2 started (MongoDB source)

Souhail asked to extend the project with PySpark, NoSQL (MongoDB, Redis,
Neo4j), Kafka streaming, CI and pytest. Audit first, then a plan with five
decisions that change the spec; all approved as recommended.

### Decisions taken (Souhail)

- **D1** Spark Structured Streaming writes bronze; the Iceberg sink is
  dropped (it was never built). **D2** `events` moves to MongoDB, plus a
  synthetic `reviews` collection. **D3** PySpark replaces dbt for silver
  and gold. **D4** serving layer (Redis, Neo4j, FastAPI) with synthetic
  basket affinity. **D5** order: P2 MongoDB → P3 Spark bronze (+ IAM
  Terraform) → P4 silver/gold + Airflow → P5 Redis/API → P6 Neo4j → P7
  contracts → P8 GDPR/backfill/drills → P9 hardening; minimal CI in P2.
  **P2–P4 are the must-have** (job application deadline); each phase must
  be demo-able on its own.
- Kept: Iceberg (not Delta: Athena writes Iceberg, only reads Delta; Glue
  catalog makes Spark commits visible at once), ruff only (lint + format),
  datacontract-cli for quality; ADR 006 amended (writer = Spark job).
- Souhail's additions: cart events get `product_id` and `price` in the same
  generator patch as D2 (synthetic, 4.3); Kafka must also be reconciled
  against MongoDB.
- Spec v2.0 written; ADRs 007–010 **proposed** (to review).

### P2 commits so far

| Commit | What | Verified |
|---|---|---|
| `chore: make up, down and ps` | `make up PROFILES="core monitoring"`, `make down` (all profiles, volumes kept) | stack up in 43 s |
| `feat(onprem): mongodb 8 single-node replica set` | `mongo` + one-shot `mongo-init` running idempotent `mongo/setup.js` | generator user can insert events, cannot delete, drop or touch reviews; keyfile 400; oplog 2048 MiB; cache 512 MiB; anonymous refused |
| `feat(connect): debezium mongodb plugin` | 3.7.0.Final, checksummed | both connector classes listed |
| `feat(generator): events to mongodb, cart product and price, basket affinity` | vendored patch + 6 unit tests | 62.2 % of new 2-item orders share a category (expected ~61.5 %) |
| `feat: copy event history to mongodb, stop capturing shop.events` | one-off seed, publication/grants/connector without events | 1,756,486 copied in 94 s, 0 missing |
| `feat(connect): debezium mongodb source` | `mongo-source.json`, read-only `debezium` user | snapshot 1.76 M docs in 79 s, streaming |

### Concepts covered

- **Why a replica set for one node**: change streams (what Debezium reads)
  only exist on replica sets. With access control, members must
  authenticate each other, so even one node needs a **keyfile**; it is a
  secret, generated in the volume, mode 400.
- **Oplog vs replication slot**: the oplog is fixed-size (2 GiB here). A
  stopped connector cannot fill the disk (slot problem), but once its
  resume point is overwritten it must re-snapshot (`ChangeStreamHistoryLost`).
  Opposite trade-off: availability of the source vs completeness of CDC.
  **A bulk load eats the window**: the seed used 1.16 GiB of the 2 GiB.
- **The member host name is like `advertised.listeners`**: clients are told
  to reconnect to `mongo:27017`; host tools need `directConnection=true`.
- **Image entrypoints hide traps**: the mongo image's first-start init runs
  without `--replSet`, `--keyFile` and `--auth`, bound to 127.0.0.1, so the
  replica set can only be initiated after the real start (one-shot service).
- **Defaults sized for the host, not the container**: WiredTiger's cache
  defaults to 50 % of (host RAM − 1 GB), ~7 GB here; capped at 0.5 GB.
- **Debezium MongoDB envelope**: `after` is a JSON string (schemaless
  documents have no fixed Avro schema; dates as `{"$date": ms}`), log
  position = `source.ts_ms` + `source.ord`, `before` null without
  pre-images. Nullable Avro fields arrive as unions (`{"string": "c"}`).
- **Avro names** allow letters, digits and `_` only; Debezium builds the
  namespace from `topic.prefix`, so `thelook-mongo` failed at Schema
  Registry → `thelook_mongo`.

### Debugging lessons

- `mongosh` throws server errors that the old shell returned as `ok: 0`
  (replica set "not initiated" arrives as an exception with a `codeName`).
- **A single-file bind mount follows the inode**: an editor that saves by
  replacing the file leaves the container on the deleted copy (on Docker
  Desktop the next start fails with "no such file or directory"). New
  services mount folders. Existing file mounts (`postgresql.conf`, Kafka,
  Connect, Prometheus) only bite if edited while running: recreate the
  container after editing them.
- `up --wait` does not fail when a one-shot container exits 1 unless a
  service depends on it with `service_completed_successfully`.
- **Check the measurement before the code**: the affinity first measured
  53 % (anchor item picked by random UUID) and 13.5 % (window included the
  old generator); restricted to 2-item orders since the restart: 62.2 %.
- A test only proves something if it fails without the fix: removing the
  `str()` key fix makes the regression test fail with `KeyError: 27395`.
- A `docker pull` hung silently after a DNS blip; killed and retried.

### P1 note (Souhail's request)

The P1 throughput baseline and slot drill were measured with `events` in
PostgreSQL (about two thirds of change events and most of the WAL). They
stay valid as P1 history; WAL rate and Postgres connector load are lower
since P2. O8 is re-measured end to end in P3. Note added to
`docs/results.md`.

### Open questions

- **E1 (P4)**: MongoDB ordering key for silver dedup. `source.ts_ms`
  appeared equal to `wallTime` (ms) while `ord` counts within the
  cluster-time second; check whether (`ts_ms`, `ord`) is a safe total order
  per `_id`, or whether the Kafka offset (single partition, keyed) must be
  the tie-breaker.
- **E2 (decide before P6)**: co-purchase weights will all be 1. At 5 it/s
  ~5,600 multi-item orders/h, a product anchors one every ~5 h (29,120
  products), and its affinity partner is drawn from ~1,000 same-category
  products, so a given pair repeats about once per 4,000 h. Options: (a)
  per-product companion sets (e.g. 5 fixed partners) plus a popularity skew
  on the first item (spec 4.3 change); (b) measure P6 by the share of
  same-category recommendations instead of weights. Recommendation: (a).
- `json.serialization.mode` left at `legacy`; it only changes the content
  of the `after` string, not the Avro schema, so it can be decided in P3.
- Docker Desktop stops containers with exit 255; Postgres then runs crash
  recovery (seen again today, harmless).

### Session 3, continued — review, ADRs accepted, P2 finished

Souhail asked for a full review; it found a broken script (`verify_cdc.py`
still compared `events` with Postgres), stale docs, no repo lint config,
no CI, the review simulator missing, and no oplog monitoring (deferred to
P7). **ADRs 006–010 accepted** by Souhail.

| Commit | What | Verified |
|---|---|---|
| `docs: accept ADRs 006 to 010` | Status lines (case normalised to `Accepted`, ADR 000) | |
| `feat(onprem): review simulator` | Reviews for delivered items; read-only Postgres role (TABLESAMPLE), Mongo user limited to `web.reviews`, unique index on `order_item_id`; 5 tests | topic: c 83, u 41, d 10 (+10 tombstones); duplicate rejected with E11000 |
| `ci: ruff, pytest and alert-rule tests` | GitHub Actions, pinned by SHA, read-only token; explicit ruff rules | first run green (4 jobs) |
| `test(drills): reconcile kafka against mongodb too` | 7 sources; Mongo key from the record key, hashed docs; E1 evidence; retention-aware | all 7 identical in 166 s; planted change detected, then healed |
| `docs: P2 wrap-up` | README (Mermaid, run, demo, decisions), source schema, runbook (MongoDB, Compose), results, spec risks | |

### Concepts covered (continued)

- **A unique index enforces a business rule in the database** (one review
  per order item): the application tries, the database refuses (E11000).
- **TABLESAMPLE SYSTEM (1)** reads ~1 % of a table's pages at random:
  cheap sampling, versus `ORDER BY random()` which scans everything.
- **Tool defaults move**: ruff 0.16's default rule set is far larger than
  older versions'. Pin the version *and* list the rules explicitly.
  Actions pinned by SHA for the same reason images are pinned by digest.
- **Kafka is not the system of record**: with 3-day retention the topics
  no longer hold rows unchanged for 3 days. Bronze must be the full
  history, so **P3 starts with a Debezium incremental snapshot** of the
  Postgres tables (spec risk table).
- **A delete in MongoDB CDC carries the id only in the record key**
  (`before` and `after` are null without pre-images).
- Measured windows: oplog ~30 h (69 MiB/h), Postgres slot ~42 h (WAL
  242 MiB/h, down from ~550 MiB/h in P1 once events left).

### Debugging lessons (continued)

- The verifier started at offset 0, which retention had deleted; the
  consumer silently jumped to the end and read nothing. Start at
  `OFFSET_BEGINNING`.
- A wait loop that greps an empty pipe "succeeds": `curl` does not exist
  in the Connect image (use `wget`), so `awk` saw no lines and exited 0.
  Check that a guard can fail, like a test.
- `pkill -f <pattern>` killed my own shell **twice** (exit 144), the P1
  lesson again. Kill by PID.
- A 2-minute WAL sample read 586 MiB/h because it included the
  generator's startup burst; the clean 10-minute window read 242 MiB/h.
  Same lesson as the affinity: check the measurement window first.

### Where we stopped

- **P2 complete** (11 commits); stack running with core + monitoring.
- **Next: P2 checkpoint** (questions in the session summary), then **P3**:
  version check → IAM Terraform (Souhail reviews `tf-plan`) → Spark image
  and `stream` profile → envelope parsing + chispa tests → streaming job
  (incremental snapshot first) → freshness, replay drill, O8.
- Open: E1 (P4), E2 (before P6); `json.serialization.mode`,
  `time.precision.mode`, `tombstones.on.delete` to settle at P3 start.
- **P2 checkpoint answered (2026-10-03):** questions 2–5 correct
  (schemaless `after` and the key for deletes; idempotent upsert, op `c`;
  seed before the snapshot and its oplog cost; why P3 re-snapshots and
  bronze becomes the history). **Gap on question 1:** used the P1 WAL rate
  (~550 MB/h, slot lost after ~18 h) instead of the P2 measurement
  (242 MiB/h, ~42 h), and left MongoDB as "if" when its window was
  measured (~30 h, so two days does lose the resume point). Same pattern
  as the P1 checkpoint: reuse the latest measurement. **Closed:** Souhail
  redid it with the P2 numbers: MongoDB loses its resume point first (~30 h
  oplog window), Postgres its slot at ~42 h (10 GiB / 242 MiB/h) while
  still accepting writes; alerts in order: SlotInactive ~31 min,
  RetainedWalHigh ~4.3 h, NearInvalidation ~34 h (~8.5 h left to act).
  Added: MongoDB loses first yet has no countdown alert (P7).
  **P2 checkpoint passed; phase P2 closed on 2026-10-03.**
- Depth added in review: a replace of an *existing* document is reported
  as an update, not an insert; compacted topics would keep the latest
  version per key (no re-snapshot needed) but keep PII indefinitely, which
  conflicts with retention as the erasure bound (FR9); whether Postgres
  incremental snapshots need write access to a signal table is the first
  P3 check (it affects the `debezium` role's least privilege).

### P3 so far (2026-10-03)

- **Commit 1, version check (ADR 011, proposed).** Spark 4.2.0 is out but
  Iceberg 1.12.0 has runtimes only up to Spark 4.1, so **Spark 4.1.3 +
  Iceberg 1.12.0**: the table format decides the engine version. Image:
  Docker Official `spark:4.1.3-python3` (Java 17, **Python 3.10**), jars
  baked in with checksums (7 jars, each verified against Maven's SHA-512
  or SHA-1). Python 3.10 vs 3.12 elsewhere matters: PySpark fails on a
  driver/executor Python mismatch, which constrains Airflow in P4.
- **Incremental snapshot without new grants:** Debezium 3.7 supports
  read-only incremental snapshots on PostgreSQL 13+ (`read.only=true`,
  watermarks from in-flight transaction ids instead of writes to a signal
  table), triggered through the **Kafka signal channel**. The `debezium`
  role keeps SELECT + heartbeat only.
- Mistake: pulled `spark:4.1.3-java17-python3`, which does not exist;
  Java 17 is the unsuffixed default (`4.1.3-python3`, same digest as
  `4.1.3-java17`).
- **ADR 011 accepted** (2026-10-04). Souhail's answers: Iceberg plugs into
  Spark internals (DataSource V2, catalog plugin, SQL extensions), so one
  runtime per Spark minor; baked jars give reproducibility and no Maven
  dependency at start-up. Added: `ADD --checksum` also fails the build on a
  tampered download (supply chain).
- **Commit 2, IAM (`iam.tf`).** One user per job: `thelook-spark-stream`
  writes bronze only (batch user in P4). Plan reviewed by Souhail (3 added,
  0 changed: no drift), applied. Access key created with the CLI (secret
  never in state), stored in `onprem/.env`. Proven with the key: bronze
  allowed; silver, gold, bucket root, `thelook_silver`, DeleteTable, Athena
  denied; SSE-KMS write works without KMS permissions (AWS-managed key).
  Runbook: create, rotate (two keys, no downtime), revoke.
- Decided: streaming checkpoints on a local Docker volume, not S3 (S3A is
  not in the image; fewer S3 requests); the batch id in each Iceberg
  commit is the duplicate guard if a checkpoint is lost.
- IAM check answers: blast radius, independent rotation, CloudTrail
  attribution (good); state readable by anyone with the state bucket, CI
  role or `terraform state pull`, and old versions keep old secrets
  (good). **Gap:** assumed the streaming key reads bronze and writes
  silver; the verified policy is bronze only (read/write/delete objects,
  get/create/update tables in `thelook_bronze`). Reuse verified facts, not
  guesses. It also exposed my own runbook overstatement ("bronze is
  rebuildable from a snapshot": only the current state is; the history is
  not), corrected.
- **Commit 3, Spark image and cluster.** Image 2.39 GB; `stream` profile
  (master + 1 worker, 4 cores / 3 GB, ~0.5 GB idle); `jobs` profile for
  throwaway drivers (`make spark-run`). Credentials are per application
  (`spark.executorEnv.*` set from the driver's environment inside the job,
  not on the command line), so the worker holds no key. Smoke test passed:
  Kafka `read_committed` on the worker (offsets step by 2: transaction
  markers take offsets), Avro round trip, `SHOW TABLES` in Glue as the
  bronze writer.
- Debugging: (1) master unhealthy: it binds and advertises its container
  IP, not localhost; `SPARK_MASTER_HOST=spark-master` (advertised-address
  trap, third time after Kafka and MongoDB). (2) "Nested databases are not
  supported by v1 session catalog": the Iceberg catalog was missing because
  `COPY --chmod=644` created `/opt/spark/conf` without an execute bit and
  Spark silently skipped the file (same trap as the Connect image in P1).
  `lake_session()` now fails fast when the catalog is not configured.
- Check answers (Spark step): per-application credentials (not on the
  shared worker, not on the command line; remaining risk: a shell on the
  worker can read the executor's environment) and `defaultCores=1` vs an
  explicit `spark.cores.max` sized to the input: both correct. Added: the
  access key *id* is not redacted (`access_key` with an underscore does not
  match `access[.]key`); 7 topics x 1 partition = 7 input partitions per
  micro-batch, run 2 at a time with `cores.max=2`.
- **Deferred Debezium settings decided: all stay at their defaults**
  (`time.precision.mode=adaptive`, `tombstones.on.delete=true`,
  `json.serialization.mode=legacy`). Lesson: a unit change inside the same
  Avro type, or the content of a JSON string, changes meaning while the
  schema looks compatible; Schema Registry cannot catch it (contracts, P7).
- **Commit 4, parsing (ADR 012 proposed: bronze table design).** Tables
  `<database>_<table>` (`shop_users`, `web_reviews`...), common columns
  (op, source_ts_ms/source_ts, ts_ms, snapshot, Kafka coordinates,
  schema_id, ingested_at), Postgres adds lsn/tx_id/typed before/after,
  MongoDB adds doc_id (from the key)/ord/after JSON/update_description;
  partitioned by `days(source_ts)`; Debezium encodings kept as sent.
  `lakehouse/cdc.py`: split wire format -> decode per schema id -> flatten.
  9 tests on real captured records + 2 fastavro-encoded (Postgres delete,
  newer schema version); an off-by-one in the frame split fails 7.
- Facts confirmed from real data: a Postgres update's `before` is NULL
  (REPLICA IDENTITY DEFAULT), only deletes carry the key there; timestamps
  in `after` are microseconds.
- Debugging: (1) `datetime.UTC` does not exist in Python 3.10 (the image's
  version): `spark/ruff.toml` targets py310 so lint never suggests 3.11+
  syntax there. (2) `F.lit()` at module level needs a running session.
  (3) pytest collected `jobs/smoke_test.py` (`*_test.py` pattern):
  `pytest.ini` limits discovery to `tests/`. (4) **PySpark collects
  timestamps in the Python process's local time zone**, ignoring the
  session time zone (+1 h on this laptop): tests run with TZ=UTC.
  (5) gitleaks blocked the push: base64 Kafka keys under a field named
  "key" looked like API keys; decoded (row UUIDs), then ignored by exact
  fingerprint in `.gitleaksignore`, not by path.
- **ADR 012 accepted.** Check answers: per-schema-id decoding (Avro binary
  is not self-describing; the wrong schema can give wrong values with no
  error) and doc_id from the key (a delete has no `after`; a null key
  would make the MERGE drop the delete silently and break erasure): both
  correct. Added: Avro schema resolution exists but needs the writer
  schema too, and Spark's from_avro does not resolve; unionByName works
  because BACKWARD only allows changes like added nullable fields.
- **Commit 5, the streaming job (bronze is live).** 7 topics -> 7 Iceberg
  tables, 60 s trigger, 200k records/batch max, `failOnDataLoss=true`,
  query id + batch id in each Iceberg snapshot (replay guard). Backlog of
  ~3.3 M records in 17 batches (~22 min, ~2,500 records/s: each batch makes
  7 sequential commits with S3/Glue round trips from the laptop, ~4 s
  each); steady batches ~30 s. Athena works: first query scanned 359 bytes
  (answered from Iceberg metadata/statistics).
- **Debezium bug (the big lesson of the day).** The read-only incremental
  snapshot killed the Postgres task under live traffic:
  ConcurrentModificationException in `sendWindowEvents` (a streamed change
  closes the window while the window's rows are emitted, and emitting them
  deduplicates the same map: re-entrancy). Read the trace, then the 3.7.0
  source; no newer release. Recovery: the stored offset had no snapshot
  state because **the failed exactly-once transaction was aborted**; deleted
  and recreated the signal topic so the signal could not replay; restarted
  the task; switched to a **blocking snapshot** (same code path as the
  initial snapshot, no new grant): 964,802 rows in 24 s, then reconciled
  **exactly** in Athena (rows = distinct ids = distinct offsets = Debezium's
  exported counts, all 5 tables).
- S3 after the run: 759 objects, 279 MB; metadata files outnumber data
  files ~3:1 (metadata.json + manifest list + manifest per commit): the
  small-files problem is already visible; expiry/compaction come with the
  maintenance DAG.
- Debugging lessons: two wait loops "succeeded" without checking (an old
  log window that already contained the expected words; `awk match()` with
  an array is gawk-only, Ubuntu has mawk). Guards must be able to fail.
- Check answers (streaming step): (1) records and offsets in one Kafka
  transaction, aborted by the crash; `read_committed` skips aborted data
  (worker `exactly.once.source.support` + connector `exactly.once.support`
  + reader isolation level): correct. Added: Debezium confirms an LSN to
  Postgres only after the offsets commit, so the slot kept the WAL of the
  aborted batch. (2) Blocking snapshot safe for silver: **verified in
  Athena** that all 366,935 `r` rows of `shop_orders` carry one LSN, the
  pause position (11639792696), and that the first event streamed after
  the resume has **the same LSN**: a real tie (same values: it committed
  before the snapshot read). P4 dedup order: highest LSN, then streamed
  before `r`, then Kafka offset; the tie-break keeps reruns deterministic
  (FR3).
- **Step 6, the proofs (P3 acceptance met).**
  - The first freshness run **timed out**: the stream had died 21 min
    earlier on a few seconds of "Connection refused" from Glue (SDK's 3
    attempts exhausted -> batch failed -> query stopped -> no restart
    policy). A drill found a real outage. Fix: transient S3/Glue errors
    retried inside the batch (5 attempts, 5-40 s), each attempt re-running
    the replay check (`append_once`), so an ambiguous commit (applied, reply
    lost) is not appended twice; plus `restart: on-failure:3` (a deliberate
    exception to "no restart policy": a stream consumer self-heals, but
    stops after 3 failures so a persistent fault stays visible).
  - **Kill drill:** `kill -9` of the driver after 2 of 5 tables committed
    in batch 35; Docker restarted it; replay logged
    `shop_order_items=skipped(replay)`, `shop_orders=skipped(replay)`;
    **0 duplicates in 4.18 M rows** (rows = distinct offsets, all tables).
  - **O1: 26-88 s** (median 75 s) source commit -> Athena. Reviews are
    ~30-40 s later because tables commit sequentially in topic order and
    reviews come last.
- **O8: pending, Souhail's decision.** As written ("200 events/s with
  consumer lag < 30 s") it cannot pass: a 60 s trigger plus ~30-45 s
  batches means bronze lags 30-90 s by design. Souhail **leans B**: a lag
  target of < 2 min (p95, source to bronze), consistent with O1, measured
  in P9, with A (shorter trigger + parallel table commits) only as an
  optimisation. **The spec is not changed until he decides.**
- **P3 checkpoint answered (2026-10-04): all 5 correct.**
  1. Bronze append-only: the only full history (Kafka keeps 3 days),
     silver rebuildable from it, SCD2 needs every version, appends are
     cheap and idempotent. Added: the one exception is GDPR erasure (A3,
     P8), which deletes from bronze too.
  2. Crash after 2 of 7 tables: offsets logged before the batch, replay
     with the same batch id, (query id, batch id) as a per-table
     idempotency key; lost checkpoint -> new query id, so the guard no
     longer matches. Added: we chose `startingOffsets=earliest`
     deliberately, i.e. duplicates over gaps (at-least-once over
     at-most-once): duplicates are detectable by Kafka coordinates and
     removed in silver, a gap is silent; and "earliest" re-reads only what
     retention still holds, not necessarily 3 days.
  3. Confluent framing (magic byte + schema id) + one fixed schema in
     from_avro: strip, look up, decode per id, align.
  4. Trigger trade-off: shorter = more files, snapshots, metadata, S3/Glue
     calls, compaction; longer = staler data, bigger batches, longer
     replays. Added: the trigger also sets the floor of O1 and O8 lag;
     when a batch takes longer than the trigger, batches run back to back
     (seen during the backlog).
  5. Ambiguous commit: Glue may have applied it although the client saw an
     error, so re-check before retrying. Added: Iceberg itself raises
     `CommitStateUnknownException` in that case and deliberately does not
     delete the written files.
  **P3 checkpoint passed; phase P3 closed on 2026-10-04.**

### Where we stopped (2026-10-04)

- P3 closed: bronze live on AWS (7 Iceberg tables, Athena), O1 26-88 s,
  kill drill 0 duplicates, everything committed and pushed, CI green.
- **Running:** core + stream profiles (generator 5/s, review simulator
  1/s, both connectors, Spark master/worker, `bronze-stream`), about
  $0.01/h on AWS. Stop the stream with
  `docker compose -f onprem/compose.yaml stop bronze-stream`, or
  everything with `make down` (volumes kept).
- **Caution when stopping for days:** if the stream is down longer than
  Kafka retention (3 days), it stops on `failOnDataLoss` at restart.
  Recovery is in the runbook ("Bronze stream"): re-snapshot, fresh
  checkpoint, duplicates removed in silver.
- **Next session: P4** (PySpark silver with MERGE and SCD2, gold star
  schema and marts, Airflow), only when Souhail says so.
- Open: **O8** (pending, leaning B), **E1** (MongoDB ordering key, before
  P4: (`source_ts_ms`, `ord`) matched Kafka order on every document so
  far), **E2** (co-purchase weights, before P6). P4 dedup order agreed:
  highest LSN, then streamed before `r`, then Kafka offset.

## Session 4 — 2026-10-04 (evening) — P4 started

P4 plan approved (silver, gold, Airflow; 11 steps). Souhail chose to
include table maintenance (snapshot expiry, compaction) in P4.

### Decisions taken

- **ADR 013 accepted** (Airflow 3.3.2 on the Spark image, Python 3.10,
  SparkSubmitOperator client mode; one override of Airflow's constraints:
  `pyspark-client` 4.1.3 instead of 4.2.0) with Souhail's three additions:
  Airflow image as a stage of the digest-pinned Spark Dockerfile (a local
  tag can move); only the batch key in Airflow's environment, never the
  stream key; a scheduler memory limit and `max_active_runs=1`.
- **Batch IAM user** `thelook-spark-batch` applied after Souhail's review:
  read bronze; read/write/delete objects in silver and gold; Glue get on
  bronze, get/create/update tables in silver/gold; no DeleteTable,
  DeleteDatabase or IAM. Proven with real calls (8 denials, 7 allowed).
- **Maintenance identity (proposed, decided at step 9):** a separate
  `thelook-maintenance` user, not the stream's identity (the always-on key
  must not gain silver/gold rights or move into Airflow); expiry keeps the
  recent snapshots the stream's replay guard needs.
- **ADR 014 proposed (silver).** E1 settled: MongoDB order
  (`source_ts_ms`, `ord`, streamed before `r`, `kafka_offset`). Correction
  of the plan found while designing: **every newer event updates, no-ops
  included**; skipping no-ops would leave an old `_position` and let a
  replayed older event overwrite the true state. FR3's "ignore no-ops"
  moves to gold's SCD2 (versions whose `_row_hash` did not change).
  Watermark = a silver table property, not atomic with the MERGE, and it
  does not need to be: the MERGE is idempotent.

### Steps done

| Step | Result |
|---|---|
| 0 restart stream | same query id (checkpoint volume survived `make down`); caught up in one batch, including the generator's 29,120 no-op product updates |
| 1 ADR 013 | accepted with additions |
| 2 batch IAM user | applied, verified |
| 3 silver logic | `lakehouse/silver.py` + 14 tests on a local Iceberg catalog; removing the position guard fails the stale-replay test |

### Concepts covered

- **Idempotency replaces atomicity**: two steps that cannot commit
  together are safe if replaying the first gives the same result.
- **Merge-on-read vs copy-on-write**: random order updates touch files
  everywhere; copy-on-write would rewrite (and download) most of a table
  per run; merge-on-read writes delete files, paid back by compaction.
- **Data transfer out costs money**: local Spark reading S3 is free up to
  100 GB/month; the design is incremental to stay under it.

### Debugging lessons

- Iceberg on Spark 4 **rejects the `snapshot-id` read option** ("use
  `versionAsOf`"): caught by testing MERGE on a real local Iceberg catalog
  rather than mocks. Older docs still show `snapshot-id`.
- A "DENIED" for a bronze read was my test, not the policy: `aws s3 cp` to
  `/dev/null` exits non-zero after a successful download; and `--dryrun`
  never calls AWS, so it proves nothing.

### Where we stopped (session 4, superseded below)

- Steps 0-3 committed and pushed, CI green (39 Spark tests). Stream
  running (core + stream profiles).
- **ADR 014 accepted.** Check answers, all correct: (1) bronze is the one
  layer that cannot be regenerated, so the batch key cannot touch it; a
  shared user = union of rights, no independent rotation, no attribution.
  (2) A -> B -> A: skipping the no-op at LSN 300 leaves LSN 100 stored, and
  a redelivered LSN 200 (B) then wins; Souhail withdrew his earlier
  "update only when the hash differs". Added: in Iceberg merge-on-read,
  updating only `_position` rewrites the row anyway (delete file + new
  row), so "update everything" costs the same. (3) at-least-once + an
  idempotent write = exactly-once result; the order matters (saving the
  position first would skip data on a crash). Added: a replayed delete is
  harmless (the row is gone and `WHEN NOT MATCHED` never inserts a `d`).
- **Step 4 done: silver is live and proven.** First run (full read) ~27
  min; incremental runs ~2-3 min (`full_read: False`, untouched tables "up
  to date"). With writers stopped: **all 7 silver tables identical to the
  sources, every column (3.8 M rows)**; a changed review was detected
  (`differ=1`), then healed once it flowed through CDC -> bronze -> silver.
  One-off jobs now run as the batch user.
- Lessons (step 4):
  - **Structured Streaming skips triggers with no new data**: "no batch
    since the writers stopped" is the caught-up signal; a loop waiting for
    an "empty batch" line waits forever.
  - `docker logs --since <time without zone>` is read as local time (UTC+1
    here): use relative durations (`--since 2m`).
  - Iceberg table properties live in the metadata file on S3, not in
    Glue's table parameters, and Athena shows only its own properties;
    the watermark was proven by behaviour (the next run was incremental).
  - `--rm` containers take their logs with them: tee long jobs to a file.
  - The tool's 30-minute limit stopped my *watcher*, not the job: killing
    `docker compose run`'s client left the container running.
  - **DNS outages are this setup's main reliability issue** (Docker's
    127.0.0.11 -> WSL/Windows resolver): one lasted ~3 min and restarted the
    stream (recovered on its own via the restart policy and replay guard);
    the silver job rode out others with its retries. During the silver
    backfill, stream batches slowed to 200-280 s (shared network and
    cores).
- Check answers (step 4): (1) no empty batches, so measure position
  (`maxOffsetsBehindLatest` in the query progress; a remainder of 1 per
  partition from transaction markers): correct. **Error:** "the heartbeat
  keeps the stream non-empty / a heartbeat row in bronze": the heartbeat
  topic is not ingested (B2, `test_heartbeat_topic_is_not_ingested`), and
  step 4 itself showed no batch at all once the writers stopped. Idea kept
  for P7: ingest the heartbeat into a tiny bronze table as an end-to-end
  liveness signal. (2) Incremental read of the new bronze snapshots only:
  correct. Refinement: the MERGE *writes* only affected rows but *reads*
  the key column of the whole target (random UUIDs defeat min/max file
  skipping); only events prunes by date partition.
- **Step 5 done: gold dimensions (ADR 015 proposed).** `dim_user` SCD2
  rebuilt from bronze each run: 48,146 versions of 40,458 users, keys
  unique, one current row per user, 0 gaps/overlaps (checked in Athena).
  Changed while coding: `user_sk` = xxhash64(user_id, **lsn** of the
  version's first event), not (user_id, valid_from): two changes in the
  same millisecond would collide; an LSN is unique and stable.
- An unexplained number, investigated before trusting the table: 15,301
  bronze updates but 7,688 later versions. Answer: 7,683 updates are the
  *first* bronze event of users created before bronze began (they become
  version 1), 7,718 are real address changes (the ~30 extra arrived after
  the build); 18,656 snapshot rows repeating the state were collapsed.
- Again `datetime.UTC` in code for Python 3.10: ruff's py310 target stops
  ruff *suggesting* it, not me writing it; the 3.10 test run caught it.
- Check answers (step 5), all correct: (1) a rebuild is a pure function of
  bronze, and a logic fix applies to all history at the next run with no
  backfill; an incremental SCD2 would have to get atomic close/open,
  several changes per batch, late events splitting an interval, no-ops and
  erasure right; (2) unstable keys orphan facts or, worse, get reused by
  another version (orders silently attributed to the wrong person); (3)
  without the created_at start, an order placed before bronze began matches
  no version. Souhail's caveat added to ADR 015: before bronze began, the
  first captured address is an approximation.
- Next: step 6, gold facts (incremental) + tests.

### Session 5 — 2026-10-05 (night) — P4 step 6

- Started by restarting the stream (it had been down ~3 h with the core
  profile up; caught up from its checkpoint).
- **Step 6 done: gold facts.** fct_order_items, fct_orders (FR6 amount
  rules), fct_sessions, incremental by silver's `_merged_at` (Iceberg's
  per-file min/max statistics skip older files), rows without a user
  version retried each run. On AWS: 575,377 / 396,842 / 666,781 rows,
  0 orders outside their user version's range, **101,683 orders keep a
  former address** (FR4 at scale). Normal silver+gold cycle ~8 min; O2
  estimate ~40 min worst case (measured in step 10).
- Lessons:
  - A logged count of 0 after a retry was correct: the first attempt had
    merged the items, the retry found nothing left. Counts are now named
    "recomputed" (what this attempt did), not table sizes.
  - A mutation that deletes a whole line can break the syntax: pytest then
    reports a collection *error*, which proves nothing about the tests.
    Mutate the logic, keep the code compiling.
  - Ruff B008: default arguments are evaluated once at definition time
    (harmless for datetimes, a classic bug for mutable defaults).
  - Catch-up runs scale with the increment (6 h of changes: 31 min of
    silver); normal 30-minute increments take ~3 min.
- Check answers (step 6), all correct: (1) volume and S3 transfer, and facts
  have no intervals to split, only rows to upsert; (3) a cancelled item
  never was a sale, a returned one was (the refund is a real outflow and
  the return rate is a metric of its own). (2) went further than the code:
  a Kimball **unknown member** (`user_sk = -1`) instead of NULL, so inner
  joins keep unresolved orders (reported as "Unknown"); **adopted**.
  Alert on rows unresolved for long: P7.
- **ADR 015 accepted** (Souhail).
- **Unknown member adopted** (Souhail's design): facts without a user
  version point to `user_sk = -1`, a real dim_user row, so inner joins keep
  them; retried each run.
- **Step 7 done: marts.** Daily revenue, session funnel, product ratings;
  revenue and funnel recompute only the days touched (facts carry
  `_computed_at`, each mart a watermark on it). 60 Spark tests. On AWS the
  marts reconcile with the facts (net 82,536,511.85, 489,346 orders).
  Reading the numbers: conversion ~98 % because the generator only creates
  sessions around orders (synthetic data, spec 12); recent return rates are
  0 because returns come days later (spec 8.4, recomputed as they land); no
  row for 2026-10-02 (stack off).
- Lessons (step 7): decimal / decimal gives a decimal rate next to double
  rates; rates are now always doubles, money always decimal. I twice wrote
  convoluted placeholder code (`if False` branches) and replaced it before
  running: code Souhail reads line by line must be plain.
- Check answers (step 7), all correct: (1) days to recompute come from the
  changed fact rows' order dates (a cohort view), so yesterday is
  recomputed when its items are returned today; (2) keep everything at the
  fact's grain, filter in the mart; (3) floats cannot hold most cent
  amounts, and Spark's parallel, unordered sums make float totals differ
  between runs of the same query; decimal is exact and deterministic.
  **Proposal pending (Souhail):** mark recent days provisional in the
  revenue mart; needs a business rule (return window), which the source
  does not define.
- Next: step 8, Airflow (image as a stage of the Spark Dockerfile, metadata
  Postgres, `airflow` profile) and the `transform` DAG every 30 minutes.
- **Step 8a done** (commit 001cf12): Airflow 3.3.2 on the Spark image (uv,
  constraints minus our overridden pins, `pyspark-client` 4.1.3), metadata
  Postgres, apiserver on 127.0.0.1:8088, only the batch key in the
  scheduler, scheduler capped at 3 GiB. Idle RAM measured: ~0.9 GB.
- **Step 8b, first half** (commit f8819f7): `transform` DAG (silver ->
  gold_dims -> gold_facts -> gold_marts, SparkSubmitOperator client mode,
  `*/30`, catchup off, `max_active_runs=1`, 2 retries, 60 min timeout) and
  a CI job that installs Airflow exactly like the image and parses the DAG
  folder (4 tests; `max_active_runs=2` mutation fails them). CI green.
- **Two green runs in a row** (2026-10-05 03:32-03:59 UTC). Unpausing ran
  the latest missed slot (catchup=False skips older slots, not the last
  one); the manual run waited *queued* until it finished: one active run.
  Normal 18-min increment: **8 min 48 s** (silver 162 s, dims 91 s, facts
  203 s, marts 67 s). After 1 h without silver: 18 min (silver 533 s).

### Session 5 (cont.) — three findings from measuring the DAG

Measuring is what found them; none would have shown as a red task.

**1. Cost: ~0.67 GB downloaded from AWS per run** (container network
counters, `/proc/net/dev` read *inside* the containers: Docker Desktop's
PIDs are not visible from WSL). gold_facts alone read ~485 MB in both runs,
whatever the increment: a fixed cost means a full read. Located with a
**Spark event log** (`--conf spark.eventLog.enabled=true`, input bytes per
SQL execution and per Iceberg scan):
- lookups by random UUID (point-in-time join, the "retry unknown users"
  path, each MERGE's target) cannot skip any file: min/max of random keys
  covers everything, and with ~9 files per table, a few hundred keys hit
  them all anyway (sorting by key would not help either);
- **a bug**: `affected` / `ids` were `persist()`ed but read the fact table
  merged right after; **Spark drops a cached frame when a table in its plan
  is written**, so the logging `count()` recomputed everything from S3
  (~110 MB) and could count something other than what was merged.
- Uncommitted fix (see "Where we stopped"): repair unknown-member rows from
  the fact's own `user_id` / `created_at` (`repair_unknown_users`, reads
  only `user_sk` when there is nothing to repair); caches now read silver
  only. Facts: ~310 -> 183 MB per run; it also repaired 51 orders stuck on
  NULL forever (orders without items: the old retry recomputed NULL again).
  60 tests pass; disabling the repair fails the retry test.
- The spec's cost row ("demo volume far below 100 GB/month") is wrong at
  ~1.3 GB per hour of the airflow profile (~75 h/month). Final figure after
  the fixes, then Souhail decides (cost row / schedule / uptime).

**2. Silver is not a consistent cut.** Silver processes tables one after
another, each at the bronze snapshot current at that moment; the stream
commits every 60 s meanwhile. Run 2: 329 items (216 orders) were merged
without their orders (orders created 03:50:15-55, read by `orders` a few
seconds before the next bronze batch). Silver heals at the next run, but
gold built those facts with no user and no date, and the new repair cannot
fix them (no `user_id` to repair from). Proposed fix (inside ADR 014):
every silver run reads all tables up to **the same bronze batch** (the
newest batch id all 7 tables committed; the ids are already in the bronze
snapshot summaries), and gold re-computes facts that were built without
their order (cheap check on a null column first).

**3. Data loss: ~16 s of Postgres changes lost on 2026-10-04 at
21:18:26-41 UTC.** Found from 6 users with orders but no bronze event.
Traced step by step:
- the users are in Postgres, not in Kafka (not even aborted records:
  `read_uncommitted`), their orders are;
- Kafka record times: an event of 21:18:44 written at 21:26:37: an outage;
- all containers started 21:25:53-21:26:26 with 0 restarts: the Docker VM
  died hard (~21:18:42) and `make up` started it again;
- Kafka log 21:25:59: "Recovering 144 logs ... no clean shutdown file";
- `_connect-offsets` kept positions up to txId **1197128** (21:18:41) while
  `thelook.shop.users` kept records only up to **1196999**; streaming
  resumed at 1197139, right after the stored offset.
- **Why**: Kafka acknowledges writes before fsync (it relies on replicas;
  we have RF=1). The crash lost the unflushed tail of some partition files
  and not others: Connect's offsets survived, the data did not, so Connect
  resumed *past* the lost records. Exactly-once is atomic only if the
  broker keeps what it acknowledged.
- Impact found so far: 6 users missing, **71 of 75 order items** created in
  the window missing from silver (orders survived); updates in the window
  left stale rows that only a full-column comparison can list. MongoDB
  (events, reviews) not checked yet: same failure mode.
- Connect's container log stops at 21:18:04 and keeps nothing after the
  restart (cause unknown): Debezium's resume message is not available.

### Debugging lessons (session 5, step 8)

- **A cost that does not scale with the increment is a full read.** Two
  runs with very different increments reading the same ~485 MB said so
  before any code was read.
- Measure with the engine's own counters (event log) rather than guessing
  from code; the first guess (the events window) was wrong.
- A measurement can include a retry: the first event log had a MERGE that
  failed on a DNS outage (`UnknownHostException`) and ran twice.
- Leftover files of the same size in `silver/events` are **orphans** from
  backfill attempts killed by DNS outages: referenced by no snapshot, never
  read, still stored (step 9's `remove_orphan_files`).
- `aws s3 ls` prints local time (UTC+1 here).
- Python 3.10 on the host: no backslash quotes inside f-strings (3.12+).

### Where we stopped (2026-10-05, ~04:45 UTC) — superseded by session 6

- **Stack down cleanly** with `make down` (Kafka flushed; volumes kept).
  The `transform` DAG is **paused** in Airflow's database: `make up` with
  the airflow profile will not start runs until it is unpaused.
- **Uncommitted** (not yet commit-ready, finish with item 3 below):
  `spark/lakehouse/gold.py` (`repair_unknown_users`, caches read silver
  only), `spark/tests/test_gold_facts.py`, ADR 015 paragraph on the repair.
- **Decisions waiting for Souhail:**
  1. Kafka `log.flush.interval.messages=1` (fsync every write; measure the
     cost) + a risk-register row + runbook rule "any unclean Kafka
     shutdown -> run the verify drill". Spec change: needs approval.
  2. Repair now: pause writers, `verify_silver` lists every differing key,
     Debezium **blocking snapshot filtered by `additional-conditions`** on
     those keys only, verify again. Check MongoDB for the same window.
  3. What happened at 22:18 local time on 2026-10-04 (sleep? Docker
     Desktop update? WSL shutdown?): decides whether it can be prevented.
- **Then, in order:** Kafka setting -> repair -> silver consistent cut
  (ADR 014 amendment) + gold facts rebuilt without their order -> tests ->
  remeasure a full run (event log + network counters) -> cost decision ->
  unpause, two green runs -> step 8 check questions -> step 9 maintenance.
- Still pending from before: O8 (leaning B), E2 (before P6), the
  "provisional days" rule for the revenue mart, P7 ideas (heartbeat
  liveness table, alert on long-unresolved unknown members, oplog window).

## Session 6 — 2026-10-05 (afternoon) — P4 step 8 fixes, step 9 started

### Decisions taken

- **ADR 016 accepted**: Kafka fsyncs every write (`log.flush.interval.messages=1`)
  + risk row + runbook rule (unclean Kafka shutdown -> verify drill).
  Measured: 43,253 -> 6,907 records/s flat out; p99 2 -> 5 ms at 200/s.
  The crash cause stays unknown (Souhail): treated as unpredictable.
- **Repair now** (Souhail): done, see below.
- **Maintenance: each layer by its writer** (Souhail, ADR 017 proposed):
  the stream maintains bronze with the key it already has; the Airflow DAG
  maintains silver and gold as the batch user. No new IAM user; spec FR12
  updated.
- Mine, inside the spec: the stream's batch **ledger** and silver's
  **consistent cut** (ADR 012/014 amendments); silver's watermark by
  `ingested_at` (ADR 014 amendment); gold repairs without re-reading silver
  (ADR 015).

### Steps done

| Step | Result |
|---|---|
| Kafka fsync | live, measured, ADR 016 (9a80b88) |
| Repair of the 2026-10-04 loss | verify listed users 6 missing + 4 stale, order_items 69 + 27, events 314 missing (all from the crash window); `drills/resnapshot.py` re-sent exactly those keys (10 + 96 rows in 46 ms, 314 docs in 41 ms); all 7 silver tables identical to the sources again (ecf54df) |
| Ledger + consistent cut | `thelook_bronze.stream_batches`, one row per finished batch; silver reads one cut per run (b11298a) |
| Gold facts | unknown members repaired from the fact itself, empty MERGE skipped, items built without their order recomputed; live: 0 items without order, 0 on the unknown member (a35ca4e) |
| 9a silver watermark | `ingested_at`, migrated live with no full read (133e74a) |
| 9b stream maintenance | hourly expiry (Java API) + metadata cleanup + ledger compaction: 36 s an hour (ddcef76) |
| 9c bronze compaction + orphans | one heavier task per hourly run, in a background thread with a FAIR pool; batches keep running (d752856) |
| 9d silver/gold maintenance DAG | `jobs/maintenance.py` daily, batch user, pool `lake` shared with transform; silver MoR tables 9+33 files -> 1+0 (528e5c9) |

### Concepts covered

- **Exactly-once is atomic at the protocol level only**: with one
  unflushed broker, a crash kept Connect's offsets and lost the records
  they covered (Souhail's answer, refined: the resume point came from
  Connect's own stored offset; the slot never runs ahead of it).
- **A re-read restores current state, not history**: intermediate versions
  lost in a gap stay lost (a user changing address twice in the gap gets
  one dim_user version), and the verify drill cannot see it.
- **Commit log / ledger**: a "this batch is complete" record written last,
  so readers get a consistent cut across tables (the idea behind Delta's
  `_delta_log` or a database's commit record).
- **Iceberg metadata grows with every commit**: `metadata.json` lists every
  snapshot and is rewritten at each commit; an always-on writer slows down
  and downloads more each batch until snapshots are expired.
- **Expiry is cheap to commit, expensive to clean up**: the commit is
  metadata only; finding the files no longer referenced reads every
  manifest (~420 per table here), which is what took 41 minutes.

### Debugging lessons

- **The `filter` of a Debezium snapshot means different things**:
  PostgreSQL blocking snapshots run it as the whole SELECT (`id IN (...)`
  failed with "syntax error at or near id", logged as a WARN only);
  MongoDB parses a query document. Checked in the connector source code.
- MongoDB incremental snapshots *write* watermark documents to a signal
  collection (write access to the source): blocking ones only read.
- A test that passes on the old code proves nothing: the first expiry test
  removed one snapshot, and Iceberg still accepted the read through the
  retained snapshot's parent id; with a gap of two, the old code fails
  ("not a parent ancestor"), the new one passes.
- `datetime.UTC` written a third time (Python 3.11+, the image runs 3.10);
  ruff cannot catch an attribute: grep for it before running.
- `grep | tee` buffers: a log file can stay empty until the job ends.
- A slow stream had two causes on top of each other: concurrent silver and
  verify runs on the same network, and metadata growth. Separate them
  before fixing (batches were 37-47 s once the other jobs stopped).

### Check answers (step 9a-9b)

- (1) Conclusion right, mechanism wrong: commits cannot land out of
  `ingested_at` order (one micro-batch at a time, one atomic commit per
  table, a retry gets a later stamp). The ledger gives cross-table
  consistency, not watermark safety. The real risk, missed by both of us
  and by ADR 014's first wording (corrected): the driver's clock going
  backwards. (2) Correct; Souhail added that expiry is what physically
  removes erased personal data (P8). Precision: silver is merge-on-read,
  so erasure = delete + compaction + expiry. (3) Correct: the cost scales
  with metadata (manifests read for reachability), not with the work;
  the Java API avoids that computation; `rewrite_manifests` kept for 9d.

### Debugging lessons (9c-9d)

- **Blocking compaction cannot keep O1**: ~0.8 s per small file over the
  internet (230 files: 3.5 min), and a full day makes 1,440 files a table.
  Moved to a background thread; Spark's default FIFO scheduler would still
  queue the thread's jobs ahead of the batches, hence a FAIR pool.
- **A mutation that survived**: the "before today" filter could be removed
  without failing the test, because today's partition had only 2 files and
  Iceberg compacts groups of 5+. The test now has 6 files per day.
- **Two ways a test row avoided a delete file**: alone in its file (Iceberg
  drops the whole file, metadata only), or alone in its *partition* of a
  `local[2]` DataFrame (two rows, two files). `coalesce(1)` fixed it.
- **Dangling deletes**: after compaction applies a delete file, the file
  stays and readers still open it (deletes are matched by partition and
  sequence number, not by data file): `remove-dangling-deletes`.
- **`No FileSystem for scheme "s3"`**: orphan removal lists files through
  Hadoop's FileSystem by default, unlike every other Iceberg access
  (S3FileIO); `prefix_listing => true`. The stream would have hit it on its
  first orphan task: the rotation had not reached orphans yet.
- **`r.day` on a date** is the day of the month: a misleading name clash,
  not an Iceberg integer.
- `Failed to load catalog: thelook_bronze` warnings: procedures given
  "db.table" try "db" as a catalog first; full names remove them.
- Again: a Python text replacement silently skipped because ruff had
  reformatted the target (the assertion caught it). Re-read before editing.

### Measurements

- Stream before maintenance: batches ~110 s (60 s trigger), driver download
  9.2 MB/min; metadata.json 487 KB per busy table.
- After the first expiry (41 min, one-off): metadata.json 46 KB, batches
  34-45 s, driver download 3.2 MB/min (~0.19 GB per hour).
- Silver with the ingested_at watermark: 2.5 min for ~25 min of changes.
- **transform after all fixes** (network counters, idle baseline removed):
  catch-up run (3 h of changes) 12.5 min, ~640 MB; **normal run (~20 min
  of changes) 6.7 min, ~337 MB** (was 8.8 min, ~670 MB). Remaining reads
  are structural: MERGE target key scans and the point-in-time join on
  random UUID keys (no file can be skipped; bucketing would not help: a few
  hundred random keys hit every bucket).
- Per hour a profile is up: transform ~0.67 GB, stream ~0.19 GB; full stack
  ~0.86 GB: the 100 GB free allowance = ~116 h/month; 24/7 = ~620 GB
  (~$47/month above the allowance). Spec cost row: Souhail's decision.
- maintenance DAG first run: 2 min 53 s, serialized with transform by the
  pool. Two scheduled transform runs green (21:00, 21:30 UTC).
- Hourly expiry, same work (~31-36 snapshots per table): SQL procedure
  `expire_snapshots` 935 s (a fixed ~2.5 min per table reading every
  manifest as Spark tasks); Iceberg Java API (incremental cleanup) 36 s.
- Lesson: **measure the steady state, not just the first run**. The 41-min
  catch-up looked like a one-off; the hourly run showed the cost was fixed
  per table, which pointed at the cleanup strategy, not the backlog.

### Where we stopped (2026-10-05, ~22:00 UTC) — superseded by session 7

- Stack up (core, stream, airflow); `transform` and `maintenance` DAGs
  **unpaused** and green. Stop with `make down` (clean Kafka shutdown).
- Steps 8 and 9 done in code (commits up to 528e5c9), CI green.
- **Cost decided (Souhail):** optimize first, then stop and record. Done:
  the facts build reads only the changed orders (fallback lookup logged as
  `orders_looked_up`, 0 live) and the repair join skips items without an
  order (b59d27b). Per run ~290-300 MB (was ~670 MB at the start of the
  day): fixed cost of MERGE key scans on random keys, not cost per change.
  Full stack ~0.8 GB per hour up = ~125 h/month free. Spec cost and risk
  rows updated; profiles up only while working or demoing. Remaining ideas
  for P9: facts partitioned by order day (marts), incremental dim_user,
  skip idle gold stages.
- Lesson: Spark's "bytes read" also counts reads from Spark's own cache, so
  it overstates S3 reads (silver's increment appeared six times); network
  counters per job are the ground truth, the event log only ranks queries.
- **Waiting for Souhail:** (1) review ADR 017 (proposed); (2) check
  questions on step 9c-9d.
- **Next: step 10**, P4 proofs: reconciliation drill (verify_silver with
  writers paused), O2 measured end to end (source change -> gold), Athena
  gold queries, README/runbook/results, then the P4 checkpoint.
- Watch: the stream's first orphan task (after all 7 tables are
  compacted, ~7 hours of runtime); `web_events` compaction (~540 files) in
  the background thread; DNS / "Connection refused" blips (P9 hardening).
- Still pending from before: O8 (leaning B), E2 (before P6), the
  "provisional days" rule for the revenue mart, P7 ideas (heartbeat
  liveness table, alert on long-unresolved unknown members, oplog window).

## Session 7 — 2026-10-07 — second crash, stale ledger, reconciliation

### What happened

- The stack had been left up; the laptop slept at ~03:35 UTC on 10-06 and
  the Docker VM died on wake (all containers exit 255; Kafka: "no clean
  shutdown file"). Cost Explorer: 3.28 GB of S3 transfer on 10-06, i.e.
  ~4 h of running, not the 30+ h first feared (October so far: 14.4 GB of
  100). Lesson: "stack up" is not "stack running"; read the ledger or the
  stream's batches before estimating.
- Runbook procedure (ADR 016 rule): writers paused, bronze caught up
  (batch 894 replayed, every table "skipped(replay)": the replay guard),
  silver, verify. **All 7 tables identical, 6.16 M rows: fsync held, no
  loss this time.** This is also P4's reconciliation proof (results.md).

### Debugging lessons

- **A stale cache, found by a log line that did not add up**: silver's
  `ingested_upto` for users stopped at 10-06 03:31 while orders reached
  today. The ledger had recorded batch 894's users snapshot in batch 895's
  row. Cause: `current_snapshot()` read Spark's cached copy of the table,
  which stays alive while in use and lagged behind commits the maintenance
  thread made through its own table object (3 stale rows in ~500, each
  right after a maintenance run). Fix: refresh from the catalog (Iceberg
  Java API); a test commits behind the cache and fails on the old code.
  The same bug explains the items built without their order on 10-05.
- An S3 open failure surfaced as a Spark `INTERNAL_ERROR` (`"this.stream"
  is null`) while planning a MERGE: retried now as transient.
- PyArrow's S3 connect timeout (3.1 s, DNS included) aborted the verify
  twice on this laptop's resolver: 30 s in the drill.

### Check answers (step 9c-9d)

- (1) Correct: appends add files, compaction replaces others, no overlap;
  a MERGE's deletes target the files compaction replaces (Souhail: a late
  compaction would resurrect rows the MERGE deleted). Our mechanism is the
  one-slot pool, not "same DAG". (2) Correct, except "tighter statistics":
  one compacted file spans every key and every `_merged_at` (one full read
  of it by the next gold run). (3) Correct and complete: an orphan and a
  file of an uncommitted write look identical; only age tells them apart.

### Later on 2026-10-07

- **Analyst user** (`thelook-analyst`, plan approved by Souhail): read-only
  Athena SQL in the `thelook` workgroup for DBeaver; 4 calls allowed and 6
  denied as expected with a temporary key (deleted). Runbook: key creation,
  DBeaver settings. Explained: DBeaver draws no relationships because
  neither the source nor Iceberg declares foreign keys; virtual foreign keys
  in DBeaver (facts point to `dim_user.user_sk`, not `user_id`).
- **Project page** (private artifact): architecture, phases, layers, the
  four findings with charts, freshness, lessons, ADRs. O2 still shown as
  "measuring": update it when measured.
- **O2 not measured yet.** Two attempts stopped: the laptop's sleep left a
  maintenance attempt and a transform run half-done; Airflow recovered on its
  own (zombie detection, retry), but the retried maintenance held the `lake`
  pool for 39 min, so transform ran ~40 min late. Cause, from the task log:
  gold fact compactions ran as one Spark task each (one file group), the
  slowest 20 min (`fct_sessions`). Fix committed (6b4d8f3): compact delete
  files before data files. Measure the next maintenance run's duration.
- **Airflow UI could not show task logs** (each component had its own random
  `[api] secret_key`): fixed in compose + `AIRFLOW_SECRET_KEY` in
  `onprem/.env` (f9746f9); takes effect at the next `make up` with airflow.
- `drills/gold_freshness.py` (O2 drill) and the README's P4 sections are in.
- ADR 017 accepted (Souhail).

### Where we stopped (2026-10-07, ~12:10 UTC) — superseded below

- Stack down cleanly; next was O2, then the P4 checkpoint.

## Session 8 — 2026-10-07 (afternoon) — logs fixed, P4 checkpoint

- Kafka started clean (yesterday's `make down`). Airflow task logs fixed
  for good: shared `[api] secret_key` (f9746f9) and a named volume
  `airflow-logs` mounted in the scheduler, api-server and dag-processor
  (b2ee8f8), so logs survive `make down` and the UI reads the files.
- **O2: third attempt failed, for two reasons.** (1) The drill died on a
  DNS outage ("Temporary failure in name resolution" for Athena): it had
  no retry; now a failed poll is retried 30 s later. (2) The laptop slept
  ~14:30-18:10 UTC: the 14:30 run started at 18:11, so both sampled orders
  (14:01, 14:27) waited ~4 h; a measurement of the laptop, not the pipeline.
  O2 needs ~45 min with the laptop awake.

### P4 checkpoint (answers without the code)

- (1) End to end: excellent, every hop named by its mechanism. Refinements:
  the old order is never recomputed (only dim_user is rebuilt; `user_sk` =
  hash(user_id, LSN) is the same at every rebuild); no-op updates do not
  open a version.
- (2) The 16 seconds: excellent (cause, trace, repair, measured fsync cost,
  runbook rule). Date: the second crash was on wake on 10-07.
- (3) Merge-on-read: correct. Add: maintenance order matters (delete files
  before data files: the 20-min compaction), and "between MERGEs" is
  enforced by the one-slot `lake` pool, not by timing.
- (4) 300 MB floor: right diagnosis. Qualified: partitioning by order day
  helps only if changes cluster by date (real shops; not this generator);
  bloom filters and key sorting do not help a MERGE over ~10,000 scattered
  keys (nearly every row group is hit). Decisive fix: compute in the same
  region as the data. "The layout can only prune if the change pattern
  correlates with it; otherwise move the compute to the data."
- (5) Proving correctness: excellent (stale rows with correct counts).
- **Checkpoint passed.** P4 closes once O2 is measured.

### O2 measured, P4 closed (2026-10-07, ~20:10 UTC)

- **O2 passed**: worst case 36.2 min (order committed just after the 19:30
  run read its cut, in gold through the 20:00 run), best case 10.1 min (3
  min before a slot). Bound: schedule + one run. Results in results.md.
- Same afternoon, what can break it: the laptop slept 4 h (runs resumed on
  their own, the stream restarted by its restart policy, the VM was paused,
  not killed: no unclean Kafka shutdown); one run's silver needed 3 tries
  for DNS outages (33 min late). DNS hardening is P9's first item.
- **P4 is done**: silver, gold, Airflow, maintenance, analyst access;
  acceptance = reconciliation (6.16 M rows identical), all tests (CI green),
  O2 < 1 h; checkpoint passed.

### Where we stopped (2026-10-07, ~20:10 UTC) — superseded by session 9

- P4 closed. **Next: P5** (Redis + FastAPI serving layer, ADR 010): plan it
  first, as for P4.
- Watch: the next maintenance run's duration (delete files compacted
  first); DNS outages (P9).
- Still pending: O8 (leaning B), E2 (before P6), "provisional days" rule,
  P7 ideas, P9 ideas (DNS hardening first; facts partitioned by day,
  incremental dim_user, skip idle gold stages; a lighter first maintenance
  after downtime).

## Session 9 — 2026-10-09 — P5 planned

P5 plan approved (Redis online features + `GET /users/{id}/features`; 11
steps, 0 to 10).

### Decisions taken

- **The features stream is its own Spark application in local mode**
  (Souhail, option C of ADR 018), not a second query in `bronze_stream.py`
  (FR15 as written, spec v2.1). Why: no AWS key in it at all; it cannot slow
  bronze (O1); the worker's 4 cores are already full while a batch job runs
  (stream 2 + batch 2), so an app on the cluster would wait minutes and miss
  the 1-minute target. Cost: one more driver JVM (~0.7 GB).
- **ADR 018 proposed**: key design (`user:{id}:viewed|events|session|cart:{sid}`,
  sorted sets with `ZADD GT` and trims, read-time window count), TTLs from
  event time (`EXPIREAT` NX then GT; 72 h = Kafka retention, 1 h for
  events), `foreachPartition` pipelines, trigger 10 s, `failOnDataLoss=false`
  (the opposite of bronze, and why), Redis 8 with AOF everysec, `volatile-ttl`,
  ACL users `features` / `api`.

### P5 plan (agreed)

| # | Commit | Content / how we verify |
|---|---|---|
| 0 | none | Stack up; check the next maintenance run's duration (P4 watch item) |
| 1 | `docs: ADR 018 online features (proposed)` | ADR 018, spec v2.1, ADR 007 amendment note |
| 2 | `feat(serving): redis with ACL users` | `serving` profile, pinned Redis 8, AOF, maxmemory, ACLs; NOPERM proven |
| 3 | `feat(spark): user feature rows` | `lakehouse/features.py`, chispa tests |
| 4 | `feat(spark): idempotent redis writes` | fakeredis tests: replay, reverse order, TTL on every key, GT trap |
| 5 | `feat(spark): features stream job` | job, Dockerfile `features` stage, compose service; live keys |
| 6 | `feat(api): GET /users/{id}/features` | FastAPI app, tests (200/404/503), CI matrix |
| 7 | `feat(serving): api container` | Dockerfile, compose, healthcheck, `/docs` |
| 8 | `test(drills): feature freshness` | change stream → poll API; < 60 s per sample. **P5 acceptance** |
| 9 | `test(drills): redis outage and rebuild` | bronze unaffected by an outage; rebuild = same keys and TTLs |
| 10 | `docs: P5 wrap-up` | README, runbook, results, learning log; P5 checkpoint |

### Check answers (step 1)

- **ADR 018 accepted** (Souhail).
- (1) Why `INCR` is wrong for "events in the last hour": correct and
  complete (not a window: only grows, a TTL wipes it all at once; counts by
  arrival, not event time; not idempotent under replay). **Correction** of
  the proposed fix ("per-minute buckets, SET the absolute value"): a
  micro-batch sees only its own events, and one minute's events can span two
  batches, so the second SET would overwrite with a partial count. An
  absolute value needs a running total somewhere: Spark stateful aggregation
  (window + watermark, state in the checkpoint) is a valid alternative. Ours
  keeps the state in Redis (the set of event ids): idempotent with no Spark
  state, exact sliding hour; costs memory per event. Buckets win at
  thousands of events per user per hour.
- (2) `failOnDataLoss`: correct (bronze = the only full history, a gap is
  permanent and spreads; features = freshness). Stronger argument added:
  Kafka deletes whole segments only once older than retention, so any
  missing record is >= 72 h old and every feature it would have made has
  already expired: the skip is invisible to users. Souhail's point kept:
  the skip must be visible (Spark logs it; alert in P7).

### Steps done

| Step | Result |
|---|---|
| 1 | ADR 018 accepted, spec v2.1 |
| 2 | Redis 8.10.2 in the `serving` profile; ACLs proven (NOAUTH without login; `features` NOPERM on SET/ZRANGE/FLUSHALL/other keys; `api` NOPERM on ZADD/DEL/KEYS/CONFIG); `ZADD GT 50` on score 100 returned 0; ~15 MB empty. Added an `admin` user for operations. The image's entrypoint loads 4 bundled modules: our start script runs `redis-server` itself without them |
| 3 | `lakehouse/features.py` (`feature_events`, `redis_rows`) + 14 chispa tests; 88 Spark tests pass |
| 4 | `lakehouse/redis_writer.py` + 9 fakeredis tests (replay, either order, GT, trims, window, expiry extended never shortened, GT-on-new-key trap, chunking); the same checks repeated on the real Redis 8.10.2 as the `features` user: identical. ACL `+expireat` -> `+pexpireat` (times are in ms) |
| 5 | `features-stream` live (core + serving only, no AWS): skipped the 1.8 M offsets retention deleted (36 empty batches of 50,000), caught up 470k records in ~10 batches of ~4.5 s, then ~200 records per 10 s batch in ~2 s. Redis: 175k keys, 35 MB; every key's expiry = its newest event + 72 h (checked). Container 1.3 GB (2.3 GB before capping CPUs) |
| 6 | `api/` FastAPI app: `GET /users/{id}/features` (id parsed as a UUID: 422 if malformed, any case finds the same keys), 404 unknown, 503 Redis down, `/health`; 9 tests (TestClient + fakeredis, fixed clock); CI matrix gains `api`. Checked against the real Redis filled by the stream: 200 with live features, ~3.5 ms per request in-process. Pins: newest releases at least a week old (FastAPI 0.142.2, not 0.143.0 of the day before); `httpx2` for Starlette's TestClient (it deprecates `httpx`) |
| 7 | API container (same pinned Python base as the simulators, hashed lock, non-root, `/health` healthcheck): 42 MB; 200 in ~4 ms through the container; 404, 422, `/docs` OK. A Redis outage: 503 in 1.0 s (was 59.7 s, see below); the API goes unhealthy and recovers on its own; the features stream outlived its retries, restarted from its checkpoint and caught up (5,076 records in one batch) |
| 8 | **P5 acceptance passed**: `drills/features_freshness.py`, 10 samples 4.5-10.6 s from MongoDB commit to the API (target < 60 s); API p50 2.0 ms, p99 4.2 ms. Results in results.md |
| 9 | Outage drill: API 503 in 1.0 s, features stream restarted 3 times and wrote again 3.2 s after Redis returned, bronze kept committing (no error, no restart). Rebuild drill: **224,587 of 224,587 servable keys identical** to the live ones, expiry to the ms; rebuild ~138 s; peak 1.4 GiB -> `mem_limit: 2g`. Bronze slower than P4 tonight (84-95 s vs 34-45 s): S3 round trips median 890 ms, a degraded link (P9) |
| 10 | README (status, diagram, components, layout, RAM, run and demo P5, ADR 018), runbook (online features: 503/restarts, rebuild, skipped offsets, password rotation, memory), spec v2.1 (5.3 versions pinned, FR15 72 h window) |

### Debugging lessons (step 3)

- **Spark 4 runs in ANSI mode**: `CAST('' AS BIGINT)` raises
  `CAST_INVALID_INPUT` instead of returning null (Spark 3). A product URI
  that does not match the regex gives `''`; with a plain cast, one such event
  would fail its micro-batch on every replay (a poison record: the stream
  never gets past it). `try_cast` returns null. Caught by a test written for
  the malformed case.
- In-batch pre-trimming must break ties as Redis does (equal scores ordered
  by member bytes), or Spark and Redis could keep different "10 newest".

### Check answers (steps 3-4)

- (1) One `expire_at_ms` per key: right conclusion (a TTL belongs to the
  key). Refined: with NX then GT, per-row expiries would also converge to
  the max, so "last write wins" is not our risk. The real reasons: a cart
  expires with its session's newest event of any type; and the "already
  expired" filter must judge the key, or a rebuild would drop old members
  of a live key that the incremental stream had kept (rebuild != live).
  Souhail wrote PEXPIREAT: correct, our times are ms (the writer uses it).
- (2) Keeping `u` events: "inflates the counts" is wrong here (members are
  event `_id`s, a `u` re-adds the same member). Correct and decisive: an
  update that changes time, user or session leaves the old contribution
  (GT cannot lower a score; the old user's key keeps the member).

### Debugging lessons (step 5)

- **Startup order**: the first batch got `Connection refused` from Schema
  Registry, still starting; `features-stream` only waited for Redis. Fix:
  `depends_on` Kafka and the registry with `required: false` (they are in
  another profile).
- **The skip path we designed exposed an empty-input bug in shared code.**
  Kafka had been down 2 days; at startup the topic still held segments
  older than 72 h (retention runs every 5 min), so Spark planned batches on
  them, then Kafka deleted them. `failOnDataLoss=false` skipped them and the
  batches came back empty; `decode_by_schema` returned an untyped (VOID)
  column and the query failed at analysis, on every restart (17). Method:
  traceback -> "no schema id in the batch" -> the checkpoint's planned
  offsets (2.88 M) vs the topic's earliest (4.65 M). Latent in bronze too
  (a batch of only tombstones). Fix: `bronze_rows` returns None; a test
  reproduces the production error on the old code (`ba7b830`).
- Spark does not jump over a lost range: it walks it at
  `maxOffsetsPerTrigger` per batch (36 empty batches here). Spark's own
  warning ("Some data may be lost. Recovering from the earliest offset")
  is the visible trace Souhail asked for.
- **`--driver-memory` caps the heap, not the process**: 2.2 GB resident for
  a 768 MB heap (356 MB used). The VM exposes 32 CPUs and the JVM sizes GC
  and JIT threads by CPU count; glibc keeps up to 8 malloc arenas per CPU.
  `cpus: 2` + `MALLOC_ARENA_MAX=2`: 1.3 GB (caveat: measured on live
  batches, not a catch-up; step 9 measures the catch-up peak, then a
  `mem_limit`).
- User ids are TEXT UUIDs (source schema): the API's `{id}` is a string.

### Check answers (step 5)

- (1) Crash loop and `None`: correct and complete, including why it
  looped (offsets/N written before the batch, commits/N only after it
  succeeds, so every restart re-ran the same empty batch) and why a
  fabricated schema would be worse in bronze (tables created or appended
  with a guessed schema).
- (2) `cpus: 2` vs `nproc` 32: correct (a CFS quota, `cpu.max = 200000
  100000`, not an affinity mask; the JVM's container support reads the
  quota; glibc does not). Verified on the live JVM: `CPU: total 32
  (initial active 2)`.

### Debugging lessons (step 7)

- **A 503 that took 60 s.** Stopping Redis, the API answered 503 after
  59.7 s despite 1 s socket timeouts. Measured each suspect separately
  instead of guessing: redis-py 8 retries **10 times** with exponential
  backoff by default (59.9 s); and a stopped container's name leaves
  Docker's DNS, which forwards it to the host resolver: 8.0 s before "Name
  or service not known" (the connect timeout starts after the lookup).
  Fixes: no retries in the API's client (the 503 tells the caller to
  retry; retrying inside the request only hides the outage behind a slow
  answer); `dns_opt: timeout:1, attempts:1` on the API container (names of
  running containers are answered locally at once). Result: 1.0 s.
- The stream keeps its retries on purpose: a background, idempotent
  writer should ride out a blip; an API with a caller waiting should not.
- Check answer (step 6, 503 vs 404/empty 200): correct; added that a 503
  lets the site degrade on purpose (hide the block) instead of showing
  generic content with confidence.

### Debugging lessons (step 8)

- **The drill was wrong twice before the pipeline was measured once.**
  Run 1: "not served after 120 s", yet Redis had the view with the right
  score. Checked the pipeline first (Redis, API by hand: `seen()` True),
  then the timing: pymongo returns naive UTC datetimes, and `.timestamp()`
  reads a naive datetime as *local* time (UTC+1): every commit looked an
  hour old, the timeout fired before the first poll. Fix: `tz_aware=True`.
  Run 2: clock offset -3600 s and every latency negative, and the drill
  **passed** (-3590 < 60). `Database.command()` does not inherit the
  client's `tz_aware`. Fixes: pass the client's codec options; fail on an
  impossible (negative) latency. Lesson: a check that accepts impossible
  values is not a check.
- The samples are phase-locked to the trigger (each starts just after a
  batch), so they measure close to the worst case: report the bound
  (trigger + batch + capture), not only the median.

### Design flaw found before step 9 (ADR 018 amendment, proposed)

- Thinking through what "rebuild = live" should mean: a key's TTL is
  extended by every new view, so an active user's viewed key never expires,
  and a member leaves only when 10 newer products push it out. An old view
  could stay for weeks: FR15's "TTL bounds personal data" held per key, not
  per view, and a view older than Kafka's 72 h cannot be rebuilt. Fix:
  a 72 h window on `viewed` (trimmed on every write, filtered at read time
  in the API, since a quiet user gets no trimming writes). Tests in Spark
  and in the API. **Lesson: a TTL bounds a key, not what is inside it.**

### Debugging lessons (step 9)

- **A watcher that never fired** (bronze "caught up" = a batch under 60 s):
  bronze was steady but at ~85 s per batch, a regression against P4's
  34-45 s. Ruled out in order: P5 (no AWS access), memory (4.5 GB free, no
  swap), DNS (10-90 ms now; the UnknownHost errors were only at 23:23),
  then measured the link: S3 HTTPS median 890 ms. A batch makes dozens of
  sequential S3/Glue calls, so latency multiplies.
- **Not overclaiming**: one slow bronze batch during the outage was
  reported as indistinguishable from network noise, with the timeline
  that shows the drift began before the outage.
- Same time-zone trap as step 8, caught before running: `time.mktime`
  reads local time; container logs are UTC (`calendar.timegm`). And
  `docker volume rm` refuses a volume a stopped container still holds.
- Bronze's first batch after `make down` was a replay (batch 1161): the
  PostgreSQL tables were skipped (`skipped(replay)`), the MongoDB tables
  appended: the P3 replay guard after a 2-day pause.

### CI was red since 2026-10-07 (found 2026-10-09, Souhail asked)

- **Symptom**: every push since `52f037b` (10-07 12:09) failed the `lint`
  job; the test jobs passed. **Cause**: `drills/gold_freshness.py` (the O2
  drill) was not `ruff format`-clean. I ran ruff on the folder I was
  working in (`spark/`), CI runs it on the whole repository (`.`), and I
  did not read the CI result after pushing. **Correction**: P4's closing
  note says "all tests (CI green)": the tests were green, `lint` was not.
  Fixed in `a644f4c`.
- **Rule from now on**: before each commit, `uvx ruff@0.16.10 check .` and
  `format --check .` from the repository root (what CI runs); after each
  push, check the run (`gh run list`).
- Separately, the last run's `alert-rules` job failed on Docker Hub's
  unauthenticated pull rate limit (GitHub runners share IPs): not our code,
  re-run. P9 idea: pull CI images from a registry without that limit.

### Where we stopped (2026-10-09)

### Decisions taken under Souhail's delegation ("go with the decisions you deem right")

- **Step 0 carried forward**, with a measured reason: tonight the whole
  link is degraded (TCP connect to S3: us-east-1 median 418 ms, Paris
  99 ms, Spain 195 ms, Ireland 269 ms with a 1.4 s spike), so a
  maintenance duration measured now would mostly measure the network.
  Next daytime session: bronze baseline + maintenance run + latency.
- **Stream profile stopped** after step 9 (bronze committed batch 1184
  cleanly); **`make down`** at the end of the session (laptop sleep killed
  the VM twice in P4).
- **Not taken**: ADR 018 amendment acceptance. The working agreement says
  Souhail approves each ADR; the fix stays deployed (it corrects an FR15
  violation) and its approval is part of the P5 checkpoint.

### Ideas kept

- P7: alert on offsets skipped by the features stream; a shared key
  contract between the features writer and the API (today a docstring
  copied in both); alert on features-stream restarts.
- P9: daytime network + bronze baseline (record latency with durations);
  Paris (eu-west-3) is ~4x closer than us-east-1 from here (a region move
  would be a spec change, budget priced in us-east-1); CI images from a
  registry without Docker Hub's anonymous pull limit; DNS hardening.

### Where we stopped (2026-10-10, ~00:30 UTC)

- **P5 built**: steps 1-10 done, acceptance passed (freshness 4.5-10.6 s,
  target < 60 s; endpoint tests pass), outage and rebuild drills passed.
  Stack down (`make down`).
- `make down` first failed: "failed to read onprem/.env: line 1: key cannot
  contain a space". A VS Code database extension had prepended
  `-- Active: <connection>@@localhost@5432@thelook` to `.env` (opened with
  a PostgreSQL connection active). Removed that one line; nothing else had
  changed (32 lines, as after the Redis passwords). Avoid opening `.env`
  while that extension has an active connection.
- **P5 checkpoint passed** (answers below). Waiting: Souhail's explicit
  decision on the ADR 018 amendment. Then P6 (plan it first).
- Carried: step 0 (Airflow maintenance duration, daytime); step 8 and 9
  check questions (folded into the checkpoint); O8 (leaning B), E2 (before
  P6), "provisional days" rule.

### P5 checkpoint questions (asked 2026-10-10)

1. A user views a product: walk every hop to the API's answer, and name
   the mechanism that keeps each hop correct.
2. The features stream dies after writing half of a batch's partitions to
   Redis. Why is nothing lost or counted twice? How does bronze reach the
   same result, and why does it need a different mechanism?
3. Why a separate Spark application in local mode rather than a second
   query in the bronze job? What did that choice cost?
4. "TTLs are 72 hours, so personal data lives at most 72 hours in Redis."
   True? What had to change, and what still escapes that bound?
5. The rebuild drill found 224,587 of 224,587 keys identical, expiry
   included. Which design choices make exact equality possible?
- P7 idea added: alert on offsets skipped by the features stream.
- Still pending from before: O8 (leaning B), E2 (before P6), "provisional
  days" rule, P7 ideas, P9 ideas (DNS hardening first).

### P5 checkpoint (answers without the code) — passed

- (1) Hop by hop: excellent, the right mechanism at every hop. Added: ties
  broken as Redis does (member bytes), so Spark's pre-trim and Redis's trim
  keep the same 10.
- (2) Half-written batch: excellent (replay + max/union/absolute expiry;
  bronze needs a replay guard because an Iceberg append is not idempotent).
  Precise version: `offsets/N` before the batch, `commits/N` after
  `foreachBatch` returns; bronze's guard is per table (batch 1161).
- (3) Separate local-mode application: excellent, cost included.
- (4) 72 h TTL: very good, including the flaw and its fix. Souhail's doubt
  about the AOF **verified** on a throwaway Redis 8.10.2: an expired key
  gets a synthesized `DEL` in the AOF, but its value stays in the file until
  a rewrite, which is automatic only once the AOF doubled and passed 64 MB;
  `BGREWRITEAOF` removes it. **P8: erasure runs `BGREWRITEAOF` after
  deleting `user:{id}:*`.** Added: Kafka holds the events 72 h (FR9's
  documented bound).
- (5) Exact rebuild: excellent. Added: the rebuild grouped events into
  entirely different batches (50,000 per batch vs ~200 per 10 s), so the
  equality proves order independence across batch boundaries.

## Session 10 — 2026-10-10 (night) — P6 planned

P6 plan approved (Neo4j co-purchase graph +
`GET /products/{id}/recommendations`; 12 steps, 0 to 11).

### Decisions taken

- **E2 closed (Souhail)**: companion products + popularity skew (spec v2.2,
  4.3). Each product gets 5 fixed companions in its category; the first
  item of an order follows Zipf weights (exponent 0.8). Rejected: companions
  only (weights stay at 1-3 for days), no change (the ranking is never
  shown).
- **ADR 018 amendment accepted** (Souhail): the 72 h window on `viewed`.
- **ADR 019 proposed**: pairs over all orders (any status), one relationship
  per unordered pair, full recompute every run (erasure-proof; `+=` is not
  idempotent), upsert tagged with the run id then delete older runs (no
  empty graph; Community has one database, so no swap), one writer from the
  Spark driver (parallel `MERGE`s on shared nodes deadlock), daily `graph`
  DAG in the `lake` pool, Neo4j 2026.08.1 Community, one database user.
- **Versions** (newest at least a week old): Neo4j 2026.08.1 (2026.09.0 was
  4 days old), driver `neo4j` 6.3.1 (6.4.0 was 5 days old).

### Concepts covered

- **Reproducible randomness**: Python guarantees only that `random()`
  repeats across versions, not `sample()` or `choice()`. Companions come
  from `sha256("<id>:<candidate>")`, so the drill (or Spark SQL) can
  recompute them anywhere.
- **Neo4j Community has no roles**: every user is an implied
  administrator, so a second "read-only" user would add a password, not a
  boundary. `execute_read` in the API guards against mistakes only.
- **Sizing a synthetic skew**: exponent 1 gives the top product 9.2 % of
  all orders; 0.8 gives 2.9 % and still ~1.3 companion-pair hits per hour
  for the 20th product.

### P6 plan (agreed)

| # | Commit | Content / how we verify |
|---|---|---|
| 0 | none | Daytime: P5 step 0 (bronze baseline, maintenance duration, S3 latency) |
| 1 | `docs: ADR 019 co-purchase graph (proposed)` | ADR 019, ADR 018 accepted, spec v2.2, versions |
| 2 | `feat(generator): companion products and popularity skew` | Tests; restart early so data accumulates; companion share ~0.6 and top product ~3 % in Postgres |
| 3 | `feat(serving): neo4j` | Compose service, volume, `.env`; RAM measured; login / wrong password |
| 4 | `feat(spark): co-purchase pairs` | `lakehouse/graph.py` + chispa tests |
| 5 | `feat(spark): idempotent graph load` | `lakehouse/graph_writer.py`; real-Neo4j tests: rerun identical, crash then rerun converges, weights go down |
| 6 | `feat(spark): graph job` | `jobs/graph.py`, driver in the airflow image; one manual run |
| 7 | `feat(airflow): graph dag` | Daily, pool `lake`; DAG tests; one triggered run |
| 8 | `feat(api): GET /products/{id}/recommendations` | 200 / `[]` / 404 / 422 / 503 tests, `/health` both stores |
| 9 | `feat(serving): api reaches neo4j` | Through the container; 503 time with Neo4j stopped |
| 10 | `test(drills): recommendations follow the affinity` | Top 20 products: >= 4 of top 5 are companions. **P6 acceptance** |
| 11 | `docs: P6 wrap-up` | README, runbook, results, log; P6 checkpoint |

### Check answers (step 1)

- **ADR 019 accepted** (Souhail).
- (a) Why not add each order's pairs to the weights: right conclusion
  (weights must go down; `+=` counts a replayed batch twice; a rebuild from
  gold handles retries and deletions for free). **Corrections**: cancellations
  and returns do not lower our weights (ADR 019 counts every status), and
  items are never removed from an order (the generator never deletes; only
  erasure does). The decisive reasons are erasure (P8) and idempotence. The
  rebuild is cheap because of its input (~1 M gold rows, one self-join),
  not because the graph is small.
- (b) Write before delete: correct and complete. Added: each 10k batch is
  one transaction, so the API sees whole batches; a half-done run can
  briefly reorder a top 5, and pairs of erased orders stay until the next
  successful run (no personal data in them).

### Steps done

| Step | Result |
|---|---|
| 1 | ADR 019 accepted, spec v2.2, ADR 018 amendment accepted; Neo4j 2026.08.1, driver 6.3.1 |
| 2 | Generator: `companion_products` (sha256 rule), `popularity_ranking` (Zipf 0.8), `pick_order_products`; 6 new tests (12 pass), the rule pinned by a literal. Live, 2,700 orders since the restart (01:46 UTC): companion share 0.574 in 2-item orders (expected 0.6, ±0.021), top product 2.70 % of 1-item orders (expected 2.9 %, ±0.39); companions and ranking recomputed **in SQL** (`sha256(convert_to(...))`): same top product (27809) as Python. Rate ~12,100 orders/h, not the 18,000 assumed (ADR 019 corrected: ~0.9 companion hits/h for the 20th product) |
| 3 | Neo4j 2026.08.1 Community in `serving`: login OK, wrong password denied, unauthenticated HTTP 401. Idle 1.02 GiB / 119 threads -> `cpus: 2`: 0.8 GiB / 60 threads (the P5 JVM lesson again); `mem_limit` 1.5 GB provisional until the load (step 6) |
| 4 | `lakehouse/graph.py` (`product_nodes`, `co_purchase_pairs`) + 6 chispa tests; removing the `distinct` fails the "same product twice" test |
| 5 | `lakehouse/graph_writer.py` (`load_graph`: constraint, UNWIND upserts stamped with the run id, stale pairs then stale nodes deleted `IN TRANSACTIONS`) + 7 tests on a **throwaway** Neo4j (`make test-graph`, `NEO4J_TEST_URI`: the tests wipe the database, and Community has only one). Rerun = same fingerprint; crash keeps the old graph, rerun = clean load; weights go down; unknown product skipped. Disabling the stale delete fails 2 tests |
| 6 | `jobs/graph.py`; Neo4j client in the airflow stage's system Python (drivers run there; the cluster image stays clean); scheduler gets `NEO4J_*`. First run on real gold: **491,014 pairs (max weight 43), 29,120 nodes, 0 skipped, 55 s** (21.8 s pairs, 33.0 s load). Top product 27809: 5 Swim neighbours at weights 34-43, next one 2, and they are **exactly** its `companion_products`. Second run: same fingerprint, 0 stale. Neo4j peak 926 MiB -> 1.5 GB limit kept |
| 7 | `graph` DAG (04:00 UTC, pool `lake`, 2 retries); triggered: success in 64 s, same fingerprint (third run) |
| 8 | `GET /products/{id}/recommendations` (limit 1-20, ties by id, 404 / `[]` / 422 / 503), `GraphStore` + `get_graph` (1 s connect, 1 s pool wait, no retries; driver defaults 30 / 60 / 30 s, read in its source), 2 s query timeout, `/health` names the store that is down. 8 tests on a fake (CI) + 5 on the real query (`make test-graph`). Removing the `CASE ... null` fails only the real-query test: a fake reproduces what I think the query does, not what it does |

### Debugging lessons (step 2)

- **Check the check (again)**: my first SQL took each order's first item
  by `ctid` (physical row position). A status update rewrites a row at a
  new `ctid`, so for updated orders the "first" item could be any item.
  Fixed by measuring on 2-item orders, where "one is the other's
  companion" needs no order (the P2 method).
- **A CTE inside a correlated `EXISTS` re-runs per row**: PostgreSQL inlines
  a CTE used once, so the ~1 M-hash companion computation ran for every
  order (cancelled after 2 min). Materialised in a temp table: 8 s.
- A test that measured 20,000 uniform orders spent 7 s hashing companions
  for 20,000 distinct products; checking 2,000 of them proves the same.

### Debugging lessons (step 5)

- **A wrong test expectation, not a wrong writer**: "a pair with a product
  that left dim_product is skipped" failed (skipped = 0). The product was
  still a node while the pairs were written (stale nodes are deleted last),
  so the pair was written, then removed by `DETACH DELETE` with its node.
  The final graph was right; the test now asserts that, and "skipped" is
  tested with a product that never existed.

### Check answers (steps 4-5)

- (1) Dedup and `<`: correct (weight = orders, not item combinations; `<`
  gives one canonical pair and excludes (A, A)). **Precision**: only
  separate item rows of the same product multiply in the join; `quantity`
  is a column of one row, so it never does. My own comment in `graph.py`
  said "(or quantity > 1)": wrong, fixed. With `<>`, our `groupBy` on
  ordered columns gives two rows (A, B, w) and (B, A, w), each with the
  right weight; weights double only if then grouped by unordered pair.
- (2) Driver writer vs Redis executors: lock contention on hub products is
  correct and decisive. **Weaker point**: "only one coordinator knows when
  to delete". `foreachPartition` is an action that returns only after
  every task finished, so the driver could delete afterwards; the leftover
  risk is a speculative attempt being killed mid-transaction. Added: Redis
  commands are atomic on one thread with no locks held across commands, so
  even overlapping writes to one key would be safe.

### Session 10, continued (2026-10-10, ~02:40 UTC): steps 0 and 6 started

- Souhail: the network is good now. Measured, not assumed: TCP connect to
  `s3.us-east-1` median 192 ms (max 993), Glue 153 ms (max 746), vs
  418 ms on the degraded night of 10-09. `core stream airflow` up (8 GB
  free); `features-stream` left off (not needed for P6, 2 GB).
- Bronze: batch 1185 was a replay (every table `skipped(replay)`, the P3
  guard), 1186 the backlog (155 s).
- Airflow: the transform run of 10-07 21:00, cut by `make down`, resumed
  its `silver` task at start-up; `max_active_runs=1` makes later runs wait
  for it. Maintenance (step 0) queued behind it in the `lake` pool.
- **Repeated a logged mistake**: `pkill -f <pattern>` matched my own shell
  (exit 144), as in P1 and P2. Stop background tasks by their task id or by
  PID, never by pattern.
- My first watcher grepped a log format that does not exist (it would
  have waited 40 min for nothing): checked one real line first the second
  time.

### Step 0 (P5 carry-over): a measured failure, not a baseline

- Maintenance (3 days of commits to compact) ran 60 min and was **killed by
  its own `execution_timeout`** (03:09 -> 04:09). Silver: 1,861 s,
  including two `UnknownHostException` on the bucket's S3 hostname (DNS),
  each re-running the whole silver pass; gold stuck on one
  `fct_order_items` compaction task (4th and last attempt), reading S3 at
  a trickle: a socket timeout fires only when *nothing* arrives.
- The link fell during the session: S3 connect 192 ms (02:43) -> 2.2 s,
  general download 73 KB/s (03:57), then back to ~170-210 ms (04:03-04:13).
  Bronze batches 62 s -> 442 s at worst (O1 exceeded for that batch), back
  to 75-100 s.
- Method: the stack dump showed the task in `S3InputStream.readFully`, so
  "slow", not "hung"; the worker's receive rate (~100 KiB/s) and a plain
  download test pointed at the link, not Spark. The first dump was the
  wrong executor (bronze's): one executor per application on the worker,
  pick it by application id.
- **Weakness found**: `jobs/maintenance.py` retries a whole database, so one
  DNS blip repeats every table, and the final report shows only the last
  attempt (tables done earlier report 0 files rewritten). Fix: retry per
  table. Kept for P9 with DNS hardening (outside P6), unless Souhail wants
  it now.
- **Then the clean figures**: the retry of that run succeeded in 7 min 15 s
  (04:40-04:47, most of silver already compacted); **today's daily run:
  2 min 36 s** (silver 68.6 s, gold 62.7 s; S3 connect 282 ms), against
  3.5 min on 10-07. It waited 04:47-05:16 for the `lake` slot (transform).
  **Step 0 closed.** S3 connect samples, 10 min apart: 208, 169, 2,232,
  217, 283 ms (04:03-04:53): spikes come and go within minutes.
- Ordering decision (Souhail approved): pause `maintenance` and `transform`
  once no task ran, recreate the scheduler (new image), run the graph job,
  unpause. A paused DAG's running task finishes; its next tasks wait.

### Check answers (step 8)

- (1) 404 vs `[]` in one query: correct and complete (MATCH the product,
  then OPTIONAL MATCH its pairs; a plain pattern match needs a
  relationship, so both cases give zero rows). Added: `collect()` skips an
  element only if it is null; a map of nulls is not, hence the `CASE`
  (the bug the mutation check found).
- (2) Neo4j down: endpoints correct (features 200, recommendations 503).
  **Wrong on `/health`**: it checks both stores, so it returns 503 "graph
  store unavailable" (ADR 019, decision 9). Souhail's reasoning is a good
  design argument though: liveness ("is the process alive?") vs readiness
  ("can it serve?"). Under Kubernetes, a liveness probe that depends on a
  store would restart healthy pods during a store outage; Compose only
  marks the container unhealthy. **Open for step 9**: `/health` (liveness,
  process only) + `/ready` (stores), which amends ADR 019; Souhail decides.

### Where we stopped (2026-10-10, ~05:20 UTC)

- **P6 steps 0-8 done** and pushed (CI green); check questions after steps
  1, 4-5 and 8 answered. `make down` at the end of the session (Souhail).
- The generator ran the companion version 01:46-05:20 UTC (~3.5 h): the
  graph (built 04:37) already ranks the top product's companions first.
- **Next session**: start with the decision on `/health` (liveness) vs
  `/ready` (readiness), which amends ADR 019; then step 9 (API container
  reaches Neo4j; 503 time with Neo4j stopped), step 10 (acceptance drill:
  top 20 products, >= 4 of top 5 are companions; rebuild the graph first,
  with more hours of companion orders by then), step 11 (wrap-up + P6
  checkpoint).
- Kept for P9: per-table retries in `jobs/maintenance.py` (a DNS blip
  re-runs a whole database and the report hides earlier attempts), DNS
  hardening, the link's variability.
- Still pending: O8 (leaning B), "provisional days" rule, P7 and P9 ideas.

## Session 11 — 2026-10-10 (afternoon) — health split, P6 step 9

### Decisions taken

- **`/health` + `/ready`** (Souhail, amends ADR 019): `/health` is
  liveness (the process answers, no store called; the Compose healthcheck
  uses it), `/ready` pings both stores, always reports both
  (`{"redis": "ok", "neo4j": "down"}`), 503 if either is down; the Redis
  drill reads `/ready`. Rejected: one `/health` (a store outage would get
  a working API restarted under Swarm or Kubernetes), liveness only (loses
  the one-call view of both stores).
- **`depends_on: neo4j` with `service_started`**, not `service_healthy`:
  Neo4j starts first, but the API does not wait for it, so features come up
  even if the graph store cannot. Redis keeps `service_healthy` (P5); the
  same argument would relax it, not changed.
- **Neo4j's query timeout is a 503**, not a 500 (found while measuring, see
  below). Only that error code; any other `ClientError` is a bug (500).

### Concepts covered

- **Liveness vs readiness**: liveness failure means "restart me",
  readiness failure means "send me no traffic". Docker's healthcheck acts
  as liveness (Swarm replaces unhealthy tasks). A readiness probe on a
  *shared* dependency fails on every replica at once: the load balancer
  removes all of them, and a partial outage (graph down, features fine)
  becomes total. So `/ready` stays a status endpoint here.
- **Cold start cost is server-side**: API restarted with Neo4j warm, first
  query 27 ms (connecting is cheap); Neo4j restarted, first query 1.99 s
  (empty query-plan cache, cold JVM), then 15-45 ms.
- **Error classes vs error codes**: the Neo4j driver has no timeout class;
  a timeout and a syntax error are both `ClientError`, only `error.code`
  tells them apart.

### Steps done

| Step | Result |
|---|---|
| - | `/health` liveness + `/ready` (ADR 019 amendment); 4 tests, 2 mutations each caught by its own test (stop at the first failing store; `/health` pinging Redis) |
| 9 | API container reaches Neo4j: product 27809 -> its 5 companions (weights 43-34), unknown -> 404. **Neo4j stopped**: 503 in 1.0 s (first one 4.5 ms: dead pooled connection), `/ready` 503 names neo4j, `/health` 200, container stays healthy, features 200 in 4 ms. **Neo4j restarted**: 200 again 8.5 s after `start`, no API restart, no 500 in its log. Query timeout -> `GraphTimeout` -> 503, tested on a real Neo4j (slow query raises it, a syntax error does not); disabling the translation fails that test |

### Debugging lesson (step 9)

- **Symptom**: the first request after `make up` took 2.29 s. **Method**:
  vary one thing at a time. API restarted alone: 27 ms, so not the driver.
  Neo4j restarted alone: 1.99 s, so Neo4j's cold caches, and just under the
  2 s query timeout. **Question that found the bug**: what happens just
  over it? Forced with a slow query and `timeout=0.2` inside the API
  container: `ClientError`, code
  `Neo.ClientError.Transaction.TransactionTimedOutClientConfiguration`,
  not in `GRAPH_ERRORS`, so a 500. The step 8 fake raised only
  `ServiceUnavailable`: a fake fails only in the ways I imagined (the step 8
  lesson, again).
- Kafka retention is 3 days (`retention.ms` 259,200,000): with only `core`
  running, companion orders accumulate and bronze can catch up later.

### Check answers (step 9)

- (1) 4.5 ms then 1.0 s: first part right in substance (a dead pooled
  connection, no retries, immediate 503). **Precision**: not a new connect
  refused, but an open socket that Neo4j closed when it stopped, found dead
  on first use. **Wrong setting for the 1.0 s**: it is the DNS lookup, capped
  by compose's `dns_opt` (`timeout:1 attempts:1` in the container's
  `/etc/resolv.conf`; a lookup of an unknown name fails in 1.02 s, shown
  live), not the driver's `connection_timeout=1` (in `app.py`, not in
  compose). The driver never reaches the connect: the name does not resolve.
  Without `dns_opt` the lookup goes to the host resolver: 8 s (P5), ~9 s per
  request. Both are 1 s, so the timing alone could not tell them apart.
- (2) 503 vs 500: correct and complete (by code, not class; catching every
  `ClientError` would make a bad deploy look like a database outage).
  Nuance added: a timeout is not always transient; a query that is always
  slow would get a 503 on every retry, a performance bug that only logs and
  query metrics reveal.

### Step 10 drill written (acceptance run pending)

- `drills/recommendations_affinity.py`: top 20 of the generator's ranking,
  companions recomputed from the sha256 rule (products' ids and categories
  from PostgreSQL; the generator is not imported: the pipeline under test
  shares no code with it), each product's top 5 from the API; pass if every
  one has >= 4 companions. Exit 1 on failure.
- **Preview on the 04:37 graph (3.5 h of companion orders): 20/20 PASS**,
  18 at 5/5. Margins thin at the bottom: 21336's last companions at
  weight 2, 2797's at 3, 3, 2, against noise pairs at 1-2 (ties broken by
  id). Not the acceptance run: that one after the rebuild at ~10 h.
- The drill can fail: with the wrong rule (`<id>-<c>` instead of
  `<id>:<c>`), 0/20 and exit 1.

### Check answer (step 10 drill)

- Why the 20th product's companions weigh 2-7 and the top product's
  34-43, and why the top 20: correct (weight scales with the anchor's
  orders; random products have too few orders for the signal to beat
  coincidental pairs). **Precisions**: only the *first* item follows the
  skew and companions are chosen relative to it, so a companion pair gains
  weight only from orders the anchor starts; Zipf 0.8 predicts rank 1 /
  rank 20 = 20^0.8 = 11x, observed ~7.6x (39.6 vs 5.2 mean companion
  weight, 27809 vs 3031), same order at these small counts. "Same verdict
  every run" holds for one graph only: the weights are random draws, a
  rebuild can flip a marginal product, hence the wait for ~8-9.

### Where we stopped (2026-10-10, ~16:30 UTC) — LATEST, start here

- Health split and P6 step 9 done, pushed, CI green. `core` + `serving` up
  since 16:05 UTC: the generator writes companion orders again (3.5 h this
  morning, 01:46-05:20).
- **Next**: step 10 acceptance run (`uv run drills/recommendations_affinity.py`,
  drill committed). Rebuild the graph at ~22:00 UTC (~10 h of companion
  orders: ~8-9 per companion pair of the 20th product): `stream`
  (bronze catches up from Kafka, within its 3 days) and `airflow`
  (transform, then a graph rebuild). Then step 11 (README, runbook,
  results.md P6 section with today's step 9 figures, P6 checkpoint).
- Kept for P9: per-table retries in `jobs/maintenance.py`, DNS hardening,
  the link's variability; maybe relax the API's Redis `depends_on` to
  `service_started`.
- Still pending: O8 (leaning B), "provisional days" rule, P7 and P9 ideas.

### P1 plan (agreed)

| # | Commit | Content / how we verify | What Souhail learns |
|---|---|---|---|
| 1 | `chore: initialise repository` | `.gitattributes`, `.gitignore`, `.editorconfig`, LICENSE, README, CLAUDE.md, spec | Line endings, what never goes into Git, licensing |
| 2 | `docs: add ADR 000 and 001 and learning log` | ADRs (accepted) and this log | ADR format and why decisions get written down |
| 3 | `chore: vendor theLook generator at <sha>` | Generator folder + its LICENSE + `NOTICE`; licence check of the folder itself (spec 4.2) | Open-source licensing, pinning a dependency you don't control |
| 4 | `docs: record the actual source schema` | `docs/source-schema.md`: real columns, types, keys, and which operations the generator runs per table (address updates? deletes? nullable `events.user_id`?) | The data model; answers spec 4.1 and 4.3 |
| 5 | `feat(onprem): postgres 17 with logical decoding` | `compose.yaml` (`core` profile), `wal_level=logical`, `max_replication_slots`, `max_wal_senders`, `max_slot_wal_keep_size`, healthcheck | What the WAL is, `logical` vs `replica`, why the WAL cap exists |
| 6 | `feat(onprem): run the generator` | Generator container at a low rate; check inserts/updates/deletes with `psql` | The source workload per table |
| 7 | `feat(onprem): kafka 4 in KRaft mode` | Single node (broker + controller), small heap, internal/external listeners, RF 1 for internal topics incl. transaction state | KRaft, partitions, replication factor, the `advertised.listeners` trap |
| 8 | `feat(onprem): schema registry` | Registry on `_schemas`, default compatibility `BACKWARD` | Avro on the wire (magic byte + schema ID), compatibility modes |
| 9 | `feat(onprem): kafka connect worker image` | Pinned Dockerfile: Debezium Postgres plugin + Confluent Avro converter; distributed mode; `exactly.once.source.support=enabled`; check `GET /connector-plugins` | Workers/connectors/tasks, Connect's internal topics, why exactly-once needs distributed mode |
| 10 | `feat(postgres): replication user, publication, heartbeat table` | Least-privilege role, pre-created publication for 6 tables + heartbeat (B1), `REPLICA IDENTITY` choice; publication created after the generator creates its tables | Publications vs slots, least privilege, what REPLICA IDENTITY controls |
| 11 | `feat(connect): debezium postgres source` | `connectors/postgres-source.json` (pgoutput, Avro, heartbeat 30s + action query, `exactly.once.support=required`, lz4, `snapshot.mode=initial`) + `make register-connectors` (idempotent PUT); 6 topics, 12 subjects, read with `kafka-avro-console-consumer`. **P1 acceptance, part 1** | Debezium envelope, snapshot to streaming handover, LSN, keys = primary keys |
| 12 | `feat(monitoring): replication slot metrics and alert` | `monitoring` profile: Prometheus + postgres_exporter (slot query: `active`, retained WAL via `pg_wal_lsn_diff`, `wal_status`) + Alertmanager rules (inactive > 30 min, retained WAL > threshold) | Metrics vs alerts, reading `pg_replication_slots` |
| 13 | `test(drills): replication slot drill` | `drills/slot-drill.sh`: stop Connect, WAL grows, alert fires, restart, slot catches up; result in `results.md`, recovery in `runbook.md`. **P1 acceptance, part 2** | The classic outage, first-hand; recovering an invalidated slot |
| 14 | `feat(monitoring): connect and debezium metrics` | JMX exporter: `MilliSecondsBehindSource`, events seen | JMX, source-side lag |
| 15 | `docs: baseline throughput run` | Method first (B5), then raise the generator rate step by step and find what saturates first; sets O8's target | Measure before optimising, find the real bottleneck |
| 16 | `docs: P1 wrap-up` | README "run P1", learning log, ADRs for P1 decisions (Postgres 17, Connect image, publication mode, alerting stack) | Then the P1 checkpoint: 3–5 interview questions |
