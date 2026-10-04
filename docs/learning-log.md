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

### Where we stopped

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
- **Next: step 4**, `spark/jobs/silver.py` run as the batch user (first run
  backfills from all of bronze) and `drills/verify_silver.py` (silver =
  sources, writers stopped).

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
