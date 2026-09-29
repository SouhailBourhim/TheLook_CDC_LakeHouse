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

### Where we stopped

- Commits 1–4 done (`f2789bb`, `93016d0`, `c504969`, `4b8ab0d` spec v1.7,
  `cf912f8` source schema). Pushed up to `c504969`; the rest awaits Souhail's go.
- Pending check questions on commit 4 (see end of session 2 chat): why SCD2
  can't use `users.updated_at`; what a no-op upsert produces in Kafka.
- Next: commit 5, Postgres 17 with logical decoding.

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
