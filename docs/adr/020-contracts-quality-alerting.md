# 020. Data contracts, quality checks and one alerting hub

- Status: Proposed
- Date: 2026-10-11 (P7; spec FR6, FR7, FR8, FR12, FR14, NFR observability)
- Deciders: Souhail Bourhim (approves), Claude Code (drafts)

## Context

P7 makes the pipeline's promises checkable and makes failures reach a
person. Before it: no contract exists; silver counts MongoDB documents it
cannot parse and drops them (ADR 014 promised a dead-letter area); no DAG
has a failure callback, so a failing `transform` tells nobody; Alertmanager
has no receiver, so even Prometheus alerts are seen only by whoever opens
its UI; MongoDB's oplog window, the first resource an outage exhausts
(~30 h, P2), has no alert. P7's acceptance: an injected schema break is
caught and an alert is raised; a compatible column addition flows end to
end.

Facts measured in P7 step 0 (datacontract-cli 1.2.2 on
`thelook_silver.dist_centers`, through Athena):

| Contract vs data | `datacontract test` |
|---|---|
| identical | pass (16 checks, ~12 s, mostly connection overhead) |
| a column the contract lacks (a new column in the data) | **pass: not seen** |
| a column the data lacks | fail |
| a different physical type | fail |

`changelog` and `breaking` list the added field but exit 0. Version 1.2.2
also requires `stagingDir` even with a workgroup that enforces its result
location, and both that path and the Glue import's draft contain the AWS
account ID, which the repository never commits.

## Options considered

**Where alerts go** (decided with Souhail).
1. Alertmanager as the single hub, delivered by its SNS receiver to email.
2. The same hub, delivered by SMTP (a mail password on the laptop).
3. CloudWatch alarms as the spec's NFR said: Airflow publishes custom
   metrics; Alertmanager still needs a receiver: two alerting systems.
4. No delivery (UI only): nobody is told.

**Where checks run.** Spark on the laptop (reads S3 over the internet: the
transfer cost P4 measured) or **Athena** (queries run inside AWS, billed by
bytes scanned, nothing downloaded).

**How a new column is caught.** `datacontract test` cannot (table above);
parsing `changelog` output is brittle; **our own comparison** of each
contract's fields with the table's columns in Glue is small and testable.

## Decision

**Contracts.** One ODCS v3 file per table under
`contracts/<layer>/<table>.odcs.yaml`, drafted with `datacontract import
glue`, then curated by hand (the draft is never committed raw: it carries
the account ID). Silver (7) and gold (10): schema, primary key, required
fields, PII classification (the FR11 list), owner, quality rules. Bronze (7
tables and the ledger): schema only, so drift fails a check. The server
block names the database, region and workgroup; the staging location is
supplied at run time from the environment, so no account ID is committed.
Kafka topics stay guarded by Schema Registry (`BACKWARD`):
datacontract-cli's Kafka test is experimental and not used.

**Quality rules (FR8).** Unique and not null on keys; accepted values
(order and item statuses, from the generator's state machine); facts to
dimensions relationships as SQL checks; metric sanity (net revenue = gross
- returns); unknown member: no fact row still on `user_sk = -1` an hour
after it was computed.

**Drift.** `contracts/drift.py`: for each contract, the set of fields vs
the set of Glue columns (`glue:GetTable`; Iceberg's Glue catalog rewrites
the column list on every commit). Any column on one side only fails the
check. The comparison is a pure function under unit tests.

**Runner and identity.** datacontract-cli 1.2.2 in its own virtualenv in
the Airflow image (its dependencies never meet Airflow's constraints),
contracts copied into the image. Checks run in Athena as a new IAM user
`thelook-quality`: Athena in workgroup `thelook` only, Glue read on the three
layer databases, S3 read on the layers, write on `athena-results/`. Never
the batch key (it can write silver and gold).

**Where checks run (FR12).** `transform` gains a `quality` task after
`gold_marts` with the cheap subset (measured in step 6); a daily
`quality_report` DAG runs every contract, the drift check, freshness per
source (bronze against O1's 5 minutes, gold against O2's hour),
source-to-silver row counts as of a cut (rows created before the silver
cut's oldest source time minus a margin, counted on both sides, so the
generator cannot move the counts during the comparison), and a health
snapshot from Prometheus' API. Readers for the sources are least-privilege
(a PostgreSQL role and a MongoDB user, read only).

**Dead letters.** `lake.thelook_silver.dead_letters` (source table, key,
log position, raw payload, reason, time), written by the silver job,
MERGEd on table and position so a replay adds nothing. Any new dead letter
raises an alert. It holds raw personal data: P8's erasure must cover it.

**Alerting.** Every DAG has an `on_failure_callback` that posts an alert to
Alertmanager's API (`/api/v2/alerts`, labels `dag`, `task`, `severity`); a
failed quality check fails its task and alerts the same way. Alertmanager
delivers through `sns_configs` to the topic `thelook-alerts`, with an email
subscription to `alert_email` (already in `terraform.tfvars`). A new IAM user
`thelook-alerts` may only `sns:Publish` to that topic; its keys reach
Alertmanager through the environment (its SNS client falls back to AWS's
default credential chain). The topic ARN holds the account ID, so the
config is rendered at container start from `onprem/.env`, as
`start-redis.sh` does for Redis' ACL file.

**MongoDB oplog window.** Percona mongodb_exporter v0.53.0 in the
`monitoring` profile with `--compatible-mode` (it then exposes the oplog's
head and tail timestamps; window = head - tail), as a user with
`clusterMonitor` and `read` on `local`. A rule alerts while time remains to
act.

**Provisional days (FR6).** The return window is measured in silver (p99 of
the time from order to return); `mart_daily_revenue.is_provisional` is true
for order days younger than it: their revenue can still fall.

**Schema drills (FR14), on `shop.dist_centers`** (10 rows the generator
writes only at start-up): (a) a nullable column flows through Avro, bronze
and silver while the contracts' drift check flags it, then the contracts
are updated; (b) an incompatible type change, with the generator stopped,
is rejected by Schema Registry, fails the connector's task and emails;
nothing reaches Kafka. Recovery: revert the type, move the connector's
offset past the rejected transaction (Connect offsets API), re-send that
key with a filtered blocking snapshot, verify silver. (c) A malformed
MongoDB document, which the registry cannot see, lands in dead letters and
emails.

**O8 (decided with Souhail, pending since P3).** "Source to bronze lag below
2 minutes (p95) at 200 change events/s", measured in P9. The old wording
(lag below 30 s) cannot pass with a 60 s trigger by design.

## Consequences

- ✅ Every alert, from Prometheus or Airflow, reaches one inbox through one
  route that can be silenced in one place.
- ✅ Contracts are the single description of each table; a drift, a type
  change, a broken key or a bad status fails a check instead of passing
  silently.
- ✅ Checks cost Athena scans, not laptop transfer; their cost is measured
  (step 6) against O6.
- ✅ Malformed records are kept and visible instead of dropped.
- ❌ Two more IAM users and one more container (the exporter); one more
  virtualenv in the Airflow image.
- ❌ The drift check is our own code, not the tool's; it covers column names
  only (types are covered by `datacontract test`).
- ❌ Email through SNS depends on AWS; a network outage delays alerts, and
  SNS email needs a one-time subscription confirmation.
- ❌ Dead letters keep raw personal data until P8's erasure handles them.
- ❌ The incompatible-type drill moves a connector offset by hand: a
  mistake there loses changes, so the recovery is scripted and verified
  with `verify_silver.py`, with a full re-snapshot as the fallback.

## References

- Open Data Contract Standard v3; datacontract-cli docs: testing with
  Amazon Athena, `lint`, `changelog`
- Alertmanager configuration: `sns_configs`, the `/api/v2/alerts` API
- Percona mongodb_exporter: compatible mode, required roles
- Kafka Connect REST API: connector offsets (KIP-875)
- Cahier des charges v2.3: FR6-FR8, FR12, FR14, NFR observability; ADR 005,
  ADR 012, ADR 014, ADR 015
