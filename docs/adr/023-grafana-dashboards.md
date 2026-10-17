# 023. Grafana dashboards: pipeline health and the gold marts

- Status: Accepted
- Date: 2026-10-17 (P9b; spec v2.4)
- Deciders: Souhail Bourhim (approves), Claude Code (drafts)

## Context

Souhail asked for "something to visualize our project" (spec v2.4, phase
P9b). The stack had no dashboard: Prometheus' own UI for the pipeline,
Athena's query editor for the marts, screenshots in the README. The demo
video (spec section 10) shows "the change reaching the dashboard". The
spec says to present the project on its engineering results (freshness,
correctness, cost), since the business data is synthetic.

## Options considered

1. **Grafana (chosen):** Prometheus data source built in; Amazon Athena
   data source plugin maintained by AWS; one container (~300 MB); dashboards
   and data sources provisioned from files.
2. **Metabase or Superset:** better ad hoc BI on the marts; 1-1.5 GB (a JVM,
   or Superset plus its metadata database); no pipeline health, so a second
   tool for half the story.
3. **Amazon QuickSight:** managed, AWS-native; about $24 per author per
   month, which breaks O6 (< $15/month).
4. **Nothing:** the Athena console as the "dashboard".

## Decision

- **Grafana OSS 13.2.3, pinned by digest**, in a `bi` profile
  (`onprem/grafana`). The newest release at least two weeks old (13.2.4
  was 4 days old).
- **Athena plugin 3.3.6** (2026-09-28, Apache 2.0), the newest version on
  grafana.com's catalog at least two weeks old (3.3.7 to 3.3.10 exist on
  GitHub, not all in the catalog), **downloaded at build time and checked
  against the catalog's SHA-256**, installed outside `/var/lib/grafana`.
  Not `GF_PLUGINS_PREINSTALL`: that would fetch it at every start, with no
  checksum.
- **Everything as code, no volume:** data sources (`editable: false`) and
  dashboards (`allowUiUpdates: false`) are provisioned from the repository at
  each start; nothing in Grafana's own database is worth keeping. A change
  is a commit, reviewed like the rest.
- **Two dashboards:** *pipeline health* (Prometheus: time since the last
  bronze batch, capture lag, slot WAL, oplog window, events/s, bronze batch
  durations against the trigger, alerts firing) and *business* (gold marts:
  revenue with provisional days, return rate, funnel, ratings).
- **Athena identity `thelook-grafana`** (`grafana.tf`): the gold **marts**
  only (Glue `thelook_gold/mart_*`, S3 `gold/mart_*`), in the `thelook`
  workgroup (1 GiB cutoff per query). The marts are aggregates; the rest of
  gold holds `dim_user` (addresses). Key created with the AWS CLI, in
  `onprem/.env`, never in the state.
- **Cost bounded twice:** each panel's query uses Athena's **result reuse**
  (30 minutes: the marts change at most every 30 minutes, with transform),
  and the dashboard refreshes every 30 minutes. A mart query scans KBs.
- **Access:** anonymous Viewer on `127.0.0.1:3000` only; editing needs the
  admin password from `.env`. No calls home (analytics, update checks, news).

## Consequences

- ✅ One place for the demo and for operations: the pipeline live, and what
  an analyst sees; the README's Prometheus screenshots can come from it.
- ✅ A fresh clone gets the same dashboards (acceptance: provisioned on a
  fresh start).
- ⚠️ Dashboards edited in the UI cannot be saved: export the JSON and
  commit it. That is the price of "as code".
- ⚠️ The plugin is pinned to one platform (linux-amd64); an ARM laptop needs
  the arm64 package and its checksum.
- ⚠️ Grafana's required variables are optional for Compose (a `:?` would
  fail every `docker compose` command until `.env` has them); the container
  refuses to start without them instead.

## Amendment (2026-10-17): orchestration and source panels

Asked by Souhail during the silver catch-up ("a section where I can
monitor this"). The pipeline-health dashboard gets rows: *Capture and
bronze* (as before), *Orchestration (Airflow)*, *Sources and Connect
internals*, *Alerts*.

- **Airflow's own metadata database** as a third data source (Grafana's
  bundled PostgreSQL plugin), not StatsD: it answers "what runs now, which
  try, for how long", needs no new container, and its schema is pinned
  with Airflow 3.3.2. Role `grafana_reader`, created and reset at each
  start by a one-shot service (`airflow-reader.sql`): **column-level
  SELECT** on `dag`, `dag_run`, `task_instance`, `slot_pool`, never
  `dag_run.conf` (it holds the erasure requests' user ids) nor hostnames,
  PIDs or executor settings; read-only by default and a 10 s statement
  timeout. Tested: `conf` and `hostname` refused, an UPDATE refused.
- Orchestration panels: gold age (O2), tasks running, failures and retries
  over 24 h, running tasks with their try and duration, the last run of
  each DAG, task and run durations of transform.
- Source panels from metrics already collected: connector task states,
  Connect record errors, Debezium's free queue capacity, PostgreSQL
  writes and sequential-scan reads per table (the missing index found by
  the load test shows there), database size.

## References

- Grafana, Provision Grafana: grafana.com/docs/grafana/latest/administration/provisioning
- Amazon Athena data source, Configure: github.com/grafana/athena-datasource (docs/sources/configure.md)
- Amazon Athena, Reuse query results: docs.aws.amazon.com/athena/latest/ug/reusing-query-results.html
