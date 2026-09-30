# 005. On-prem monitoring and alerting

- Status: Proposed
- Date: 2026-09-30
- Deciders: Souhail Bourhim (approves), Claude Code (drafts)

## Context

FR13 requires alerts when the replication slot is inactive for more than
30 minutes or retains too much WAL; the NFR asks for connector status and
consumer lag to be monitored, "for example Prometheus and Grafana
on-prem". The on-prem stack has no cloud monitoring. Alerts must be
testable without waiting for real outages.

## Options considered

1. **Scripts polling SQL and REST** (cron + mail): little to run, but no
   history, no alert lifecycle, every check hand-written.
2. **CloudWatch agent on the laptop**: one tool for both sides, but costs
   money, needs AWS credentials on-prem for metrics, and couples on-prem
   health to the cloud.
3. **Prometheus + Alertmanager on-prem**, exporters for Postgres and the
   Connect JVM.

## Decision

Option 3, in a separate `monitoring` compose profile:

- **postgres_exporter** v0.20.1 as a `pg_monitor`-only role; its built-in
  replication-slot collector (retained WAL measured from `restart_lsn`,
  checked against SQL) replaces a custom query;
- **Prometheus JMX exporter agent** in the Connect worker (Debezium lag and
  connection, connector and task state);
- **Prometheus** v3.15.0 with 11 alert rules, each covered by `promtool`
  unit tests; **Alertmanager** v0.34.1 with an inhibit rule (exporter down
  suppresses "slot missing");
- `absent()` and `up == 0` rules so that missing metrics are an alert, not
  silence.
- No delivery channel yet: alerts are visible in Alertmanager's UI and API;
  the channel is chosen with the rest of alerting in P4. Grafana dashboards
  are not added in P1.

## Consequences

- ✅ The slot drill proved the path end to end: alert pending at minute 1,
  firing at minute 31, resolved after catch-up.
- ✅ Rule tests caught real bugs before deployment (a millisecond/second
  unit error in an alert text) and fail when a threshold is weakened.
- ✅ Thresholds are written next to the measurements that justify them.
- ❌ Three more containers to run and upgrade (~65 MB RAM in total).
- ❌ Until P4 nobody is notified: an alert fires silently unless someone
  looks.
- ❌ Thresholds depend on the WAL rate, which changes with data volume; they
  need re-checking after the baseline run and in P2.

## References

- Prometheus docs: alerting rules, `promtool test rules`, Alertmanager inhibition
- postgres_exporter replication_slot collector; jmx_exporter rules
- docs/results.md, docs/runbook.md, onprem/monitoring/
