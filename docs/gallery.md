# Gallery: the running stack

Every user interface of the project, captured from the running stack on
2026-10-17 (`scripts/readme_screenshots.py`, 1440 x 900 at 2x, in one
browser frame), at the end of P9. What the lake held is in
[lake-inventory.md](lake-inventory.md); every measurement in
[results.md](results.md); one order followed end to end in [demo.md](demo.md).

## Orchestration: Airflow (`http://localhost:8088`)

<img src="images/screens/airflow-dags.png" alt="Airflow DAG list: backfill, gdpr_erasure, graph, maintenance, quality_report, transform with their schedules, last runs and run history" width="100%">

The six DAGs. `transform` every 30 minutes, `maintenance` and `graph` daily,
`quality_report` daily, `gdpr_erasure` and `backfill` on demand. The red
marks are real failures, left as they were: `graph` could not reach Neo4j
(the serving profile was stopped: `Name or service not known`), and a
`quality_report` run of the same morning failed.

<img src="images/screens/airflow-transform-runs.png" alt="Airflow transform runs: 91 runs, scheduled and manual, their state, start and duration" width="100%">

`transform`'s run history: the slow runs before compaction (40+ minutes),
then about 10 minutes after it (P9 scan cost).

<img src="images/screens/airflow-transform.png" alt="Airflow graph of transform: silver, gold_dims, gold_facts, gold_marts, quality, all successful" width="100%">

<img src="images/screens/airflow-gdpr_erasure.png" alt="Airflow graph of gdpr_erasure: seven successful steps, 1 h 44 min" width="100%">

A GDPR erasure: the user deleted from both sources, bronze purged by the
stream, Redis cleaned, the lake erased and verified (1 h 44 min, O5: 24 h).

<img src="images/screens/demo-transform-run.png" alt="Airflow graph of the transform run that carried the demo order, all tasks successful, 10 min 35 s" width="100%">

The run that carried the demo order to gold ([demo.md](demo.md)).

## Dashboards: Grafana (`http://localhost:3000`, P9b)

<img src="images/screens/grafana-pipeline.png" alt="Grafana pipeline health, capture and bronze: time since the last bronze batch, capture lag, slot WAL, alerts, events per second, bronze batch durations against the trigger" width="100%">

<img src="images/screens/grafana-orchestration.png" alt="Grafana pipeline health, orchestration: gold age against O2, tasks running, failures and retries, running tasks, last run of each DAG, transform durations" width="100%">

<img src="images/screens/grafana-sources.png" alt="Grafana pipeline health, sources: connector task states, record errors, database size, Debezium queue capacity, rows written and scanned per table, alerts firing" width="100%">

<img src="images/screens/grafana-business.png" alt="Grafana business dashboard: orders, net revenue, return rate, conversion, daily revenue, provisional days, funnel, best-rated products" width="100%">

Pipeline health (Prometheus and Airflow's own database) and the gold marts
(Athena, a marts-only identity, result reuse).

## Monitoring: Prometheus (`http://localhost:9090`)

<img src="images/screens/prometheus-alerts.png" alt="Prometheus alerts by group; the MongoDB oplog window alert firing after the load test" width="100%">

<img src="images/screens/prometheus-rules.png" alt="Prometheus rule health: the alert rules in their groups" width="100%">

<img src="images/screens/prometheus-targets.png" alt="Prometheus targets: postgres, connect, mongodb, bronze-stream up; features-stream down while the serving profile was stopped" width="100%">

<img src="images/screens/prometheus-slot-drill.png" alt="Prometheus: retained WAL climbing for an hour while Debezium was stopped, then dropping; the slot alert" width="100%">

The alert rules (17, every one with a runbook entry checked in CI), the
scrape targets, and the slot drill as Prometheus recorded it.

## Spark (`http://localhost:8080`, `http://localhost:4040`)

<img src="images/screens/spark-cluster.png" alt="Spark master: one worker, 4 cores, the bronze stream and a transform job running" width="100%">

<img src="images/screens/spark-bronze-streaming.png" alt="Spark Structured Streaming statistics of the bronze query: input and processing rates, input rows, batch durations" width="100%">

The cluster (the stream holds 2 of the worker's 4 cores, the batch jobs the
other 2) and the bronze query's own statistics.

## Serving: Neo4j and the API (`http://localhost:7474`, `http://localhost:8000`)

<img src="images/screens/neo4j-recommendations.png" alt="Neo4j Browser: a product and its five most co-purchased products" width="100%">

<img src="images/screens/api-docs.png" alt="FastAPI docs: GET /products/27809/recommendations answering 200 with five products and their weights" width="100%">
