# theLook CDC Lakehouse

Real-time change data capture from an operational PostgreSQL database into a
governed, cost-controlled Apache Iceberg lakehouse on AWS.

> **Status:** phase P1 (on-prem stack) in progress. This is a learning project;
> the full specification is the [cahier des charges](docs/cahier-des-charges.md).

## Architecture

```
On-prem (Docker)
  theLook generator --> PostgreSQL --WAL--> Kafka Connect [Debezium source]
    --> Kafka (KRaft) + Schema Registry (Avro) --> Kafka Connect [Iceberg sink]
AWS
  --> Iceberg bronze (S3 + Glue) --dbt on Athena--> silver (current state) --> gold (star schema)

Orchestration: Apache Airflow (on-prem) | IaC: Terraform | CI: GitHub Actions (OIDC)
```

## Repository layout

Folders are added in the commit that first needs them.

| Path | Contents | Phase |
|---|---|---|
| `onprem/` | Docker Compose stack: Postgres, generator, Kafka, Schema Registry, Kafka Connect, monitoring, Airflow | P1 (Airflow in P3) |
| `infra/terraform/` | AWS resources: S3, Glue, IAM, Athena, budget | P2 |
| `dbt/` | dbt project: silver, gold, metrics | P3 |
| `contracts/` | ODCS data contracts, one per table | P4 |
| `drills/` | Scripted failure drills | P1 onwards |
| `scripts/` | Small helper scripts | P1 onwards |
| `.githooks/` | Git hooks: secret scan before every push | All |
| `docs/` | Specification, ADRs, learning log, runbook, results | All |

## Run P1 (on-prem capture)

Needs Docker with Compose v2 and about 3 GB of free RAM.

```bash
cp onprem/.env.example onprem/.env        # then set every password
cd onprem
docker compose --profile core --profile monitoring up -d --wait
./postgres/apply-sql.sh postgres/cdc-setup.sql         # Debezium role + publication
./postgres/apply-sql.sh postgres/monitoring-setup.sql  # exporter role
cd .. && make register-connectors                      # Debezium source, then status
```

| What | Where |
|---|---|
| Change events (Avro) | Kafka `localhost:9092`, topics `thelook.shop.<table>` |
| Schemas | Schema Registry `http://localhost:8081/subjects` |
| Connector state | `make connector-status`, Connect REST `http://localhost:8083` |
| Metrics and alerts | Prometheus `http://localhost:9090`, Alertmanager `http://localhost:9093` |

The two SQL scripts need the generator's tables, which it creates on its
first start; `--wait` returns once Postgres is healthy, so if a script
reports a missing table, wait a few seconds and run it again (both are
idempotent). Stop the load with `docker compose --profile core stop
generator`; after a reboot, rerun the `up` command (no restart policy on
purpose).

Checks and drills: `uv run drills/verify_cdc.py` (Kafka vs Postgres, with
the generator stopped), `drills/slot-drill.sh` (one-hour slot outage),
`drills/throughput-baseline.sh`. Results in [docs/results.md](docs/results.md),
procedures in [docs/runbook.md](docs/runbook.md).

## After cloning

Enable the versioned Git hooks once per clone. The pre-push hook scans the
commits being pushed with gitleaks (in Docker) and blocks the push if it
finds a secret:

```bash
git config core.hooksPath .githooks
```

## Documentation

- [Cahier des charges](docs/cahier-des-charges.md)
- [Architecture decision records](docs/adr/)
- [Learning log](docs/learning-log.md)

## License

Apache License 2.0, see [LICENSE](LICENSE).
