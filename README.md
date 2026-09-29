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
| `docs/` | Specification, ADRs, learning log, runbook, results | All |

## Documentation

- [Cahier des charges](docs/cahier-des-charges.md)
- [Architecture decision records](docs/adr/)
- [Learning log](docs/learning-log.md)

## License

Apache License 2.0, see [LICENSE](LICENSE).
