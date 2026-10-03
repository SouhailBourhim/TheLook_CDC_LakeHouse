# 009. PySpark builds silver and gold instead of dbt

- Status: Proposed
- Date: 2026-10-03 (decision D3, spec v2.0)
- Deciders: Souhail Bourhim (approves), Claude Code (drafts)

## Context

Spec v1.9 used dbt on Athena for silver (incremental MERGE), SCD2 and
gold, with dbt tests and unit tests. No dbt code was written. ADR 007
makes Spark the bronze writer, and the extension's target skills include
PySpark batch processing and pytest.

## Options considered

| Option | Assessment |
|---|---|
| A. PySpark for silver and gold | One engine end to end; logic unit-tested with pytest and chispa; Spark SQL `MERGE INTO` on Iceberg |
| B. PySpark for silver, dbt-athena for gold and metrics | Realistic split (Spark for heavy CDC logic, dbt for SQL marts), but two engines, two test stacks, two deployment paths |
| C. Keep dbt for everything | Strong SQL tooling and lineage, but no PySpark batch work |

## Decision

Option A.

- Silver: a PySpark job per entity reads bronze rows committed after the
  last processed point, keeps the latest version per key (log position
  ordering), drops no-op updates by a hash of business columns, and
  applies the result with `MERGE INTO` (update, insert, delete).
- SCD2 `dim_user` is built from the change log (FR4).
- Gold: star schema and marts as Iceberg tables in `thelook_gold`.
- Tests: pytest + chispa on small in-memory DataFrames for every
  transformation (FR8); data checks on the real tables come from the
  contracts with datacontract-cli (P7).
- Jobs run locally (Spark standalone) against the S3 lake through the
  Glue catalog; Airflow runs them with spark-submit (P4).

## Consequences

- ✅ One language and one engine for ingestion, transformation and the
  graph job.
- ✅ The hardest logic (dedup, deletes, SCD2) is tested as code, fast and
  without AWS.
- ❌ No dbt lineage graph and docs site; lineage comes from the code and
  optional OpenLineage (P9).
- ❌ SQL-only colleagues could not maintain the models as easily.
- ❌ Local Spark reads bronze from S3 over the internet: data transfer
  out of AWS, free up to 100 GB/month, far above demo volume.
- ❌ dbt, a widely requested skill, is no longer shown by this project.

## References

- Apache Iceberg docs: Spark `MERGE INTO`, writes
- chispa: github.com/MrPowers/chispa
- Cahier des charges v2.0: FR3, FR4, FR5, FR8
