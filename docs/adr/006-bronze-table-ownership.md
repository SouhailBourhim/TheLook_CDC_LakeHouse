# 006. Bronze tables: created by their writer, validated by the contracts

- Status: Accepted
- Date: 2026-09-30 (records the decision on open question A4, spec v1.9)
- Amended: 2026-10-03, spec v2.0: the writer is now the Spark streaming
  job (ADR 007) instead of the Iceberg sink. The principle is unchanged.
- Deciders: Souhail Bourhim (approves), Claude Code (drafts)

## Context

Spec v1.8 said Terraform reads the data contracts to create the bronze
tables (FR7), while the NFR requires a new nullable source column to flow
through without breaking anything. The Iceberg Kafka Connect sink can
create tables and evolve their schema from the incoming Avro records.
Contracts only arrive in P4, after bronze exists (P2).

## Options considered

1. **Terraform creates the tables from the contracts.** Explicit and
   reviewed, but every source column change needs a contract and Terraform
   change *before* data can land; if the sink evolves a table anyway,
   Terraform sees drift and tries to undo it. Also impossible before P4.
2. **The sink creates and evolves the tables; contracts validate them.**
   Terraform owns the Glue databases, bucket, IAM and Athena only.

## Decision

Option 2. The bronze writer creates and evolves the tables. Bronze tables
have no Terraform resource.

Since v2.0 the writer is the Spark streaming job (ADR 007): it creates a
missing bronze table on its first batch and appends with schema merging,
so a new nullable column becomes a new table column. (Originally: the
Iceberg sink with auto-creation and schema evolution enabled.) From P4 the
ODCS contracts are checked against the Iceberg tables and Kafka topics by
datacontract-cli; a mismatch fails a check and alerts.

## Consequences

- ✅ A new nullable column reaches bronze with no deployment step (NFR
  schema evolution).
- ✅ No drift between Terraform state and the real table schema.
- ✅ P2 does not depend on P4.
- ✅ With Spark as the writer, partitioning and table properties are set
  in the job's `CREATE TABLE` (in the repository), not by hand.
- ❌ A bad column can land in bronze before a contract check notices it;
  bronze is append-only, so it is caught downstream, not prevented.
- ❌ Terraform did not create the tables, but destroying a Glue database
  removes its tables' metadata, and the bucket's `force_destroy` removes
  the data.

## References

- Apache Iceberg docs: Spark writes (schema merge), Kafka Connect sink
  (auto-create, schema evolution)
- Cahier des charges v1.9 and v2.0: FR2, FR7, NFR schema evolution
