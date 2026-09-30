# 006. Bronze tables: created by the sink, validated by the contracts

- Status: Proposed
- Date: 2026-09-30 (records the decision on open question A4, spec v1.9)
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

Option 2. The Iceberg sink runs with table auto-creation and schema
evolution enabled. Bronze tables have no Terraform resource. From P4 the
ODCS contracts are checked against the Iceberg tables and Kafka topics by
datacontract-cli; a mismatch fails a check and alerts.

## Consequences

- ✅ A new nullable column reaches bronze with no deployment step (NFR
  schema evolution).
- ✅ No drift between Terraform state and the real table schema.
- ✅ P2 does not depend on P4.
- ❌ Table properties the sink does not set (partitioning, sort order,
  file format options) must be set another way: sink configuration where
  possible, otherwise a one-off `ALTER TABLE` recorded in the repository.
- ❌ A bad column can land in bronze before a contract check notices it;
  bronze is append-only, so it is caught downstream, not prevented.
- ❌ `terraform destroy` does not remove the tables' metadata by itself
  (the sink created it); teardown must empty the Glue database too.

## References

- Apache Iceberg docs: Kafka Connect sink (auto-create, schema evolution)
- Cahier des charges v1.9: FR7, NFR schema evolution
