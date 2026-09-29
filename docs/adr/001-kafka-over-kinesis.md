# 001. Self-hosted Kafka over Kinesis as the event platform

- Status: Accepted
- Date: 2026-09-29 (records the change made in spec v1.2)
- Deciders: Souhail Bourhim

## Context

Change events must travel from the on-prem PostgreSQL database to the AWS
lakehouse. The transport must:

- be durable and replayable, and keep order per primary key;
- enforce schemas at write time;
- deliver into Iceberg tables with exactly-once commits;
- fit the budget (O6: < $15/month at demo volume);
- fit the hybrid story: operational systems in the company's data centre, lake in the cloud.

It should also build skills that employers ask for. Spec v1.1 used Kinesis Data
Streams with Firehose.

## Options considered

| Option | Cost | Assessment |
|---|---|---|
| A. Kinesis Data Streams + Firehose (Debezium Server with Kinesis sink) | 1 provisioned shard ≈ $0.015/h ≈ $11/month if left running, plus PUT units and Firehose per-GB fees | Fully managed and AWS-native, but uses most of the budget. AWS-only skills. Debezium Server is less representative of production than Debezium on Kafka Connect. Weaker schema-registry integration |
| B. Amazon MSK, provisioned | Smallest cluster < $2.50/day (≈ $75/month) | Managed Kafka, but breaks O6 |
| C. Amazon MSK Serverless | ≈ $0.75/h (≈ $540/month) | Breaks O6 by far |
| D. Self-hosted Apache Kafka 4.x (KRaft), Kafka Connect, Schema Registry, on-prem in Docker | $0 on AWS | Industry standard. Debezium's standard production deployment. Official Iceberg sink with exactly-once commits. Avro schemas enforced at write |

## Decision

Option D: a self-hosted Apache Kafka 4.x cluster in KRaft mode (single node locally),
with Kafka Connect in distributed mode running both the Debezium PostgreSQL source
and the Apache Iceberg sink, and Schema Registry with Avro.

## Consequences

- ✅ No AWS cost for the event platform.
- ✅ Skills that transfer to most companies.
- ✅ The Connect ecosystem replaces custom code: Debezium offsets and restarts,
  exactly-once source, exactly-once Iceberg commits.
- ✅ Replays are possible by resetting offsets (used in the FR14 drills).
- ❌ We operate everything ourselves: broker, Connect, Registry, upgrades,
  monitoring (Prometheus/Grafana).
- ❌ One broker with replication factor 1 means no high availability; a broker
  restart pauses the pipeline. The production design is documented as 3 brokers,
  RF = 3, `min.insync.replicas = 2`.
- ❌ Uses laptop resources (≈ 8–10 GB RAM for the full on-prem stack).
- ❌ The on-prem Connect worker needs long-lived IAM access keys (there is no
  instance role on a laptop). It gets a dedicated least-privilege user and a
  documented rotation procedure.
- ❌ Personal data stays in Kafka until topic retention expires, which puts an
  upper bound on the erasure delay (see the open issue on O5 vs retention).
- ❌ No hands-on practice with AWS-managed streaming. Kinesis is kept for the
  optional P7 (clickstream with Managed Flink).
- ℹ️ Confluent Schema Registry uses the Confluent Community License
  (source-available). That is acceptable here; Apicurio Registry (Apache 2.0)
  is the open-source alternative.

## References

- Cahier des charges v1.6: sections 5.2 and 11; revision 1.2
- Debezium documentation: PostgreSQL connector, exactly-once delivery
- Apache Iceberg documentation: Kafka Connect
- AWS pricing pages for Kinesis Data Streams, Firehose and MSK (prices to re-check before publishing)
