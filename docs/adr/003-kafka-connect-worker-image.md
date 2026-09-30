# 003. Kafka Connect worker image

- Status: Accepted
- Date: 2026-09-30
- Deciders: Souhail Bourhim (approves), Claude Code (drafts)

## Context

The Connect worker must run the Debezium PostgreSQL source now and the
Iceberg sink in P2, with Avro and the Schema Registry, exactly-once source
support, and every artifact pinned (spec 5.3). Open question B6: do
Debezium 3.x, Connect 4.x and the Confluent Avro converter work together?

## Options considered

| Option | Assessment |
|---|---|
| A. `quay.io/debezium/connect` | Debezium preinstalled, but no Avro converter; Debezium's own Kafka build |
| B. `confluentinc/cp-kafka-connect` | Avro converter included; Confluent's Kafka build and versioning, ~1 GB more to pull, plugins still added by hand |
| C. `apache/kafka:4.3.1` (the broker's image) + plugins downloaded at build time | One Kafka version everywhere; we choose and verify every plugin |

## Decision

Option C. `onprem/connect/Dockerfile`, multi-stage:

- base `apache/kafka:4.3.1`, pinned by digest (same image as the broker);
- Debezium PostgreSQL connector **3.7.0.Final**, built against Kafka 4.3.1
  (closes B6 for the source side);
- Confluent Avro converter **8.3.2** with its dependencies (matches the
  Schema Registry version);
- Prometheus JMX exporter agent 1.6.0 for metrics;
- every download with `ADD --checksum=sha256:…` (a wrong digest fails the
  build, tested); archives unpacked in a throwaway stage;
- one folder per plugin under `plugin.path` (separate class loaders).

Worker settings live in a mounted `connect-distributed.properties`:
distributed mode, `exactly.once.source.support=enabled`, Avro converters as
defaults, secrets through `EnvVarConfigProvider` with an allowlist, lz4 on
source producers.

## Consequences

- ✅ Broker, worker and Debezium share the exact Kafka version.
- ✅ Supply chain is explicit: every jar's source, version and hash is in
  the Dockerfile.
- ✅ The same image takes the Iceberg sink in P2 (another plugin folder).
- ❌ We maintain the Dockerfile: each upgrade means new URLs and hashes.
- ❌ The Avro converter and Schema Registry are under the Confluent Community
  License (source-available), not Apache 2.0 (C4).
- ❌ `ADD` from a URL has sharp edges we hit: files arrive root-owned with
  mode 600, and `--chmod` also applies to directories it creates.

## References

- Debezium 3.7 release (Kafka 4.3.1 in its build parent POM)
- KIP-618 (exactly-once source connectors), KIP-297 (config providers)
- Dockerfile reference: `ADD --checksum`, `--chmod`
