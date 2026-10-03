# 011. Spark runtime and versions for the lake jobs

- Status: Accepted
- Date: 2026-10-03 (P3, first commit; spec 5.3 asks to pin versions after a compatibility check)
- Deciders: Souhail Bourhim (approves), Claude Code (drafts)

## Context

ADR 007 makes Spark Structured Streaming the bronze writer, with Iceberg
tables in the Glue catalog; ADR 009 adds PySpark batch jobs. Spark, the
Iceberg runtime, the AWS bundle, the Kafka and Avro connectors, the Docker
image and the Python used by the tests must all match. Checked on Maven
Central, PyPI and Docker Hub on 2026-10-03.

| Component | Newest | Constraint found |
|---|---|---|
| Apache Spark | 4.2.0 (2026-07-11) | Iceberg has **no runtime for Spark 4.2** yet |
| Iceberg | 1.12.0 (2026-09-25) | Runtimes for Spark 3.5, 4.0 and 4.1 |
| Spark 4.1 line | 4.1.3 (2026-07-12) | Built for Java 17, Scala 2.13 |
| `spark-sql-kafka-0-10` 4.1.3 | | Not in the Spark distribution; needs `spark-token-provider-kafka`, `kafka-clients` 3.9.1, `commons-pool2` 2.12.1 (`jsr305` and `scala-parallel-collections` are already in the image) |
| `kafka-clients` 3.9.1 vs our broker 4.3.1 | | Compatible: Kafka 4.0 dropped only protocol versions older than 2.1 |
| Docker image `spark:4.1.3-python3` | | Docker Official Image, rebuilt 2026-10-02; Ubuntu 22.04, OpenJDK 17.0.20, **Python 3.10.12**, user `spark` (uid 185) |

## Options considered

| Option | Assessment |
|---|---|
| A. Spark 4.2.0 | Newest engine, but no Iceberg runtime: cannot write the tables at all |
| B. Spark 4.0.x + Iceberg 1.12.0 | Works, but an older line with no advantage over 4.1 |
| C. **Spark 4.1.3 + Iceberg 1.12.0** | Newest combination the table format supports |
| Jars at run time (`--packages`) | Resolved from Maven by Ivy at every start: needs the network, slower start, no checksum, versions of transitive jars chosen at run time |
| **Jars baked into the image** | Downloaded once at build with `ADD --checksum`, as in the Connect image (ADR 003) |
| `apache/spark` image | Published by the Spark project, last rebuilt 2026-07-24 |
| **`spark` Docker Official Image** | Same Spark build, rebuilt when its base OS gets security fixes |

## Decision

Option C, jars baked into the image, Docker Official Image:

- base `spark:4.1.3-python3` pinned by digest
  (`sha256:8abe31671cf03d5bcf0c3746c31c62e76b65e450a005eafb2c017db76e56b294`);
- added jars (each verified against Maven Central's SHA-512 or SHA-1, then
  pinned by SHA-256): `iceberg-spark-runtime-4.1_2.13` 1.12.0,
  `iceberg-aws-bundle` 1.12.0 (AWS SDK v2 for S3FileIO and GlueCatalog,
  so no Hadoop S3A), `spark-sql-kafka-0-10_2.13` 4.1.3,
  `spark-token-provider-kafka-0-10_2.13` 4.1.3, `kafka-clients` 3.9.1,
  `commons-pool2` 2.12.1, `spark-avro_2.13` 4.1.3;
- unit tests and CI run **pyspark 4.1.3 on Python 3.10 with Java 17**,
  the image's versions, plus chispa 0.12.0.

## Consequences

- ✅ Every jar's source, version and hash is in the Dockerfile; the
  container starts without network access to Maven.
- ✅ Tests run on the same Spark, Python and Java as the jobs.
- ❌ Spark 4.2 is out of reach until Iceberg ships a runtime for it.
- ❌ **Python 3.10** in the Spark image, 3.12 elsewhere. PySpark refuses to
  run when the driver's and the executors' Python minor versions differ
  (`PYTHON_VERSION_MISMATCH`), so in P4 Airflow cannot be the Spark driver
  from a Python 3.12 container; jobs must be submitted from a container
  built on this image (decided in P4).
- ❌ A 2.1 GB base image (plus ~120 MB of jars) to pull and store.
- ❌ We maintain the jar list and hashes on every upgrade.

## References

- Maven Central metadata: `iceberg-spark-runtime-*_2.13`, `spark-sql-kafka-0-10_2.13` (POM dependencies), `spark-parent_2.13` 4.1.3 (`kafka.version`, `java.version`)
- Docker Hub: `spark` official image tags; `apache/spark`
- Apache Iceberg docs: Spark runtime per Spark version; AWS integration (GlueCatalog, S3FileIO)
- KIP-896 (Kafka 4.0 removes protocol versions older than 2.1)
