# syntax=docker/dockerfile:1.7
# Kafka Connect worker: the same Apache Kafka image as the broker, plus
# plugins. Debezium 3.7.0 is built against Kafka 4.3.1, the version used here.

ARG KAFKA_IMAGE=apache/kafka:4.3.1@sha256:77e3df9054047a88b520d0cc46e16696d3b22022e1d580aeccd2632df6532837

# --- Stage 1: download, verify and unpack the plugins ------------------------
FROM ${KAFKA_IMAGE} AS plugins
USER root

# ADD --checksum refuses to build if the downloaded file's SHA-256 differs.
# Debezium's hash was cross-checked against Maven Central's published SHA-512.
ADD --checksum=sha256:660673cbabd2f269da21409208ebd0f0d9977bd92e824fc9daa272f240e88e51 \
    https://repo1.maven.org/maven2/io/debezium/debezium-connector-postgres/3.7.0.Final/debezium-connector-postgres-3.7.0.Final-plugin.tar.gz \
    /tmp/debezium-postgres.tar.gz

# Debezium MongoDB connector, same release as the PostgreSQL one (they share
# debezium-core). Hash cross-checked against Maven Central's SHA-512.
ADD --checksum=sha256:81273790cae0df8072ccef4d96c143231b917345c075cc78735cbb0cd3f8bf11 \
    https://repo1.maven.org/maven2/io/debezium/debezium-connector-mongodb/3.7.0.Final/debezium-connector-mongodb-3.7.0.Final-plugin.tar.gz \
    /tmp/debezium-mongodb.tar.gz

# Confluent's Avro converter with all its dependencies (Schema Registry
# client, Avro, Jackson...). Confluent Community License.
ADD --checksum=sha256:f849f49ae500d1c02f94a50d1dd4b3e63d26433843007ac114a7f8e9fb3d6be8 \
    https://hub-downloads.confluent.io/api/plugins/confluentinc/kafka-connect-avro-converter/versions/8.3.2/confluentinc-kafka-connect-avro-converter-8.3.2.zip \
    /tmp/avro-converter.zip

# One folder per plugin: Connect gives each folder its own class loader, so
# the plugins' dependencies (Jackson, Guava...) cannot clash with each other.
RUN mkdir -p /plugins \
 && tar -xzf /tmp/debezium-postgres.tar.gz -C /plugins \
 && tar -xzf /tmp/debezium-mongodb.tar.gz -C /plugins \
 && unzip -q /tmp/avro-converter.zip -d /tmp/avro \
 && mv /tmp/avro/confluentinc-kafka-connect-avro-converter-8.3.2/lib /plugins/kafka-connect-avro-converter

# --- Stage 2: the worker image (the archives stay in stage 1) ---------------
FROM ${KAFKA_IMAGE}
COPY --from=plugins /plugins /opt/connect-plugins

# Prometheus JMX exporter agent 1.6.0 (Apache 2.0). Published on GitHub
# releases (Maven Central stops at 1.0.1); the hash matches the
# release's .sha256 file. Enabled with -javaagent in compose (KAFKA_OPTS).
# ADD from a URL creates the file root-owned with mode 600; the worker runs
# as appuser, so make it readable. Added into /opt, which already exists:
# --chmod also applies to any directory ADD creates, and a 644 directory
# cannot be entered (no execute bit).
ADD --chmod=644 --checksum=sha256:a95983fd96e865d2bcdf911cc500e7c82808c27ab9fd226bf96732b6c3d8c46e \
    https://github.com/prometheus/jmx_exporter/releases/download/1.6.0/jmx_prometheus_javaagent-1.6.0.jar \
    /opt/jmx_prometheus_javaagent.jar

# The base image starts a broker; this image starts a distributed worker
# with the configuration mounted by compose.
ENTRYPOINT ["/opt/kafka/bin/connect-distributed.sh"]
CMD ["/etc/kafka-connect/connect-distributed.properties"]
