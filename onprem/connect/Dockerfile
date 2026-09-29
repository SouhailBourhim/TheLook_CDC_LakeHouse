# syntax=docker/dockerfile:1.7
# Kafka Connect worker: the same Apache Kafka image as the broker, plus two
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

# Confluent's Avro converter with all its dependencies (Schema Registry
# client, Avro, Jackson...). Confluent Community License.
ADD --checksum=sha256:f849f49ae500d1c02f94a50d1dd4b3e63d26433843007ac114a7f8e9fb3d6be8 \
    https://hub-downloads.confluent.io/api/plugins/confluentinc/kafka-connect-avro-converter/versions/8.3.2/confluentinc-kafka-connect-avro-converter-8.3.2.zip \
    /tmp/avro-converter.zip

# One folder per plugin: Connect gives each folder its own class loader, so
# the plugins' dependencies (Jackson, Guava...) cannot clash with each other.
RUN mkdir -p /plugins \
 && tar -xzf /tmp/debezium-postgres.tar.gz -C /plugins \
 && unzip -q /tmp/avro-converter.zip -d /tmp/avro \
 && mv /tmp/avro/confluentinc-kafka-connect-avro-converter-8.3.2/lib /plugins/kafka-connect-avro-converter

# --- Stage 2: the worker image (the archives stay in stage 1) ---------------
FROM ${KAFKA_IMAGE}
COPY --from=plugins /plugins /opt/connect-plugins

# The base image starts a broker; this image starts a distributed worker
# with the configuration mounted by compose.
ENTRYPOINT ["/opt/kafka/bin/connect-distributed.sh"]
CMD ["/etc/kafka-connect/connect-distributed.properties"]
