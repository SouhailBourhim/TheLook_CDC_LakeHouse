# syntax=docker/dockerfile:1.7
# Three stages (ADR 011, ADR 013, ADR 018):
#   spark     the cluster and job image: the Docker Official Spark image plus
#             the jars Spark does not ship (Compose builds it with target: spark)
#   features  that stage plus the Redis client, for the features stream only:
#             the cluster image (and the bronze stream) stays without it
#   airflow   Airflow on top of the spark stage, so its spark-submit drivers
#             share Python 3.10, Java 17 and the jars with the executors;
#             plus the Neo4j client for the graph job's driver (ADR 019)
#
# Spark image for the lake jobs (ADR 011): the Docker Official Spark image
# plus the jars Spark does not ship. Each jar was verified against Maven
# Central's published SHA-512 or SHA-1, then pinned here by SHA-256;
# ADD --checksum fails the build if a download differs.
FROM spark:4.1.3-python3@sha256:8abe31671cf03d5bcf0c3746c31c62e76b65e450a005eafb2c017db76e56b294 AS spark

ARG MAVEN=https://repo1.maven.org/maven2

# Iceberg 1.12.0: the Spark 4.1 runtime (catalog, DataSource V2, SQL
# extensions for MERGE INTO and CALL) and the AWS bundle (AWS SDK v2 for
# S3FileIO and GlueCatalog, so no Hadoop S3A is needed).
ADD --chmod=644 --checksum=sha256:3123c3798af642dd537230923548e013ef41cc28b6626e5225808cf5670a0f82 \
    ${MAVEN}/org/apache/iceberg/iceberg-spark-runtime-4.1_2.13/1.12.0/iceberg-spark-runtime-4.1_2.13-1.12.0.jar /opt/spark/jars/
ADD --chmod=644 --checksum=sha256:dd7e49ffa4ac9b215bed846581991259e3aec9bb2fa08d6cec029c46cd66d957 \
    ${MAVEN}/org/apache/iceberg/iceberg-aws-bundle/1.12.0/iceberg-aws-bundle-1.12.0.jar /opt/spark/jars/

# Kafka source for Structured Streaming and its compile dependencies
# (from its POM; jsr305 and scala-parallel-collections are already in the
# image). kafka-clients 3.9.1 talks to our 4.3.1 broker (Kafka 4.0 removed
# only protocol versions older than 2.1).
ADD --chmod=644 --checksum=sha256:cb31ffd9c8f5527dba061db8af671d295562092e4f3f5ff42fc249fab8a8fde6 \
    ${MAVEN}/org/apache/spark/spark-sql-kafka-0-10_2.13/4.1.3/spark-sql-kafka-0-10_2.13-4.1.3.jar /opt/spark/jars/
ADD --chmod=644 --checksum=sha256:0535a111d7d11b2859c0a0548d5c8d172c6925f9b8d041d0f0dd38ddc5a3497c \
    ${MAVEN}/org/apache/spark/spark-token-provider-kafka-0-10_2.13/4.1.3/spark-token-provider-kafka-0-10_2.13-4.1.3.jar /opt/spark/jars/
ADD --chmod=644 --checksum=sha256:7568b998572d256f0b7bc0afdc1b7a2588b8b08415c62ce314c864a6851ae9d9 \
    ${MAVEN}/org/apache/kafka/kafka-clients/3.9.1/kafka-clients-3.9.1.jar /opt/spark/jars/
ADD --chmod=644 --checksum=sha256:5273c8bc8c0dca2211175c0d27efbd7228efae99689aac001a8e1ef8e872e9ef \
    ${MAVEN}/org/apache/commons/commons-pool2/2.12.1/commons-pool2-2.12.1.jar /opt/spark/jars/

# Avro decoding (from_avro) for Debezium's records.
ADD --chmod=644 --checksum=sha256:83f848dae53cfe511d61d81026c1b08321390b778684552f3ddaa44c02a77b9c \
    ${MAVEN}/org/apache/spark/spark-avro_2.13/4.1.3/spark-avro_2.13-4.1.3.jar /opt/spark/jars/

# Settings shared by every job (Iceberg catalog, extensions); per-job
# values (bucket, credentials, cores) are set by spark/lakehouse/session.py.
# The base image has no /opt/spark/conf: create it first (mode 755). If
# COPY --chmod created it, the folder would get mode 644, no execute bit,
# so the spark user could not enter it and Spark would silently skip the
# file (the same trap as ADD --chmod in the Connect image, P1).
USER root
RUN mkdir -p /opt/spark/conf
COPY --chmod=644 spark-defaults.conf /opt/spark/conf/spark-defaults.conf
# Streaming checkpoints (a named volume). An empty named volume copies the
# owner of the image's folder, so the spark user (uid 185) can write it;
# otherwise the volume would be root-owned.
RUN mkdir -p /opt/checkpoints && chown spark:spark /opt/checkpoints
USER spark

# --- Features stage (ADR 018) --------------------------------------------------------
FROM spark AS features
USER root
# redis-py and its one dependency on Python 3.10, from a hashed lock file
# (uv pip compile --generate-hashes): pip refuses anything else.
COPY features-requirements.lock /tmp/features-requirements.lock
RUN python3 -m pip install --no-cache-dir --require-hashes -r /tmp/features-requirements.lock \
 && rm /tmp/features-requirements.lock
USER spark

# --- Airflow stage (ADR 013) -------------------------------------------------------
FROM spark AS airflow
USER root

# uv 0.12.20, the same version as locally and in CI: the image's Python has
# no ensurepip, so `python3 -m venv` fails; uv creates the virtualenv itself
# and supports --override. Checksum from the release's .sha256 file.
ADD --checksum=sha256:6590717592ace991ff83a63fef799e3ad9d33ecc8f96c5d6bdd732496e79337f \
    https://github.com/astral-sh/uv/releases/download/0.12.20/uv-x86_64-unknown-linux-gnu.tar.gz /tmp/uv.tgz
RUN tar -xzf /tmp/uv.tgz -C /usr/local/bin --strip-components=1 && rm /tmp/uv.tgz

# Airflow's tested dependency versions for 3.3.2 on Python 3.10, pinned by
# checksum, plus our one override (airflow-overrides.txt).
ADD --checksum=sha256:9c78ff08ccbe56f9a8cc0a3171927d53a95bd49d12d6bd7eaa91281b34b654fb \
    https://raw.githubusercontent.com/apache/airflow/constraints-3.3.2/constraints-3.10.txt /opt/airflow/constraints.txt
COPY airflow-overrides.txt /opt/airflow/overrides.txt

# Airflow in its own virtualenv, so the image's system Python (which runs
# the Spark drivers and executors) is untouched. The pins we override are
# removed from our copy of the constraints first: an --override replaces
# what packages *declare*, but a constraint still applies on top, so
# keeping pyspark-client==4.2.0 there next to our 4.1.3 is unsatisfiable.
RUN sed -E "/^($(grep -v '^#' /opt/airflow/overrides.txt | cut -d= -f1 | paste -sd'|'))==/d" \
      /opt/airflow/constraints.txt > /opt/airflow/constraints-thelook.txt \
 && uv venv --python /usr/bin/python3 /opt/airflow/venv \
 && uv pip install --python /opt/airflow/venv --no-cache \
      apache-airflow==3.3.2 \
      apache-airflow-providers-apache-spark==6.3.2 \
      apache-airflow-providers-postgres==7.0.2 \
      --constraint /opt/airflow/constraints-thelook.txt \
      --override /opt/airflow/overrides.txt \
 && mkdir -p /opt/airflow/logs /opt/airflow/dags \
 && chown -R spark:spark /opt/airflow

# The graph job's driver (ADR 019) writes to Neo4j. Drivers run on the
# system Python (PYSPARK_DRIVER_PYTHON), not in Airflow's virtualenv, so the
# client goes there, from a hashed lock file. The cluster image stays
# without it: executors never import it.
COPY graph-requirements.lock /tmp/graph-requirements.lock
RUN python3 -m pip install --no-cache-dir --require-hashes -r /tmp/graph-requirements.lock \
 && rm /tmp/graph-requirements.lock

ENV AIRFLOW_HOME=/opt/airflow \
    PATH=/opt/airflow/venv/bin:$PATH
USER spark
