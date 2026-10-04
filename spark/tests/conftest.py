"""Shared fixtures for the PySpark tests.

Runs on the image's versions (ADR 011): pyspark 4.1.3, Python 3.10, Java 17.
pyspark from PyPI does not ship spark-avro, so its jar is downloaded once
into a cache and verified with the same SHA-256 as onprem/spark/Dockerfile.
"""

import base64
import datetime
import hashlib
import json
import os
import time
import urllib.request
from pathlib import Path

import pytest
from pyspark.sql import DataFrame, SparkSession

# PySpark converts timestamps to the *Python process's* local time zone
# when rows are collected, whatever spark.sql.session.timeZone says. The
# containers and the data are UTC (spec 4.1), so the tests are too;
# otherwise results shift by the laptop's offset (seen: +1 h).
os.environ["TZ"] = "UTC"
time.tzset()

FIXTURES = Path(__file__).parent / "fixtures"

AVRO_JAR_URL = (
    "https://repo1.maven.org/maven2/org/apache/spark/spark-avro_2.13/4.1.3/"
    "spark-avro_2.13-4.1.3.jar"
)
AVRO_JAR_SHA256 = "83f848dae53cfe511d61d81026c1b08321390b778684552f3ddaa44c02a77b9c"

# Schema ids as registered on the local stack when the fixtures were captured.
SCHEMA_FILES = {
    3: "thelook.shop.users-value.avsc",
    2: "thelook.shop.users-key.avsc",
    21: "thelook_mongo.web.reviews-value.avsc",
    20: "thelook_mongo.web.reviews-key.avsc",
}

KAFKA_SCHEMA = (
    "key binary, value binary, topic string, partition int, offset long, "
    "timestamp timestamp, timestampType int"
)


def avro_jar() -> str:
    cache = Path(
        os.environ.get("THELOOK_JAR_CACHE", Path.home() / ".cache" / "thelook-jars")
    )
    jar = cache / "spark-avro_2.13-4.1.3.jar"
    if not jar.exists():
        cache.mkdir(parents=True, exist_ok=True)
        urllib.request.urlretrieve(AVRO_JAR_URL, jar)
    digest = hashlib.sha256(jar.read_bytes()).hexdigest()
    if digest != AVRO_JAR_SHA256:
        jar.unlink()
        raise RuntimeError(f"spark-avro jar checksum mismatch: {digest}")
    return str(jar)


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.master("local[2]")
        .appName("lakehouse-tests")
        .config("spark.jars", avro_jar())
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.extraJavaOptions", "-Duser.timezone=UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture(scope="session")
def schemas() -> dict[int, str]:
    return {i: (FIXTURES / name).read_text() for i, name in SCHEMA_FILES.items()}


def kafka_row(record: dict) -> tuple:
    """One captured record as a row of Spark's Kafka source."""
    ts = datetime.datetime.fromtimestamp(
        record["timestamp_ms"] / 1000, datetime.timezone.utc
    )
    return (
        base64.b64decode(record["key"]) if record["key"] else None,
        base64.b64decode(record["value"]) if record["value"] else None,
        record["topic"],
        record["partition"],
        record["offset"],
        ts.replace(tzinfo=None),
        0,
    )


@pytest.fixture(scope="session")
def captured() -> dict[str, dict[str, dict]]:
    """Real records captured from the local topics: {topic: {op: record}}."""
    data = json.loads((FIXTURES / "records.json").read_text())
    return {topic: {r["op"]: r for r in records} for topic, records in data.items()}


@pytest.fixture
def kafka_df(spark):
    """Build a Kafka-source DataFrame from captured-style record dicts."""

    def build(records: list[dict]) -> DataFrame:
        return spark.createDataFrame([kafka_row(r) for r in records], KAFKA_SCHEMA)

    return build
