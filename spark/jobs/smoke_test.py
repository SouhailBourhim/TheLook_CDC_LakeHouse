"""Smoke test of the Spark image and cluster (P3, ADR 011).

Runs on the standalone cluster and checks each added piece once:
  1. the Kafka source jar, reading a few records of a CDC topic on the
     worker (read_committed, as the streaming job will);
  2. the Avro jar (to_avro/from_avro round trip);
  3. the Iceberg runtime and the Glue catalog, as the bronze writer's IAM
     user (listing the tables of thelook_bronze; nothing is created).

Usage: make spark-run JOB=jobs/smoke_test.py
"""

from pyspark.sql import functions as F
from pyspark.sql.avro.functions import from_avro, to_avro

from lakehouse.session import lake_session

spark = lake_session("smoke-test", cores_max=1)
jvm = spark.sparkContext._jvm
print(f"spark {spark.version}, iceberg {jvm.org.apache.iceberg.IcebergBuild.version()}")

# 1. Kafka: the value starts with Confluent's framing, magic byte 0 then a
# 4-byte schema id; Spark sees only bytes (decoding comes in the job).
records = (
    spark.read.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:29092")
    .option("subscribe", "thelook.shop.users")
    .option("startingOffsets", "earliest")
    .option("kafka.isolation.level", "read_committed")
    .load()
    .limit(3)
    .select(
        "topic",
        "offset",
        F.conv(F.hex(F.substring("value", 1, 1)), 16, 10).alias("magic_byte"),
        F.conv(F.hex(F.substring("value", 2, 4)), 16, 10).alias("schema_id"),
        F.length("value").alias("bytes"),
    )
)
records.show(truncate=False)

# 2. Avro: encode and decode a column.
avro = spark.range(3).select(from_avro(to_avro("id"), '"long"').alias("id"))
assert [r.id for r in avro.collect()] == [0, 1, 2]
print("avro round trip: ok")

# 3. Iceberg + Glue: GetDatabase and GetTables on thelook_bronze.
spark.sql("SHOW TABLES IN lake.thelook_bronze").show()
print("SMOKE TEST PASSED")
spark.stop()
