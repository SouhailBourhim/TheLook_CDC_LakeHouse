"""SparkSession factory shared by every lake job."""

import os

from pyspark.sql import SparkSession

# Passed from the driver to its executors (spark.executorEnv.*): each
# application brings its own AWS identity, so the shared worker holds no
# key and two jobs on one cluster keep separate IAM users (iam.tf).
AWS_VARIABLES = ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_REGION")


def lake_session(
    app_name: str, cores_max: int | None = None, conf: dict | None = None
) -> SparkSession:
    """A session with the Iceberg catalog "lake" (Glue) configured.

    cores_max caps the cores this application takes from the shared
    standalone cluster, so a long-running streaming job leaves room for the
    batch jobs (by default an application takes every free core). conf adds
    job-specific settings (e.g. executor memory).
    """
    builder = SparkSession.builder.appName(app_name).config(
        # The bucket name contains the account id, so it comes from
        # onprem/.env (LAKE_BUCKET), not from the committed defaults.
        "spark.sql.catalog.lake.warehouse",
        f"s3://{os.environ['LAKE_BUCKET']}/",
    )
    for name in AWS_VARIABLES:
        if name in os.environ:
            # Set from the environment inside the driver, not on the
            # spark-submit command line (visible in `ps`). The UI redacts
            # the secret (Spark's redaction regex matches "secret").
            builder = builder.config(f"spark.executorEnv.{name}", os.environ[name])
    if cores_max:
        builder = builder.config("spark.cores.max", str(cores_max))
    for key, value in (conf or {}).items():
        builder = builder.config(key, value)
    spark = builder.getOrCreate()
    # Spark silently skips an unreadable spark-defaults.conf; fail loudly
    # instead of later with a confusing "nested databases" error.
    if spark.conf.get("spark.sql.catalog.lake", None) is None:
        raise RuntimeError(
            "catalog 'lake' is not configured: /opt/spark/conf/spark-defaults.conf "
            "was not loaded (see onprem/spark/Dockerfile)"
        )
    return spark
