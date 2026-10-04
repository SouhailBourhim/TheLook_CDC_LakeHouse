# 013. Airflow runtime: on the Spark image, spark-submit in client mode

- Status: Accepted
- Date: 2026-10-04 (P4, first commit; spec FR12, ADR 011)
- Deciders: Souhail Bourhim (approves), Claude Code (drafts)

## Context

P4 needs Apache Airflow (spec 5.2, FR12) to run the PySpark silver and gold
jobs every 30 minutes, and a daily maintenance job. ADR 011 left a
constraint: the Spark image runs **Python 3.10**, and PySpark refuses to run
when the driver's and the executors' Python minor versions differ
(`PYTHON_VERSION_MISMATCH`). In client mode the driver runs where
spark-submit runs, i.e. inside the Airflow task.

Checked on 2026-10-04: Airflow **3.3.2** supports Python 3.10-3.14; Airflow
publishes a constraints file for 3.3.2 on Python 3.10 (727 pins);
`apache-airflow-providers-apache-spark` 6.3.2 (SparkSubmitOperator)
requires `pyspark-client>=4.0.0`, and the constraints pin it to **4.2.0**,
newer than our Spark 4.1.3.

## Options considered

| Option | Assessment |
|---|---|
| A. Official `apache/airflow` image (Python 3.12) + SparkSubmitOperator | The driver would run Python 3.12 against 3.10 executors: refused by PySpark; also needs Java and the 7 jars added |
| B. DockerOperator starting a job container from the Spark image | Matching Python, but Airflow needs the Docker socket: root-equivalent access to the host |
| C. Spark Connect server, Airflow as a thin client | No Python constraint for DataFrame code, but a newer moving part and a long-running server to operate |
| D. **Airflow installed on top of our Spark image** | Same Python 3.10, Java 17, Spark 4.1.3 and jars as the cluster; SparkSubmitOperator runs the image's own spark-submit |

## Decision

Option D.

- `onprem/airflow/Dockerfile`: `FROM thelook-spark:local`; Airflow in its
  own virtualenv (`/opt/airflow/venv`, built with uv) so the image's system
  Python is untouched; `apache-airflow==3.3.2`,
  `apache-airflow-providers-apache-spark==6.3.2`,
  `apache-airflow-providers-postgres`, installed **with the official
  constraints file**, plus one documented override:
  `pyspark-client==4.1.3` (matches the cluster; the provider needs
  `>=4.0.0`). spark-submit puts `$SPARK_HOME/python/lib/pyspark.zip` first
  on the path, so jobs always run the image's PySpark.
- Tasks use **SparkSubmitOperator in client mode** against
  `spark://spark-master:7077`; each job brings its own AWS identity through
  `lakehouse.session` (no key in Airflow's connections).
- **LocalExecutor**: tasks run as subprocesses of the scheduler; enough for
  one DAG run at a time on a laptop.
- Its own metadata database (a small PostgreSQL 17 container), never the
  source database.
- Components (Airflow 3): api-server (UI on `127.0.0.1:8088`, since 8080
  is Spark's UI), scheduler, dag-processor, a one-shot init (database
  migration), in a new `airflow` Compose profile (~2-2.5 GB plus ~1 GB per
  running Spark driver).

### Additions at acceptance (Souhail, 2026-10-04)

1. **Base image pinned beyond the tag.** `thelook-spark:local` is a local
   tag that any rebuild can move. The Airflow image is therefore a second
   stage of `onprem/spark/Dockerfile` (`FROM <spark stage>`), whose own
   base is pinned by digest: Airflow gets the exact Spark bytes of the
   cluster by construction. The constraints file is downloaded with
   `ADD --checksum`.
2. **Only the batch key in Airflow's environment.** Airflow containers get
   `SPARK_BATCH_*` (and, from step 9, the maintenance user's key), never
   `SPARK_STREAM_*`.
3. **Bounded resources.** A memory limit on the scheduler container (it
   hosts the Spark drivers under LocalExecutor) and `max_active_runs=1` on
   the DAGs, so overlapping runs cannot stack drivers or contend for the
   cluster's 2 batch cores.

## Consequences

- ✅ Driver and executors share Python 3.10, Java 17 and Spark 4.1.3 by
  construction: no version drift between Airflow and the cluster.
- ✅ No Docker socket, no extra server.
- ✅ Dependencies reproducible (constraints file, one explicit override).
- ❌ A large image (Spark ~2.4 GB plus Airflow), rebuilt when the Spark
  image changes.
- ❌ We diverge from Airflow's official image (its entrypoint conveniences
  and security patches); upgrades mean regenerating the override.
- ❌ The Spark driver runs inside the scheduler container: a heavy job
  takes scheduler memory; parallel runs are capped (one at a time).

## References

- Airflow docs: installation from PyPI with constraints; Airflow 3 architecture (api-server, dag-processor, Task SDK)
- apache-airflow-providers-apache-spark: SparkSubmitOperator
- ADR 011 (Spark runtime, Python 3.10); cahier des charges v2.0: FR12
