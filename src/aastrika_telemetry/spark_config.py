"""Builds the SparkSession used by the PySpark ETL path.

Local single-machine master for now (`local[*]`) -- swap `.master(...)` for a
cluster URL later, nothing else in this module needs to change for that.
"""

import logging
import os
import sys

logger = logging.getLogger(__name__)

# Confirm these against the actual ES server version (GET / on the ES host) and
# the Postgres server version before bumping -- don't guess at upgrade time.
ELASTICSEARCH_SPARK_CONNECTOR = "org.elasticsearch:elasticsearch-spark-30_2.12:8.13.4"
POSTGRESQL_JDBC_DRIVER = "org.postgresql:postgresql:42.7.4"


def _apply_local_env() -> None:
    """Windows-only local-dev setup -- winutils.exe/HADOOP_HOME is a Windows
    problem that doesn't exist on Linux, so nothing here runs on a VM/prod.

    Must run before `pyspark.sql` is imported -- py4j reads these env vars
    when it starts the JVM, so setting them any later has no effect.
    """
    if sys.platform == "win32":
        os.environ["HADOOP_HOME"] = "C:\\dp_pyspark\\hadoop"
        os.environ["JAVA_HOME"] = r"C:\Program Files\Eclipse Adoptium\jdk-17.0.18.8-hotspot"
        os.environ["PATH"] += r";C:\Program Files\Eclipse Adoptium\jdk-17.0.18.8-hotspot\bin"

    # Point Spark's JVM at the same Python interpreter running this script.
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


_apply_local_env()

from pyspark.sql import SparkSession  # noqa: E402 -- must import after env vars are set


def build_spark_session(app_name: str = "aastrika-telemetry-etl") -> SparkSession:
    """Create (or fetch) the SparkSession for the ETL job."""
    spark = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config(
            "spark.jars.packages",
            f"{ELASTICSEARCH_SPARK_CONNECTOR},{POSTGRESQL_JDBC_DRIVER}",
        )
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        # Pin the driver to loopback -- on a multi-adapter Windows machine
        # (VPN/virtual adapters alongside the real LAN NIC), Spark can resolve
        # the driver to the wrong interface between the driver and its own
        # local[*] executor, which crashes BlockManager registration with a
        # NullPointerException on "idWithoutTopologyInfo". Irrelevant for a
        # real cluster later, but required for `local[*]` on Windows.
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    logger.info("SparkSession created: %s", spark.sparkContext.appName)
    return spark
