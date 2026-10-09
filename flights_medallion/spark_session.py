"""Local Spark session for the BTS flights medallion pipeline.

Every path and tuning value comes from the repo-root .env (see
.env.example), so nothing machine-specific is committed.
"""
# `steeped_dives` lives at the repo root, but running this file as a script
# puts only its own directory on sys.path - never the repo root - so the
# import below would raise ModuleNotFoundError. Adding the root explicitly
# keeps every script runnable straight from a clone; it is a no-op once the
# package is installed with `pip install -e .`.
import sys
from pathlib import Path

_REPO_ROOT = str(Path(__file__).resolve().parents[1])
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

import os

from pyspark.sql import SparkSession

from steeped_dives.config import env_str

## absolute paths so tables resolve no matter which directory a script is run from
DATA_DIR = env_str("FLIGHTS_DATA_DIR")
WAREHOUSE_DIR = f"{DATA_DIR}/spark-warehouse"
METASTORE_DIR = f"{DATA_DIR}/metastore_db"


def get_spark(app_name: str) -> SparkSession:

    # Spark + Hive metastore configuration
    # Derby is single-process, so only one Spark session/script should
    # use this local metastore at a time.

    os.environ.setdefault("JAVA_HOME", env_str("JAVA_HOME"))

    return (
        SparkSession.builder
        .appName(app_name)

        # Use all available local CPU cores
        .master(env_str("SPARK_MASTER", "local[*]"))

        # Give Spark enough JVM heap for ~17M flight rows
        .config("spark.driver.memory", env_str("SPARK_DRIVER_MEMORY", "8g"))
        .config("spark.executor.memory", env_str("SPARK_EXECUTOR_MEMORY", "8g"))

        # More parallelism for large aggregations / GROUP BY operations
        .config("spark.sql.shuffle.partitions", env_str("SPARK_SHUFFLE_PARTITIONS", "64"))

        # Local networking
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")

        # Spark warehouse
        .config("spark.sql.warehouse.dir", WAREHOUSE_DIR)

        # Local Derby Hive metastore
        .config(
            "spark.hadoop.javax.jdo.option.ConnectionURL",
            f"jdbc:derby:;databaseName={METASTORE_DIR};create=true"
        )

        # Keep derby.log inside the project data directory
        .config(
            "spark.driver.extraJavaOptions",
            f"-Dderby.system.home={DATA_DIR}"
        )

        .enableHiveSupport()

        .getOrCreate()
    )
