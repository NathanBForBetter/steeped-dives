"""Local Spark session for the natality + names Kimball pipeline.

Every path and tuning value comes from the repo-root .env (see
.env.example), so nothing machine-specific is committed. The same warehouse
and metastore are configured in dbt/profiles.yml, which reaches the identical
values through env_var().
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
DATA_DIR = env_str("NATALITY_DATA_DIR")
WAREHOUSE_DIR = f"{DATA_DIR}/spark-warehouse"
METASTORE_DIR = f"{DATA_DIR}/metastore_db"


def get_spark(app_name: str) -> SparkSession:
    ## hive support = table names persist in a Derby metastore between runs
    ## Derby is single-process: only one script can hold the session at a time
    os.environ.setdefault("JAVA_HOME", env_str("JAVA_HOME"))
    return (SparkSession.builder
            .appName(app_name)
            .master(env_str("NATALITY_SPARK_MASTER", "local[4]"))
            .config("spark.sql.shuffle.partitions",
                    env_str("NATALITY_SPARK_SHUFFLE_PARTITIONS", "8"))
            .config("spark.driver.bindAddress", "127.0.0.1")
            .config("spark.driver.host", "127.0.0.1")
            .config("spark.sql.warehouse.dir", WAREHOUSE_DIR)
            .config("spark.hadoop.javax.jdo.option.ConnectionURL",
                    f"jdbc:derby:;databaseName={METASTORE_DIR};create=true")
            ## keeps derby.log out of whatever directory you ran from
            .config("spark.driver.extraJavaOptions", f"-Dderby.system.home={DATA_DIR}")
            .enableHiveSupport()
            .getOrCreate())
