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

import logging
from pathlib import Path

from pyspark.sql.types import StringType, StructField, StructType
from spark_session import get_spark

from steeped_dives.config import env_path

logger = logging.getLogger(__name__)


def extract_names(input_dir):
    input_columns = ['state', 'gender', 'year', 'name', 'name_count']
    output_table = 'nn_state_raw'
    spark = get_spark('nn-extract-names')

    ## SSA ships one headerless comma-delimited file per state, named .TXT
    ## collect the paths here rather than globbing: Hadoop matches globs
    ## case-sensitively even on macOS, so '*.txt' would find nothing
    input_files = sorted(
        p for p in Path(input_dir).iterdir() if p.suffix.lower() == '.txt'
    )

    if not input_files:
        logger.warning(f"No .txt files found in directory: {input_dir}")
        return

    logger.info(f"Found {len(input_files)} .txt file(s) to read in '{input_dir}'")

    ## every column lands as string - this is the raw layer, so a bad value
    ## should survive to be seen, not get silently nulled by a cast
    ## an explicit schema also skips the inference pass over 6.7m rows
    schema = StructType([
        StructField(column, StringType(), True) for column in input_columns
    ])

    df = (spark.read
          .schema(schema)
          .option('header', 'false')
          .csv([str(p) for p in input_files]))

    df.write.mode('overwrite').saveAsTable(output_table)

    logger.info(
        f"Wrote {spark.table(output_table).count()} row(s) to table: {output_table}"
    )

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    extract_names(
        input_dir=env_path('NATALITY_NAMES_DIR', must_exist=True),
    )
