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
from functools import reduce
from pathlib import Path

from spark_session import get_spark
from unaccent import unaccent

from steeped_dives.config import env_path

logger = logging.getLogger(__name__)


def clean_column_name(column: str) -> str:
    ## spark sql and parquet both dislike spaces in column names
    return '_'.join(column.split())


def extract_origins(input_dir):
    output_table = 'nn_origins_raw'
    spark = get_spark('nn-extract-origins')

    input_files = sorted(
        p for p in Path(input_dir).iterdir() if p.suffix.lower() == '.csv'
    )

    if not input_files:
        logger.warning(f"No .csv files found in directory: {input_dir}")
        return

    logger.info(f"Found {len(input_files)} .csv file(s) to read in '{input_dir}'")

    ## every file carries its own header and they disagree - the columns
    ## differ in count (11-17), order and spelling from one letter to the
    ## next, so each file is read on its own terms and unioned BY NAME
    ## reading them as one glob would file c.csv's 'fem' under 'masc'
    frames = []
    for path in input_files:
        ## no inferSchema: this is the raw layer, so every column stays a
        ## string and nothing gets nulled by a bad cast
        ## escape='"' because these files quote by doubling ('""elf""'),
        ## which is the csv standard but not spark's default (a backslash) -
        ## left alone, any quoted field holding a comma splits into pieces
        frame = (spark.read
                 .option('header', 'true')
                 .option('escape', '"')
                 .csv(str(path)))
        frames.append(frame.toDF(*[clean_column_name(c) for c in frame.columns]))

    df = reduce(
        lambda left, right: left.unionByName(right, allowMissingColumns=True),
        frames,
    )

    ## a column a given letter never used comes back null for those rows
    logger.info(f"Unioned to {len(df.columns)} column(s): {df.columns}")

    ## fold accents into plain ascii so these line up with the census names
    df = df.withColumn('names_clean', unaccent('nll'))

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

    extract_origins(
        input_dir=env_path('NATALITY_ORIGINS_DIR', must_exist=True),
    )
