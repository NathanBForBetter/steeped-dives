"""Load the three source CSVs into the DuckDB file that the dbt project reads.

Each CSV becomes one raw table, replaced on every run:

    chatgpt_random_clean.csv   random_num_ai_raw       ChatGPT picks
    random-humans.csv          random_num_people_raw   people's picks
    random-numbers.csv         random_num_true_raw     true random picks, from true_random.py

Run this before dbt. DuckDB lets one process write to a file at a time.
"""
# `steeped_dives` lives at the repo root, but running this file as a script
# puts only its own directory on sys.path, so the import below would raise
# ModuleNotFoundError. Adding the root keeps the script runnable straight from
# a clone.
import sys
from pathlib import Path

_REPO_ROOT = str(Path(__file__).resolve().parents[1])
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

import logging
import os

import duckdb

from steeped_dives.config import env_path

logger = logging.getLogger(__name__)

output_path = str(env_path("RANDOM_DATA_DIR", must_exist=True))

duckdb_file = 'random_numbers.duckdb'

ai_random_file = 'chatgpt_random_clean.csv'
ai_random_table_name = 'random_num_ai_raw'

people_random_file = 'random-humans.csv'
people_random_table_name = 'random_num_people_raw'

true_random_file = 'random-numbers.csv'
true_random_table_name = 'random_num_true_raw'


def csvs_to_duckdb_tables(con, csv_tables):
    """Load each CSV into its own DuckDB table, replacing the table if it exists.

    `csv_tables` maps a CSV path to the name of the table it lands in.
    """
    for csv_path, table_name in csv_tables.items():
        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"No CSV at {csv_path} for table: {table_name}")

        ## every column lands as text. This is the raw layer, so a bad value
        ## should reach staging and be seen there, not be nulled by a cast here
        con.execute(
            f'create or replace table "{table_name}" as '
            'select * from read_csv(?, header = true, all_varchar = true)',
            [str(csv_path)],
        )

        row_count = con.execute(f'select count(*) from "{table_name}"').fetchone()[0]
        logger.info(f"Wrote {row_count} row(s) to table: {table_name}")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    with duckdb.connect(os.path.join(output_path, duckdb_file)) as con:
        csvs_to_duckdb_tables(con, {
            os.path.join(output_path, ai_random_file): ai_random_table_name,
            os.path.join(output_path, people_random_file): people_random_table_name,
            os.path.join(output_path, true_random_file): true_random_table_name,
        })
