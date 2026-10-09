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
import re
from collections import defaultdict
from functools import reduce
from pathlib import Path

from pyspark.sql import functions as F
from spark_session import get_spark

from steeped_dives.config import env_path

logger = logging.getLogger(__name__)

## "Natality, 1995-2002_age.csv" -> date_range '1995-2002', data_type 'age'
## greedy on the range so a range of its own ('2023 through Last Month')
## splits on the last underscore
FILENAME_PATTERN = re.compile(r'^Natality, (?P<date_range>.+)_(?P<data_type>[a-z]+)\.csv$')

## wonder renames these between dataset vintages - 'State' becomes 'State of
## Residence', 'Sex' becomes 'Sex of Infant' - so they are mapped by name
## rather than by position
SHARED_COLUMNS = {
    'notes': 'notes',
    'state': 'state',
    'state of residence': 'state',
    'state code': 'state_code',
    'state of residence code': 'state_code',
    ## year and year code are NOT redundant: the provisional extract carries
    ## '2025 (Provisional)' as the label and a bare year as the code
    'year': 'year',
    'year code': 'year_code',
    'sex': 'sex',
    'sex of infant': 'sex',
    'sex code': 'sex_code',
    'sex of infant code': 'sex_code',
    'births': 'births',
}

## whatever column is left over is the dimension being broken out, and it is
## spelled differently in every vintage ('Age of Mother', 'Age of Mother 9',
## "Mother's Race", "Mother's Bridged Race", "Mother's Single/Multi Race 31")
DIMENSION_COLUMNS = {
    'age': 'age_of_mother',
    'edu': 'mothers_education',
    'hisp': 'mothers_hispanic_origin',
    'race': 'mothers_race',
}

OUTPUT_TABLE = 'nn_natality_raw_{data_type}'


def target_columns(data_type: str) -> list:
    dimension = DIMENSION_COLUMNS[data_type]
    return ['state', 'state_code', 'year', 'year_code', 'sex', 'sex_code',
            dimension, f'{dimension}_code', 'births', 'source']


def rename_columns(columns: list, data_type: str) -> list:
    """Map one file's headers onto the shared names for its data_type."""
    dimension = DIMENSION_COLUMNS[data_type]
    renamed = []
    for column in columns:
        key = ' '.join(column.split()).lower()
        if key in SHARED_COLUMNS:
            renamed.append(SHARED_COLUMNS[key])
        elif key.endswith(' code'):
            renamed.append(f'{dimension}_code')
        else:
            renamed.append(dimension)
    return renamed


def read_file(spark, path: Path, data_type: str, date_range: str):
    ## escape='"' for the same reason as the origins csvs - wonder quotes by
    ## doubling, and spark defaults the escape character to a backslash
    frame = (spark.read
             .option('header', 'true')
             .option('escape', '"')
             .csv(str(path)))

    frame = frame.toDF(*rename_columns(frame.columns, data_type))

    ## wonder appends a footer (Dataset/Messages/Caveats, separated by '---')
    ## which parses as rows with only the first field filled, and emits
    ## subtotal rows marked Notes='Total' that would double count
    frame = frame.where(
        "(notes IS NULL OR notes = '') AND state IS NOT NULL AND births IS NOT NULL"
    ).drop('notes')

    ## the filename's range is the wonder dataset it came from - it is the
    ## only record of which vintage, and so which taxonomy, a row follows
    frame = frame.withColumn('source', F.lit(date_range))

    return frame.select(*target_columns(data_type))


def extract_natality(input_dir):
    spark = get_spark('nn-extract-natality')

    by_data_type = defaultdict(list)
    skipped = []
    for path in sorted(Path(input_dir).iterdir()):
        if path.suffix.lower() != '.csv':
            continue
        match = FILENAME_PATTERN.match(path.name)
        if match is None or match.group('data_type') not in DIMENSION_COLUMNS:
            skipped.append(path.name)
            continue
        by_data_type[match.group('data_type')].append((path, match.group('date_range')))

    if skipped:
        logger.warning(f"Ignoring {len(skipped)} unrecognized file(s): {skipped}")
    if not by_data_type:
        logger.warning(f"No 'Natality, [range]_[type].csv' files found in: {input_dir}")
        return

    for data_type, entries in sorted(by_data_type.items()):
        output_table = OUTPUT_TABLE.format(data_type=data_type)
        ranges = [date_range for _, date_range in entries]
        logger.info(f"{data_type}: {len(entries)} file(s) covering {ranges}")

        frames = [read_file(spark, path, data_type, date_range)
                  for path, date_range in entries]
        df = reduce(lambda left, right: left.unionByName(right), frames)

        df.write.mode('overwrite').saveAsTable(output_table)

        written = spark.table(output_table)
        logger.info(
            f"Wrote {written.count()} row(s) to table: {output_table} "
            f"({len(written.columns)} columns)"
        )

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    extract_natality(
        input_dir=env_path('NATALITY_NATALITY_DIR', must_exist=True),
    )
