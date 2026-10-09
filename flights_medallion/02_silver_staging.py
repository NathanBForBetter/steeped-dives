"""Silver staging: bronze Parquet -> one typed, lowercased stg_flights table.

Rename, cast, null out the source's placeholder strings, drop the columns BTS
ships empty, and mint the flight grain key. No joins and no business logic -
those start in 03_silver_intermediate.py.
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

import logging

from pyspark.sql import functions as F
from spark_session import get_spark

from steeped_dives.config import env_path, env_str

logger = logging.getLogger(__name__)

def clean_up_int_cols(df, numeric_columns: list):
    """Cast to double and lowercase the name.

    Double rather than int on purpose: these arrive as text, a bad value has
    to become NULL instead of aborting the cast for the whole partition, and
    several of them (complexity, delay minutes) are genuinely fractional.
    """
    for int_column in numeric_columns:
        df = df.withColumn(
            int_column,
            F.col(int_column).cast('double')
        )
        df = df.withColumnRenamed(int_column, int_column.lower())
        logger.info(f"Cleaned up : {int_column}")
    return df


def clean_up_text_cols(df, text_columns: list):
    for text_column in text_columns:
        df = df.withColumn(
            text_column,
            F.when(F.trim(F.col(text_column)).isin('', 'NA', 'N/A', 'NULL'), None)
             .otherwise(F.lower(F.col(text_column)))
        )
        df = df.withColumnRenamed(text_column, text_column.lower())
        logger.info(f"Cleaned up : {text_column}")
    return df


def clean_up_export_staging(
    parquet_dir: str,
):
    # Cast to double, lowercase the name.
    numeric_columns = [
        'AIR_TIME', 'ARR_DELAY', 'ARR_DELAY_NEW', 'ARR_TIME',
        'CARRIER_DELAY', 'CRS_ARR_TIME', 'CRS_DEP_TIME', 
        'CRS_ELAPSED_TIME', 'DEP_DELAY', 'ACTUAL_ELAPSED_TIME', 'DEP_DELAY_NEW', 'DEP_TIME',
        'DISTANCE', 'FIRST_DEP_TIME', 'FLIGHTS', 'SECURITY_DELAY', 
        'LATE_AIRCRAFT_DELAY', 'NAS_DELAY', 'WEATHER_DELAY', 'TOTAL_ADD_GTIME',
        'LONGEST_ADD_GTIME', 'TAXI_IN', 'TAXI_OUT', 'WHEELS_OFF', 'WHEELS_ON',
        ## CANCELLED is BTS's 0/1 flag; carried as a double like the rest so
        ## one cast path covers every numeric column, and read as a flag in
        ## 03_silver_intermediate.py
        'CANCELLED', 
    ]

    text_columns = [
        'DEST', 'DEST_CITY_NAME', 'DEST_STATE_ABR', 'DIV1_AIRPORT', 'FL_DATE', 'ORIGIN', 
        'ORIGIN_CITY_NAME', 'ORIGIN_STATE_ABR', 'OP_UNIQUE_CARRIER', 'CANCELLATION_CODE'
    ]

    drop_columns = [
        'DEST_AIRPORT_ID', 'ORIGIN_AIRPORT_ID', 
        ## there's diverted airports but all 0
        'DIVERTED', 'DIV_ACTUAL_ELAPSED_TIME', 'DIV_AIRPORT_LANDINGS', 'DIV_ARR_DELAY', 
        'DIV_DISTANCE', 'DIV_REACHED_DEST'

    ]

    spark = get_spark("bts-silver-staging")
    df = spark.read.parquet(parquet_dir)

    # The flight grain key.
    #
    # Scheduled-time based rather than flight-number based: FL_DATE, carrier,
    # origin, dest and the three CRS (scheduled) times. BTS does ship
    # OP_CARRIER_FL_NUM, and including it would make the key unique on its
    # own - the deliberate trade-off here is that this key intentionally
    # collides for the near-duplicate rows BTS emits for the same physical
    # flight, which int_flights then collapses with ANY_VALUE/MAX after
    # checking that the colliding rows differ only in rounding. The cost is
    # that two genuinely distinct same-carrier flights on the same route with
    # identical scheduled times would also merge. See the README section
    # "The flight grain key" for how to tighten this.
    #
    # Computed before clean_up_text_cols so the hash is over the source's own
    # casing; that is stable for BTS, which has always shipped these upper
    # case, and keeping it here means the key does not move if the
    # normalization rules ever change.
    df = df.withColumn(
        "flight_id",
        F.sha2(
            F.concat_ws(
            "||",
            F.coalesce(F.col("FL_DATE").cast("string"), F.lit("NULL")),
            F.coalesce(F.col("OP_UNIQUE_CARRIER"), F.lit("NULL")),
            F.coalesce(F.col("DEST"), F.lit("NULL")),
            F.coalesce(F.col("ORIGIN"), F.lit("NULL")),
            F.coalesce(F.col("CRS_ELAPSED_TIME").cast("string"), F.lit("NULL")),
            F.coalesce(F.col("CRS_DEP_TIME").cast("string"), F.lit("NULL")),
            F.coalesce(F.col("CRS_ARR_TIME").cast("string"), F.lit("NULL"))
            ),
            256
        )
    )

    df = df.withColumn("extract_date", F.lit(env_str("FLIGHTS_EXTRACT_DATE")).cast("date"))

    df = clean_up_int_cols(df, numeric_columns)
    df = clean_up_text_cols(df, text_columns)

    df = df.drop(*drop_columns)

    ## partitioned on the bronze partition columns so 03's reads of
    ## int_flights, and any year-scoped analysis, can prune instead of
    ## scanning all ~17M rows
    (df.write
       .mode('overwrite')
       .partitionBy('year', 'month')
       .saveAsTable('stg_flights'))

    row_count = spark.table('stg_flights').count()
    logger.info(f"wrote {row_count:,} row(s) to stg_flights")

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    clean_up_export_staging(
        parquet_dir=str(env_path('FLIGHTS_PARQUET_DIR', must_exist=True)),
    )