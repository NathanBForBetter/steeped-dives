"""Full data-test suite for the flights pipeline.

A uniqueness check over ~17M rows is not free, so the assertions do not run
inside each transform. They live here, as one script that runs the whole
suite on demand:

    python flights_medallion/06_run_data_tests.py              # everything
    python flights_medallion/06_run_data_tests.py --layer gold # gold only
    python flights_medallion/06_run_data_tests.py --fail-fast

Every assertion the dbt projects in this repo get for free - unique, not_null,
relationships, accepted_values, assert_grain - is covered, plus one they do
not have: rows_are_real, which checks that de-duplication picked a whole
source row rather than assembling one from several. Failures are collected
and reported together by default, because knowing all five things that broke
is more useful than knowing the first one.
"""
import argparse
import logging
import sys

import spark_data_tests as st
from spark_session import get_spark

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

DELAY_CATEGORIES = [
    'unknown', 'severe early', 'moderate early', 'mild early', 'minor early',
    'on time', 'minor delay', 'mild delay', 'moderate delay', 'severe delay',
]
TIME_CATEGORIES = ['unknown', 'night', 'morning', 'afternoon', 'evening']
REGIONS = ['Northeast', 'Midwest', 'South', 'West']
# The nine types in seeds/flight_calendar_events.csv. A typo in a new row
# shows up here rather than as a silently unmatched category in the mart.
EVENT_TYPES = [
    'energy increase', 'energy restriction', 'government restriction',
    'incident', 'route closure', 'staffing shortage', 'tech issue',
    'war or conflict', 'weather or natural disaster',
]


def seed_tests(spark):
    """The event calendar is reference data joined 1:1 onto flight dates.

    A duplicate event_date fans out every flight on that day, which no grain
    test downstream would attribute back to a CSV edit - so it is asserted
    here, at the source.
    """
    yield 'flight_calendar_events.event_date unique', lambda: st.test_unique(
        spark, 'flight_calendar_events', 'event_date')
    yield 'flight_calendar_events.event_date not_null', lambda: st.test_not_null(
        spark, 'flight_calendar_events', 'event_date')
    yield 'flight_calendar_events.related_event not_null', lambda: st.test_not_null(
        spark, 'flight_calendar_events', 'related_event')
    yield 'flight_calendar_events.related_event_type accepted_values', (
        lambda: st.test_accepted_values(
            spark, 'flight_calendar_events', 'related_event_type', EVENT_TYPES))


def silver_tests(spark):
    """Flight grain holds across every silver view before anything aggregates."""
    # The seed join must not have multiplied the flight grain.
    yield 'int_flight_times grain after event join', lambda: st.test_assert_grain(
        spark, 'int_flight_times', 'flight_id')
    # int_flights is the de-duplicated spine; everything else is keyed to it.
    yield 'int_flights grain', lambda: st.test_assert_grain(spark, 'int_flights', 'flight_id')
    yield 'int_flights.flight_id not_null', lambda: st.test_not_null(
        spark, 'int_flights', 'flight_id')
    yield 'int_flights.flight_id unique', lambda: st.test_unique(
        spark, 'int_flights', 'flight_id')

    # The de-duplication must pick a whole row, not assemble one. A grain test
    # cannot tell the difference, because an assembled row still has a unique
    # key and no nulls. This compares every surviving row back to stg_flights.
    yield 'int_flights rows are real', lambda: st.test_rows_are_real(
        spark, 'int_flights', 'stg_flights', 'flight_id',
        ['dep_time', 'arr_time', 'dep_delay', 'arr_delay', 'actual_elapsed_time',
         'air_time', 'taxi_in', 'taxi_out', 'wheels_off', 'wheels_on',
         'cancelled', 'cancellation_code'])

    for view in ('int_flight_delay', 'int_flight_routes', 'int_flight_times',
                 'int_flight_durations', 'int_flight_cancel_div'):
        # Each subject view is a projection of int_flights, so each must come
        # back at exactly the same grain. A fan-out in any one of them would
        # multiply the fact when 04_gold_core joins all five.
        yield f'{view} grain', (lambda v=view: st.test_assert_grain(spark, v, 'flight_id'))
        yield f'{view}.flight_id not_null', (
            lambda v=view: st.test_not_null(spark, v, 'flight_id'))
        yield f'{view}.flight_id unique', (
            lambda v=view: st.test_unique(spark, v, 'flight_id'))

    # Bucketing is shared by three columns, so a typo in one shows up here.
    for column in ('dep_delay_category', 'arr_delay_category', 'delay_change_category'):
        yield f'int_flight_delay.{column} accepted_values', (
            lambda c=column: st.test_accepted_values(
                spark, 'int_flight_delay', c, DELAY_CATEGORIES))

    for column in ('dep_time_category', 'arr_time_category'):
        yield f'int_flight_times.{column} accepted_values', (
            lambda c=column: st.test_accepted_values(
                spark, 'int_flight_times', c, TIME_CATEGORIES))

    for column in ('origin_region', 'dest_region'):
        yield f'int_flight_routes.{column} accepted_values', (
            lambda c=column: st.test_accepted_values(
                spark, 'int_flight_routes', c, REGIONS))


def gold_tests(spark):
    """Star integrity: dimension grains, a gapless date spine, no orphaned foreign key."""
    def spine_has_no_gaps():
        # dim_flight_dates is generated, so it is contiguous by construction -
        # which makes this a guard on the construction, not on the data. Fewer
        # rows than days is a day dropped by a later filter or join; more is a
        # day fanned out in two, e.g. by a duplicate event_date in the seed.
        # A missing SOURCE file is not this test's business: that is a run of
        # has_flight_data = FALSE inside a spine that is still gapless.
        row = spark.sql("""
            SELECT
                COUNT(*) AS n,
                datediff(MAX(flight_date), MIN(flight_date)) + 1 AS expected
            FROM dim_flight_dates
        """).first()
        if row["expected"] is None:
            raise ValueError(
                "dim_flight_dates has no non-null flight_date, so there is no spine to check"
            )
        if row["n"] != row["expected"]:
            raise ValueError(
                f"dim_flight_dates is not one row per calendar day: {row['n']:,} row(s) "
                f"between its min and max flight_date, expected {row['expected']:,}"
            )
        logger.info(f"PASS contiguous: dim_flight_dates ({row['n']:,} consecutive days)")

    yield 'dim_flight_routes grain', lambda: st.test_assert_grain(
        spark, 'dim_flight_routes', 'origin', 'dest')
    yield 'dim_flight_routes.route_key unique', lambda: st.test_unique(
        spark, 'dim_flight_routes', 'route_key')
    yield 'dim_flight_dates.flight_date unique', lambda: st.test_unique(
        spark, 'dim_flight_dates', 'flight_date')
    yield 'dim_flight_dates.flight_date not_null', lambda: st.test_not_null(
        spark, 'dim_flight_dates', 'flight_date')
    yield 'dim_flight_dates.date_key not_null', lambda: st.test_not_null(
        spark, 'dim_flight_dates', 'date_key')
    yield 'dim_flight_dates.has_flight_data not_null', lambda: st.test_not_null(
        spark, 'dim_flight_dates', 'has_flight_data')
    yield 'dim_flight_dates spine has no gaps', spine_has_no_gaps
    yield 'dim_flight_airlines.op_unique_carrier unique', lambda: st.test_unique(
        spark, 'dim_flight_airlines', 'op_unique_carrier')

    yield 'fct_flights grain', lambda: st.test_assert_grain(
        spark, 'fct_flights', 'flight_id')
    yield 'fct_flights.flight_id unique', lambda: st.test_unique(
        spark, 'fct_flights', 'flight_id')
    yield 'fct_flights.flight_count not_null', lambda: st.test_not_null(
        spark, 'fct_flights', 'flight_count')

    for column, parent in (('route_key', 'dim_flight_routes'),
                           ('airline_key', 'dim_flight_airlines'),
                           ('date_key', 'dim_flight_dates')):
        yield f'fct_flights.{column} -> {parent}', (
            lambda c=column, t=parent: st.test_relationship(
                spark, 'fct_flights', c, t, c))


def mart_tests(spark):
    """The mart must not have lost or duplicated a flight."""
    def reconcile():
        fact = spark.sql("SELECT SUM(flight_count) AS n FROM fct_flights").first()["n"]
        mart = spark.sql(
            "SELECT SUM(flight_count) AS n FROM mart_flight_steeped_obt").first()["n"]
        if fact != mart:
            raise ValueError(
                f"mart lost or duplicated flights: fct_flights has {fact:,}, "
                f"mart_flight_steeped_obt has {mart:,}"
            )
        logger.info(f"PASS reconciliation: {mart:,} flights in both fact and mart")

    yield 'mart reconciles to fact', reconcile
    yield 'mart.flight_count not_null', lambda: st.test_not_null(
        spark, 'mart_flight_steeped_obt', 'flight_count')


LAYERS = {'seed': seed_tests, 'silver': silver_tests,
          'gold': gold_tests, 'mart': mart_tests}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--layer', choices=sorted(LAYERS), action='append',
                        help='run only this layer (repeatable); default is all')
    parser.add_argument('--fail-fast', action='store_true',
                        help='stop at the first failure instead of collecting them')
    args = parser.parse_args()

    layers = args.layer or ['seed', 'silver', 'gold', 'mart']
    spark = get_spark('bts-data-tests')

    passed, failures = 0, []
    for layer in layers:
        logger.info(f'--- {layer} ---')
        for name, check in LAYERS[layer](spark):
            try:
                check()
                passed += 1
            except Exception as error:
                failures.append((layer, name, str(error)))
                logger.error(f'FAIL {name}: {error}')
                if args.fail_fast:
                    break
        if failures and args.fail_fast:
            break

    logger.info(f'{passed} passed, {len(failures)} failed')
    if failures:
        for layer, name, error in failures:
            logger.error(f'  [{layer}] {name}: {error}')
        sys.exit(1)


if __name__ == '__main__':
    main()
