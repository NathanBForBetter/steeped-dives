"""Gold core: Kimball star over the silver subject views.

Three conformed dimensions (route, date, airline) and one flight-grain fact.
Every dimension carries a hashed surrogate key and the fact joins on that key,
not on the natural text - see dim_flight_routes below for why that matters.

Run after 03_silver_intermediate.py. Tests run inline here and again, in full,
from 06_run_data_tests.py.
"""
import logging

import spark_data_tests as st
from spark_session import get_spark

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

logger = logging.getLogger(__name__)

spark = get_spark("bts-gold-core")

# ---------------------------------------------------------------------------
# dim_flight_routes
#
# Grain is (origin, dest). route_key hashes that pair and the fact joins on
# it. The `route` text is not a safe key: int_flight_routes builds it as
# origin-dest but falls back to one airport when the other is null, so a
# flight with a null origin into LAX and a flight out of LAX with a null dest
# both get the route 'lax'.
# ---------------------------------------------------------------------------
spark.sql("DROP TABLE IF EXISTS dim_flight_routes;")
spark.sql("""
    CREATE TABLE dim_flight_routes AS
    SELECT
        sha2(
            concat_ws('|',
                coalesce(origin, 'UNKNOWN'),
                coalesce(dest, 'UNKNOWN')),
            256
        ) AS route_key,
        origin,
        dest,
        first(route) AS route,
        first(city_route) AS city_route,
        first(state_route) AS state_route,
        first(region_route) AS region_route,
        first(origin_city) AS origin_city,
        first(origin_state) AS origin_state,
        first(origin_region) AS origin_region,
        first(dest_city) AS dest_city,
        first(dest_state) AS dest_state,
        first(dest_region) AS dest_region
    FROM int_flight_routes
    GROUP BY
        origin,
        dest
""")
logger.info('table dim_flight_routes created successfully!')
st.test_not_null(spark, 'dim_flight_routes', 'route_key')
st.test_unique(spark, 'dim_flight_routes', 'route_key')
st.test_assert_grain(spark, 'dim_flight_routes', 'origin', 'dest')

# ---------------------------------------------------------------------------
# dim_flight_dates
#
# A generated date spine: one row per calendar day from the first to the last
# flight_date in the data, built with sequence() + explode() rather than read
# off the flights. Keyed on a real DATE, not on BTS's '1/1/2024 12:00:00 AM'
# string, and it carries the full calendar breakdown (quarter, day of month,
# weekday, weekend flag) rather than only the three attributes the mart
# happened to need.
#
# Every calendar attribute is derived from the spine date itself, so a day
# with no flights is a complete row, not a hole. The one thing read off the
# flights is whether the day had any: has_flight_data. A missing source file
# shows up as a run of has_flight_data = FALSE, which a query or a test can
# see.
#
# related_event / related_event_type are joined from the seed table
# (flight_calendar_events, loaded from seeds/flight_calendar_events.csv by
# 03_silver_intermediate.py), not read through int_flight_times, so an event
# on a day with no flights still appears and adding an event is still a CSV
# edit, not a code change. An event dated outside the data's range gets no
# row: the spine ends where the data does.
#
# See the README section "Date dimension with a spine".
# ---------------------------------------------------------------------------
spark.sql("DROP TABLE IF EXISTS dim_flight_dates;")
spark.sql("""
    CREATE TABLE dim_flight_dates AS
    WITH
        -- The only thing read off the flights: which dates have at least one,
        -- and the raw BTS text for each. One row per date, so the aggregate
        -- collapses ~17M flights to one row per day. MIN rather than first()
        -- so fl_date_raw is deterministic without arguing that fl_date is
        -- functionally determined by flight_date.
        FlightDays AS (
            SELECT
                flight_date,
                min(fl_date) AS fl_date_raw
            FROM int_flight_times
            WHERE flight_date IS NOT NULL
            GROUP BY
                flight_date
        ),
        -- MIN / MAX of int_flight_times.flight_date, read off the per-date
        -- rows above: the same answer from far fewer rows than the flights.
        Bounds AS (
            SELECT
                min(flight_date) AS first_date,
                max(flight_date) AS last_date
            FROM FlightDays
        ),
        -- Stays a DATE: the date_key hash below casts it to 'yyyy-MM-dd', and
        -- a timestamp spine would cast to 'yyyy-MM-dd HH:mm:ss' instead.
        Spine AS (
            SELECT
                explode(sequence(first_date, last_date, INTERVAL 1 DAY)) AS flight_date
            FROM Bounds
        )
    SELECT
        -- MUST hash flight_date, not fl_date. fct_flights below recomputes
        -- sha2(CAST(flight_date AS STRING)) to get its own date_key, and
        -- flight_date is a DATE ('2024-01-01') while fl_date is BTS's raw
        -- text ('1/1/2024 12:00:00 am'). Hashing a different column in each
        -- place would leave every date_key in the fact without a match, and
        -- both tables would still build. The relationships test at the
        -- bottom of this file checks for it.
        sha2(CAST(S.flight_date AS STRING), 256) AS date_key,
        S.flight_date,
        -- NULL on a spine-only day: there is no flight to have carried it.
        F.fl_date_raw,
        date_format(S.flight_date, 'yyyy-MM-dd') AS date_text,
        date_format(S.flight_date, 'yyyy-MM') AS month,
        date_format(S.flight_date, 'MMMM') AS month_name,
        year(S.flight_date) AS year,
        quarter(S.flight_date) AS quarter,
        day(S.flight_date) AS day_of_month,
        date_format(S.flight_date, 'EEEE') AS day_of_week,
        dayofweek(S.flight_date) IN (1, 7) AS is_weekend,
        E.related_event,
        E.related_event_type,
        -- FlightDays holds only dates that appear in int_flight_times, so a
        -- match IS the answer. A boolean that is never NULL, unlike the
        -- fl_date_raw beside it.
        F.flight_date IS NOT NULL AS has_flight_data
    FROM Spine AS S
    LEFT JOIN FlightDays AS F ON S.flight_date = F.flight_date
    LEFT JOIN flight_calendar_events AS E ON S.flight_date = E.event_date
""")
logger.info('table dim_flight_dates created successfully!')
st.test_not_null(spark, 'dim_flight_dates', 'date_key')
st.test_not_null(spark, 'dim_flight_dates', 'flight_date')
st.test_unique(spark, 'dim_flight_dates', 'flight_date')
st.test_assert_grain(spark, 'dim_flight_dates', 'date_key')

# ---------------------------------------------------------------------------
# dim_flight_airlines
#
# The strongest SCD Type 2 candidate in the repo: airline_name, airline_type
# and airline_still_exists are all attributes that change over time, and BTS
# keeps reporting a carrier code after the carrier is gone. See the README
# section "Where SCD Type 2 belongs" for the walkthrough.
# ---------------------------------------------------------------------------
spark.sql("DROP TABLE IF EXISTS dim_flight_airlines;")
spark.sql("""
    CREATE TABLE dim_flight_airlines AS
    SELECT
        sha2(op_unique_carrier, 256) AS airline_key,
        op_unique_carrier,
        first(airline_name) AS airline_name,
        first(airline_type) AS airline_type,
        first(airline_still_exists) AS airline_still_exists
    FROM int_flight_routes
    GROUP BY
        op_unique_carrier
""")
logger.info('table dim_flight_airlines created successfully!')
st.test_not_null(spark, 'dim_flight_airlines', 'airline_key')
st.test_not_null(spark, 'dim_flight_airlines', 'op_unique_carrier')
st.test_unique(spark, 'dim_flight_airlines', 'op_unique_carrier')
st.test_assert_grain(spark, 'dim_flight_airlines', 'airline_key')

# ---------------------------------------------------------------------------
# fct_flights
#
# Transaction-grain fact: one row per flight_id, ~17M rows.
#
# The three surrogate keys are recomputed with the same hash expression the
# dimensions used rather than joined in, so they line up by construction and
# the fact build costs no extra shuffle. origin/dest are coalesced to
# 'UNKNOWN' before hashing so a missing airport still produces a key that
# dim_flight_routes has a row for. date_key is null only if flight_date is,
# and 01_ingest_to_bronze rejects a file where that would happen.
# ---------------------------------------------------------------------------
spark.sql("DROP TABLE IF EXISTS fct_flights;")
spark.sql("""
    CREATE TABLE fct_flights AS
    WITH
        FlightMetrics AS (
            SELECT
                I.flight_id,
                I.extract_date,
                I.op_unique_carrier,
                I.origin,
                I.dest,
                T.flight_date,
                T.dep_time,
                T.dep_hour,
                T.dep_time_category,
                T.dep_wheels_off,
                T.arr_time,
                T.arr_hour,
                T.arr_time_category,
                T.arr_wheels_on,
                D.dep_delay,
                D.dep_delay_category,
                D.arr_delay,
                D.arr_delay_category,
                D.delay_change,
                D.delay_change_category,
                D.security_delay,
                D.late_aircraft_delay,
                D.nas_delay,
                D.weather_delay,
                D.carrier_delay,
                D.security_arr_delay_share,
                D.late_aircraft_arr_delay_share,
                D.nas_arr_delay_share,
                D.weather_arr_delay_share,
                D.carrier_arr_delay_share,
                DU.total_duration,
                DU.dep_taxi_duration,
                DU.arr_taxi_duration,
                DU.air_time_duration,
                C.cancelled_reason,
                C.cancelled,
                C.diverted,
                C.diverted_airport
            FROM int_flight_routes AS I
            LEFT JOIN int_flight_times AS T USING(flight_id)
            LEFT JOIN int_flight_cancel_div AS C USING(flight_id)
            LEFT JOIN int_flight_delay AS D USING(flight_id)
            LEFT JOIN int_flight_durations AS DU USING(flight_id)
        )
    SELECT
        F.flight_id,
        F.extract_date,

        -- Surrogate keys, recomputed here rather than joined on text. Same
        -- hash expression as the dimensions, so the keys line up by
        -- construction and this costs no shuffle at all.
        sha2(concat_ws('|',
             coalesce(F.origin, 'UNKNOWN'),
             coalesce(F.dest, 'UNKNOWN')), 256) AS route_key,
        sha2(CAST(F.flight_date AS STRING), 256) AS date_key,
        sha2(F.op_unique_carrier, 256) AS airline_key,

        -- One flight per row, so the additive count every rate in the mart
        -- divides by. Without it the mart could only ever report totals.
        1 AS flight_count,
        CASE WHEN F.cancelled = 'cancelled' THEN 1 ELSE 0 END AS cancelled_count,
        CASE WHEN F.diverted = 'diverted' THEN 1 ELSE 0 END AS diverted_count,
        CASE WHEN F.arr_delay >= 15 THEN 1 ELSE 0 END AS arr_delayed_15_count,
        CASE WHEN F.dep_delay >= 15 THEN 1 ELSE 0 END AS dep_delayed_15_count,

        F.dep_time,
        F.dep_hour,
        F.dep_time_category,
        F.dep_wheels_off,
        F.arr_time,
        F.arr_hour,
        F.arr_time_category,
        F.arr_wheels_on,
        F.dep_delay,
        F.dep_delay_category,
        F.arr_delay,
        F.arr_delay_category,
        F.delay_change,
        F.delay_change_category,
        F.security_delay,
        F.late_aircraft_delay,
        F.nas_delay,
        F.weather_delay,
        F.carrier_delay,
        F.security_arr_delay_share,
        F.late_aircraft_arr_delay_share,
        F.nas_arr_delay_share,
        F.weather_arr_delay_share,
        F.carrier_arr_delay_share,
        F.total_duration,
        F.dep_taxi_duration,
        F.arr_taxi_duration,
        F.air_time_duration,
        F.cancelled_reason,
        F.cancelled,
        F.diverted,
        F.diverted_airport
    FROM FlightMetrics AS F
    ;
""")
logger.info('table fct_flights created successfully!')
st.test_not_null(spark, 'fct_flights', 'flight_id')
st.test_unique(spark, 'fct_flights', 'flight_id')
st.test_assert_grain(spark, 'fct_flights', 'flight_id')
# Referential integrity: a key in the fact that no dimension row answers to
# is the failure mode a `unique` test on the dimension can never catch.
st.test_relationship(spark, 'fct_flights', 'route_key', 'dim_flight_routes', 'route_key')
st.test_relationship(spark, 'fct_flights', 'airline_key', 'dim_flight_airlines', 'airline_key')
st.test_relationship(spark, 'fct_flights', 'date_key', 'dim_flight_dates', 'date_key')
logger.info('gold core complete')
