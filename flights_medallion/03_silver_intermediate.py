"""Silver intermediate: one de-duplicated flight row, fanned out into five
subject views.

The views are deliberately narrow and single-subject (delay, routes, times,
durations, cancellations) so every transformation can be tested at flight
grain - all ~17M of them - before 05_gold_mart aggregates anything away. The
gold fact stitches them back together on flight_id.

Run after 02_silver_staging.py; tests live in 06_run_data_tests.py.
"""
import logging
from pathlib import Path

from pyspark.sql import functions as F
from spark_session import get_spark

spark = get_spark("bts-silver-intermediate")

logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
)

logger = logging.getLogger(__name__)

## US state reference data: Census region for the origin and destination state.
## Loaded the same way as flight_calendar_events further down, with one
## difference: a temp view rather than a table. The only reader is the
## CREATE TABLE AS below, which copies the rows out, so nothing persistent
## references the view. (int_flight_times is why the calendar has to be a table.)
##
## seeds/us_states.csv is a copy of natality_names_kimball/dbt/seeds/us_states.csv.
## The two projects share no seed directory, so a change to one has to be made
## to the other. The note lives here because a comment line inside the CSV
## would be read as the header row by dbt.
##
## BTS state codes are lowercase, so the seed is lowercased on load rather than
## lowercasing the 17M-row side of the join.
states_csv = Path(__file__).resolve().parent / "seeds" / "us_states.csv"
states = (spark.read
          .option("header", "true")
          .csv(str(states_csv))
          .withColumn("state_code", F.lower("state_code")))

## A duplicate state_code would fan out every flight to or from that state.
_state_dupes = (states.groupBy("state_code").count()
                .where("count > 1").collect())
if _state_dupes:
    raise ValueError(
        f"{states_csv} has duplicate state_code(s): "
        + ", ".join(r["state_code"] for r in _state_dupes)
    )
states.createOrReplaceTempView("us_states")
logger.info(f"loaded {states.count()} state(s) from {states_csv}")

## The de-duplicated flight spine: one row per flight_id, chosen whole.
##
## stg_flights is not unique on flight_id. Measured on the 2023-2026 extract:
## 389 of 17,569,073 rows collide (0.0022%), every collision is exactly two
## rows, and all 389 share an identical `distance` with air_time differing by
## 1.66 minutes on average - BTS reporting one physical flight twice, not two
## flights the key cannot tell apart. 131 of the 389 are a cancellation
## superseded by an operated record.
##
## ROW_NUMBER keeps one whole row per key. Resolving a collision column by
## column, with ANY_VALUE or MAX, can put an arr_delay from one source row
## beside a dep_delay from the other and produce a flight nobody reported.
## 06_run_data_tests.py checks for that with `rows_are_real`.
##
## Materialized as a table, not a view. Five views below read int_flights and
## the gold fact joins four of them, so as a view this window over ~17M rows
## would run again on every one of those reads. The five subject views stay
## views because they only project columns and need no shuffle.
spark.sql("DROP TABLE IF EXISTS int_flights;")
spark.sql("""
    CREATE TABLE int_flights AS
    WITH Ranked AS (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY flight_id
                ORDER BY
                    -- 1. prefer the row that actually observed the flight. A
                    --    cancelled record carries no measurements at all, so
                    --    taking it would discard the only observation there is.
                    --    131 of the 389 collisions are exactly this: BTS
                    --    reported a cancellation and then superseded it.
                    CASE WHEN arr_time IS NULL THEN 1 ELSE 0 END,
                    CASE WHEN actual_elapsed_time IS NULL THEN 1 ELSE 0 END,
                    -- 2. then the longer elapsed time
                    actual_elapsed_time DESC,
                    -- 3. then a total order over the payload, so the winner is
                    --    reproducible. Without this tail, 8 of the 389 groups
                    --    tie on the keys above while holding DIFFERENT rows,
                    --    and the build is a coin flip.
                    dep_delay, arr_delay, air_time, taxi_in, taxi_out
            ) AS dedupe_rank
        FROM stg_flights
    )
    SELECT
        flight_id,
        extract_date,
        fl_date,
        arr_delay_new,
        carrier_delay,
        crs_arr_time,
        crs_dep_time,
        crs_elapsed_time,
        dep_delay,
        dep_delay_new,
        dep_time,
        distance,
        first_dep_time,
        security_delay,
        late_aircraft_delay,
        nas_delay,
        weather_delay,
        total_add_gtime,
        longest_add_gtime,
        cancelled,
        dest,
        dest_city_name,
        dest_state_abr,
        D.census_region AS dest_region,
        div1_airport,
        origin,
        origin_city_name,
        origin_state_abr,
        O.census_region AS origin_region,
        op_unique_carrier,
        flights,
        cancellation_code,
        taxi_out,
        wheels_off,
        wheels_on,
        taxi_in,
        arr_time,
        arr_delay,
        actual_elapsed_time,
        air_time
    FROM Ranked AS R
    LEFT JOIN us_states AS O ON R.origin_state_abr = O.state_code
    LEFT JOIN us_states AS D ON R.dest_state_abr = D.state_code
    WHERE dedupe_rank = 1
    ;
""")
logger.info('created table int_flights (one row per flight_id)')

## https://www.transtats.bts.gov/Fields.asp?gnoyr_VQ=FGK
spark.sql("""
    CREATE OR REPLACE VIEW int_flight_delay AS
    WITH 
        DelayRaw AS (
            SELECT 
                flight_id,
                COALESCE(
                    CASE
                        -- Confirmed comparing side by side
                        WHEN dep_delay_new = 0 THEN NULL ELSE dep_delay_new END, 
                        dep_delay) AS dep_delay,
                COALESCE(
                    CASE
                        WHEN arr_delay_new = 0 THEN NULL ELSE arr_delay_new END, 
                        arr_delay) AS arr_delay,
                -- arr - dep. Positive means the flight lost more time en
                -- route, negative means it made time up. The sign has to run
                -- this way for the shared delay bucketing below.
                COALESCE(
                    CASE
                        WHEN arr_delay_new = 0 THEN NULL ELSE arr_delay_new END,
                        arr_delay)
                            - COALESCE(
                                CASE
                                    WHEN dep_delay_new = 0 THEN NULL ELSE dep_delay_new END,
                                    dep_delay) AS delay_change,
                security_delay,
                late_aircraft_delay,
                nas_delay,
                weather_delay,
                carrier_delay
            FROM int_flights
        )
    SELECT
        flight_id,
        dep_delay,
        CASE
            WHEN dep_delay IS NULL THEN 'unknown'
            WHEN dep_delay < -60 THEN 'severe early'
            WHEN dep_delay <= -30 THEN 'moderate early'
            WHEN dep_delay <= -15 THEN 'mild early'
            WHEN dep_delay <= -5 THEN 'minor early'
            WHEN dep_delay <= 0 THEN 'on time'
            WHEN dep_delay <= 15 THEN 'minor delay'
            WHEN dep_delay <= 30 THEN 'mild delay'
            WHEN dep_delay <= 60 THEN 'moderate delay'
            ELSE 'severe delay'
            END AS dep_delay_category,
        -- BTS attributes the five delay causes to ARRIVAL delay, not
        -- departure delay: CARRIER_DELAY + WEATHER_DELAY + NAS_DELAY +
        -- SECURITY_DELAY + LATE_AIRCRAFT_DELAY sums to ARR_DELAY for any
        -- flight arriving 15+ minutes late, and the fields are null
        -- otherwise. Dividing by dep_delay produced shares that did not sum
        -- to 1 and frequently exceeded it. Renamed to match the denominator.
        try_divide(security_delay, arr_delay) AS security_arr_delay_share,
        try_divide(late_aircraft_delay, arr_delay) AS late_aircraft_arr_delay_share,
        try_divide(nas_delay, arr_delay) AS nas_arr_delay_share,
        try_divide(weather_delay, arr_delay) AS weather_arr_delay_share,
        try_divide(carrier_delay, arr_delay) AS carrier_arr_delay_share,
        -- the five causes as raw minutes too, so the mart can build a
        -- ratio of sums rather than averaging per-flight ratios
        security_delay,
        late_aircraft_delay,
        nas_delay,
        weather_delay,
        carrier_delay,
        arr_delay,
        CASE
            WHEN arr_delay IS NULL THEN 'unknown'
            WHEN arr_delay < -60 THEN 'severe early'
            WHEN arr_delay <= -30 THEN 'moderate early'
            WHEN arr_delay <= -15 THEN 'mild early'
            WHEN arr_delay <= -5 THEN 'minor early'
            WHEN arr_delay <= 0 THEN 'on time'
            WHEN arr_delay <= 15 THEN 'minor delay'
            WHEN arr_delay <= 30 THEN 'mild delay'
            WHEN arr_delay <= 60 THEN 'moderate delay'
            ELSE 'severe delay'
            END AS arr_delay_category,
        delay_change,
        CASE
            WHEN delay_change IS NULL THEN 'unknown'
            WHEN delay_change < -60 THEN 'severe early'
            WHEN delay_change <= -30 THEN 'moderate early'
            WHEN delay_change <= -15 THEN 'mild early'
            WHEN delay_change <= -5 THEN 'minor early'
            WHEN delay_change <= 0 THEN 'on time'
            WHEN delay_change <= 15 THEN 'minor delay'
            WHEN delay_change <= 30 THEN 'mild delay'
            WHEN delay_change <= 60 THEN 'moderate delay'
            ELSE 'severe delay'
            END AS delay_change_category
    FROM DelayRaw
    ;
""")
logger.info('created view int_flight_delay')

spark.sql("""
CREATE OR REPLACE VIEW int_flight_routes AS
SELECT
    flight_id,
    -- Extract provenance, carried here rather than in int_flight_times
    -- because this is the driving table of FlightMetrics in 04_gold_core.py:
    -- a value on the FROM side cannot be lost to an unmatched LEFT JOIN.
    extract_date,
    op_unique_carrier,
    CASE
        WHEN LOWER(op_unique_carrier) = 'mq' THEN 'envoy air'
        WHEN LOWER(op_unique_carrier) = 'g4' THEN 'allegiant air'
        WHEN LOWER(op_unique_carrier) = 'ha' THEN 'hawaiian airlines'
        WHEN LOWER(op_unique_carrier) = 'yx' THEN 'republic airways'
        WHEN LOWER(op_unique_carrier) = 'wn' THEN 'southwest airlines'
        WHEN LOWER(op_unique_carrier) = 'f9' THEN 'frontier airlines'
        WHEN LOWER(op_unique_carrier) = 'dl' THEN 'delta air lines'
        WHEN LOWER(op_unique_carrier) = 'nk' THEN 'spirit airlines'
        WHEN LOWER(op_unique_carrier) = 'as' THEN 'alaska airlines'
        WHEN LOWER(op_unique_carrier) = 'b6' THEN 'jetblue airways'
        WHEN LOWER(op_unique_carrier) = 'oh' THEN 'psa airlines'
        WHEN LOWER(op_unique_carrier) = 'oo' THEN 'skywest airlines'
        WHEN LOWER(op_unique_carrier) = 'ua' THEN 'united airlines'
        WHEN LOWER(op_unique_carrier) = '9e' THEN 'endeavor air'
        WHEN LOWER(op_unique_carrier) = 'aa' THEN 'american airlines'
    END AS airline_name,
    CASE
        WHEN LOWER(op_unique_carrier) IN ('mq', 'yx', 'oh', 'oo', '9e') THEN 'regional'
        ELSE 'major'
    END AS airline_type,
    CASE
        WHEN LOWER(op_unique_carrier) IN (
            'mq', 'g4', 'ha', 'yx', 'wn', 'f9', 'dl',
            'nk', 'as', 'b6', 'oh', 'oo', 'ua', '9e', 'aa'
        ) THEN TRUE
        ELSE FALSE
    END AS airline_still_exists,
    origin,
    origin_city_name AS origin_city,
    origin_state_abr AS origin_state,
    origin_region,
    dest,
    dest_city_name AS dest_city,
    dest_state_abr AS dest_state,
    dest_region,
    CASE 
        WHEN origin IS NULL THEN dest
        WHEN dest IS NULL THEN origin
        ELSE CONCAT(origin, '-', dest) 
        END AS route,
    CASE
        WHEN origin_city_name IS NULL THEN dest_city_name
        WHEN dest_city_name IS NULL THEN origin_city_name
        ELSE CONCAT(origin_city_name, '-', dest_city_name) 
        END AS city_route,
    CASE
        WHEN origin_state_abr IS NULL THEN dest_state_abr
        WHEN dest_state_abr IS NULL THEN origin_state_abr
        ELSE CONCAT(origin_state_abr, '-', dest_state_abr) 
        END AS state_route,
    CASE
        WHEN origin_region IS NULL THEN dest_region
        WHEN dest_region IS NULL THEN origin_region
        ELSE CONCAT(origin_region, '-', dest_region) 
        END AS region_route
FROM int_flights
;
""")
logger.info('created view int_flight_routes')

## The 45-row event calendar is reference data, so it lives in a CSV
## (seeds/flight_calendar_events.csv) and is joined in. Adding an event is a
## CSV edit. This is what dbt's `seeds` directory is for.
events_csv = Path(__file__).resolve().parent / "seeds" / "flight_calendar_events.csv"
events = (spark.read
          .option("header", "true")
          .csv(str(events_csv))
          .withColumn("event_date", F.to_date("event_date")))

## A duplicate event_date would fan out EVERY flight on that day in the join
## below, so it fails here instead - 45 rows, so the check is free.
_dupes = (events.groupBy("event_date").count()
          .where("count > 1").collect())
if _dupes:
    raise ValueError(
        f"{events_csv} has duplicate event_date(s): "
        + ", ".join(f"{r['event_date']} x{r['count']}" for r in _dupes)
    )
if events.where("event_date IS NULL").count():
    raise ValueError(f"{events_csv} has a row whose event_date did not parse as a date")

## saveAsTable, not createOrReplaceTempView, for two reasons:
##   1. int_flight_times below is a PERSISTENT view, and Spark refuses to let
##      one reference a temporary object at all
##      ([INVALID_TEMP_OBJ_REFERENCE]).
##   2. a temp view dies with the session, so 04_gold_core.py - a separate
##      process - could not have resolved int_flight_times either.
## This is what dbt's `dbt seed` does: reference data lands in the warehouse
## as a table, re-read from the CSV on every run so the file stays the source
## of truth.
events.write.mode("overwrite").saveAsTable("flight_calendar_events")
logger.info(f"loaded {events.count()} calendar event(s) from {events_csv}")

spark.sql("""
CREATE OR REPLACE VIEW int_flight_times AS
WITH
    RawMetrics AS (
        SELECT
            flight_id,
            fl_date,
            -- parsed once into a real DATE instead of re-parsing the same
            -- string for each of the five attributes below, and carried
            -- forward so dim_flight_dates can key on a date rather than on
            -- BTS's '1/1/2024 12:00:00 AM' text
            to_date(to_timestamp(fl_date, 'M/d/yyyy h:mm:ss a')) AS flight_date,
            security_delay,
            late_aircraft_delay,
            nas_delay,
            weather_delay,
            carrier_delay,

            -- BTS stores local clock time as an hhmm integer and uses 2400
            -- for midnight. 2400 / 100 is 24, which is outside every hour
            -- bucket, so the hour below is taken with MOD(..., 24).
            COALESCE(dep_time, crs_dep_time) AS dep_time,
            wheels_off AS dep_wheels_off,
            COALESCE(arr_time, crs_arr_time) AS arr_time,
            wheels_on AS arr_wheels_on
        FROM int_flights
    ),
    Normalized AS (
        SELECT
            *,
            date_format(flight_date, 'yyyy-MM-dd') AS date_text,
            date_format(flight_date, 'MMMM') AS month_name,
            date_format(flight_date, 'yyyy-MM') AS month_text,
            year(flight_date) AS year,
            quarter(flight_date) AS quarter,
            day(flight_date) AS day_of_month,
            date_format(flight_date, 'EEEE') AS day_of_week,
            dayofweek(flight_date) IN (1, 7) AS is_weekend,
            MOD(FLOOR(dep_time / 100), 24) AS dep_hour,
            MOD(FLOOR(arr_time / 100), 24) AS arr_hour
        FROM RawMetrics
    )
SELECT
    N.flight_id,
    N.fl_date,
    N.flight_date,
    N.date_text,
    N.month_name,
    N.month_text,
    N.year,
    N.quarter,
    N.day_of_month,
    N.day_of_week,
    N.is_weekend,

    N.dep_time,
    N.dep_hour,
    CASE
        WHEN N.dep_hour IS NULL THEN 'unknown'
        WHEN N.dep_hour BETWEEN 0 AND 5 THEN 'night'
        WHEN N.dep_hour BETWEEN 6 AND 11 THEN 'morning'
        WHEN N.dep_hour BETWEEN 12 AND 17 THEN 'afternoon'
        ELSE 'evening'
        END AS dep_time_category,
    N.dep_wheels_off,

    N.arr_time,
    N.arr_hour,
    CASE
        WHEN N.arr_hour IS NULL THEN 'unknown'
        WHEN N.arr_hour BETWEEN 0 AND 5 THEN 'night'
        WHEN N.arr_hour BETWEEN 6 AND 11 THEN 'morning'
        WHEN N.arr_hour BETWEEN 12 AND 17 THEN 'afternoon'
        ELSE 'evening'
        END AS arr_time_category,
    N.arr_wheels_on,

    E.related_event,
    E.related_event_type
FROM Normalized AS N
LEFT JOIN flight_calendar_events AS E ON N.flight_date = E.event_date
;
""")
logger.info('created view int_flight_times')

spark.sql("""
CREATE OR REPLACE VIEW int_flight_durations AS
SELECT
    flight_id,
    COALESCE(actual_elapsed_time, crs_elapsed_time) AS total_duration,
    taxi_out AS dep_taxi_duration,
    taxi_in AS arr_taxi_duration,
    air_time AS air_time_duration
FROM int_flights
;
""")
logger.info('created view int_flight_durations')

spark.sql("""
-- No GROUP BY. int_flights is one row per flight_id, so this only projects
-- columns. Aggregating here would cost a shuffle over ~17M rows and would
-- hide a fan-out from the grain test.
CREATE OR REPLACE VIEW int_flight_cancel_div AS
SELECT
    flight_id,
    CASE
        WHEN cancellation_code = 'a' THEN 'carrier'
        WHEN cancellation_code = 'b' THEN 'weather'
        WHEN cancellation_code = 'c' THEN 'nas'
        WHEN cancellation_code = 'd' THEN 'security'
        WHEN cancelled = 1 THEN 'other'
        END AS cancelled_reason,
    CASE
        WHEN cancelled = 1 THEN 'cancelled'
        WHEN cancellation_code IS NOT NULL THEN 'cancelled'
        ELSE NULL
        END AS cancelled,
    CASE
        WHEN div1_airport IS NOT NULL THEN 'diverted'
        ELSE NULL
        END AS diverted,
    div1_airport AS diverted_airport
FROM int_flights
;
""")
logger.info('created view int_flight_cancel_div')