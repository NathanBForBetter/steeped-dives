"""Gold mart: the One Big Table the Steeped AI platform ingests.

Aggregates the ~17M-row flight fact up to a month x airline x category grain.
The platform profiles whatever columns it is handed, which puts two
requirements on this model:

  1. Every total needs its own denominator in the same row. A SUM(dep_delay)
     with no flight count beside it cannot answer "what is the average
     delay". flight_count and the four event counts are here for that, and
     every rate is computed in this model.

  2. A rate has to be a ratio of sums, not a mean of per-flight ratios.
     AVG(carrier_arr_delay_share) weights a flight that arrived 5 minutes
     late the same as one that arrived 5 hours late. SUM(carrier_delay) /
     SUM(arr_delay) is the share of delay minutes that belong to the carrier.
     Both are emitted, and named so they cannot be confused.

Run after 04_gold_core.py.
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

spark = get_spark("bts-gold-mart")

spark.sql("DROP TABLE IF EXISTS mart_flight_steeped_obt;")
spark.sql("""
    CREATE TABLE mart_flight_steeped_obt AS
    WITH
        DimJoin AS (
            SELECT
                F.*,
                R.origin,
                R.dest,
                R.route,
                R.city_route,
                R.state_route,
                R.region_route,
                R.origin_city,
                R.origin_state,
                R.origin_region,
                R.dest_city,
                R.dest_state,
                R.dest_region,

                D.month,
                D.month_name,
                D.year,
                D.quarter,
                D.day_of_week,
                D.is_weekend,
                D.related_event,
                D.related_event_type,

                A.airline_name,
                A.airline_type,
                A.airline_still_exists

            FROM fct_flights AS F
            LEFT JOIN dim_flight_routes AS R USING(route_key)
            LEFT JOIN dim_flight_dates AS D USING(date_key)
            LEFT JOIN dim_flight_airlines AS A USING(airline_key)
        )
    SELECT
        -- ---- grain ----------------------------------------------------
        airline_name,
        month,
        -- related_event_type is a day-level attribute, so a month holding
        -- three event types produces three rows and days with no event
        -- produce a null one. The day_ prefix says which level it sits at.
        related_event_type AS day_related_event_type,
        dep_time_category,
        dep_delay_category,
        arr_time_category,
        arr_delay_category,
        delay_change_category,
        cancelled_reason,
        diverted_airport,

        -- ---- grain attributes -----------------------------------------
        -- functionally determined by month / airline_name, so first() is
        -- deterministic here in a way ANY_VALUE was not
        first(month_name) AS month_name,
        first(year) AS year,
        first(quarter) AS quarter,
        -- extract provenance, constant across the whole build, so first() is
        -- deterministic. Carried to the mart because a Steeped Dive needs to
        -- say which BTS pull its numbers came from.
        first(extract_date) AS extract_date,
        first(airline_type) AS airline_type,
        first(airline_still_exists) AS airline_still_exists,

        -- ---- additive counts: the denominators ------------------------
        SUM(flight_count) AS flight_count,
        SUM(cancelled_count) AS cancelled_flight_count,
        SUM(diverted_count) AS diverted_flight_count,
        SUM(dep_delayed_15_count) AS dep_delayed_15_flight_count,
        SUM(arr_delayed_15_count) AS arr_delayed_15_flight_count,
        -- rows where the measure is populated, so a mean over it is not
        -- divided by the cancelled flights too
        COUNT(dep_delay) AS dep_delay_reported_count,
        COUNT(arr_delay) AS arr_delay_reported_count,

        -- ---- additive totals ------------------------------------------
        SUM(dep_delay) AS dep_delay_minutes,
        SUM(arr_delay) AS arr_delay_minutes,
        SUM(delay_change) AS delay_change_minutes,
        SUM(total_duration) AS total_duration_minutes,
        SUM(dep_taxi_duration) AS dep_taxi_duration_minutes,
        SUM(arr_taxi_duration) AS arr_taxi_duration_minutes,
        SUM(air_time_duration) AS air_time_duration_minutes,
        SUM(security_delay) AS security_delay_minutes,
        SUM(late_aircraft_delay) AS late_aircraft_delay_minutes,
        SUM(nas_delay) AS nas_delay_minutes,
        SUM(weather_delay) AS weather_delay_minutes,
        SUM(carrier_delay) AS carrier_delay_minutes,

        -- ---- rates: ratio of sums, computed here not downstream -------
        try_divide(SUM(dep_delay), COUNT(dep_delay)) AS avg_dep_delay_minutes,
        try_divide(SUM(arr_delay), COUNT(arr_delay)) AS avg_arr_delay_minutes,
        try_divide(SUM(delay_change), COUNT(delay_change)) AS avg_delay_change_minutes,
        try_divide(SUM(cancelled_count), SUM(flight_count)) AS cancelled_rate,
        try_divide(SUM(diverted_count), SUM(flight_count)) AS diverted_rate,
        try_divide(SUM(dep_delayed_15_count), SUM(flight_count)) AS dep_delayed_15_rate,
        try_divide(SUM(arr_delayed_15_count), SUM(flight_count)) AS arr_delayed_15_rate,

        -- share of total arrival-delay MINUTES by cause: the five BTS cause
        -- fields sum to arr_delay, so these five sum to ~1 across a group
        try_divide(SUM(security_delay), SUM(arr_delay)) AS security_arr_delay_share,
        try_divide(SUM(late_aircraft_delay), SUM(arr_delay)) AS late_aircraft_arr_delay_share,
        try_divide(SUM(nas_delay), SUM(arr_delay)) AS nas_arr_delay_share,
        try_divide(SUM(weather_delay), SUM(arr_delay)) AS weather_arr_delay_share,
        try_divide(SUM(carrier_delay), SUM(arr_delay)) AS carrier_arr_delay_share,

        -- ---- per-flight means of the same shares ----------------------
        -- kept because "the typical flight's delay mix" is a different and
        -- also interesting question from "the delay mix of all the minutes",
        -- but named so nobody mistakes one for the other
        AVG(security_arr_delay_share) AS mean_flight_security_arr_delay_share,
        AVG(late_aircraft_arr_delay_share) AS mean_flight_late_aircraft_arr_delay_share,
        AVG(nas_arr_delay_share) AS mean_flight_nas_arr_delay_share,
        AVG(weather_arr_delay_share) AS mean_flight_weather_arr_delay_share,
        AVG(carrier_arr_delay_share) AS mean_flight_carrier_arr_delay_share
    FROM DimJoin
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
    ;
""")
logger.info('table mart_flight_steeped_obt created successfully!')

st.test_assert_grain(
    spark,
    'mart_flight_steeped_obt',
    'airline_name', 'month', 'day_related_event_type',
    'dep_time_category', 'dep_delay_category',
    'arr_time_category', 'arr_delay_category', 'delay_change_category',
    'cancelled_reason', 'diverted_airport',
)
st.test_not_null(spark, 'mart_flight_steeped_obt', 'flight_count')

# Reconciliation: the mart must not have lost or duplicated a flight.
fact_flights = spark.sql("SELECT SUM(flight_count) AS n FROM fct_flights").first()["n"]
mart_flights = spark.sql(
    "SELECT SUM(flight_count) AS n FROM mart_flight_steeped_obt").first()["n"]
if fact_flights != mart_flights:
    raise ValueError(
        f"mart lost or duplicated flights: fct_flights has {fact_flights:,}, "
        f"mart_flight_steeped_obt has {mart_flights:,}"
    )
logger.info(f'reconciled: {mart_flights:,} flights in both fact and mart')
