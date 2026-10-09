# flights_medallion

17.6 million US domestic flights from the [BTS On-Time Performance](https://www.transtats.bts.gov/ontime/) dataset, modeled in Spark SQL.

There is no dbt in this project. It follows dbt's conventions by hand: staging, intermediate, core and mart layers, the `stg_`, `int_`, `dim_`, `fct_` and `mart_` prefixes, generic data tests and a seed directory for reference data. I wanted to show that the approach does not depend on the tool. [`spark_data_tests.py`](spark_data_tests.py) has the same grain test the dbt projects use.

```
monthly .zip CSVs
      │  01_ingest_to_bronze.py        pyarrow, Hive-partitioned Parquet (year/month/salt)
      ▼
   BRONZE  parquet/year=/month=/salt=
      │  02_silver_staging.py          typed columns, flight grain key
      ▼
   stg_flights                          partitioned by year/month
      │  03_silver_intermediate.py
      ▼
   int_flights  (table, one row per flight)
      ├── int_flight_delay       delay minutes, causes, buckets
      ├── int_flight_routes      carrier names, route, city, state and region pairs
      ├── int_flight_times       calendar attributes
      ├── int_flight_durations   elapsed, taxi and air time
      └── int_flight_cancel_div  cancellation reason, diversion
      │  04_gold_core.py
      ▼
   GOLD   dim_flight_routes + dim_flight_dates + dim_flight_airlines + fct_flights
      │  05_gold_mart.py
      ▼
   mart_flight_steeped_obt              one row per month, airline and category combination
```

## Why a medallion

BTS data arrives as monthly zipped CSVs, and the analysis plan means slicing the same 17.6M rows many ways: traffic, delays and early arrivals across administrations, the DOGE cuts, the Iran conflict.

- **Bronze** makes re-reading a month cheap. It is Hive-partitioned on `year` and `month`, with a `salt` column so the files in a partition stay about the same size.
- **Silver** is where the flight grain is tested on every row, before anything is aggregated. The five subject views each cover one subject, so each transformation can be tested at flight grain.
- **Gold** is a Kimball star and the OBT.

`int_flights` is a table and the five subject views read from it. As a view, its window over 17.6M rows would run again on each of those reads. The subject views stay views because they only project columns.

## Things that are easy to get wrong in this dataset

**The five delay cause fields add up to `ARR_DELAY`, not `DEP_DELAY`.** Per [BTS](https://www.transtats.bts.gov/Fields.asp?gnoyr_VQ=FGK), `CARRIER_DELAY + WEATHER_DELAY + NAS_DELAY + SECURITY_DELAY + LATE_AIRCRAFT_DELAY` equals the arrival delay for any flight that arrives 15 or more minutes late, and the fields are null otherwise. So the cause shares are taken against arrival delay.

**`DEP_DELAY_NEW` sets negative delays to 0.** `DEP_DELAY` is the signed value. `int_flight_delay` rebuilds the signed delay by using `DEP_DELAY_NEW` when it is not zero and `DEP_DELAY` otherwise.

**`delay_change` is `arr_delay - dep_delay`.** A positive value means the flight lost more time in the air. A negative value means it made time up.

**BTS stores clock time as an `hhmm` integer and uses `2400` for midnight.** `FLOOR(2400 / 100)` is 24, which is outside every hour bucket, so the hour is taken with `MOD(..., 24)`.

**A rate has to be a ratio of sums.** `AVG(carrier_arr_delay_share)` gives a flight that is 5 minutes late the same weight as one that is 5 hours late. The mart reports `SUM(carrier_delay) / SUM(arr_delay)` as the share and keeps the per-flight average under a `mean_flight_` prefix, so the two cannot be mixed up.

## Reference data in seed files

The 45 calendar events (OPEC cuts, hurricanes, the CrowdStrike outage, the DCA midair collision, FAA outages, Strait of Hormuz closures) are in [`seeds/flight_calendar_events.csv`](seeds/flight_calendar_events.csv). They are joined onto the date spine in `dim_flight_dates`, so an event on a day with no flights still appears, and adding an event is a CSV edit.

The CSV is read and written to the warehouse as a table on every run, which is what `dbt seed` does. It has to be a table and not a temp view, because a later script reads it in a separate Spark session. Loading fails on a duplicate `event_date`, since a duplicate would add a second row for that day.

The 50 states plus DC are in [`seeds/us_states.csv`](seeds/us_states.csv). `int_flights` joins it twice, for `origin_region` and `dest_region`. BTS ships lowercase state codes, so the seed is lowercased when it loads. It is a copy of [`natality_names_kimball/dbt/seeds/us_states.csv`](../natality_names_kimball/dbt/seeds/us_states.csv). The two projects are independent and share no seed directory, so a change to one has to be made to the other.

## The date dimension is a spine

`dim_flight_dates` has one row for every calendar day from the first flight date to the last, built with `sequence()` and `explode()`. The calendar attributes are derived from the spine date, so a day with no flights is still a complete row. `fl_date_raw`, BTS's own text for the date, is null on a day with no flights.

`has_flight_data` is true when at least one flight exists for the date. A missing source file shows up as a run of `FALSE`:

```sql
SELECT flight_date FROM dim_flight_dates WHERE NOT has_flight_data ORDER BY flight_date
```

The tests check that `flight_date` is unique and not null and that the row count equals the number of days between the first and last date. No test fails on a `FALSE`. The column is there to be read. The range comes from the data, so a file missing at either end shortens the spine and is not flagged.

`date_key` is `sha2(CAST(flight_date AS STRING), 256)`. `fct_flights` computes the same expression, so its keys match without a join. The mart left joins the dimension from the fact, so a day with no flights cannot add a row to the mart.

## The flight grain key

`flight_id` is a hash of `FL_DATE`, carrier, origin, destination and the three scheduled times. It leaves out the flight number, which is not in this BTS download.

I measured the effect: 389 keys have two rows, out of 17,569,073 rows (0.0022%). In every pair the distance is identical, air time differs by 1.66 minutes on average, and departure and arrival delay move together. That is one flight reported twice, not two flights sharing a key.

`int_flights` keeps one whole row per key with `ROW_NUMBER()`. It prefers the row with actual times, then the longer elapsed time, then a full tiebreak so every build picks the same row. The [root README](../README.md#the-flight-grain-key) has the numbers and the SQL.

## Testing

[`06_run_data_tests.py`](06_run_data_tests.py) runs 29 assertions across the seed, silver, gold and mart layers:

```bash
python flights_medallion/06_run_data_tests.py              # everything
python flights_medallion/06_run_data_tests.py --layer seed   # or silver / gold / mart
python flights_medallion/06_run_data_tests.py --fail-fast
```

[`spark_data_tests.py`](spark_data_tests.py) implements the five tests the dbt projects get from dbt: `unique`, `not_null`, `relationships`, `accepted_values` and `assert_grain`. Each one raises on failure, so a run cannot carry on past a broken grain and write a wrong mart. The mart also checks that its flight count matches the fact.

There is one more test that the dbt projects do not have. `rows_are_real` checks that de-duplication kept a whole source row and did not build one from several. A grain test cannot check that, because a row built from two source rows still has a unique key and no nulls.

The assertions run from this script and not inside each transform, because a uniqueness check over 17.6M rows is not free.

## Running

```bash
python flights_medallion/01_ingest_to_bronze.py      # zips   -> bronze Parquet
python flights_medallion/02_silver_staging.py        # bronze -> stg_flights
python flights_medallion/03_silver_intermediate.py   # int_flights and 5 subject views
python flights_medallion/04_gold_core.py             # Kimball star
python flights_medallion/05_gold_mart.py             # OBT
python flights_medallion/06_run_data_tests.py        # all assertions
```

Paths and Spark settings come from the root `.env` through [`spark_session.py`](spark_session.py). Derby allows one process at a time, so run the scripts one after another.
