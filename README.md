# Steeped Dives: data modeling

This repository holds the data models behind [Steeped Dives](https://steeped.ai/resources/), the articles I publish on open datasets. I'm sharing the modeling and transformations in full so readers can check the work and tell me where it could be better.

There are three pipelines. Each one takes a public dataset, models it, and ends in a One Big Table that the [Steeped AI](https://steeped.ai) platform profiles and analyzes. The numbers in each article come from these models. If a grain, a crosswalk or a denominator looks wrong, please open an issue.

| Project | Source | Engine | Orchestration | Architecture | Scale | Grain |
|---|---|---|---|---|---|---|
| [`board_games/`](board_games) | [BoardGameGeek](https://boardgamegeek.com) rankings and game pages | PostgreSQL | dbt | dbt layers, Kimball core, OBT mart | 140,107 games | one game |
| [`natality_names_kimball/`](natality_names_kimball) | [SSA baby names](https://www.ssa.gov/oact/babynames/limits.html), [CDC WONDER natality](https://wonder.cdc.gov/natality.htm), name origin references | Spark SQL (Hive, Derby metastore) | dbt (`dbt-spark`, session method) | Kimball: two facts, five conformed dimensions, OBT mart | 6.7M name rows across 51 states and 116 years | a name and gender in a state and year; births by state, year and mother attribute |
| [`flights_medallion/`](flights_medallion) | [BTS On-Time Performance](https://www.transtats.bts.gov/ontime/) | Spark SQL | Python and Spark SQL, following dbt conventions by hand | Medallion (bronze, silver, gold), Kimball core, OBT mart | 17.6M flights | one flight |

About 6,800 lines of SQL, Python and YAML. The two dbt projects have 282 tests across 41 models, one snapshot and one seed: 159 `not_null`, 54 `unique`, 28 grain, 22 referential integrity, 14 `accepted_values`, 3 completeness and 2 singular. The flights pipeline has no dbt, so it has 29 hand-written assertions of the same kinds.

---

## Table of contents

- [Why three architectures instead of one](#why-three-architectures-instead-of-one)
- [Slowly changing dimensions: Type 2 on board games](#slowly-changing-dimensions-type-2-on-board-games)
- [The data is not in this repo](#the-data-is-not-in-this-repo)
- [Testing](#testing)
- [Macros](#macros)
- [The model worth reading first](#the-model-worth-reading-first)
- [Design decisions](#design-decisions)
  - [Why the mart is One Big Table](#why-the-mart-is-one-big-table)
  - [Why the fact is wide](#why-the-fact-is-wide)
  - [Name gender is not birth sex](#name-gender-is-not-birth-sex)
  - [Aggregates on a dimension](#aggregates-on-a-dimension)
  - [Every fact says which extract it came from](#every-fact-says-which-extract-it-came-from)
  - [Date dimension with a spine](#date-dimension-with-a-spine)
  - [The flight grain key](#the-flight-grain-key)
  - [CDC WONDER taxonomy drift](#cdc-wonder-taxonomy-drift)
- [Known limitations](#known-limitations)
- [Running it](#running-it)
- [Repository layout](#repository-layout)

---

## Why three architectures instead of one

The three datasets have different shapes, so the three projects are modeled differently. Forcing one pattern onto all of them would have made the repo easier to read and the models worse.

**`natality_names_kimball` is the closest to textbook Kimball.** It has two facts (`fct_nn_baby_names`, `fct_nn_baby_natality`) over five conformed dimensions. It gets a full star because it is the only project where two independent sources have to be compared. SSA name counts and CDC birth counts share state, year and gender and nothing else, and conformed dimensions turn that comparison into a join. It runs on Spark because the names grain is state by year by name across 116 years.

**`flights_medallion` is a medallion.** BTS on-time data arrives as monthly zipped CSVs, 17.6M rows in all. Bronze is Hive-partitioned Parquet, so re-reading a month is cheap. Silver is where the flight grain is tested, before anything is aggregated. Gold is a Kimball star and the OBT. The analysis plan (traffic, delays and early arrivals across administrations, the DOGE cuts, the Iran conflict) means slicing the same rows many ways, and the bronze layer is what makes each new slice cheap.

There is no dbt in this project. It follows dbt's conventions by hand in Spark SQL: the same layers, the same `stg_`, `int_`, `dim_`, `fct_` and `mart_` prefixes, and the same kinds of data tests, written in [`spark_data_tests.py`](flights_medallion/spark_data_tests.py). I wanted to show that the approach does not depend on the tool.

**`board_games` is dbt layers around a Kimball core.** It covers 140,107 games described by about 4,000 sparse category terms spread over four list columns. Most of the work is turning those terms into categories that can be analyzed. So staging is thin, the intermediate layer is heavy, and the core exists mainly to prove the category grains are clean. It runs on Postgres because 140k rows do not need a cluster, and `UNNEST`, array aggregation and regex are easier there.

### What they share

All three go from `stg_` to `int_` to `dim_` and `fct_` to `mart_*_steeped_obt`, and test the grain along the way.

---

## Slowly changing dimensions: Type 2 on board games

BoardGameGeek editors revise a game's name, publishers, year and description, and each Steeped Dive starts from a new scrape. [`scd_board_game`](board_games/dbt/snapshots/scd_board_game.yml) is a dbt snapshot that stores a new version of a game whenever one of those attributes changes. Ratings and vote counts are left out, because they change on every scrape and would add a row per game per run.

```yaml
strategy: check
check_cols: [board_game_name, year_made, short_descr, main_publisher, secondary_publisher]
updated_at: scraped_at
hard_deletes: new_record
```

The `updated_at` line is the important one. With the check strategy, dbt dates each version by the time the snapshot ran unless it is told otherwise. The fact is dated by its scrape, so the versions have to be dated by the scrape too. `scraped_at` is the scrape date as a timestamp.

[`fct_board_games`](board_games/dbt/models/core/fct_board_games.sql) reads the game's attributes through the snapshot and takes the version that was in force on the scrape date:

```sql
LEFT JOIN {{ ref('scd_board_game') }} AS V
    ON  V.board_game_url = I.board_game_url
    AND I.scraped_at >= V.dbt_valid_from
    AND (I.scraped_at < V.dbt_valid_to OR V.dbt_valid_to IS NULL)
    AND V.dbt_is_deleted = 'False'
```

Three tests guard the join. One checks that no game has two open versions. Another checks that the version the fact picked up says what the scrape said, which fails if the join finds nothing or finds an out-of-date version. And because the fact does not aggregate, a join that matched two versions would duplicate a game and fail the grain test.

Only one scrape has been taken, so every game has one version today. To check the behavior I simulated a second scrape in a scratch schema. A changed publisher and a renamed game each closed the old version on the new scrape date and opened a new one. A delisted game got a deletion record. Running the snapshot again on the same scrape added nothing.

`dim_flight_airlines` is the next place this would fit, since carriers merge and stop operating. It is still Type 1.

---

## The data is not in this repo

No raw or modeled data is committed. It lives in a local Postgres instance and two local Spark warehouses, outside this directory. [`.env.example`](.env.example) lists the paths the code expects.

That means nobody else can run these pipelines as they stand. The sources are public and each project documents where its data came from, but the tables themselves are not available and in most cases will not be shared. The BTS extract alone is tens of gigabytes, and some of the reference data came with terms that do not allow redistribution.

If you want schemas, row counts or profiling output, or want to talk through a decision, open a GitHub issue.

### Where the extractors went

The scripts that make live HTTP requests, the BoardGameGeek scraper and the Ancestry name origin scraper, are in a private repo. They rate limit, stop when a site pushes back, check `robots.txt` and can resume an interrupted run. They also send a browser User-Agent, and one of them fetches from a site whose terms do not allow automated collection, so I kept them out of a public modeling repo.

The tables they produce (`board_game_raw`, `board_game_discovered`, `board_game_scrape`, `nn_ancestry_raw`) are declared as dbt sources. The extractors that read public government downloads from disk are here: [`extract_names.py`](natality_names_kimball/extract_names.py), [`extract_natality.py`](natality_names_kimball/extract_natality.py), [`extract_origins_btn.py`](natality_names_kimball/extract_origins_btn.py) and [`01_ingest_to_bronze.py`](flights_medallion/01_ingest_to_bronze.py).

---

## Testing

There are 282 dbt tests and 29 Spark assertions. The ones I rely on most check a rule about the data, not a property of a single column.

### Grain

Every fact and dimension declares its grain and tests it, and so do the intermediate models that feed them:

```yaml
- name: fct_nn_baby_natality
  data_tests:
    - assert_grain:
        arguments:
          columns: [year_key, state_key, mother_key, gender_key]
```

I wrote [`assert_grain`](natality_names_kimball/dbt/tests/generic/assert_grain.sql) instead of using `dbt_utils.unique_combination_of_columns` because it returns the rows that break the grain. A failure shows which group fanned out, which saves a lot of time on a 17.6M row table.

### Referential integrity

There are 22 `relationships` tests in dbt and three written by hand for Spark:

```python
st.test_relationship(spark, 'fct_flights', 'route_key', 'dim_flight_routes', 'route_key')
```

A dimension can be unique and complete while the fact holds a key that no dimension row matches. When that happens, every rollup by that dimension loses rows and nothing reports it. These tests catch it. To keep keys from being null in the first place, `dim_board_games_year` has an explicit unknown member for games with no usable year.

### Completeness

Three tests check that nothing is missing:

- [`assert_state_coverage`](natality_names_kimball/dbt/tests/generic/assert_state_coverage.sql) checks that the state dimension has 51 members. `not_null` and `unique` both pass on a dimension that has lost half its rows.
- [`assert_contiguous_years`](natality_names_kimball/dbt/tests/generic/assert_contiguous_years.sql) checks that no year is missing between the first and the last. If one source file fails to load, the minimum and maximum do not move and no other test notices.
- The flights date spine is checked the same way: the row count has to equal the number of days between its first and last date.

### Reconciliation

The flights mart checks that it holds the same number of flights as the fact:

```python
fact = spark.sql("SELECT SUM(flight_count) AS n FROM fct_flights").first()["n"]
mart = spark.sql("SELECT SUM(flight_count) AS n FROM mart_flight_steeped_obt").first()["n"]
if fact != mart:
    raise ValueError(...)
```

A mart grouped by ten columns is an easy place for a join to duplicate rows, and a count is the cheapest way to find out.

### De-duplicated rows exist in the source

`rows_are_real` checks that every row left after de-duplication exists, column for column, in the table it came from:

```python
st.test_rows_are_real(spark, 'int_flights', 'stg_flights', 'flight_id',
                      ['dep_time', 'arr_time', 'dep_delay', 'arr_delay', ...])
```

A grain test cannot do this job. A row assembled from two source rows still has a unique key and no nulls. See [The flight grain key](#the-flight-grain-key) for why it matters here.

### Running the Spark tests

A uniqueness check over 17.6M rows is not free, so the Spark assertions are not run inside every transform. [`06_run_data_tests.py`](flights_medallion/06_run_data_tests.py) runs them by layer and reports every failure instead of stopping at the first:

```bash
python flights_medallion/06_run_data_tests.py --layer silver
python flights_medallion/06_run_data_tests.py --fail-fast
```

---

## Macros

There are five.

**[`safe_divide`](board_games/dbt/macros/safe_divide.sql)** returns `NULL` when the denominator is zero or null. It uses `adapter.dispatch` because the repo runs on two warehouses. Postgres raises `division_by_zero` and Spark has `try_divide`, so one macro name gives the right behavior on both:

```sql
{% macro safe_divide(numerator, denominator) -%}
    {{ return(adapter.dispatch('safe_divide', 'steeped_macros')(numerator, denominator)) }}
{%- endmacro %}

{% macro postgres__safe_divide(n, d) -%} ({{ n }})::numeric / NULLIF(({{ d }}), 0) {%- endmacro %}
{% macro spark__safe_divide(n, d) -%}    TRY_DIVIDE(CAST(({{ n }}) AS DOUBLE), CAST(({{ d }}) AS DOUBLE)) {%- endmacro %}
```

**[`mixed_int_cast`](board_games/dbt/macros/mixed_int_cast.sql)** is a numeric cast guarded by a regex. The BGG scrape arrives as text with blanks, `'N/A'` and the occasional number in scientific notation. A plain `::float` stops the whole model on the first bad value. This macro turns the bad value into `NULL` and lets the other rows through. One staging model calls it 36 times.

**[`format_category`](board_games/dbt/macros/format_category.sql)** turns a BGG term into a lookup key in six steps: lowercase, slashes written out as "or", parentheses, colons and the words "game" and "games" removed, hyphens to spaces, runs of spaces collapsed, and a trim. `Card Game` becomes `card` and `Trick-taking` becomes `trick taking`. The vocabulary model and the categories model both call it, so a term has the same key where it is clustered and where it is looked up.

**[`array_agg_order_by`](natality_names_kimball/dbt/macros/array_agg_order_by.sql)** stands in for `ARRAY_AGG(x ORDER BY y)`, which Spark does not have. It builds `sort_array(collect_list(struct(...)))` and names the struct fields `sort_key` and `value`, so the caller reads `[0].value` and gets the value, not a struct.

**`dbt_utils.generate_surrogate_key`** builds every dimension key, so hashing is consistent and handles nulls the same way everywhere.

---

## The model worth reading first

[`int_board_games_categories.sql`](board_games/dbt/models/intermediate/int_board_games_categories.sql) is about 1,100 lines and is the model I would point someone to first.

BoardGameGeek describes each game with four sparse list columns (`sub_category`, `mechanism`, `family`, `main_category`) that hold about 4,000 distinct terms between them. One-hot encoding them gives 4,000 columns that nobody can read. Hand-picking a dozen categories throws most of the information away.

The model does this instead:

1. [`int_board_games_category_vocabulary`](board_games/dbt/models/intermediate/int_board_games_category_vocabulary.sql) flattens the lists into one set of distinct terms, with a game count for each.
2. The terms are embedded with a sentence transformer and clustered outside dbt.
3. The clusters come back as the `IN` lists in this model's `CASE` statements. That is why terms like `'spies or secret agents'`, `'environmental'` and `'political'` land in the same bucket. All 514 terms in those lists match a key the model produces.
4. Each of the 13 category families gets three columns: `_type` (the specific term), `_cat` (the bucket it rolls up to) and `_source` (which of the four lists the value came from).
5. The four lists are merged with a `COALESCE` in priority order: `sub_category`, then `mechanism`, then `family`, then `main_category`. The most specific list wins, and `_source` records which one that was.

The result is 532,959 category assignments across 139,887 of the 140,107 games: 294,682 from sub-categories, 145,439 from mechanisms, 86,106 from families and 6,732 from main categories. Because `_source` is kept, a question like "why is this game tagged historical?" takes one query to answer.

The model starts from a list of every game and left joins the four lists to it, so a game with no sub-category still gets its mechanism and family categories.

---

## Design decisions

### Why the mart is One Big Table

Every pipeline ends in `mart_*_steeped_obt`, a wide table with repeated values in it. The usual worry about that shape is that someone sums a repeated measure and gets a wrong number.

The table is built for Steeped AI, a platform I built that takes a dataset and builds an analytical model over it: typed, multi-grain PostgreSQL tables, profiling, cross-tabulation, regression and AI-guided analysis. It works out the grain of each column and aggregates with that in mind, so the platform handles the repeated values. A star schema leaves the grain to whoever writes the query. That is fine for an analyst and risky for everyone else. I built the platform so that someone who is not an analyst can upload a dataset and not get a wrong answer.

So the mart gives the platform every attribute combination as a row, with no joins to specify. In `mart_nn_combined_steeped_obt`, each names row (a name and the gender SSA recorded for it, in a state and year) appears once for every mother attribute value in that state and year. That repetition is what lets the platform cross name choice against mother age, race, Hispanic origin and education. Nothing is de-duplicated in advance, and anyone aggregating the table by hand should group to its grain first.

The star schema is still there. Each mart is built from the conformed, tested models in `core/`.

### Why the fact is wide

`fct_board_games` carries each of the 13 category families twice, as a surrogate key and as text.

The keys each have a `relationships` test to their dimension, so a category that is in the fact and missing from its dimension fails the build. Those are 13 of the 22 referential integrity tests. The text is there because the fact's only consumer is the OBT. Joining 13 dimensions to get the same text back would cost 13 joins and save almost nothing at 140k rows.

### Name gender is not birth sex

SSA records a gender with each name count. CDC records the sex of each infant at birth. They measure different things, and treating them as one would make every gender finding in a Steeped Dive wrong.

Both facts use `dim_nn_gender`, which makes it a role-playing dimension:

- `fct_nn_baby_names.gender_key` is the gender SSA recorded for the name
- `fct_nn_baby_natality.gender_key` is the infant's sex at birth

One dimension with one set of keys makes the comparison a join. The mart puts `year_state_name_female_share` (names) next to `year_state_nat_female_birth_share` (births) so the gap between them can be measured. The dimension is built from both sources, so neither fact can hold a value the dimension lacks.

`dim_nn_name` also gives each name one gender. It uses the origin references when they have an answer. Otherwise it uses the gender the name was given to more often in the SSA files, and labels a near even split `either`. It keeps both inputs as `name_gender_from_origins` and `name_gender_from_ssa_usage`.

SSA counts boys and girls separately, so a name given to both in a state and year has two rows in the mart. `year_state_name_gender` says which gender each row's count is for. `name_gender` describes the name overall, so the two can differ.

### Aggregates on a dimension

`dim_nn_name` carries `total_name_count`, `total_name_count_1995_2025` and `name_in_states_count`. Putting aggregates on a dimension breaks the textbook rule, and I did it on purpose. Steeped AI profiles the name as an entity and needs its overall size without rolling up a 6.7M row fact. The values add up across the dimension and should never be summed through the fact. An aggregate fact at name grain would be more correct, and it would add a join the one consumer does not need.

### Every fact says which extract it came from

All three pipelines are snapshots, and each one records which snapshot it is. `scrape_date` (board games) and `extract_date` (natality, flights) are set in `.env`, carried through staging, intermediate, fact and mart, and tested `not_null` on the facts:

```
# .env
BOARD_GAMES_SCRAPE_DATE=2026-09-23
FLIGHTS_EXTRACT_DATE=2026-10-05
NATALITY_EXTRACT_DATE=2026-10-02
```

They come from the environment, not from `current_date()`, so rebuilding the same extract gives the same value. Otherwise the column would date the build and not the data.

This makes the numbers in an article traceable to one pull and makes two Dives over the same dataset comparable. It is also what the [Type 2 join](#slowly-changing-dimensions-type-2-on-board-games) depends on. The natality fact also keeps `wonder_vintage`, the CDC dataset each count came from.

### Date dimension with a spine

`dim_flight_dates` is a generated spine: one row for every calendar day from the first flight date to the last, built with Spark's `sequence()` and `explode()`. The calendar attributes are derived from the spine date, so a day with no flights is still a complete row.

`has_flight_data` is true when at least one flight exists for the date. A missing source file shows up as a run of `FALSE` that anyone can query:

```sql
SELECT flight_date FROM dim_flight_dates WHERE NOT has_flight_data ORDER BY flight_date
```

Events are joined from a seed file, [`flight_calendar_events.csv`](flights_medallion/seeds/flight_calendar_events.csv), so an event on a day with no flights still appears and adding an event is a CSV edit. `date_key` is a hash of the date. `fct_flights` computes it the same way, so the fact's keys match without a join. Tests check that the spine has one row per day and no gaps.

One limit: the range still comes from the data. A missing file at either end shortens the spine and is not flagged.

### The flight grain key

`flight_id` is a hash of the flight date, carrier, origin, destination and the three scheduled times. It leaves out the flight number, which is not in my BTS download. I measured how much that matters on the 2023 to 2026 extract:

| | |
|---|---|
| rows in `stg_flights` | 17,569,073 |
| distinct `flight_id` | 17,568,684 |
| keys with more than one row | 389, every one of them a pair |
| share of the dataset | 0.0022% |
| pairs where `distance` differs | 0 |
| average `air_time` difference within a pair | 1.66 minutes |
| pairs with one row cancelled and one operated | 131 |

The 389 pairs look like one flight reported twice, not two flights sharing a key. Distance is identical in every pair and air time barely differs. Departure and arrival delay also move together: where one row departs 700 minutes later than the other, it arrives about 700 minutes later too. Two unrelated flights would not do that.

Two different flights by the same carrier on the same route with identical scheduled times would still merge. At 389 pairs in 17.6M rows, with no sign that any of them is that case, I left the key as it is.

#### How duplicates are resolved

`int_flights` keeps one whole row per `flight_id`:

```sql
ROW_NUMBER() OVER (
    PARTITION BY flight_id
    ORDER BY
        CASE WHEN arr_time IS NULL THEN 1 ELSE 0 END,
        CASE WHEN actual_elapsed_time IS NULL THEN 1 ELSE 0 END,
        actual_elapsed_time DESC,
        dep_delay, arr_delay, air_time, taxi_in, taxi_out
) AS dedupe_rank
```

It prefers the row that has actual times, because a cancelled record carries no measurements. The last line of the `ORDER BY` makes the choice the same on every build. Without it, 8 of the 389 pairs tie on the lines above.

Keeping a whole row is the important part. Resolving each column on its own with `ANY_VALUE` and `MAX` can put the arrival delay from one row next to the departure delay from the other, which describes a flight that was never reported. [`rows_are_real`](#de-duplicated-rows-exist-in-the-source) checks that every row in `int_flights` exists in `stg_flights`, and nothing after it in the silver layer aggregates.

### CDC WONDER taxonomy drift

WONDER reports a mother's race and Hispanic origin differently depending on the year:

| Years | Race | Hispanic origin |
|---|---|---|
| 1995-2002 | White, Black, American Indian, five Asian or Pacific Islander groups | five origin groups |
| 2003-2019 | four bridged groups | one figure |
| 2020-2024 | not reported | one figure |
| 2025 (Provisional) | single race: Asian and Pacific Islander separate, plus More than one race | one figure |

Natality is loaded for every year from 1995 to 2025, from four WONDER dataset vintages. The staging models keep each vintage's own label and add a bridged value every vintage can roll up to: the four 1977 OMB race groups that NCHS bridges to, and `hispanic or latino`. A label is only rolled up into a group that contains it, never mapped to a label from another vintage.

The fact carries the bridged values as `race` and `hisp_origin` and the finer groups as `race_detail` and `hisp_origin_detail`, so the two levels are never added together. The bridged counts reconcile to the raw files.

`More than one race` exists only in 2025 and stays its own value, because reassigning those mothers takes NCHS's bridging model. The four groups in 2025 therefore count single-race mothers only, about 3% fewer births.

Education is mapped onto five levels found in every vintage. Each count keeps its vintage as `wonder_vintage`. The provisional year arrives as `'2025 (Provisional)'`. The suffix is dropped so it joins the year dimension, and a final extract wins over a provisional one for the same year.

---

## Known limitations

- **Race from 2020 to 2024.** WONDER has no bridged race for these years, so they have no race rows. 2025 race counts single-race mothers only.
- **Education from 2003 to 2015.** States adopted the 2003 birth certificate in different years, and WONDER excludes a state's births from the education breakout in the years its certificate does not match the dataset. `stg_nn_natality_edu` leaves those 177 state and year pairs out, so they have no education rows. Education covers 47% of US births in 2006, 49% in 2007 and 99% from 2016 on.
- **Flight key.** `flight_id` leaves out the flight number, as described in [The flight grain key](#the-flight-grain-key).
- **Date spine range.** The spine runs from the first flight date to the last, so a missing file at either end is not flagged.

---

## Running it

The data is not available (see [above](#the-data-is-not-in-this-repo)), so this section documents how the pieces fit together. It is not a quickstart.

```bash
git clone <this repo> && cd SteepedDives
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt  # or `pip install -e .` to install the config package too
cp .env.example .env             # then fill in paths and PGPASSWORD
```

Everything specific to a machine lives in one root `.env`. [`steeped_dives/config.py`](steeped_dives/config.py) finds it from any directory and fails at startup on a missing variable or a path that does not exist. Both dbt projects read the same file through `env_var()`, so their `profiles.yml` holds no credentials and is committed.

Each entry point script adds the repo root to `sys.path` before importing `steeped_dives`, so the scripts run straight from a clone without an install.

```bash
# board_games (Postgres + dbt). dbt build runs the snapshot before the fact.
python board_games/build_board_game_filtered.py
cd board_games/dbt && set -a && source ../../.env && set +a
dbt deps && dbt build

# natality_names_kimball (Spark + dbt)
#   Derby allows one process at a time, so no extract script can hold a
#   session while dbt runs. dbt will not set JAVA_HOME for you.
python natality_names_kimball/extract_names.py
python natality_names_kimball/extract_natality.py
python natality_names_kimball/extract_origins_btn.py
cd natality_names_kimball/dbt && set -a && source ../../.env && set +a
dbt deps && dbt build

# flights_medallion (Spark, ordered scripts)
python flights_medallion/01_ingest_to_bronze.py      # zips   -> bronze Parquet
python flights_medallion/02_silver_staging.py        # bronze -> stg_flights
python flights_medallion/03_silver_intermediate.py   # int_flights and 5 subject views
python flights_medallion/04_gold_core.py             # Kimball star
python flights_medallion/05_gold_mart.py             # OBT
python flights_medallion/06_run_data_tests.py        # all assertions
```

---

## Repository layout

```
SteepedDives/
├── .env.example                     the variables the one root .env needs
├── pyproject.toml                   optional editable install, ruff config
├── steeped_dives/config.py          finds .env, typed getters, fails on missing values
│
├── board_games/                     PostgreSQL + dbt
│   ├── build_board_game_filtered.py source prep: ranked list plus newly found games
│   └── dbt/
│       ├── macros/                  safe_divide, mixed_int_cast, format_category
│       ├── snapshots/               scd_board_game (SCD Type 2)
│       ├── tests/                   assert_grain, two singular tests for the snapshot
│       └── models/
│           ├── staging/             stg_board_games
│           ├── intermediate/        the category clustering work
│           ├── core/                fct + 13 category dims + year dim
│           └── marts/               mart_board_games_steeped_obt
│
├── natality_names_kimball/          Spark SQL + dbt
│   ├── extract_names.py             SSA state files
│   ├── extract_natality.py          CDC WONDER, records each file's vintage
│   ├── extract_origins_btn.py       name origin reference CSVs
│   ├── unaccent.py                  accent folding without a Python UDF
│   └── dbt/
│       ├── macros/                  safe_divide, array_agg_order_by
│       ├── seeds/                   us_states
│       ├── tests/generic/           assert_grain, state coverage, contiguous years
│       └── models/
│           ├── staging/             8 models, one per source
│           ├── intermediate/        names, natality breakouts, origins
│           ├── core/                2 facts + 5 conformed dims
│           └── marts/               mart_nn_combined_steeped_obt
│
└── flights_medallion/               Spark SQL, dbt conventions by hand
    ├── 01_ingest_to_bronze.py       zips -> Hive-partitioned Parquet
    ├── 02_silver_staging.py         typed columns, flight grain key
    ├── 03_silver_intermediate.py    de-duplicated flights and 5 subject views
    ├── 04_gold_core.py              Kimball star with a date spine
    ├── 05_gold_mart.py              OBT
    ├── 06_run_data_tests.py         all assertions, runnable by layer
    ├── spark_data_tests.py          unique, not_null, grain, relationships, accepted_values, rows_are_real
    └── seeds/                       flight_calendar_events.csv, us_states.csv
```

---

## License

MIT. See [LICENSE](LICENSE). The models are yours to borrow. The data belongs to SSA, CDC, BTS and BoardGameGeek under their own terms, and none of it is in this repo.

## Feedback

I put this repo up to get feedback. If a grain, a crosswalk, a denominator or a dimension looks wrong, please open an issue. The same goes if you want schemas, row counts or profiling output that is not here.

Published Steeped Dives built on these models: [steeped.ai/resources](https://steeped.ai/resources/)
