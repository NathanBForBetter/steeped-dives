# natality_names_kimball

US baby names and births, modeled in Spark SQL with dbt. This is the closest thing in the repo to textbook Kimball: two facts over five conformed dimensions.

```
SSA state files ──── stg_nn_baby_names ──── int_nn_baby_names ─────┐
                                                                   ├─ fct_nn_baby_names ──┐
name origins ─────── stg_nn_origins_* ───── int_nn_origins ────────┘                      │
                                                          dim_nn_name / _state / _year    ├─ mart_nn_combined_steeped_obt
CDC WONDER ───────── stg_nn_natality_* ──── int_nn_natality ───────┐      / _gender       │
  (4 breakouts)        (age/edu/hisp/race)                         ├─ fct_nn_baby_natality┘
                                                           dim_nn_mother
```

## Sources

| Source | Grain | How it is read |
|---|---|---|
| [SSA baby names, state level](https://www.ssa.gov/oact/babynames/limits.html) | state + gender + year + name | one headerless `.TXT` per state, read by [`extract_names.py`](extract_names.py) |
| [CDC WONDER Natality](https://wonder.cdc.gov/natality.htm) | state + year + infant sex + one mother attribute | four breakouts (age, education, Hispanic origin, race) from four dataset vintages, read by [`extract_natality.py`](extract_natality.py) |
| BehindTheName | name | reference CSVs, read by [`extract_origins_btn.py`](extract_origins_btn.py) |
| Ancestry first name pages | name | live scrape, [kept in a private repo](../README.md#where-the-extractors-went) |

It runs on Spark because the names grain is state by year by name across 116 years.

## Two things to know before reading the models

### Name gender is not birth sex

SSA records a gender with each name count. CDC records each infant's sex at birth. They measure different things, and the mart exists to compare them: `year_state_name_female_share` (names) sits next to `year_state_nat_female_birth_share` (births).

Both facts use `dim_nn_gender`, so it is a role-playing dimension. The [root README](../README.md#name-gender-is-not-birth-sex) has the reasoning.

SSA counts boys and girls separately, so a name given to both in a state and year has two rows in the mart. `year_state_name_gender` says which gender each row's count is for.

### CDC WONDER reports race and Hispanic origin differently across years

- `source` holds the vintage, taken from the WONDER filename, and reaches the fact as `wonder_vintage`.
- `stg_nn_natality_race` and `stg_nn_natality_hisp` keep the label each vintage reported and add a bridged value that every vintage can be rolled up to.
- `int_nn_natality` puts the bridged values under `race` and `hisp_origin`, and the finer groups under `race_detail` and `hisp_origin_detail`, so the two levels are never added together.
- 2020 to 2024 have no race in WONDER. 2025 is provisional and reports single-race groups, with `More than one race` kept as its own value.

The [root README](../README.md#cdc-wonder-taxonomy-drift) has the table of what each range of years reports.

## Birth counts and shares

`total_birth_count` is all births in a state, year and infant sex. It comes from the age breakout, which excludes nothing. Divide any attribute count by it to get a share of all births. Education and Hispanic origin leave out unknown rows, so their shares add up to less than 1.

Natality is loaded for every year from 1995 to 2025. 2025 is provisional.

Education has a gap. From 2003 to 2015 WONDER excludes a state's births from the education breakout in the years its birth certificate does not match the dataset, and reports 0 for every level. `stg_nn_natality_edu` leaves those state and year pairs out, so they have no education rows. The [root README](../README.md#known-limitations) has the numbers.

## The state seed

[`dbt/seeds/us_states.csv`](dbt/seeds/us_states.csv) holds the 50 states plus DC with their Census region and division. `stg_us_states` is a view over it, and `int_nn_baby_names` left joins it on `state_code`. So `state_name`, `region` and `census_division` come from the seed and flow into `dim_nn_state`. `state_name` is spelled the way CDC WONDER spells it, including `District of Columbia`, because `fct_nn_baby_natality` joins on it.

The seed has its own tests: codes and names are unique and not null, and `assert_state_coverage` checks for 51 members.

[`flights_medallion/seeds/us_states.csv`](../flights_medallion/seeds/us_states.csv) is a copy of the same file. The two projects are independent and share no seed directory, so a change to one has to be made to the other.

## Layers

| Layer | Models | Materialization | Job |
|---|---|---|---|
| `staging/` | 8 | view | One model per source plus the state seed: rename, cast, lowercase, bridge a taxonomy. No joins. |
| `intermediate/` | 3 | table | Calendar and geography for names, the four natality breakouts on one grain, origins merged from two sources |
| `core/` | 7 | table | Two facts, five conformed dimensions |
| `marts/` | 1 | table | OBT for the Steeped AI platform |

## Macros and tests

- [`array_agg_order_by`](dbt/macros/array_agg_order_by.sql) stands in for `ARRAY_AGG(x ORDER BY y)`, which Spark does not have. It names its struct fields, so callers read `[0].value`.
- [`safe_divide`](dbt/macros/safe_divide.sql) uses `adapter.dispatch`. It renders `TRY_DIVIDE` here and `::numeric / NULLIF(...)` in the Postgres project.
- [`assert_grain`](dbt/tests/generic/assert_grain.sql) tests a composite grain and returns the rows that break it.
- [`assert_state_coverage`](dbt/tests/generic/assert_state_coverage.sql) checks for 51 members, on both the `us_states` seed and `dim_nn_state`.
- [`assert_contiguous_years`](dbt/tests/generic/assert_contiguous_years.sql) checks that no year is missing between the first and the last, which catches a source file that failed to load.

[`unaccent.py`](unaccent.py) folds accents to ASCII so origin names line up with the SSA files. It uses one native `translate()` call instead of a Python UDF, which would send every value to a worker process and back.

156 dbt tests across 19 models and one seed.

## Running

Derby allows one process at a time, so no extract script can hold a Spark session while dbt runs. dbt will not set `JAVA_HOME` for you the way `spark_session.py` does.

```bash
python natality_names_kimball/extract_names.py
python natality_names_kimball/extract_natality.py
python natality_names_kimball/extract_origins_btn.py

cd natality_names_kimball/dbt
set -a; source ../../.env; set +a      # JAVA_HOME + NATALITY_DATA_DIR
echo "$JAVA_HOME"                      # confirm before running
dbt deps && dbt build
```

It needs `pip install "dbt-spark[session]"`. The `session` method runs Spark inside dbt's own process, so there is no thrift server.
