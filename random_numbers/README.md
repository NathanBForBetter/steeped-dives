# random_numbers

26,981 picks of a number from 1 to 100 by ChatGPT, by people and by a true random generator, modeled in DuckDB with dbt. It is the model behind the Steeped Dive [People are 4x more random than ChatGPT when picking 1-100](https://steeped.ai/people-are-4x-more-random-than-chatgpt-when-picking-1-100/).

```
chatgpt_random_clean.csv ┐                        ┌─ stg_random_num_ai ─────┐
random-humans.csv ───────┼─ random_num_build.py ──┼─ stg_random_num_people ─┼─ int_random_num_combine ─┬─ dim_random_num_chosen ─┐
random-numbers.csv ──────┘  (3 raw DuckDB tables) └─ stg_random_num_true ───┘                          └─ fct_random_num ────────┴─ 4 analytical marts
  (from true_random.py)
```

## Analytical marts instead of an OBT

The other projects in this repo end in a One Big Table that the Steeped AI platform profiles and analyzes. This dataset has very few columns: a number, who picked it and how many times. Because of that I went with a more traditional analytical mart approach here. The dbt marts hold the analysis, instead of an OBT for Steeped to handle the analytical layer.

The article's figures first came from a pandas script. I rebuilt the calculations in DuckDB and dbt, and the marts give the same figures.

## Sources

| Source | Picks | What it is |
|---|---|---|
| ChatGPT | 10,000 | A [public dataset](https://github.com/exmergo/research-chatgpt-guesses-between-1-and-100) shared in a [Reddit post](https://www.reddit.com/r/dataisbeautiful/comments/1tn5s18/oc_i_asked_gpt_to_pick_a_random_number_between_1/). Each row is one call that asked GPT-4.1 for a random number from 1 to 100. |
| People | 6,981 | A [dataset a Redditor collected](https://www.reddit.com/r/dataisbeautiful/comments/88m2mj/pick_a_number_from_1100_results_from_6750/) about nine years ago with a Google Form that asked for a random number from 1 to 100. |
| True random | 10,000 | Numbers drawn by [`true_random.py`](true_random.py) from the operating system's random source. |

[`random_num_build.py`](random_num_build.py) loads the three CSVs into raw DuckDB tables with every column as text. The staging models set the types, so a bad value stops the build there.

### The true random baseline

`true_random.py` reads bytes from `os.urandom` and turns them into numbers with rejection sampling. 256 is not a multiple of 100, so taking `byte % 100` would make 1 to 56 come up 1.5 times as often as 57 to 100. The script throws away bytes of 200 and above, which leaves every number equally likely.

Each run also tests a fresh sample of a million draws: a chi-square test for uniformity and a lag-1 correlation for independence. It then runs the biased `byte % 100` method as a control, which is supposed to fail.

## Layers

| Layer | Models | Materialization | Job |
|---|---|---|---|
| `staging/` | 3 | view | Sets the types, and keeps the ChatGPT calls that returned a usable number |
| `intermediate/` | 1 | table | Counts picks per number and source, on a spine of 100 numbers by 3 sources |
| `core/` | 2 | table | `fct_random_num` at number and source grain, and `dim_random_num_chosen` |
| `marts/` | 4 | table | The comparisons the article reports |

## Every number for every source

[`int_random_num_combine`](dbt/models/intermediate/int_random_num_combine.sql) crosses the numbers 1 to 100 with the three sources, so the fact always has 300 rows.

That matters because ChatGPT never picked 12 of the numbers: 1, 5, 6, 20, 30, 40, 50, 60, 70, 80, 90 and 100. Each of those is a real 0% share. The distance measures compare two sources number by number, and a number missing from one side would be left out of the comparison and make ChatGPT look closer to random than it is.

## Marts

| Mart | Grain | What it holds |
|---|---|---|
| [`mart_random_num_diff`](dbt/models/marts/mart_random_num_diff.sql) | number | Each source's share of picks side by side, and the difference between each pair of sources |
| [`mart_random_num_bias`](dbt/models/marts/mart_random_num_bias.sql) | source and number | The 15 numbers ChatGPT and people each picked most above and most below true random |
| [`mart_random_num_diff_patterns`](dbt/models/marts/mart_random_num_diff_patterns.sql) | pattern | Shares by tens range, by even or odd, and by last digit, with the same differences |
| [`mart_random_num_divergence`](dbt/models/marts/mart_random_num_divergence.sql) | pair of sources | How far apart two sources' distributions are, and how many times further from true random a source is than people are |

### Measuring distance

The [`divergence_aggs`](dbt/macros/divergence_aggs.sql) macro computes two measures for a pair of sources:

- **Total variation distance** is half the sum of the absolute differences in share across the 100 numbers. 0% means the two sources pick identically and 100% means they never pick the same number.
- **Jensen-Shannon divergence**, base 2, runs from 0 to 1. It works on the ratio between two shares, so it reacts more to a number that one source picks and the other almost never does.

| Pair | Total variation distance | Jensen-Shannon divergence | Times further from random than people |
|---|---|---|---|
| ChatGPT and true random | 48.21% | 0.2663 | 4.06 |
| People and true random | 11.89% | 0.0175 | 1.00 |
| ChatGPT and people | 52.98% | 0.3094 | |

The last column is `times_further_from_random_than_people_tvd`: a pair's total variation distance divided by the distance between people and true random. It is filled on the pairs that include true random. The 4.06 on the ChatGPT row is the 4x in the article's title. `times_further_from_random_than_people_jsd` is the same ratio on Jensen-Shannon divergence, and comes to 15.23.

### Other figures from the article

| Figure | Mart | Value |
|---|---|---|
| ChatGPT picks that end in 7 | `mart_random_num_diff_patterns` | 28.65% |
| ChatGPT picks that end in 0 | `mart_random_num_diff_patterns` | 0.01% |
| ChatGPT picks from 1 to 9 | `mart_random_num_diff_patterns` | 0.12% |
| ChatGPT picking 47, against true random | `mart_random_num_bias` | 5.98 times as often |
| People picking 69, against true random | `mart_random_num_bias` | 2.88 times as often |

`times_vs_random` in the bias mart is 0 where a source never picked a number. The article's underpick ratios turn that around and count a zero as one pick, so they stay finite.

## Macros and tests

- [`divergence_aggs`](dbt/macros/divergence_aggs.sql) returns the five distance columns for two share columns. Its helpers wrap what they return in parentheses, so each one can sit inside a larger expression.
- [`assert_grain`](dbt/tests/generic/assert_grain.sql) tests a composite grain and returns the rows that break it.
- [`assert_fct_random_num_reconciles`](dbt/tests/assert_fct_random_num_reconciles.sql) checks three things for each source: it has all 100 numbers, every pick in staging is counted once, and its shares add up to 100.
- Staging tests that every pick is from 1 to 100. A number outside that range would not be on the spine but would still count toward its source's total.
- The divergence mart tests that the total variation distance is between 0 and 100 and the Jensen-Shannon divergence between 0 and 1.

69 dbt tests across 10 models.

## Download date

`dl_date` comes from `RANDOM_DL_DATE` in the root `.env` and is carried through staging, intermediate and the fact. It comes from the environment and not from `current_date()`, so a rebuild of the same files gives the same value.

## Known limitations

- **The baseline is a sample.** True random here is 10,000 draws, not an exact 1% for each number, so every distance from it includes sampling noise. A perfectly uniform set of 6,981 picks would still measure about 6% from it. Against an exact 1% the distances are 48.27% for ChatGPT and 11.06% for people, a ratio of 4.36.
- **One model and one prompt.** The ChatGPT picks are all from `gpt-4.1-2025-04-14` with a single prompt.
- **The people are not a random sample.** The form was shared on Reddit.
- **Tens ranges are not the same size.** `0s` is 1 to 9 and `100s` is 100 alone, so compare a range to the true random column and not to another range.

## Running

```bash
python random_numbers/random_num_build.py   # CSVs -> raw DuckDB tables
cd random_numbers/dbt
set -a; source ../../.env; set +a           # RANDOM_DATA_DIR and RANDOM_DL_DATE for profiles.yml
dbt deps && dbt build                       # 10 models, 69 tests
```

`true_random.py` appends to `random-numbers.csv`, so it is only run to create the sample or add to it.

DuckDB runs inside dbt's own process, so there is no server to start. A DuckDB file takes one writer at a time, so `random_num_build.py` has to finish before dbt starts.

`profiles.yml` is committed because every value in it comes from `env_var()`. dbt looks for it in the current directory, so running from this folder needs no flags.
