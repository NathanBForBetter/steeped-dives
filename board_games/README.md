# board_games

140,107 board games from [BoardGameGeek](https://boardgamegeek.com), modeled in PostgreSQL with dbt. The layers are staging, intermediate, a Kimball core and a One Big Table mart.

```
board_game_filtered ┐                                       ┌─ 13 category dims ─┐
                    ├─ stg_board_games ─ int_* (5 models) ──┤                    ├─ fct_board_games ─ mart_board_games_steeped_obt
board_game_scrape   ┘                    └─ scd_board_game ─┴─ dim_..._year ─────┘
```

## What makes this project interesting

[`int_board_games_categories.sql`](dbt/models/intermediate/int_board_games_categories.sql) is about 1,100 lines. It turns roughly 4,000 sparse BGG category terms, spread over four list columns, into 13 category families that can be read and joined. Each family has a `_type`, a `_cat` and a `_source` that records which of the four lists the value came from.

The `IN` lists in its `CASE` statements were not picked by hand. [`int_board_games_category_vocabulary`](dbt/models/intermediate/int_board_games_category_vocabulary.sql) flattened the terms, a sentence transformer embedded them, they were clustered outside dbt, and the clusters came back as those lists. The [root README](../README.md#the-model-worth-reading-first) has the full write-up.

## Layers

| Layer | Models | Materialization | Job |
|---|---|---|---|
| `staging/` | 1 | view | Joins the two source halves and guards 36 numeric casts with a regex |
| `intermediate/` | 5 | table | The category clustering work, the shopping journey funnel, publisher ranking |
| `snapshots/` | 1 | snapshot | `scd_board_game`, a Type 2 history of each game's descriptive attributes |
| `core/` | 15 | table | One fact, 13 category dimensions, one year dimension |
| `marts/` | 1 | table | OBT for the Steeped AI platform |

The fact carries all 13 category keys next to their text, each with a `relationships` test.

## Slowly changing dimensions

[`scd_board_game`](dbt/snapshots/scd_board_game.yml) keeps a version of a game's name, publishers, year and description whenever one of them changes between scrapes. It uses the check strategy with `updated_at: scraped_at`, so each version is dated by the scrape it came from and not by the time dbt ran.

`fct_board_games` reads those attributes through the snapshot, using the version in force on its scrape date. Two singular tests guard it: [`assert_one_current_board_game_row`](dbt/tests/assert_one_current_board_game_row.sql) and [`assert_fct_board_games_matches_scrape`](dbt/tests/assert_fct_board_games_matches_scrape.sql). The [root README](../README.md#slowly-changing-dimensions-type-2-on-board-games) explains the design.

dbt puts the snapshot in `<target schema>_snapshots`, which is `public_snapshots` with the default profile.

## Macros

- [`mixed_int_cast`](dbt/macros/mixed_int_cast.sql) is a numeric cast guarded by a regex. The scrape arrives as text with blanks, `'N/A'` and scientific notation, and a plain `::float` would stop the model on the first bad value. The staging model calls it 36 times.
- [`format_category`](dbt/macros/format_category.sql) turns a BGG term into a lookup key in six steps. `int_board_games_category_vocabulary` and `int_board_games_categories` both call it, so a term has the same key where it is clustered and where it is looked up.
- [`safe_divide`](dbt/macros/safe_divide.sql) uses `adapter.dispatch` and is shared with the natality project, which runs on a different warehouse.

## Scrape date

`scrape_date` comes from `BOARD_GAMES_SCRAPE_DATE` in the root `.env` and is carried through staging, intermediate, fact and mart. It comes from the environment and not from `current_date()`, so rebuilding the same scrape gives the same value. `scraped_at` is the same date as a timestamp, for the snapshot.

## Sources

The sources are declared in [`_staging__sources.yml`](dbt/models/staging/_staging__sources.yml). dbt reads them and never creates them.

| Table | Produced by |
|---|---|
| `board_game_raw` | BGG ranked search extract (private repo) |
| `board_game_discovered` | a sweep for games added since the ranked extract (private repo) |
| `board_game_filtered` | [`build_board_game_filtered.py`](build_board_game_filtered.py), which combines the two, drops expansions and removes duplicate BGG submissions |
| `board_game_scrape` | per-game page extract (private repo) |

The extractors that make live HTTP requests are not in this repo. See [Where the extractors went](../README.md#where-the-extractors-went).

## Running

```bash
python board_games/build_board_game_filtered.py   # rebuild board_game_filtered
cd board_games/dbt
set -a; source ../../.env; set +a                 # PG* vars for profiles.yml
dbt deps && dbt build                             # 22 models, 1 snapshot, 126 tests
```

`dbt build` runs the snapshot before the fact, and running it twice on the same scrape adds no versions.

`profiles.yml` is committed because every value in it comes from `env_var()`. dbt looks for it in the current directory, so running from this folder needs no flags.
