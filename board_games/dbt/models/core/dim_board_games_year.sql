-- Year dimension for the board game fact.
--
-- A fact's foreign key should never be null, so this dimension has a member
-- for every case. BCE years are real members (Senet is -3000, Go is -2200).
-- Games with no year on BGG, or with BGG's 0 "unknown" placeholder, point at
-- one explicit 'unknown' member. That is 11,080 of the 140,107 games.
WITH
    DistinctValues AS (
        SELECT DISTINCT
            year_made_int,
            decade,
            CASE WHEN year_made_int < 0 THEN 'bce' ELSE 'ce' END AS era
        FROM {{ ref('int_board_games_main_dims') }}
        WHERE year_made_int IS NOT NULL
    ),
    KnownYears AS (
        SELECT
            {{ dbt_utils.generate_surrogate_key(['year_made_int']) }} AS year_key,
            year_made_int,
            decade,
            era,
            FALSE AS is_unknown_year
        FROM DistinctValues
    ),
    -- The one row every game with no usable year points at. Keyed off a
    -- literal so the hash is stable across runs and across both projects.
    UnknownYear AS (
        SELECT
            {{ dbt_utils.generate_surrogate_key(["'unknown'"]) }} AS year_key,
            NULL::int AS year_made_int,
            'unknown' AS decade,
            'unknown' AS era,
            TRUE AS is_unknown_year
    )
SELECT * FROM KnownYears
UNION ALL
SELECT * FROM UnknownYear
