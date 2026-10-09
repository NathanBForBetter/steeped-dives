-- Conformed year dimension.
--
-- Two facts point at it: fct_nn_baby_names (SSA, 1910 onward) and
-- fct_nn_baby_natality (CDC WONDER, 1995 onward). Both sources contribute
-- members, so a year CDC has reported and SSA has not yet published, such as
-- the provisional year, still has a year_key. source_coverage records which
-- sources have each year.
WITH
    NameYears AS (
        SELECT DISTINCT year FROM {{ ref('int_nn_baby_names') }}
    ),
    NatalityYears AS (
        SELECT DISTINCT year FROM {{ ref('int_nn_natality') }}
    ),
    AllYears AS (
        SELECT year FROM NameYears
        UNION
        SELECT year FROM NatalityYears
    ),
    YearAttributes AS (
        SELECT
            Y.year AS year_text,
            CAST(Y.year AS INT) AS year_int,
            CONCAT(CAST(FLOOR(CAST(Y.year AS INT) / 10) * 10 AS STRING), 's') AS decade,
            -- Generational bands, derived here rather than carried up from
            -- int_nn_baby_names so a natality-only year still gets one.
            CASE
                WHEN CAST(Y.year AS INT) BETWEEN 1901 AND 1927 THEN 'greatest gen (1901-1927)'
                WHEN CAST(Y.year AS INT) BETWEEN 1928 AND 1945 THEN 'silent gen (1928-1945)'
                WHEN CAST(Y.year AS INT) BETWEEN 1946 AND 1964 THEN 'baby boomer (1946-1964)'
                WHEN CAST(Y.year AS INT) BETWEEN 1965 AND 1980 THEN 'gen x (1965-1980)'
                WHEN CAST(Y.year AS INT) BETWEEN 1981 AND 1996 THEN 'millennial (1981-1996)'
                WHEN CAST(Y.year AS INT) BETWEEN 1997 AND 2010 THEN 'gen z (1997-2010)'
                WHEN CAST(Y.year AS INT) BETWEEN 2011 AND 2024 THEN 'gen alpha (2011-2024)'
                WHEN CAST(Y.year AS INT) BETWEEN 2025 AND 2039 THEN 'gen beta (2025-2039)'
                ELSE 'unclassified'
                END AS generation,
            CASE
                WHEN N.year IS NOT NULL AND T.year IS NOT NULL THEN 'both'
                WHEN N.year IS NOT NULL THEN 'names_only'
                ELSE 'natality_only'
                END AS source_coverage
        FROM AllYears AS Y
        LEFT JOIN NameYears AS N ON Y.year = N.year
        LEFT JOIN NatalityYears AS T ON Y.year = T.year
    )
SELECT
    {{ dbt_utils.generate_surrogate_key(['year_text']) }} AS year_key,
    year_text AS year,
    year_int,
    decade,
    generation,
    source_coverage
FROM YearAttributes
