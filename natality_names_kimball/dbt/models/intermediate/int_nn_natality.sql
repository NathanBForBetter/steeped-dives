-- CDC WONDER natality, the four mother-attribute breakouts on one long grain:
-- state + year + infant_gender + attribute label + attribute value.
--
-- Race and Hispanic origin each appear under two labels, because the vintages
-- do not report them at the same level of detail (see the staging models):
--
--   race, hisp_origin                 the bridged value, comparable across
--                                     every vintage
--   race_detail, hisp_origin_detail   the finer groups, only for the vintages
--                                     that report them
--
-- Keeping them under separate labels means the two levels are never added
-- together.
WITH
    Breakouts AS (
        SELECT extract_date, state, year, infant_gender, birth_count, source,
               'age' AS mother_attr_label, mothers_age AS mother_attr_value
        FROM {{ ref('stg_nn_natality_age') }}
        UNION ALL
        SELECT extract_date, state, year, infant_gender, birth_count, source,
               'edu', mothers_education
        FROM {{ ref('stg_nn_natality_edu') }}
        UNION ALL
        SELECT extract_date, state, year, infant_gender, birth_count, source,
               'race', mothers_race_bridged
        FROM {{ ref('stg_nn_natality_race') }}
        UNION ALL
        SELECT extract_date, state, year, infant_gender, birth_count, source,
               'race_detail', mothers_race
        FROM {{ ref('stg_nn_natality_race') }}
        WHERE mothers_race <> mothers_race_bridged
        UNION ALL
        SELECT extract_date, state, year, infant_gender, birth_count, source,
               'hisp_origin', mothers_hisp_origin_bridged
        FROM {{ ref('stg_nn_natality_hisp') }}
        UNION ALL
        SELECT extract_date, state, year, infant_gender, birth_count, source,
               'hisp_origin_detail', mothers_hisp_origin
        FROM {{ ref('stg_nn_natality_hisp') }}
        WHERE mothers_hisp_origin <> mothers_hisp_origin_bridged
    ),
    -- The provisional extract labels its year '2025 (Provisional)'. The suffix
    -- has to come off for the year to join dim_nn_year, and once it is off a
    -- provisional year and a final year would look the same. If two vintages
    -- ever cover one state and year, the final one wins, then the newer one.
    -- DENSE_RANK keeps every row of the winning vintage, which matters
    -- because several source rows can roll up to one bridged value.
    Ranked AS (
        SELECT
            extract_date,
            state,
            REPLACE(year, ' (Provisional)') AS year,
            infant_gender,
            mother_attr_label,
            mother_attr_value,
            birth_count,
            source,
            DENSE_RANK() OVER (
                PARTITION BY state, REPLACE(year, ' (Provisional)'), mother_attr_label
                ORDER BY CASE WHEN year LIKE '%(Provisional)%' THEN 1 ELSE 0 END,
                         source DESC
            ) AS vintage_rank
        FROM Breakouts
    ),
    Resolved AS (
        SELECT
            extract_date,
            state,
            year,
            infant_gender,
            mother_attr_label,
            mother_attr_value,
            source,
            SUM(birth_count) AS mother_attr_birth_count
        FROM Ranked
        WHERE vintage_rank = 1
        GROUP BY extract_date, state, year, infant_gender,
                 mother_attr_label, mother_attr_value, source
    ),
    -- All births for a state, year and infant sex. Taken from the age
    -- breakout because it is the one that leaves nothing out.
    StateYearGenderTotal AS (
        SELECT state, year, infant_gender,
               SUM(mother_attr_birth_count) AS total_birth_count
        FROM Resolved
        WHERE mother_attr_label = 'age'
        GROUP BY state, year, infant_gender
    )
SELECT
    R.state,
    R.year,
    R.extract_date,
    R.infant_gender,
    R.mother_attr_label,
    R.mother_attr_value,
    R.mother_attr_birth_count,
    T.total_birth_count,
    -- the WONDER vintage the count came from
    R.source
FROM Resolved AS R
LEFT JOIN StateYearGenderTotal AS T USING(state, year, infant_gender)
