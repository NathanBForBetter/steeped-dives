-- SSA state-level baby names, one row per state + gender + year + name,
-- enriched with the calendar and geography attributes the dimensions are
-- built from.
--
-- IMPORTANT: `gender` here is the gender SSA recorded for the NAME, which is
-- not the same thing as the infant's sex at birth in the CDC natality data.
-- The two are deliberately modeled apart and compared in the mart - see the
-- README section "Name gender is not birth sex".
SELECT
    extract_date,
    state,
    CASE 
        WHEN LOWER(gender) = 'f' THEN 'female'
        WHEN LOWER(gender) = 'm' THEN 'male'
        END AS gender,
    year,
    CAST(year AS INT) AS year_int, 
    -- computed from the year, so a new decade needs no upkeep
    CONCAT(CAST(FLOOR(CAST(year AS INT) / 10) * 10 AS STRING), 's') AS decade,
    
    CASE 
        WHEN CAST(year AS INT) BETWEEN 1901 AND 1927 THEN 'greatest gen (1901-1927)'
        WHEN CAST(year AS INT) BETWEEN 1928 AND 1945 THEN 'silent gen (1928-1945)'
        WHEN CAST(year AS INT) BETWEEN 1946 AND 1964 THEN 'baby boomer (1946-1964)'
        WHEN CAST(year AS INT) BETWEEN 1965 AND 1980 THEN 'gen x (1965-1980)'
        WHEN CAST(year AS INT) BETWEEN 1981 AND 1996 THEN 'millennial (1981-1996)'
        WHEN CAST(year AS INT) BETWEEN 1997 AND 2010 THEN 'gen z (1997-2010)'
        WHEN CAST(year AS INT) BETWEEN 2011 AND 2024 THEN 'gen alpha (2011-2024)'
        WHEN CAST(year AS INT) BETWEEN 2025 AND 2039 THEN 'gen beta (2025-2039)'
        END AS generation,
    name,
    SUBSTRING(name, 1, 1) AS name_starting_letter,
    name_count,
    -- Geography comes from the us_states seed rather than a CASE ladder per
    -- attribute. LEFT JOIN, so a state code the seed lacks keeps its rows with
    -- NULL geography instead of vanishing; not_null on state_name is what
    -- would flag it.
    S.state_name,
    S.census_region AS region,
    S.census_division,
    SUBSTRING(state, 1, 1) AS state_starting_letter
FROM {{ ref('stg_nn_baby_names') }} AS B
LEFT JOIN {{ ref('stg_us_states') }} AS S ON B.state = S.state_code
