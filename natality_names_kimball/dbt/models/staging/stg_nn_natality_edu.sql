-- CDC WONDER natality, mother's education.
--
-- The 1995-2006 vintages report years of schooling ('12 years'). The later
-- ones report attainment ('High school graduate or GED completed'). Both are
-- mapped onto five levels that exist in every vintage. The old top band,
-- '16 years and over', has no ceiling, so bachelor's, master's and doctorates
-- all go to 'bachelors or higher' to match it.
--
-- States adopted the 2003 birth certificate in different years. Where a
-- state's certificate does not match the dataset, WONDER puts its births
-- under 'Excluded' and reports 0 for every education level. Those zeros mean
-- "not collected", so a state and year is left out whenever WONDER excludes
-- any of its births. That is 177 state and year pairs between 2003 and 2015.
-- Four of them are New York from 2004 to 2007, where about half the births
-- are excluded and the rest do not describe the whole state.
WITH
    Flagged AS (
        SELECT
            *,
            SUM(CASE WHEN mothers_education = 'Excluded' THEN CAST(births AS BIGINT) ELSE 0 END)
                OVER (PARTITION BY source, state, year) AS excluded_birth_count
        FROM {{ source('national_names', 'nn_natality_raw_edu') }}
    )
SELECT
    CAST('{{ env_var("NATALITY_EXTRACT_DATE") }}' AS DATE) AS extract_date,
    state,
    year,
    sex AS infant_gender,
    CAST(births AS BIGINT) AS birth_count,
    source,
    LOWER(
        CASE
            WHEN mothers_education IN ('0 -  8 years', '8th grade or less') THEN '8th grade or less'
            WHEN mothers_education IN ('12 years', 'High school graduate or GED completed')
                THEN 'high school or ged'
            WHEN mothers_education IN ('9th through 12th grade with no diploma', '9 - 11 years')
                THEN 'some high school'
            WHEN mothers_education IN (
                'Associate degree (AA, AS)',
                'Some college credit, but not a degree', '13 - 15 years')
                THEN 'some college'
            -- CHR(39) is a single quote: Bachelor's and Master's carry one
            WHEN REPLACE(mothers_education, CHR(39), '') IN (
                '16 years and over', 'Masters degree (MA, MS, MEng, MEd, MSW, MBA)',
                'Bachelors degree (BA, AB, BS)',
                'Doctorate (PhD, EdD) or Professional Degree (MD, DDS, DVM, LLB, JD)')
                THEN 'bachelors or higher'
        ELSE mothers_education
        END) AS mothers_education
FROM Flagged
WHERE mothers_education NOT IN (
    'Not Reported', 'Unknown or Not Stated', 'Not stated/Not on certificate', 'Excluded')
    AND excluded_birth_count = 0
