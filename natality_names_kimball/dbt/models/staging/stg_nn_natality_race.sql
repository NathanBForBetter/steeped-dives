-- CDC WONDER natality, mother's race.
--
-- WONDER reports race differently in each dataset vintage:
--
--   1995-2002   White, Black, American Indian, and five Asian or Pacific
--               Islander groups: Chinese, Filipino, Hawaiian, Japanese,
--               Other Asian
--   2003-2019   bridged race: White, Black, American Indian, and one combined
--               Asian or Pacific Islander
--   2020-2024   no race at all. Bridged race was retired, and WONDER writes
--               'Missing Race' where the count would be
--   2025        single race: Asian and Native Hawaiian or Other Pacific
--               Islander reported separately, plus More than one race
--
-- Two columns come out of this model:
--
--   mothers_race          the label exactly as that vintage reported it
--   mothers_race_bridged  the four groups every vintage can be rolled up to
--                         (White, Black, American Indian or Alaska Native,
--                         Asian or Pacific Islander), which are the 1977 OMB
--                         categories NCHS bridges to
--
-- A label is only ever rolled up into a group that contains it. Nothing is
-- mapped sideways to a label from another vintage.
--
-- 'More than one race' exists only in 2025 and is kept as its own value.
-- NCHS's bridging model reassigns those mothers to a single group using
-- probabilities, and a staging model has no business imitating that. So the
-- four groups in 2025 count single-race mothers only, about 3% fewer births
-- than a bridged figure would.
SELECT
    CAST('{{ env_var("NATALITY_EXTRACT_DATE") }}' AS DATE) AS extract_date,
    state,
    year,
    sex AS infant_gender,
    CAST(births AS BIGINT) AS birth_count,
    source,
    LOWER(TRIM(mothers_race)) AS mothers_race,
    CASE
        WHEN LOWER(TRIM(mothers_race)) IN (
            -- 1995-2002 detailed groups
            'chinese', 'filipino', 'hawaiian', 'japanese', 'other asian',
            -- 2003-2019 bridged
            'asian or pacific islander',
            -- 2025 single race
            'asian', 'native hawaiian or other pacific islander')
            THEN 'asian or pacific islander'
        ELSE LOWER(TRIM(mothers_race))
        END AS mothers_race_bridged
FROM {{ source('national_names', 'nn_natality_raw_race') }}
WHERE mothers_race NOT IN ('Not Reported', 'Not Available')
    AND mothers_race IS NOT NULL
    -- 2020-2024: the count column holds this text instead of a number
    AND births <> 'Missing Race'
