-- CDC WONDER natality, mother's Hispanic origin.
--
-- Only the 1995-2002 vintage breaks Hispanic births into origin groups
-- (Mexican, Puerto Rican, Cuban, Central or South American, Other and Unknown
-- Hispanic). Every later vintage reports one 'Hispanic or Latino' figure.
--
-- Two columns come out of this model:
--
--   mothers_hisp_origin          the label exactly as that vintage reported it
--   mothers_hisp_origin_bridged  'hispanic or latino' on every row, so the
--                                1995-2002 groups add up to the same figure
--                                the later vintages report directly
--
-- Non-Hispanic and unknown rows are left out on purpose. This breakout
-- answers "which Hispanic origin", and shares are taken against all births
-- from the age breakout.
SELECT
    CAST('{{ env_var("NATALITY_EXTRACT_DATE") }}' AS DATE) AS extract_date,
    state,
    year,
    sex AS infant_gender,
    CAST(births AS BIGINT) AS birth_count,
    source,
    LOWER(TRIM(mothers_hispanic_origin)) AS mothers_hisp_origin,
    'hispanic or latino' AS mothers_hisp_origin_bridged
FROM {{ source('national_names', 'nn_natality_raw_hisp') }}
WHERE mothers_hispanic_origin NOT IN (
    'Non-Hispanic White', 'Non-Hispanic Black', 'Non-Hispanic other races',
    'Non-Hispanic Other Races', 'Origin unknown or not stated',
    'Unknown or Not Stated', 'Not Hispanic or Latino')
    AND mothers_hispanic_origin IS NOT NULL
