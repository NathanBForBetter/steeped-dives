-- Conformed state dimension. Carries both natural keys because the two
-- sources spell state differently: SSA ships two-letter codes, CDC WONDER
-- ships full names. Each fact joins on the one its source has, which is why
-- state_name has to match WONDER's spelling exactly - including
-- 'District of Columbia'.
WITH
    DistinctValues AS (
        SELECT DISTINCT
            state AS state_code,
            state_name,
            state_starting_letter,
            region,
            census_division
        FROM {{ ref('int_nn_baby_names') }}
        WHERE state IS NOT NULL
    )
SELECT
    {{ dbt_utils.generate_surrogate_key(['state_code']) }} AS state_key,
    state_code,
    state_name,
    state_starting_letter,
    region,
    census_division
FROM DistinctValues
