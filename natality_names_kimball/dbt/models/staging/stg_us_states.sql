SELECT
    state_code,
    state_name,
    census_region,
    census_division
FROM {{ ref('us_states') }}
