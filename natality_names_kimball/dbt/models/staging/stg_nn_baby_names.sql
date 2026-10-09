SELECT
    CAST('{{ env_var("NATALITY_EXTRACT_DATE") }}' AS DATE) AS extract_date,
    state,
    gender,
    year,
    name,
    name_count
FROM {{ source('national_names', 'nn_state_raw') }}