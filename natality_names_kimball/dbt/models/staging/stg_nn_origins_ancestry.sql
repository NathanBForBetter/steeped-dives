SELECT
    CAST('{{ env_var("NATALITY_EXTRACT_DATE") }}' AS DATE) AS extract_date,
    name,
    LOWER(origin) AS origin,
    meaning,
    variations
FROM {{ source('national_names', 'nn_ancestry_raw') }}
WHERE status = 'ok'