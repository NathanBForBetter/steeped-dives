-- CDC WONDER natality, mother's age. The nine age bands are the same in every
-- vintage and nothing is excluded, so this breakout also supplies the total
-- births for a state, year and infant sex.
SELECT
    CAST('{{ env_var("NATALITY_EXTRACT_DATE") }}' AS DATE) AS extract_date,
    state,
    year,
    sex AS infant_gender,
    CAST(births AS BIGINT) AS birth_count,
    source,
    LOWER(age_of_mother) AS mothers_age
FROM {{ source('national_names', 'nn_natality_raw_age') }}
