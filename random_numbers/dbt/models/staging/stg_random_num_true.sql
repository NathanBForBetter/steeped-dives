-- True random picks: one row per number drawn by true_random.py from the
-- operating system's random source.
SELECT
    CAST('{{ env_var("RANDOM_DL_DATE") }}' AS DATE) AS dl_date,
    CAST(timestamp AS TIMESTAMPTZ) AS timestamp,
    CAST(number AS INTEGER) AS number_chosen
FROM {{ source('random_num_raw', 'random_num_true_raw') }}
