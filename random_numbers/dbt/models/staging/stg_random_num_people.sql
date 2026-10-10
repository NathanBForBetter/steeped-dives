-- People's picks: one row per response to a form that asked for a random
-- number from 1 to 100.
SELECT
    CAST('{{ env_var("RANDOM_DL_DATE") }}' AS DATE) AS dl_date,
    CAST(id AS INTEGER) AS id,
    CAST(number AS INTEGER) AS number_chosen
FROM {{ source('random_num_raw', 'random_num_people_raw') }}
