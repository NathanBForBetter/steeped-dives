-- ChatGPT picks: one row per call that asked GPT-4.1 for a random number from
-- 1 to 100. Calls that did not come back with a usable number are left out.
--
-- The raw table is all text, so the types are set here. A value that will not
-- cast stops the build.
SELECT
    CAST('{{ env_var("RANDOM_DL_DATE") }}' AS DATE) AS dl_date,
    CAST(Number_Chosen AS INTEGER) AS number_chosen,
    CAST(Timestamp AS TIMESTAMPTZ) AS timestamp,
    CAST(Iteration AS INTEGER) AS iteration,
    Model AS model
FROM {{ source('random_num_raw', 'random_num_ai_raw') }}
WHERE Status = 'ok'
